-- The seed schema for the capped MySQL -> ClickHouse **CDC** campaign: thirty
-- identical tables of 1,000,000 rows x 15 columns.
--
-- This is `bench.bench_my_1m`'s `SHOW CREATE TABLE`, copied out of the rig's
-- existing MySQL, so the CDC arm and the capped MySQL -> ClickHouse bulk campaign
-- describe ONE dataset rather than two, and any difference in a landing can only
-- have come from the transfer. Only the table name differs from the bulk arm's
-- `cmp_my_tNN` prefix.
--
-- ONE COLUMN'S DECLARED TYPE IS CHANGED FROM THE BULK ARM'S, and it is changed for
-- a reason that is a product fact, not a convenience. apitap 0.57.0's MySQL CDC
-- lane REFUSES a native `json` column, because MySQL logs JSON in its binary
-- encoding and the engine cannot re-render it — its own error (verbatim, from the
-- control leg's log) is:
--
--   RAISED ValueError: log_based: bench.cdc_ctrl has JSON column(s) json_val and
--   apitap cannot yet render MySQL's binary JSON encoding from the binlog. A CDC
--   update would write the raw envelope where the full load wrote the document, so
--   the run refuses instead of corrupting the column. Use mode='replace' or
--   'append' for this table, or store the document in a text column.
--
-- The error names its own workaround — "store the document in a text column" — and
-- that is what this file does: `json_val longtext`. It is also not an exotic shape,
-- because MariaDB's JSON *is* LONGTEXT and the error says so ("MariaDB is
-- unaffected — its JSON is LONGTEXT"). The bulk arm's `mode='replace'` lane DID take
-- the native JSON column, so this is a restriction of the CDC lane specifically.
--
-- The VALUES are unchanged: the load copies server-side, so the text stored here is
-- byte-for-byte the same normalized JSON text the JSON column held, which is the
-- same expression the checksum reads (`CAST(json_val AS CHAR)`). The campaign
-- prints both sides' `json_crc` aggregate to show they agree. Fifteen columns, ten
-- tables' worth of NULL behaviour, and the digest's coverage of all fifteen
-- columns are unchanged.
--
-- Properties that are inherited from the bulk arm and are DISCLOSURES, not
-- conveniences:
--
--   * every value is NULLable and four of the columns (medium_str on a prime
--     modulus of ids, decimal_val on another, json_val on a third, extra_text on a
--     fourth) carry NULLs on roughly 1% of rows, so the checksum also proves a NULL
--     survives the trip;
--   * `tinyint(1)` is apitap's documented tinyint(1) -> smallint at the
--     destination. The validator folds each side through `toString`, so the
--     on-disk width cannot change the verdict, and it is printed as a result.
--
-- Note for ingestr: its CDC requirements name ENUM, SET and BIT as disqualifying
-- column types. None of these fifteen is one — `bool_val` is `tinyint(1)` — so
-- the tables are eligible for that connector, which is asserted from the server
-- in the report, not assumed here.
CREATE TABLE IF NOT EXISTS `cdc_my_t01` (
  `id` int NOT NULL,
  `small_str` varchar(20) DEFAULT NULL,
  `medium_str` varchar(100) DEFAULT NULL,
  `large_str` varchar(500) DEFAULT NULL,
  `tiny_int` smallint DEFAULT NULL,
  `regular_int` int DEFAULT NULL,
  `big_int` bigint DEFAULT NULL,
  `float_val` double DEFAULT NULL,
  `decimal_val` decimal(18,4) DEFAULT NULL,
  `bool_val` tinyint(1) DEFAULT NULL,
  `date_val` date DEFAULT NULL,
  `ts_val` datetime(6) DEFAULT NULL,
  `ts_tz_val` datetime(6) DEFAULT NULL,
  `json_val` longtext DEFAULT NULL,
  `extra_text` longtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;