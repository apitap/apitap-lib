-- The seed for the capped PostgreSQL -> ClickHouse arm: one 15-column table,
-- \rows rows, then \ntbls byte-identical clones of it.
--
-- The 15 columns are the SAME shape as the MySQL arm's seed
-- (benchmarks/bench-capped-my-ch-0.57.md), so the two campaigns describe one
-- dataset rather than two:
--
--   id int · small_str varchar(20) · medium_str varchar(100) · large_str varchar(500)
--   tiny_int smallint · regular_int int · big_int bigint · float_val double
--   decimal_val numeric(18,4) · bool_val boolean · date_val date
--   ts_val timestamp(6) · ts_tz_val timestamptz · json_val json · extra_text text
--   PRIMARY KEY (id)
--
-- Every value is generated from `id` alone, so all ten tables are identical and
-- a mismatch can only come from a transfer, never from the generator. Three
-- properties are deliberate, and all three are disclosed in the report:
--
--   * every text value stays under 200 bytes, so nothing is ever TOASTed and
--     walshadow's TOAST-chunk mirror tables are never exercised;
--   * float values are exact multiples of 1/8 below 1250, so PostgreSQL's
--     shortest-round-trip text and ClickHouse's toString agree byte for byte;
--   * json_val is the `json` type, not `jsonb`, because `json` preserves the
--     input text verbatim while `jsonb` would re-render it (key order,
--     whitespace) and make the checksum a test of JSON normalisation instead of
--     of the transfer.
--
-- Four columns carry NULLs on a prime-modulus subset of ids (~1% each), so the
-- checksum also proves NULL survives the trip and that a NULL is rendered the
-- same way on both sides of it. The validator's NULL sentinel is the five
-- characters <NUL>, and no generated value contains a `<` at all.
--
--   psql -v rows=1000 -v tbl=cmp_ctrl -f this-file.sql

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS public.:tbl;

CREATE TABLE public.:tbl (
    id          integer          NOT NULL,
    small_str   varchar(20),
    medium_str  varchar(100),
    large_str   varchar(500),
    tiny_int    smallint,
    regular_int integer,
    big_int     bigint,
    float_val   double precision,
    decimal_val numeric(18,4),
    bool_val    boolean,
    date_val    date,
    ts_val      timestamp(6),
    ts_tz_val   timestamptz,
    json_val    json,
    extra_text  text,
    PRIMARY KEY (id)
);

INSERT INTO public.:tbl
SELECT g,
       substr(md5(g::text), 1, 8),
       CASE WHEN g % 97  = 0 THEN NULL ELSE substr(md5((g * 7)::text), 1, 40) END,
       substr(md5((g * 13)::text), 1, 200),
       (g % 100)::smallint,
       g,
       g::bigint * 1000000,   -- bigint: g * 1000000 overflows int4 at g=1e6
       (g % 10000)::double precision / 8,
       CASE WHEN g % 89  = 0 THEN NULL ELSE ((g % 1000000)::numeric(18,4) / 100) END,
       (g % 2 = 0),
       date '2024-01-01' + (g % 900),
       timestamp '2024-01-01 00:00:00' + (g % 1000000) * interval '1 microsecond',
       timestamptz '2024-01-01 00:00:00+00' + (g % 1000000) * interval '1 microsecond',
       CASE WHEN g % 83  = 0 THEN NULL
            ELSE ('{"k":' || g || ',"s":"' || substr(md5(g::text), 1, 16) || '"}')::json END,
       CASE WHEN g % 79  = 0 THEN NULL ELSE 'extra-' || substr(md5((g * 3)::text), 1, 60) END
FROM generate_series(1, :rows) AS g;

ANALYZE public.:tbl;