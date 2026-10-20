# apitap — MASTER review (2026-10-15) · one checklist, read this first

**This document replaces rounds 1–3 as the work queue.** It is the result of an
exhaustive pass at `main` = `7a6f36d`: every line of all 75 source files (64,597 lines,
`crates/` + `py-apitap/`) was read by one of eight slice reviewers, plus a test/release
audit and the VPS repros from round 3. The older docs stay as evidence:
`2026-10-14-handoff-review-rust.md` (P0-x/P1-x/P2-x), `2026-10-14-rust-engineer-pass2.md`
(T1–T19), `2026-10-15-review-round2.md` (R2-x), `2026-10-15-review-round3.md` (R3-x).
IDs from those docs are kept. New IDs are `M-n`.

**Why the rounds kept finding things:** fixes landed with helper-only unit tests, no CI runs
any test, and the real-server gate was not run on any round-2/3 fix (§2). The rules in §1
are what stops a round 4. Every fix must satisfy them, with no exceptions.

**Verdict: not production-ready.** About 30 P0/P1 items are open. Many of them are silent
data loss on normal inputs (§3). Per-lane status is in §9.

Labels:
- **REPRO**: reproduced on the VPS.
- **READ✓**: read by the slice reviewer and re-read by the integrator.
- **READ**: one reviewer read the lines and the mechanism is certain.
- **PLAUSIBLE**: the mechanism is real but needs a repro before fixing.

Every PLAUSIBLE item must be repro'd first. Every READ P0 should be repro'd (it becomes
the RED test).

---

## 1. Rules — definition of done for EVERY fix (non-negotiable)

Round-1 §0 still applies: VPS-only builds (`ssh -i ~/.ssh/apitap_vps
ubuntu@vps-f1d96aff.vps.ovh.ca`), QA rig `~/apitap-qa`, rsync with an absolute source, touch
before cargo, ≤1000 rows, drop QA tables after, never prune docker, no Claude trailer.

1. **RED at the call site.** Revert only the production call-site change (not the helper),
   run the new test, and see it fail. Write "RED without fix: <test> <error>" in the
   commit message. A helper-only test does not count. Round 3 showed the suite stays
   463/463 green when the call sites for 9503105, ffb3338, 29f52a9, 74caa8b, 79d9bfe and
   dc8bf4e are reverted.
2. **Expectations come from a server, never from memory.** Wire bytes, catalog rows and
   type spellings are captured from a live server. ffb3338's test pinned bytes the
   author believed, and they were wrong.
3. **Real-server leg for every source, destination or wire change.** It compares full rows
   (a sorted canonical text digest of source vs destination), not count or sum. It is
   registered in `benchmarks/gate.py` with a `proves=` claim and uses ≤1000 rows.
4. **Run the area's gate legs on the VPS wheel** (`gate.py --only <area>`), after the
   touch and after checking that the installed `.so` changed:
   - MySQL/MariaDB: `cdc_types`, `mariadb_cdc`, `review_gate`, `mysql84`
   - PG wire or URL: `tls`, `logbased`, `logbased_dests`, `read`
   - sink: `guard_matrix <sink>`, `worker_cancel <sink>`, `state_contract <sink>`
   - plus the §8 matrix legs for the area
5. **Full gate before every tag** (87 legs, about 2 h), and `--matrix` exits 0. The last
   green run was 0.59.0.
6. **No new clippy warnings and no new fmt hunks** on the files a commit touches. Restoring
   CI (`git show 279e7b0:.github/workflows/ci.yml`, lost in merge 2a5039c) needs a PAT
   with workflow scope, so **ask the user first**. Until then, run a scripted
   `cargo test && clippy-delta` on the VPS before every push.
7. **A fault-path fix needs a fault-injection test** (mock server or proxy) that drives the
   real function, not just the classifier.
8. **Never write "idempotent by construction"** without a replay test. Three comments in
   the tree that say it are false (M-31, R3-6, R2-4).

---

## 2. Process facts (from the test audit)

- **CI:** only `.github/workflows/publish.yml` (on tag: the wheel imports, and the README
  lists every scheme). There is no cargo test, no clippy, no fmt, no e2e.
  `benchmarks/ci_transfers.py` is orphaned.
- **Rust:** 468 test attributes, all in-process. 3 `#[ignore]` live probes never run. There
  are mocks only for CH (`sink/clickhouse.rs:2409`, `dest_ch.rs:2486`); none for Iceberg
  REST, S3, GCS or BQ.
- **Very thin test coverage:**

  | File | Lines | Tests |
  |---|---|---|
  | `sink/postgres.rs` | 1488 | 1 |
  | `sink/mysql.rs` | 1582 | 2 |
  | `source/postgres.rs` | 974 | 1 |
  | `dest_my.rs` | 886 | 2 |
  | `dest_pg.rs` | 907 | 3 |
  | `read_impl.rs` | 421 | 0 |

  `drain()` (the CDC state machine) has **zero** tests. The `apply_windows` parallel arm
  has zero. The 7d3a9b9 pushdown refusals have **no committed test** at all.
- **gate.py:** 87 legs on real servers, and it is good. It *would* have caught R3-1
  (`cdc_types` and `review_gate` create ENUM/SET tables). Only 1 of about 30 fix commits
  since 10-13 touched `benchmarks/`.
- **Values-compared e2e exists only for:**
  - pg→pg / my / ch / ice / bq CDC
  - mariadb→ch CDC
  - my8.0→ch CDC (narrow)
  - ch→ch replace
  - `read()` from PG

  **There is none for:**
  - any `merge` (except one memory leg)
  - any non-PG `read()`
  - mysql→pg / my / bq / s3 / ice
  - csv, github, gsheets, gcs
  - MariaDB 11
  - MySQL DATE / LONGTEXT / UNSIGNED / DECIMAL in CDC
- **Bench servers** (VPS docker):

  | Server | Port |
  |---|---|
  | PG16 | :5544 / :5545 |
  | PG TLS | :5546 |
  | MySQL 8.0 | :3307 / :3308 |
  | MariaDB 10.6 | :3309 |
  | MySQL 8.4 | :3310 |
  | MySQL GTID | :3311 |
  | MySQL TLS | :3312 |
  | CH 24.8 | :8124 / :8125 |
  | CH 2-node cluster + LB | :18200 |
  | CH body-cap proxy | :18125 |
  | Iceberg REST + MinIO | :8181 / :9100 |
  | BigQuery | needs `BQ_SA` |

  There is no MariaDB 11.

---

## 3. P0 — silent data loss / corruption / stuck lane on normal input

| ID | Label | Where | Defect → failure | Fix |
|---|---|---|---|---|
| **M-1** (R3-1) | **REPRO** | `wire/mybinlog.rs:332-363` `catalog_kind`; used `mysource.rs:632-650`; wrong test `:1151` | Live servers send DATE=0x0A, ENUM/SET=0xFE (the real type is in the metadata), and every TEXT/BLOB size=0xFC. The catalog maps them to 0x0E, F7/F8 and F9–FB. So every MySQL/MariaDB table with DATE, ENUM, SET or TINY/MEDIUM/LONG TEXT or BLOB is refused on every drain, and re-bootstrap does not help. Paysera trial shape. | Use the table in §3.1. Return the allowed (type, real-type) pairs, and compare the unfolded real type for 0xFE. Add the M1 e2e (§8). |
| **M-2** | READ✓ | `wire/mybinlog.rs:686-695` `string_meta` | It unfolds the length only when `rt >= 0xF1`. Folded CHAR bytes are 0xEE/0xDE/0xCE, so they are never unfolded. CHAR with field_length > 255 bytes (utf8mb4 **CHAR(64)** and up, e.g. hash columns) reads a 1-byte length where the server wrote 2. Every later column of the row is misaligned: silent corruption or a stuck lane. | MySQL's rule: `if rt & 0x30 != 0x30 { len \|= ((rt & 0x30) ^ 0x30) << 4; rt \|= 0x30 }`. Unit test with server-captured bytes, plus M1. |
| **M-3** | READ | `wire/mybinlog.rs:588-591` | The server strips trailing 0x00 from `BINARY(n)`, and the decoder never re-pads. A BINARY(16) UUID ending in 0x00 (about 1/256 of rows) arrives 15 bytes long. As a PK: UPDATE gives a duplicate row and DELETE misses. | Right-pad to the declared byte length when DATA_TYPE=`binary`. |
| **M-4** | READ✓ (PG) / READ (MySQL) | PG: `logbased/drain.rs:219,261` + `run.rs:1787-1805`. MySQL: `mysource.rs:515,521` + `myrun.rs:288-331` | **Follow mode loses half-transactions.** The window deadline fires mid-transaction and the half-read `tx_buf` is dropped, but the next drain continues on the **same stream**. It gets the transaction's tail without a BEGIN and commits it, so the watermark/slot passes the lost head. MySQL: about once per 30 s window under steady writes. PG: under `APITAP_WINDOW_MAX_SECS`, or the default with medium traffic. | Honour the deadline only at a transaction boundary (`tx_buf` empty, no stream open), or end the session after a mid-transaction break. Unit test on a scripted event stream, plus an e2e with FOLLOW=60 and WINDOW_MAX=5. |
| **M-5** | READ✓ | `plan.rs:256-263` `Greatest`; `sink/mysql.rs:1192-1235`, `sink/clickhouse.rs:1868-1907`, `sink/bigquery.rs:2789-2813` | **Fan-in skips rows.** With its own state row, the watermark is `max(own_state, MAX(cursor) over the whole table)`, and that max includes other sources' rows. A=100, B=40 → B resumes at 100, and B's 41..100 are never loaded. Green run. Fan-in is advertised in the README (316) and in usage.md:384. | When sibling state rows exist, use the state row alone (StateAuthoritative). Add an e2e with two sources at different heights on MySQL, CH and BQ. |
| **M-6** (R3-2 + ext.) | **REPRO** | `source/postgres.rs:687`; `wire/walsender.rs:681-766` `parse_url`; `pipeline/mod.rs:53-71` `source_origin` | **Wrong server/DB/role.** The raw COPY plane and the walsender use the URL authority and ignore `?host/hostaddr/port/dbname/user/password/options`, the `PG*` env vars and `.pgpass`, all of which sqlx honours (last wins). `read()` returned rows from a different container (REPRO). `?options=-csearch_path=sales` reads `public.orders`. `source_origin` drops the query string, so two `?host=` servers share one watermark row. | One resolver built from sqlx's own `PgConnectOptions`, used by the pool, the raw plane, the walsender and `source_origin`. Or refuse those keys on the raw and replication planes. M5 e2e. |
| **M-7** | READ✓ | `wire/walsender.rs:237-248`, `run.rs:1636-1639`; `wire/pgbindec.rs:24-72` | **PG→CH CDC stuck on common types.** Since 0.61.0, binary pgoutput is ON by default for CH. pgbindec renders only scalars, so arrays, enum, domain, citext, hstore, interval, inet, money, bit, ranges, tsvector etc. error on every window. The hint "unset APITAP_PG_BINARY" is wrong (it must be `=0`), and README:488 says the default is 0. | Enable binary only when every tracked OID is decodable (check the catalog before START_REPLICATION); otherwise use text. Fix the hint and the README. |
| **M-8** | READ✓ | `read_impl.rs:234-251` + `source/postgres.rs:581` (`_ => Delivered::Text`) | **`read()` returns PG binary as strings.** The `::text` rewrite runs only when `arrow_kind` is None. Every unlisted PG type (time, timetz, interval, money, inet, arrays, ranges, domains, oid, …) puts **send-format bytes** into a Utf8 column, which are often invalid UTF-8 (UB per P1-8). | For PG, use `({col})::text` unless the udt is in the decodable set. e2e: `read()` over these types vs server `::text`. |
| **M-9** (R3-3) | READ✓ | `logbased/run.rs:1907` vs `:2066-2071` | The parallel-apply gate runs only on the entry window. Later windows (residue/truncate) apply concurrently, giving a stale TOAST value or surviving pre-truncate rows. | Check `parallel_safe()` on every window; on false, drain `inflight` and go serial. Test at the arm level. |
| **M-10** (R3-4) | READ | `sink/bigquery.rs:2866-2884` | BQ flipped bootstrap does `WRITE_TRUNCATE` plus a `*` barrier. Two first-run fan-in appends: B wipes A's rows and watermark. | `WRITE_EMPTY` (or refuse) on a flipped bootstrap; no barrier. |
| **M-11** (R3-5 + ext.) | READ | `sink/clickhouse.rs:2002-2023` → `2041-2060/2070-2102/2171-2185`; `:2080` `unwrap_or(0)`; `DELETE … WHERE dest_table` | Check-then-EXCHANGE race (destroys the peer's table). The engine branch re-probe reads an unreadable body as "absent", turning a replace into a duplicating append. The state DELETE wipes every source's row. | Default: `RENAME TABLE staging TO final` (code 57 if it exists). Engine: `CREATE New_tok …; ATTACH PARTITION ID 'all' FROM staging; RENAME New_tok TO final`, and `DROP New_tok SYNC` on failure. ON CLUSTER: `RENAME … ON CLUSTER` with `distributed_ddl_output_mode='throw'`; a 159 is unknown, so write no state. Delete only this `source_id`'s state. |
| **M-12** (R3-6 + S2-7/S2-8) | READ | `dest_ch.rs:648-721, 850-917`; `dest_bq.rs:1222-1243` | **Rekey replay loses or overwrites rows.** CH: `Rekey 1→2; Rekey 3→1` loses C. `DELETE 2; UPDATE id 1→2` (Residue{del}) loses the moved row on any replay. `Rekey 1→2; INSERT 1(X)` puts X's TOAST value at key 2. A crash between the old-key delete and the insert loses the row. BQ: the MERGE joins a reused old key, so TOAST columns are overwritten. The comment at `dest_ch.rs:899-902` is false. | Version-aware readback (`_apitap_ver < window`), or refuse to replay a Rekey whose old key carries a later image. Never `continue` silently. Real-CH e2e with each chain killed mid-replay. |
| **M-13** | READ | `dest_ch.rs:799-805, 819-827` | **MaskedUpdate is not atomic:** readback → DELETE → INSERT. Any failure between DELETE and INSERT loses the row, and the replay then says "row missing… replay out of order?". The lane is wedged. This is the most common TOAST shape. | Insert the patched row before deleting (by version), or refetch from the source when the readback is empty. Mock fault test. |
| **M-14** (R3-7) | READ | `sink/iceberg.rs:1348-1421` (bulk), `2064-2114` (CDC); sweep at `1265`, `1976` | After `ambiguous=true`, any `?` (S3 GET, manifest write, put) returns Err, and the run sweeps data files that a late commit may reference. | Wrap the attempt body and map every Err to Unknown once ambiguous. Fault proxy: 504 on commit, then 503 on GET. |
| **M-15** | READ | `dest_bq.rs:1300, 1416-1424` | MySQL→BQ CDC with BYTES: the cast is `FROM_HEX(SUBSTR(c,3))`, which assumes PG `\x`. BIT(1) 0x01 becomes empty bytes (every flag blanked, silently); blobs are garbled; non-UTF-8 BINARY is stuck. | For OID 0 (MySQL), encode BYTES in Rust (hex/base64) and decode to match. |
| **M-16** | READ | `dest_pg.rs:210-212, 257-258` | MySQL→PG CDC writes raw binary into COPY text for `bytea`: invalid UTF-8 or 0x00 wedges the lane; a `\ooo` pattern is silently re-decoded. | Emit `\\x`+hex for bytea when OID=0. |
| **M-17** | READ | `sink/clickhouse.rs:599-613` | A 502/504 with no `DB::Exception` is retried as "did not run". A 504 means the proxy forwarded it. A retried `EXCHANGE` swaps twice (old data published), then `DROP staging` deletes the new data. A retried ATTACH duplicates. CH behind an LB is the Paysera shape. | For mutating statements, retry only on 503 or connect errors; treat 502/504 as an unknown outcome. |
| **M-18** | READ | `sink/postgres.rs` / `source/postgres.rs:440-458` ctid spans (snapshot pinned only for CDC, `:843-881`) | **Parallel PG read of a PK-less table (default parallel ≥4).** Each ctid span has its own snapshot. A non-HOT UPDATE committing mid-read moves the tuple across span boundaries: the row is lost (or duplicated). Replace publishes the short table. | `pg_export_snapshot()` on a held connection, then `SET TRANSACTION SNAPSHOT` on every span (the machinery is at `:694-706`). |
| **M-19** | READ | `sink/postgres.rs:520-547` `bind`, `483-491` | An unqualified destination puts staging/final in `current_schema()`, while the guard/lease/state use the resolved schema. With `search_path="$user",public` and the table in `public`, replace drops `public.t` and moves the table into `$user`. | Rebuild `staging_t`/`final_t`/`copy_in_sql` qualified after `resolve_parts`. |
| **M-20** | READ | `sink/mysql.rs:968` (no COLLATE), `:1019-1023` LOAD DATA LOCAL | Staging uses the server's default `_ci` collation, and LOAD DATA LOCAL turns duplicate-key errors into warnings that are never read. A PG text PK `'abc'`/`'ABC'` (or `'e'`/`'é'`): the second row is silently dropped and the table publishes short. | `COLLATE utf8mb4_bin` for non-MySQL text PKs; check warnings after LOAD DATA and fail on 1062/1265/1292. |

### 3.1 Correct catalog → TABLE_MAP mapping for M-1

| DATA_TYPE | type byte | metadata |
|---|---|---|
| tinyint / smallint / mediumint / int / bigint | 01 / 02 / 09 / 03 / 08 | none |
| float / double | 04 / 05 | 1 byte |
| decimal | F6 | 2 (p, s) |
| **date** | **0A** (0E never appears) | none |
| year | 0D | none |
| time / datetime / timestamp | 13 / 12 / 11 (fsp); legacy 0B / 0C / 07 → see M-36 | 1 (fsp) |
| char, binary | **FE**, real FE after unfolding (M-2) | 2 |
| **enum** | **FE**, real F7 | pack length 1/2 |
| **set** | **FE**, real F8 | 1/2/3/4/8 |
| varchar, varbinary | 0F | 2 LE |
| tinytext/blob · text/blob · medium · long | **FC** (all sizes) | 1 / 2 / 3 / 4 |
| MariaDB json (shows as longtext) | FC | 4 |
| MySQL json | F5 (refused upstream) | 1 |
| bit | 10 | 2 (bits%8, bytes) |
| geometry family | FF | 1 |
| MariaDB inet4/inet6/uuid, MySQL 9 vector | → `None` (bulk refuses) | — |

Capture each byte from a live 8.0, 8.4 and MariaDB 10.6 server before writing the test (rule 2).

---

## 4. P1 — wrong under realistic conditions, or a loud failure on a supported setup

| ID | Label | Where | Defect / failure | Fix |
|---|---|---|---|---|
| M-21 (R3-8) | READ | `wire/walsender.rs:681-766, 487` | The walsender defaults to verify-full but ignores `PGSSLMODE`, `PGSSLROOTCERT`, `ssl-root-cert`/`ssl-ca`; webpki only; `sslrootcert` *replaces* roots (sqlx adds). RDS / self-signed CDC fails. The raw plane is also blind to PGSSLMODE (3 wasted SSLRequests, perf loss). | Part of the M-6 resolver: one TLS config for every plane. |
| M-22 | READ | `wire/walsender.rs:181-200` | The silence deadline is anchored at the last socket read. After any apply longer than 120 s (BQ MERGE, Iceberg) the next refill fails "silent for 120s" without reading. Follow dies at the first slow apply. | Restart the clock when the consumer returns, or time only the read itself. |
| M-23 | READ | `logbased/run.rs:1000-1033, 1316-1343, 1393-1397` | Slots are orphaned on a name change: a legacy pre-0.59 slot, a group-membership change, or `slots=N`. The old slot retains WAL until the source disk fills. Errors name the *new* (non-existent) slot. | Detect and name the old slots on bootstrap; offer to drop them; report them in `slot_wal_report`. |
| M-24 | READ | `rowtext.rs:378-390` (used `dest_my.rs:824`, `dest_bq.rs:1304`) | `strip_utc_offset` on timetz: `'10:00+02'` wedges PG→MySQL/BQ CDC on every rerun; timestamptz infinity/BC too. | Convert to UTC or refuse at admission. |
| M-25 | READ | `dest_pg` `open_unit` `:676-678` | The PG destination TimeZone is not pinned. MySQL TIMESTAMP (naive UTC text) into `timestamptz` shifts by the server offset, silently. | `SET LOCAL TimeZone='UTC'`. |
| M-26 | READ | `dest_ch.rs:117-123, 939-941, 1300-1324` | Insert-only detection uses `system.tables.engine` (no params), and the bootstrap ALTERs **any** Replacing table. With `ReplacingMergeTree(updated_at)`, deletes never apply. Regression from 0.60.0. | Require `engine_full` to name `(_apitap_ver, _apitap_deleted)`; never ALTER other tables. |
| M-27 (S1-11/S2-6) | READ | `dest_ch.rs:384-391`, `changelog.rs:239` | CH changelog `read_base` matches CH-rendered key text against WAL text (numeric `1.50`/`1.5`, tz, bool). A masked update reports "torn" and the lane is stuck. BQ already fixed this (`dest_bq.rs:606-615`). | Index by request slot, as BQ does. |
| M-28 | READ | `rowbinary.rs:93-97, 137-142` | `Swap(w)` CDC type check is width-only: float8 → Int64/UInt64, int8 → Float64. ALTER TYPE mid-CDC, or a pre-created table, stores float bits as ints. Silent. | Carry signed/unsigned/float in `RbType` and require an exact match; otherwise use TSV. |
| M-29 | READ | `sink/clickhouse.rs:2171-2185` | Replace without `engine=` EXCHANGEs over an existing Replicated/Distributed/partitioned table, losing engine, replication, TTL and codecs. | Read `engine_full`; refuse or mirror. |
| M-30 (R3-10) | READ | `sink/clickhouse.rs:491-570`, `source/clickhouse.rs:525-531, 613-660` | Profile settings shorten results with HTTP 200, and the `count()` cross-check is gone. | Pin on every request: `limit=0`, `offset=0`, every `*_overflow_mode=throw` (result, read, read_leaf, timeout, timeout_leaf, transfer, sort, group_by, set, join, distinct), `skip_unavailable_shards=0`, `final=0`, `use_query_cache=0`, `max_parallel_replicas=1`, empty `additional_table_filters`. Also pin `input_format_allow_errors_num=0`, `_ratio=0`, `async_insert=0`, `insert_deduplicate=0` on inserts. |
| M-31 (S6-4/S6-17) | READ | `sink/clickhouse.rs:597-598, 632`; `s3.rs:394-401` | A 2xx with a failed body read → `Ok("")`. Then: `rows_staged`=0 (staging dropped, "success"); the guard list is empty (blind); the lease is Absent; the watermark is `""`; an S3 `<Error>`-in-200 on Complete/Copy is missed. | Return Err on any body-read failure; every count parse uses `map_err` (T6). |
| M-32 | READ | `s3.rs:984-999, 943-945, 585-610`; `gcs.rs:812-829, 756` | Object-store replace is not atomic. A copy overwrites published parts before success (mixed old/new). Stale-part deletes are `let _` (silent duplicates). CopyObject is capped at 5 GiB per pipe (fails at finalize). `discard`'s `list()?` skips abort and `release()`, so the lock leaks and the table refuses forever. | Generation dir + pointer, or copy all then delete with errors checked; UploadPartCopy over 5 GiB; best-effort discard that always releases. |
| M-33 (R3-13/S7-2) | READ | `pipeline/mod.rs:728-740` | Post-announce refusal and re-read `?` skip `discard`. On MySQL the leftover staging blocks the table permanently ("locked:"), and the R2-2 scenario itself triggers it. | Route every exit after `prepare` through `discard()`. Fake-sink call-site test. |
| M-34 (R3-12/S5-5) + S5-4 | READ | `sink/iceberg.rs:582-588, 967-1011, 1153-1156, 1424-1481` | Flipped bootstrap = Overwrite, so the CAS loser overwrites the winner. It writes no watermark, and run 2 can't derive one from FLBA decimal stats, so **every Iceberg append on a decimal/UNSIGNED cursor fails after run 1**, and the advice ("replace once") loops. | Refuse on a flipped bootstrap if a snapshot exists; write the watermark; decode decimal stats. |
| M-35 | READ | `sink/iceberg.rs:226-232`, `aws.rs:83,143` | The catalog uses bearer auth only and SigV4 is hardcoded to `s3`, so Glue REST / S3 Tables (README:175) 403. A static Polaris token expires; the 401 is treated as a definite failure, the files are swept and the load is lost. | SigV4 with a service param plus OAuth2 refresh, or remove them from the README. |
| M-36 | READ | `mybinlog.rs:454-457, 633` | MariaDB legacy temporals (0x0B/0x0C, hires 0x07) are not decoded. A naive M-1 fix that accepts 0x07 would silently corrupt. | Decode only when `DATETIME_PRECISION=0`; otherwise refuse with an `ALTER TABLE … FORCE` hint. |
| M-37 | READ | `mysource.rs:1005-1022` (test `:1076` asserts the bug) | `query_verb` reads the first word only. `/* DBeaver */ TRUNCATE` is dropped silently, and a commented ALTER doesn't clear the schema cache. | Skip leading comments. |
| M-38 | READ | `mysource.rs:260-288, 706-801` | A session with `binlog_format=STATEMENT/MIXED` writes DML as QUERY events, which are ignored. Rows vanish. | Refuse DML-verb QUERY events. |
| M-39 (R2-17 raised) | READ | `mysource.rs:562, 572, 1005-1015` | With `binlog_checksum=NONE`, 4 bytes are cut per event, so the ROTATE name is truncated, the watermark goes **backwards**, and the TRUNCATE target is cut. ROLLBACK followed by DDL applies rolled-back rows; ROLLBACK TO SAVEPOINT is ignored. | Read the checksum from each FDE; ROLLBACK means discard. |
| M-40 | READ | `mybinlog.rs:852-867` | `decode_decimal` zero-pads the leading group (`0000000123.4500`). DECIMAL p>38 is delivered as text, so the destination holds two spellings of one value. | Skip leading zero groups. |
| M-41 | READ | `source/mysql.rs:371-396` | Bulk binary lanes: `'2024-05-00'` (MariaDB default sql_mode) becomes 2024-04-30, silently. | Refuse month/day 0. |
| M-42 | READ | `mywire.rs:282-291, 613-627` | No ed25519/parsec/sha256_password. MariaDB ed25519 users can't run CDC, and the error mentions a non-existent sqlx fallback. | Implement ed25519, or give a CDC-specific error. |
| M-43 | PLAUSIBLE | `run.rs:1126` vs `myrun.rs:197,251` | Binlog-connection identity is never checked; behind an LB or Galera VIP the dump may come from another node. | Compare `@@server_uuid` on the MyWire connection. |
| M-44 (S3-14, R2-19, S7-5) | READ / PLAUSIBLE | `myrun.rs:85-89`; `sink/mysql.rs:332-358`; `naming.rs:777-781`; `dispatch.rs:595-611` | `lower_case_table_names=1`: CDC `table="Orders"` never matches TABLE_MAP (zero changes, forever); the bulk guard is blind; `Events`/`events` drop each other's staging. | Read `@@lower_case_table_names` and canonicalise everywhere. |
| M-45 | READ | `read_impl.rs` / `postgres.rs:568-573`; `arrowcol.rs:47-54` | `read()` of unconstrained NUMERIC fails ("unexpected float8"), or gives garbage for 0/NaN. `NUMERIC(3,5)` truncates. | `::text`; p = max(p, s). |
| M-46 (R3-17 + S8-4/6) | READ | `py-apitap/python/apitap/__init__.py:387-496` | Pushdown: i16/i32 overflow (polars wraps); Bool under Cast; Float32 literal; cast elision 3.7→Int64; predicates on `::text`-rewritten columns (MySQL JSON); a server error is raised with no fallback. | Refuse each; re-read without pushdown on a server error; committed pytest plus a differential test. |
| M-47 | READ (**security**) | `urlerr.rs:42-46, 64` | `rsplit_once('@')` runs over the query string: `?user=a@corp&password=hunter2` leaks the password into connect errors. | Strip `?`/`#` before redacting; never print the query. |
| M-48 | READ | `gsheets.rs:354-358` | Cells beyond the header width are dropped silently (Sheets omits trailing blank headers). | Refuse, or name them `col_N`. |
| M-49 (R3-11) | READ | `source/clickhouse.rs:536-544` | UInt64 cursor > i64::MAX: parallel CH read fails. | i128, one-span fallback. |
| M-50 (R3-9) | READ | `dialect/postgres.rs:18-31` | `postgres://%2Fvar%2Flib%2Fpostgresql/db` gets verify-full over a socket and fails. | Percent-decode (folds into M-6). |
| M-51 | READ / PLAUSIBLE | `sink/mysql.rs:1031` | `sql_mode=''` plus unread warnings: infinity/BC timestamps become `0000-00-00`, and out-of-range values are clipped. | Same warnings check as M-20. |
| M-52 | PLAUSIBLE | `sink/mysql.rs:364, 967` | MEMORY marker without PK, and PK-less staging: fails under `sql_require_primary_key=ON` (DigitalOcean default). | PK on the marker; invisible PK or refuse. |
| M-53 | PLAUSIBLE | `sink/clickhouse.rs:466-552` | A `readonly=1` source user can't take per-query settings, so ch→* fails at connect. | Detect readonly, or document `readonly=2`. |
| M-54 | PLAUSIBLE | `sink/clickhouse.rs:875-881, 1603-1618, 1978-2214`; `dest_ch.rs:1714-1717, 1776-1806` | CH Cloud / Replicated without on_cluster: no `select_sequential_consistency` or sync before count/max/ATTACH/key-table reads (subset attached, duplicates). Mutations with `now64()`/`IN (SELECT)` need `allow_nondeterministic_mutations=1`. | Detect Shared/Replicated: set both, or refuse. Verify on Cloud. |
| M-55 (R2-4 PG/MySQL) | READ | `run.rs:1047, 1243`; `dest_pg.rs:392`, `dest_my.rs:258` | Group replay re-applies a Rekey to members already past it, so the group is wedged or the wrong row moved. The "idempotent" comments are false. | Per-member window skip/trim. |
| M-56 (R2-13) | READ | `collapse.rs:383-387` | A DEFERRABLE PK swap loses a row. | Route to residue or refuse. |
| M-57 (R2-12) | READ | `drain.rs:114, 388-417` | Streamed transactions flush under the last Relation (stuck, or silently mislabelled). | `Arc<Layout>` per op. |
| M-58 (P1-4) | READ | `run.rs:1746-1806`, `myrun.rs:288-303` | Follow mode ignores SIGTERM until the follow deadline, so SIGKILL lands. | Check `shutdown::requested()` in the loop. |
| M-59 | READ | `resolve.rs:243-247` | The TOAST refetch `"pk"::text = $1` defeats the index: one sequential scan per masked key (Iceberg CDC, hours on large tables). | `"pk" = $1::<type>`, batched with `= ANY`. |
| M-60 (sysrev P13) | READ | `run.rs:2264-2275` | `$1::regclass` on an unquoted name: mixed-case tables fail or read the wrong PK; INCLUDE columns counted. | Bind the oid; use `indnkeyatts`. |
| M-61 (P1-13) | READ | `run.rs:1252` | The MySQL window budget ignores overlap: RSS over 256 MB. | `cdc_window_budget()`. |
| M-62 (P1-2 all sinks) | READ | `sink/clickhouse.rs:2100-2217`, `dest_ch.rs:1995`, `sink/mysql.rs:1315,1345`, `bigquery.rs:2182-2209` | `DROP …?` runs between publish and the state write. CH > 50 GB (`max_table_size_to_drop`), an FK on MySQL, or a BQ copy without a job id: failure after publish, then a wedge or duplicates. | State first, drop best-effort; BQ copy job id. |
| M-63 (R2-7b) | READ | `dest_bq.rs:2071-2076, 2206-2209` (7a6f36d) | Four buckets: a 10 s follow cadence still exhausts 1,500 ops/table/day in about 16 h. | Bucket count from cadence (≥ windows/1400), or rotate daily. |
| M-64 (R2-18) | READ | `dest_bq.rs:1271, 1277, 1298` | Column names inside an `ERROR(FORMAT('…'))` literal: injection on user-created tables. | `sql_str` and double `%`. |
| M-65 (R2-21, P1-7) | READ | `sink/mysql.rs:1327-1331`, `dest_my.rs:716-722`; `dest_pg.rs:547,595,719,831` | State under two spellings (`db.t` vs `t`): a replace leaves a stale CDC watermark. PG `_apitap_state` is unqualified; `sales.orders` and `public.orders` share one row. | Delete both spellings; qualify the state table. |
| M-66 (R2-16) | READ | `mybinlog.rs:454-462` | Zero TIMESTAMP becomes 1970; TIMESTAMP2 is read as i32 (post-2038 MariaDB). | Relay the zero literal; u32. |

---

## 5. P2 — hardening (fix with the area; one line each)

**CDC core:**
- `drain.rs:494-570` expect on protocol input (T11).
- Maps never pruned (T15).
- Streamed-transaction cap is session-wide (`drain.rs:490-545`).
- Empty `tables` panics `clamp`/`div_ceil` (`run.rs:643-738`).
- `ALTER PUBLICATION` on every drain is fatal for a non-owner (`run.rs:2332`).
- Refetch GUC pins (`resolve.rs:250`).
- TOASTed unchanged PK (`collapse.rs:217`).
- Changelog NULL key maps to `""` (`changelog.rs:289`).
- `seal` checks count only (`window.rs:161`).
- Bool CH key `'t'` (`rowtext.rs:361`).
- Hot standby `pg_current_wal_lsn` errors.
- `WARNED` is global and progress is frozen (R3-14).
- No timeouts (T19).
- Blocking `join()` in async (`run.rs:767`).
- TLS hint on every error (R3-15).
- Open stream blocks caught-up detection (P2-1).
- Perf P-a/b/c.

**PG wire:**
- float4 scientific threshold (`pgbindec.rs:157`).
- `block_size` hard-coded 8192.
- SCRAM without channel binding, and the comments overclaim.
- Walsender db path not percent-decoded.
- Replace drops NOT NULL / FK pre-check / grants / publication.
- Merge dedupe NULLS FIRST (`sink/postgres.rs:1256`).
- Options redial swallows errors.
- State CREATE race matched by English text.
- No SASLprep.
- `numeric(p,-s)`.
- `"char"` OID 18.
- T5 negative ndigits returns `Ok(0)` (`pgcopy.rs:469`).
- T9, P2-8, P2-13, P-j.
- `PgRead` visibility (clippy).
- README:488.

**MySQL lane:**
- `server_uuid` fallback on any error.
- MT_DECIMAL 0x00 metadata.
- Misattached docs/allow attributes.
- Refetch after DROP.
- ENUM label backslash.
- 1 GiB packet / unbounded pgmytsv field (OOM).
- `pos as u32` above 4 GiB.
- Tagged GTID event 42.
- `b[0]`/`b[1]` unchecked.
- XA ONE PHASE (PLAUSIBLE).
- T1, T2, T4 (`ndigits -1` panics), T10, T17, P1-3, P2-9/R3-16 (ssl-ca, socket, verify_ca, hint), P2-10 (`827` underflow, `834` panic, negative TIME2 fraction, CRC never verified), `VerifyCa` dead code, macOS `TCP_KEEPIDLE`, perf.

**CDC destinations:**
- Residue cast with typmod truncates (`dest_pg.rs:294`).
- `_apitap_state` collation ci (MySQL).
- CH state versioned by the server clock.
- `detect_insert_only(lease key)`.
- Comment split by the 54a29fd test (`dest_bq.rs:2695`).
- `_apitap_deleted` user type.
- Changelog on Replicated with the escape hatch.
- ORDER BY unquoted PK names.
- P2-3, P2-4, P2-11/T13, T14, R2-14 fsync, R2-15, R2-24, R2-26, R3-21, perf.

**BQ / Iceberg:**
- Unencoded table names in URL paths.
- `contains("409")` on the whole message.
- Session URI in errors (secret-like).
- Lost final PUT not tracked.
- Decimal cursor compared lexicographically (also `plan.rs:164-206`, S7-16).
- s>p numeric truncation (bqparquet/iceberg/pgcopy).
- Iceberg decimal FLBA(16).
- Bytes after the trailer ignored.
- `cdc_clear_watermark` Conflict→Ok.
- `dest_table` qualifier dropped.
- No `CurrentSchemaIdMatch`.
- `AWS_SESSION_TOKEN` mixing.
- Fence deleted after an unknown outcome.
- Snapshot timestamp from the client clock.
- 54a29fd test asserts the obsolete shape (`bigquery.rs:1249`).
- T3, T7, T16, P2-10 (bqparquet 303/882), P-e, R2-22..R2-26, R3-22.

**CH / S3 / GCS:**
- R3-20 marker: use `wait_end_of_query=1` with a small `http_response_buffer_size`.
- P2-5, P2-6.
- DateTime64(7..9) truncated.
- `NOT LIKE '__apitap%'` wildcard.
- FORMAT TSV vs TSVRaw.
- Stale docs (`APITAP_CH_MAX_BODY_MB`).
- `clickhouse://` plain HTTP Basic auth off-loopback.
- GCS session URI in errors.
- S3 XML entities.
- P2-7, T16.
- Retry 3×0.5 s with no 429 or checksums.
- `finish_file?` with no abort.
- `numeric_cursor` true for temporal/String.
- `SYNC REPLICA` 180 s.
- Date32/DateTime64 range (`9999-12-31`, PLAUSIBLE).

**Pipeline:**
- `parse_peer` byte-slices a non-ASCII S3 key (panic).
- `KILL QUERY` race on a pooled connection id.
- Misleading refusal advice (`sink/mysql.rs:1297`).
- A panic in one table of `run_many` skips its siblings' discard.
- Lease rows leak on announce failure.
- PK-width check is uppercase-only.
- T8, stale comments, P2-12, R2-20, R3-19 fork tokens.

**Python / small sources:**
- `urlerr` no-scheme prints 12 characters.
- `:` treated as reserved.
- Progress JSON (R2-30, plus a reporter-thread overlap that underflows), nested progress killed, `INTERVAL=inf` panics.
- gsheets blank rows; R3-18.
- github ref re-resolved per request, no retry or page cap, owner/repo unencoded, LFS (R2-28).
- csv: unbounded field and BOM (R2-27).
- Capsule child release; R3-19 RT mutex across fork, `export_batch` outside `catch_unwind`, GIL.
- `transfer_tables` error discards the report.
- Projection drops the cursor.
- No HTTP per-request deadline.
- `CString` with an interior NUL.
- `o as u32` offsets.
- Torture gaps.
- P1-8 UTF-8 validation (`arrowcol.rs:417, 944-1010`).
- T18; handoff §6.7–6.9.

**Repo-wide:**
- Lost `\` continuations in about 10 error strings (R3-23).
- 102 clippy errors and 1229 fmt hunks (one mechanical commit).

---

## 6. Fixed and verified — do not re-review

| Item | Evidence |
|---|---|
| P0-1 CH ident injection | REPRO-fixed; every ident goes through `ch_ident`/`q` |
| P0-3 PG pool TLS; walsender no longer Prefer | READ (remaining gaps are M-6/M-21) |
| P0-4 MySQL CDC TLS sites | READ (usability limited by P2-9) |
| P0-6 numeric pow / dscale 200 | test present |
| P1-5 MySQL UNSIGNED `read()` | REPRO-fixed, incl. 2^64 and the 2^63 crossing |
| P1-6 `spans()` i128 | REPRO-fixed, pg and MySQL |
| P1-9 handshake, P1-10 rowbinary, P1-11 char excerpt (all sites) | READ/tests |
| P1-12 CH count | removed (residual M-30) |
| R2-2 on PG/MySQL | atomic RENAME (no call-site test) |
| R2-3 | holds (cleanup is M-33) |
| R2-5 | refuse partitioned (no test) |
| R2-6 | cancel + settle; close_run fence claim |
| R2-7 100/tx | 95 cap, test RED-checked |
| R2-8 three shapes | REPRO-fixed |
| R2-9, R2-11 | fixed |
| P2-2 | depth counter |
| P0-2 stated case, P1-1 | fixed (no unit test) |

---

## 7. Attacked and held (don't re-attack)

- **Slot / LSN:** confirmation never passes applied (keepalive sends `applied`; the parallel
  arm confirms the contiguous prefix only).
- **Drain:** caught-up `end_lsn`; relayout carry; `run_overlapped` / AbortOnDrop.
- **Replay plan:** R0–R2.
- **Collapse:** fast path; NULL / NOTHING refusals.
- **Bootstrap:** snapshot pin.
- **Fences and leases:**
  - PG/MySQL: lease `FOR UPDATE` in the apply tx; watermark in the same tx.
  - CH: `written_rows == marks`; seq bias.
  - BQ: fenced script with COMMIT as the last statement, job ids, 409 adopt-by-text,
    groups never split.
  - Lease expiry judged on the destination clock; release ordering.
- **Naming:** announce-then-check gives no double winner; exact-length anchor; `Mine` =
  exact token; guard collector never closes the victim's lease; `pipe.rs` latch / `join_all`
  / JoinOnce; memory pricing saturating.
- **Escaping:** MySQL `sql_lit`/LIKE escape/UNHEX/RENAME atomic; CH escaping and
  whitelists; BQ `sql_str` `'`/`\`; PG residue binds.
- **Iceberg:** sequence numbers, snapshot ids vs `tried`, UuidMatch + RefSnapshotIdMatch,
  field ids from the catalog, no column bounds written.
- **Python boundary:** Arrow FFI bounds and capsule ownership/formats/alignment;
  `read.rs` completed-before-drop; shutdown async-signal-safety; pushdown NULL/3VL,
  collation, quoting.
- **Wire:** PG frame caps, SCRAM nonce/iterations/ct-compare, pgoutput reader checks,
  pgbindec numeric/NaN/jsonb, pgcopy bounds; MySQL packet layer, `parse_rows` guards,
  BIT/VARCHAR/BLOB/ENUM/SET/TIME2/DATETIME2 decoders, commit boundaries, AHEAD/purged
  refusals.
- **Secrets:** not in `Debug`; `urlerr` sanitizes except M-47 and the no-scheme case.

---

## 8. Minimum e2e matrix (add to gate.py; ≤1000 rows each; about 30 min on the VPS)

Each leg compares full rows. Each must be **RED at HEAD** before the fix (rule 1).

| Leg | Setup → assertion | Catches |
|---|---|---|
| M1 `my_type_matrix` (8.0, 8.4, MariaDB 10.6) | DATE, ENUM, SET, TINY/TEXT/MEDIUM/LONG text and blob, BIT, TIME±(3), YEAR, DATETIME(6), TIMESTAMP(6), DECIMAL(19,4)/(65,30), INT/BIGINT UNSIGNED max, utf8mb4 CHAR(64)/CHAR(255), BINARY(16) PK ending in 0x00, zero/partial dates. Bootstrap → update/delete → drain ×2 → bulk == CDC == source; a same-count ALTER is refused. | M-1, M-2, M-3, M-36, M-40, M-41, M-66, R2-1 |
| M2 `my_cdc_dests` | The M1 table → pg, bq, ch, ice with a dest TimeZone of Europe/Vilnius | M-15, M-16, M-25 |
| M3 `follow_edges` (pg and my) | FOLLOW=60, WINDOW_MAX=5, steady multi-row transactions; count and checksum | M-4 |
| M4 `fan_in` (my, ch, bq, ice, pg) | Two sources at different cursor heights, plus two concurrent first runs | M-5, M-10, M-11, M-33, M-34 |
| M5 `pg_url_planes` | Decoy on the authority, sentinel on `?host`/`port`/`dbname`/`options`/`PG*` env; `%2F` socket; PGSSLMODE; `read()`, transfer, pg→ch, log_based all hit the sentinel | M-6, M-21, M-50 |
| M6 `pg_types` | read() and CDC→ch over arrays, enum, domain, interval, inet, money, citext, unconstrained numeric, NUMERIC(3,5), timetz +02, dscale 200, NaN | M-7, M-8, M-24, M-45 |
| M7 `ch_replay_chains` | Rekey chains and DELETE+rekey, killed mid-replay; MaskedUpdate failing between DELETE and INSERT (mock); `ReplacingMergeTree(updated_at)` dest; changelog with numeric/tz/bool PK | M-12, M-13, M-26, M-27 |
| M8 `parallel_apply` | APITAP_PARALLEL_APPLY=1, depth>1, insert-only → TOAST masked → truncate | M-9 |
| M9 `ch_hostile` | LB returning 504 on EXCHANGE once; profile with `max_result_rows` + break, `limit`; `readonly=1` user; UInt64 cursor > 2^63; replace over a Replicated table | M-17, M-29, M-30, M-49, M-53 |
| M10 `pg_parallel_ctid` | PK-less table, parallel=4, concurrent non-HOT updates; checksum vs snapshot | M-18 |
| M11 `pg_search_path` | `$user` schema + `public.t`, unqualified replace | M-19 |
| M12 `my_sink_collation` | pg→mysql text PK `'abc'`/`'ABC'`; infinity timestamp | M-20, M-51 |
| M13 `iceberg_faults` | Fault proxy: 504 commit then 503 GET; decimal cursor append ×3 | M-14, M-34 |
| M14 `objstore_faults` | MinIO behind a fault proxy: fail the second copy / a delete / the list | M-32 |
| M15 `polars_pushdown` | Differential lazy == eager on pg and my: overflow, Bool cast, JSON, Float32 | M-46 |
| M16 `binlog_misc` | `/* c */ TRUNCATE`, session STATEMENT, `binlog_checksum=NONE`, ed25519 user, lctn=1 container | M-37, M-38, M-39, M-42, M-44 |

Rust unit or mock tests for the rest: `drain()` scripted stream, `apply_windows` arm, walsender
silence clock (M-22), `urlerr` with `@` in the query (M-47), capsule panic/release, fork tokens.

---

## 9. Per-lane status (what may be used today)

| Lane | Status |
|---|---|
| PG→PG/MySQL/CH bulk replace/append, URL without `?host=`-style keys, tables with a PK | **usable with care**: M-19 (schema), M-20 (MySQL dest collation), M-5 (fan-in only) |
| PG `read()` | **only for tables of int/float/bool/date/timestamp/text/bytea/numeric(p,s)** (M-8, M-45) |
| PG→CH CDC | **blocked** if any array/enum/domain/interval/etc. column (M-7); replay chains (M-12, M-13) |
| PG CDC follow mode | **blocked** (M-4, M-22) |
| MySQL/MariaDB CDC | **blocked** (M-1 for DATE/ENUM/TEXT; M-2 for CHAR ≥ 64 utf8mb4; M-3, M-4) |
| MySQL bulk → anything | usable for common types; e2e coverage is thin (none to pg/my/bq/s3/ice) |
| Fan-in (multiple sources, one table) | **blocked** on MySQL/CH/BQ (M-5), and first-run on BQ/CH/Iceberg (M-10/11/34) |
| ClickHouse behind an LB / Cloud | **blocked** (M-17, M-54) |
| BigQuery CDC follow | **blocked** beyond about 16 h at 10 s cadence (M-63); MySQL source BYTES (M-15) |
| Iceberg | append on decimal cursor broken (M-34); Glue/S3 Tables not working (M-35); ambiguity sweep (M-14) |
| S3/GCS replace | not atomic (M-32) |
| csv / github / gsheets | usable for small, clean inputs; no e2e |

---

## 10. Order of work

1. **Stop the bleeding (one PR each, each RED first):**
   - M-1 + M-2 + M-3 + M-36 (one MySQL decode PR, with M1).
   - M-4 (both lanes, M3).
   - M-5 (M4).
   - M-6 + M-21 + M-50 + `source_origin` (one resolver, M5).
   - M-7 (M6).
   - M-8 + M-45 (M6).
2. **Replay safety:** M-9, M-12, M-13, M-55, M-56, M-57 (M7, M8).
3. **Publish atomicity:** M-10, M-11, M-17, M-33, M-34, M-14, M-32, M-62 (M4, M9, M13, M14).
4. **Encoding and time:** M-15, M-16, M-24, M-25, M-26, M-27, M-28, M-40, M-41, M-66 (M2, M7).
5. **Bulk correctness:** M-18, M-19, M-20, M-51, M-29, M-30, M-31, M-49 (M9–M12).
6. **Ops and security:** M-47 (small, do it early), M-22, M-23, M-35, M-37, M-38, M-39,
   M-42–M-44, M-52–M-54, M-58–M-61, M-63–M-65; then M-46, M-48.
7. **P2 sweep per area.** Then the clippy/fmt mechanical commit. Then restore CI (ask the
   user about the PAT first).
8. **Full gate (rule 5).** Re-run this master checklist's §8 legs. Only then call any lane
   production-ready.

VPS state note: `~/qa-venv` is an editable 0.61.0 install whose `.so` is untracked in
`~/apitap-qa/py-apitap/python/apitap/`. The next `rsync --delete` removes it, so rebuild with
`maturin develop --release` (about 9 minutes). Round-3 scripts are in `~/qa-r3-scripts/`.
