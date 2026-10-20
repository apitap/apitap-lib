# apitap — deep review, round 2 (2026-10-15)

**For the implementer.** Round 2 of the review queue in
`docs/review/2026-10-14-handoff-review-rust.md` (round 1, "the handoff") and its companion
`docs/review/2026-10-14-rust-engineer-pass2.md` ("pass 2", items T1–T19). Nothing here
repeats those two; read them first. §0 rules of the handoff still apply in full (VPS-only
builds, QA rig `~/apitap-qa`, touch before cargo, RED-before-GREEN, ask the server, 1000-row
budget, no Claude trailer, never prune docker).

Tree reviewed: `main` at `a0312d6` — i.e. **after** the ten fix commits
`9f0c72e..a0312d6` that landed P0-1…P0-7 and P1-9/10/11 on 2026-10-14.

**Scope of round 2.**
1. A review of those ten fix commits (§1).
2. End-to-end reads of the lanes round 1 only skimmed: BigQuery (`sink/bigquery.rs`,
   `logbased/dest_bq.rs`, `gcp.rs`), Iceberg (`sink/iceberg.rs`, `logbased/dest_ice.rs`
   against the v2 spec and iceberg-rust 0.10), the concurrency/identity core (`lease.rs`,
   `guard.rs`, `naming.rs`, `pipeline/`, `pipe.rs`), and the Python boundary + minor
   sources (`py-apitap/`, `source/gsheets.rs`, `github*.rs`, `csvfile.rs`, `progress.rs`).
3. An adversarial attack on the CDC exactly-once claims round 1 called sound.
4. Repros on the VPS.

**Labels** (same as the handoff): **REPRO** reproduced on the VPS · **READ** the integrator
re-read the lines · **AGENT** one reviewer traced it · **PLAUSIBLE** mechanism real, impact
needs a repro.

---

## 1. The ten fix commits — verdict

| Commit | Item | Verdict | What is still wrong | Test RED-capable? |
|---|---|---|---|---|
| `9f0c72e` | P0-1 ch_ident | **COMPLETE** | `ON CLUSTER \`{c}\`` unescaped (`sink/clickhouse.rs:270,1246`, operator config → low). No NUL/control refusal. | yes (string level) |
| `5aff53a` | P0-6 numeric | **COMPLETE** | Test uses dscale=40, not the k≥128 divide-by-zero; the `Less` assertion passes on old code in release. Add dscale=200. | partly |
| `57bba12` | P1-11 truncate | **PARTIAL** | Sibling missed: `logbased/dest_bq.rs:2226` `&bad[..bad.len().min(120)]` byte-slices a String → panic on non-ASCII SQL. | helper only |
| `835b30d` | P1-9 handshake | **COMPLETE** | Not added to `torture.rs` as the handoff asked. | yes |
| `f6cbb31` | P1-10 rowbinary | **COMPLETE** | — | yes |
| `af8569b` | P0-3 PG TLS | **PARTIAL + REGRESSION** | see §1.1 — **unix sockets now fail**, `?host=` bypass, `PGSSLMODE` overridden, webpki-only roots, walsender still Prefer, no docs | helper only |
| `e5e1f49` | P0-4 MySQL CDC TLS | **COMPLETE (sites) / PARTIAL (usable)** | Raw binlog plane has no `ssl-ca=` (P2-9) → RDS/private-CA CDC users can only pick unverified `required`. No README note. | helper only |
| `50247e7` | P0-7 parallel apply | **PARTIAL** | Entry window fixed, insert-only gate added. But insert-only windows are **still not commutative**: residue tail DELETE+INSERT, `truncate` → `clear_owned`, TOAST readback all need the previous window landed. Flag silently ignored (not refused) on other tables. `progress::add_rows` skipped → frozen counter. No test. | none |
| `e5f8724` | P0-2 + P1-1 | **COMPLETE** | P1-1 has no test. | P0-2 yes (e2e, RED shown in commit) |
| `a0312d6` | P0-5 iceberg | **PARTIAL** | see §1.2 | none |

### 1.1 `af8569b` — the TLS default breaks real setups · **READ**

`dialect/postgres.rs:16-24` `secure_url` reuses `dialect::mysql::host_is_loopback`
(`dialect/mysql.rs:57-68`), which looks only at the URL authority:

1. **Unix sockets (regression).** `postgres:///db?host=/var/run/postgresql` → `host_str()`
   is `None` → "not loopback" → `sslmode=verify-full` appended. sqlx sends SSLRequest over
   the socket too (`sqlx-postgres-0.8.6/src/connection/stream.rs:46`, `connect_uds(path,
   MaybeUpgradeTls(..))`) → "server does not support TLS" / hostname failure. **Every local
   socket deployment that worked on 0.61.0 now fails.** Fix: a `host=` query value starting
   with `/` (or `%2F`) is local → no default.
2. **`?host=` / `?hostaddr=` override the authority in sqlx** (`options/parse.rs:69-80`):
   `postgres://u@localhost/db?host=prod.example.com` is judged loopback → no TLS → remote
   connect under `prefer`. The fix is bypassed. Fix: decide on the effective host
   (query `host`/`hostaddr` win, as in sqlx).
3. **`PGSSLMODE` is silently overridden** (sqlx reads it at `options/mod.rs:84`, the URL
   param wins). Fix: treat a set `PGSSLMODE` as explicit.
4. **webpki roots only** (rustls build): RDS / Cloud SQL / Azure / self-signed and bare-IP
   hosts now fail verify-full unless `sslrootcert=` is given — and there is no PG
   equivalent of `mysql::tls_required_hint`, so the error names no opt-out. Fix: add the
   hint (names `sslmode=require` / `sslrootcert=`), and document it.
5. **Walsender still `Prefer{explicit:false}`** (`wire/walsender.rs:689`): the CDC pool
   verifies, the replication socket does not; an on-path attacker can strip TLS there and
   relay SCRAM (the guard only refuses cleartext/MD5). Fix: same default on the walsender.
6. **No docs**: README.md:169 still documents PG sslmode with no default; no troubleshooting
   row like the MySQL one at :501. This is a user-visible behaviour change → release note.

Test: the 6-case mapping test calls the helper; reverting any of the four call sites stays
green. Add one test per call site that inspects the options actually passed (or route all
four through one `pg_pool()` constructor and test that).

### 1.2 `a0312d6` — ambiguity is forgotten after the first Unknown · **READ**

After a `CommitTry::Unknown`, nothing remembers that an earlier attempt may still land.
Every later plain `Err` still reaches the sweep (`finalize` ~`:1251`, `cdc_commit` ~`:1940`):

- `sink/iceberg.rs:1511-1512` `if attempt == 2 { return Err(e) }` — three 504s in a row
  (the classic overloaded-catalog-behind-a-60s-LB case) → sweep → a late landing points
  `main` at deleted files → table unreadable; on CDC the watermark rode that commit, so the
  window is never replayed.
- `:1484` `commit_table(..).await?` → a 4xx on the retry (401 on token expiry).
- `:1331,1348,1351,1368,1371,1403` S3 get/put `?` on the retry; `:1535` "conflicted 3 times".
- CDC twins: `:2149-2150, 2127, 2025, 2044, 2046, 2075, 2173`.

Fix: `let mut ambiguous = false;` set on the first Unknown; wrap the loop body and map
**every** later `Err` (incl. attempt==2 and the conflict exhaustion) to `CommitEnd::Unknown`.
Document the side effect: a landed-but-Unknown bulk *append* returns an error, and a re-run
appends again (duplicates beat a corrupt table, but the operator must know).

---

## 2. NEW findings — P0 / P1

### R2-1 · P0 · MySQL CDC decodes old events with TODAY's catalog · **READ**
- `logbased/mysource.rs:583-626`: on TABLE_MAP, `fetch_schema` reads **current**
  `information_schema`; names come from it (`window.rs:88-103`), and `unsigned` + ENUM/SET
  `labels` are stamped **by position**. The only guard is the column **count** (`:607`);
  the comment there acknowledges the hazard but only catches a length change.
- Same-count DDL between the event and the drain passes silently: `DROP COLUMN a, ADD
  COLUMN c` (b gets a's value), `MODIFY x INT UNSIGNED` (-1 → 4294967295), ENUM reorder
  (index 1 → the new label). An hourly schedule draining a 20-minute backlog across a DBA's
  ALTER is the normal case, not an edge case. **Relevant to every MariaDB/MySQL → CH trial.**
- Fix: with `binlog_row_metadata=FULL` (MySQL 8.0.1+ / MariaDB 10.5+), decode from the
  TABLE_MAP optional metadata (names, signedness, enum/set strings) and stop consulting the
  catalog for shape. Otherwise store a schema fingerprint with the watermark and refuse
  when the catalog fingerprint changed while the stored position predates it. At minimum,
  compare names/types, not just the count, against the TABLE_MAP column types.
- RED test: rows event written before `ALTER … DROP a, ADD c`, drained after → assert
  column values on the destination.

### R2-2 · P0 · Two first-run fan-in appends destroy each other · **READ**
- `pipeline/mod.rs:620-622` flips `mode` to `Replace` when the destination does not exist,
  but the run token was minted from `opts.mode` (`dispatch.rs:134`, `land_kind` `:164`) =
  `Incremental`. `peer_blocks((Incremental, Incremental), hashA≠hashB)` → false
  (`naming.rs:636`) → both proceed as legal fan-in; both finalize as replace:
  `DROP TABLE IF EXISTS final` + rename + delete all state (`sink/postgres.rs:1123,1152`).
  The second drops the first's freshly published table. Both report success. MySQL
  (`mysql.rs:1298,1317`) and CH (`clickhouse.rs:2143-2150`) the same.
- `failure-modes.md:48` promises a refusal here.
- Fix: a bootstrap run is a `Swap` for peer purposes (mint/upgrade the kind), or finalize
  with a non-destructive publish (`RENAME staging TO final` that fails if `final` exists).
- RED test: two concurrent first-run appends from different sources into one new table.

### R2-3 · P1 · Same-source append reads its watermark before announcing · **AGENT**
- `pipeline/mod.rs:620` (`dest_state`) runs before `:703` (`prepare` → announce → check).
  Run B reads W; run A commits (W→W2) and removes its staging/lock; B announces, sees no
  peer, loads `> W` → (W, W2] twice on a keyless table. `naming.rs:211-214` names this
  exact duplicate as the reason the source hash exists; the CDC lane orders it correctly
  (`run.rs:951`), the bulk lanes don't.
- Fix: re-read the state row after `check_peers` and refuse if it moved; on PG also CAS in
  the finalize tx (`… WHERE watermark = W_read`).

### R2-4 · P1 · CH CDC: replay of a window with Rekey + old-key reuse · **READ** (premise) / **AGENT** (chain)
- `collapse.rs:286-303` → `Rekey{1→2}`; a later `INSERT 1 (X)` → residue `Upsert`.
  `dest_ch.rs:850-901` reads TOAST at the **old** key, deletes it, inserts at the new key
  without deleting it; the comment at `:887-889` assumes "on a replayed window … the old key
  is not there". If the state INSERT failed after the data landed, the replay reads **X**:
  MergeTree → two rows at id=2; Replacing → key 2 silently gets X's TOAST value. Chain
  `Rekey 1→2; Rekey 3→1` loses row C.
- PG/MySQL: group replays (`run.rs:1040` drains from the group minimum, no
  "member already ahead" skip) → `UPDATE … SET id=2 WHERE id=1` hits X → duplicate key on
  every rerun → group wedged.
- Fix: each member skips/trims windows at or below its own stored watermark (general fix);
  on CH make Rekey idempotent (delete new key first).

### R2-5 · P1 · Iceberg bulk load into a partitioned table panics at commit · **AGENT**
- `sink/iceberg.rs` `dest_state`/`prepare` never check the partition spec (only
  `cdc_bind:1854` does; its "(same as merge)" comment is wrong); commit writes
  `partition(Struct::empty())` (`:1295`) → iceberg-rust 0.10
  `construct_partition_summaries` `zip_eq` panics. Whole load wasted, claim marker left →
  every later run refused. Fix: refuse a non-empty `default_partition_spec()` up front.

### R2-6 · P1 · BigQuery CDC releases while its own job may still run · **AGENT**
- `dest_bq.rs:2157-2159` `close_run`, `lease.rs:532-546`: when polling gives up (5 poll
  errors, a non-retried 404/400, 30-min timeout) nothing cancels the job; the run releases
  and deletes the fence. The next run takes over and appends window W; the old job then
  commits W too (both INSERT-only) → changelog events doubled. The takeover path already
  does the right thing (`lease_claim` standalone fence `UPDATE`, `bigquery.rs:2345-2353`);
  the voluntary release doesn't. The bulk loader has `quiesce` (`:1744-1783`).
- Fix: on unknown outcome `jobs.cancel` + bounded poll to a terminal state; in `close_run`,
  `UPDATE fence SET claimed=TRUE` before the delete.

### R2-7 · P1 · BigQuery quotas the 100/1000-table fan-out will hit · **AGENT**
- **100 tables per transaction:** `pack_whole_groups` (`dest_bq.rs:58,71-87`) caps chunks by
  bytes (256 KiB) only; ~1.2–2.3 KB per narrow MERGE+state → 110–180 groups per chunk, +3
  fixed tables. Past ~97 active members in one window the script fails non-retryably on
  every rerun → slot pins WAL. Fix: also cap distinct modified tables (≤95).
- **1 500 ops/table/day:** CDC staging is one table per (table, run) reloaded with
  WRITE_TRUNCATE every window (`dest_bq.rs:2056-2082`) → a 10 s follow-mode drain exhausts
  it in ~4 h, then `quotaExceeded` is fatal. Bulk `_apitap_state` is one table per dataset
  with one load per run (`bigquery.rs:2078-2100`): 100 tables hourly = 2 400/day. Fix:
  rotate staging names; batch state rows per run / DML where allowed.

### R2-8 · P1 · polars pushdown loses rows · **REPRO** (semantics, both sides)
Python renderer `py-apitap/python/apitap/__init__.py`:
- **Float32:** polars 1.43.2 keeps `Float32(0.1) == 0.1` (1 row); PG `0.1::real = 0.1` →
  **f**. The renderer elides the literal cast (`:436-448`, "Float" allowed) → the server
  never sends the row. Fix: refuse pushdown touching a Float32 column/literal.
- **Bitwise `&`/`|`:** polars `(flags & 4) == 4` → 2 rows; `LOGIC` (`:375`, `:456-457`)
  maps `And`/`Or` to SQL `AND`/`OR` with no boolean check → MySQL drops all rows, PG type
  error. Fix: allow LOGIC only on boolean operands (track "booly" through `walk`).
- **Modulus:** polars `-3 % 2 = 1` (floored), PG `-3 % 2 = -1` (truncated). Fix: drop
  `Modulus` from `ARITH`.
The docstring promises pushdown is "bandwidth, never correctness" — these three break it.

### R2-9 · P1 · Google Sheets source stops at the first blank region · **AGENT**
- `source/gsheets.rs:323-346`: pages `A{start}:ZZZ{start+page-1}`; the API omits trailing
  empty rows, the loop stops on `got < page_rows`. Blank row at the page edge → everything
  after it lost, run "succeeds". Fix: page until `start > gridProperties.rowCount` (already
  read at `:218`).

### R2-10 · P1 · `to_polars()` / stream callbacks · **AGENT**
- `py-apitap/src/capsule.rs:355-436`: `extern "C"` stream callbacks without `catch_unwind`
  → any panic aborts the Python process (Rust ≥1.81); tokio `blocking_recv` panics when the
  consumer pulls from inside a tokio runtime (deltalake/datafusion). Fix: `catch_unwind` →
  `EIO` + `last_error`; `futures::executor::block_on(rx.recv())`.
- `capsule.rs:370-373`: `blocking_recv` holds the GIL if the consumer didn't release it
  (polars import, as far as known) → all Python threads frozen for the whole scan, Ctrl-C
  dead. Fix: release the GIL around the receive when held.
- `py-apitap/src/lib.rs:51-54`: process-global multi-thread runtime is not fork-safe →
  a `multiprocessing`/Celery-prefork child hangs forever on the first spawned task. Fix:
  store the PID with the runtime; rebuild (and leak the old one) when it changes.

### R2-11 · P1 · `github+api` commits incremental on committer dates · **AGENT**
- `source/github_api.rs:146-163,805-823`: committer dates are client-set and non-monotonic;
  merged branches below the watermark never load; one commit dated 2099 freezes the table
  (0 rows forever, no error). Fix: commits = replace only, or re-crawl a lookback and merge
  on `sha`.

---

## 3. P2 — hardening

| # | Label | Where | Item |
|---|---|---|---|
| R2-12 | AGENT | `logbased/drain.rs:393-407,473-477,494-552` | Streamed (proto v2) tx ops buffered without their layout; flushed at StreamCommit under whatever Relation arrived last (a concurrent tx can resend the OLD shape) → stuck (ADD COLUMN) or silent (rename/retype). Capture `Arc<Layout>` per op. |
| R2-13 | AGENT | `collapse.rs:383-387` | DEFERRABLE PK swap (`SET id = 3 - id`) → r1 nulled, row lost. Refuse or route to residue. |
| R2-14 | AGENT | `dest_ch.rs:1717-1739,2101,2116` | CH INSERTs acknowledged before fsync; slot confirmed on that ack → host power loss = lost window. `fsync_after_insert=1, fsync_part_directory=1` on owned inserts, or document. |
| R2-15 | PLAUSIBLE | `dest_ch.rs:2202` | ClickHouse Cloud `Shared*` allowed but no `select_sequential_consistency` on key-table / lease / readback → stale reads across replicas. Set it or refuse Shared*. |
| R2-16 | AGENT | `mybinlog.rs:412-419,417` | Zero TIMESTAMP → `1970-01-01` (bulk relays zero literal → silent rewrite on any UPDATE). TIMESTAMP2 read as `i32` (MariaDB 11.5+ post-2038 → 1901). |
| R2-17 | AGENT | `mysource.rs:976-986,561,259-282` | `ROLLBACK` in mixed-engine tx classed `Other` → persisted MyISAM rows dropped; `ROLLBACK TO SAVEPOINT` ignored. `binlog_checksum=NONE` not detected (4 bytes cut per event → loud failure). |
| R2-18 | READ | `dest_bq.rs:1264,1270,1291` | Column name pasted inside `ERROR(FORMAT('… {name}'))` string literal; `bq_ident` allows `'` and `%` → literal break-out on user-created target tables. `sql_str` + double `%`. |
| R2-19 | AGENT | `sink/mysql.rs:332-356`, `naming.rs:720-724`, `dispatch.rs:596-608` | MySQL `lower_case_table_names=1`: guard listing comes back lowercase, classify is case-sensitive → all peers `Foreign` (guard blind); `Events`/`events` in one multi-table run → second drops first's staging mid-load. Repro on an lctn=1 server first. |
| R2-20 | AGENT | `sink/postgres.rs:536`, `naming.rs:918-939` | PG destination names > 63 bytes truncated by the server but hashed in full by the guard → two names, one table, no mutual exclusion. Refuse > `PG_IDENT_MAX`. |
| R2-21 | AGENT | `dest_my.rs:713-720`, `mysql.rs:1201,1317` | MySQL twin of P1-7: CDC state under `db.orders`, bulk replace deletes only bare `orders` → stale CDC watermark survives a replace. Delete both spellings. |
| R2-22 | AGENT | `sink/iceberg.rs:1141` | Decimal cursor (MySQL BIGINT UNSIGNED) watermark compared as text ("999" > "1000") → re-appends each run (narrow path: empty table bootstrap). |
| R2-23 | AGENT | `sink/iceberg.rs:1328,2023` | Replace / CDC TRUNCATE never mark old files DELETED → `expire_snapshots` never frees them (50 GB/day leak on daily replace). |
| R2-24 | AGENT | `sink/iceberg.rs:2023-2050`, `dest_ice.rs:122,508` | CDC metadata grows O(windows): manifests never merged, every idle table gets a props-only commit per window → planning time/OOM for readers after weeks. |
| R2-25 | AGENT | `sink/iceberg.rs:844,302-309` | v3 tables not refused; `create_table` doesn't send `format-version: 2`. |
| R2-26 | AGENT | BQ L1–L5 | `sql_str` doesn't escape newline (text PK with `\n` wedges a changelog drain); resumable upload ignores 308 `Range`; token exchange no retry + `iat` skew; fence leak if a run dies before first announce; numeric PK scale-only change → MERGE "at most one source row". |
| R2-27 | AGENT | `source/csvfile.rs:110-117,144-150` | Unbalanced quote → one field grows to file size (no record cap → OOM in the cage); UTF-8 BOM ends up in the first column name. |
| R2-28 | AGENT | `source/github.rs:297-310` | Git LFS pointer files loaded as data (run "succeeds" with the pointer text). |
| R2-29 | AGENT | `gsheets.rs:76,207-221` | Token never refreshed (1 h) and no 429 retry; dotted tab names become schema qualifiers. Sheet/CSV headers not sanitized (control chars/`\`) → feeds identifier quoting. |
| R2-30 | AGENT | `progress.rs:180-182,236-238,326,417` | JSON progress escapes only `"` (backslash/newline/NaN → invalid JSON); concurrent runs → `rows - last.0` wraps. |

---

## 4. Claims attacked and still standing (don't re-review)

- **PG slot confirmation** never passes applied: `applied_tx.send` after every member's
  close (`run.rs:2108`), keepalives send `*applied.borrow()` (`drain.rs:298`), a dead apply
  confirms nothing (`run.rs:1775-1776,1806-1809`).
- **Fences**: PG/MySQL lease row `FOR UPDATE` in the apply tx with the watermark;
  renewal uses `clock_timestamp()`; CH predicate evaluated server-side with `now64` and the
  pinned deadline, state batch `written_rows == marks`; BQ fence UPDATE + `@@row_count`
  RAISE inside the tx; CH `seq` bias makes claim/close outrank in-flight renewals.
- **Stream abort** (whole tx and subtransaction) drops exactly the right ops
  (`drain.rs:418-457`); 2PC not requested; XA_PREPARE refused loudly.
- **Collapse**: NULL key, REPLICA IDENTITY NOTHING, USING INDEX without key all refused;
  TRUNCATE mid-window resets; non-deferred A→B→A through a third value correct.
- **Time**: walsender pins `TimeZone=UTC`/`DateStyle=ISO`; MySQL TIMESTAMP is epoch.
- **BigQuery**: job ids random 128-bit per attempt chain, 409 adopted only on byte-identical
  query; fenced scripts BEGIN/COMMIT; groups never split; int64 as STRING in NDJSON;
  masked readback is the exact inverse of the cast.
- **Iceberg**: sequence numbers (data and eq-delete share `next_sequence_number()`, no
  off-by-one); snapshot ids random + checked against every `tried`; UuidMatch +
  RefSnapshotIdMatch; field ids from the table; **no column bounds written → no pruning
  risk**; timestamp micros/UTC flags consistent; merge-key NULLs refused.
- **Lease/naming/pipe**: `artifact_ident`/`artifact_match` char-boundary safe and fit
  63/64 bytes; exact-length anchor; tokens one-directional; `pipe.rs` latch + `join_all`
  cannot deadlock on a sink error; memory pricing saturating; multi-table failure isolation.
- **Python boundary**: capsule ownership/release, end-of-stream idempotent, i32 offset
  overflow caught at seal, pyfunction panics → `PanicException`, no nested-runtime panic
  under asyncio, one-line API holds; GitHub token pinned to api.github.com; Sheets host fixed.

---

## 5. VPS repro log

Run on the QA mirror at `597f8e0` (**before** the fix commits) — so these prove the
original defects; the fixed items are re-checked by §1, not here. Scratch tests ran in
debug and `--release`; originals of the edited mirror files are in `~/qa-r2-orig/` on the
VPS. All QA tables dropped. Baseline `cargo test -p apitap-core --lib` at `597f8e0`:
**452 passed, EXIT=0** (`~/qa-test-1015.log`).

| Item | Result | Evidence (release unless noted) |
|---|---|---|
| P0-6 numeric | **REPRO** panic | ndigits=0, dscale=200, scale=4 → `pgcopy.rs:348:37: attempt to divide by zero`; dscale=136 with a real digit → same. Shifts 39–127 returned `Ok(0)` (correct by luck) — the "garbage value" half is **not** shown. **Fixed in `5aff53a`.** |
| P1-6 spans | **REPRO, worse than reported** | `spans(-1|0, i64::MAX, 4)` → every span `rlo > rhi` → 0 rows "success". `spans(i64::MIN, i64::MAX, 4)` → **4 copies of the full range → every row read 4×** (step wraps to 0). Debug: `source/mod.rs:97:17 subtract with overflow`. **Still open at `a0312d6`.** Test all three cases. |
| P1-11 truncate | **REPRO** via real `S3Conn::check` | local 403 with 399×`a` + `é…` → `s3.rs:399:21: assertion failed: self.is_char_boundary(new_len)`. Fixed in `57bba12` (sibling in `dest_bq.rs:2226` still open, §1). |
| P1-9 handshake | **REPRO** ×2 | 21-byte packet → `mywire.rs:325:52 index out of bounds: len 21 index 21`; 23-byte PLUGIN_AUTH w/o SECURE_CONNECTION → `mywire.rs:339:24 range start 33 out of range for len 23`. Fixed in `835b30d`. |
| P1-10 rowbinary | **REPRO** ×3 | `rb_type_from_ch("Decimal(76, 10)")` (CH's own spelling of Decimal256) → width 16; 300000.0000 into Decimal(9,4) → `wrote i32=-1294967296 (want 3000000000)` silently; Bool on `&[]` → `rowbinary.rs:370:34 index out of bounds`. Fixed in `f6cbb31`. |
| P1-5 MySQL UNSIGNED PK | **REPRO, worse than reported** | MySQL 8.0.46, `id INT UNSIGNED AUTO_INCREMENT PK`, 1000 rows, wheel 0.57.0. `parallel=1` transfer OK (1000 rows in pg). `parallel=4` → `min/max of cursor id: … Option<i64> (as SQL type BIGINT) is not compatible with SQL type INT UNSIGNED`. **`apitap.read(S, table=…)` fails the same way with NO `parallel` argument** → every `read()` of a Laravel `increments` table is broken. Same decode still at `source/mysql.rs:1089`. **Still open → promote to P0.** |
| P1-12 CH count vs stream | **REPRO** (mechanism) | ch→ch from a view whose row count changes per query: 3/3 runs `span returned 478 rows, count() says 528 — refusing to swap a short read in`. Proves no shared snapshot; a real live-insert / LB-replica repro was impractical in the 1000-row budget. **Still open.** |
| P0-4, P1-13, P2-8 | not attempted | P0-4 already fixed (`e5e1f49`); others non-trivial. |

Also verified on the rig during round 2 (§2 R2-8): polars 1.43.2 `Float32(0.1)==0.1` keeps
the row while PG `0.1::real = 0.1` is `f`; polars `(flags & 4)==4` → 2 rows; polars
`-3 % 2 = 1` vs PG `-1`.

---

## 6. Order of work

1. **Fix the fixes first** (they shipped to `main`): §1.1 unix-socket/`?host=`/PGSSLMODE
   regression + docs + walsender default; §1.2 Iceberg ambiguity flag; P0-7 refuse the
   flag on windows with residue/truncate and say so loudly; P1-11 sibling `dest_bq.rs:2226`;
   P0-6 dscale=200 test; call-site tests for both TLS helpers.
2. Handoff **P1-5 (now P0: every MySQL `read()` of an UNSIGNED-PK table fails, REPRO)** and
   **P1-6 (now P0: full-range ids read every row 4×, REPRO)** — still open at `a0312d6`.
3. R2-1 (MySQL schema-by-position) and R2-2 (first-run fan-in) — silent data corruption.
4. R2-8 (pushdown) — three one-line refusals in Python, with polars tests.
5. R2-3, R2-4, R2-5, R2-6, R2-7, R2-9, R2-10, R2-11, handoff P1-12.
6. Then the handoff's remaining P1/P2, pass-2 T-items, and §3 here.

Gate (`benchmarks/gate.py`) on the wheel you ship before calling any of it production.
