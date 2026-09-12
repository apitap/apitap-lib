# The two lenses that never ran — findings, 2026-09-12 (v0.55.1)

Run after 0.55.1 shipped, as four narrow readers (the two original lenses each
split in half, which is why they completed this time — see
[[workflow-verifier-budget]] in memory). 16 agents, 0 errors. Every
blocker/high below carried a skeptic verdict of real=true and **zero** were
refuted, which is itself a warning: a panel that confirms everything is not
obviously calibrated. Treat `high` as unverified-in-practice until reproduced.

I independently re-read and confirmed the two blockers verbatim; the rest carry
the agents' citations only.

---

## Status, 2026-09-12 — both blockers CLOSED, reproduced live first

Both were reproduced against the released 0.55.1 wheel before a line was
changed, and both e2e legs go red without the fix.

* **MySQL/MariaDB TRUNCATE.** Fixed in `mysource.rs`: a `TRUNCATE` QUERY event
  is its own commit boundary (DDL auto-commits and never reaches an XID), so the
  buffered transaction is drained, the wipe recorded, and the buffer drained
  again. Two traps found on the way, both caught by the e2e and not by the unit
  tests:
  - the first guard asked `collapsers`/`key_idx`, which are filled by the FIRST
    ROWS event — a window shaped `TRUNCATE t; INSERT …` reaches the truncate
    before either exists, so it was dropped exactly as before while 270 unit
    tests passed. `sess.tracked` is the authority.
  - the obvious repair — create the accumulator on the spot — is silent data
    loss: `Collapser::new(vec![])` hashes every later row to the same empty key,
    so the two inserts after the wipe collapse into one. The truncate is HELD
    instead, and applied to the accumulator the first rows event builds with the
    real key layout. `an_empty_key_collapser_would_fold_every_row_into_one`
    pins that.
  Verified: `benchmarks/e2e_mariadb_cdc.py` — `truncate replicated: 3 → 2`,
  where 0.55.1 gave `dest was 3, is 5`.

* **ClickHouse changelog replay.** The stamp is now `outcome.start_lsn` — the
  watermark the window was drained FROM, the one position a re-drain reproduces
  — instead of `end_lsn`, which every re-drain recomputes. `(lsn, seq)` is a
  real event identity now. On top of that, `_apitap_cdc_pending` records the
  window about to be appended, so a replay counts what is already there and
  skips it: the ordinary replay appends **nothing**, rather than appending a
  duplicate that is merely identifiable. The same stamp fix landed on BigQuery,
  together with the `high` finding below it — the statement batcher now packs
  whole `INSERT`+watermark PAIRS, so a size-driven chunk boundary can no longer
  split them into two transactions.

  Two things the lens did not predict, both found by the new e2e leg:
  - the prefix probe must exclude baseline rows. The bootstrap stamps them with
    its consistent point and the first window starts at exactly that point, so
    counting them in made an intact prefix look torn.
  - for the same reason a baseline row and a window's first event can now share
    `(lsn, seq)`. `__current` breaks the tie in favour of the change, on both
    engines. Without that the winner was whichever row the engine returned
    first — and it returned the right one on the run that measured it, which is
    why the e2e asserts the view's ORDER BY and not just the outcome.

  Verified: `benchmarks/e2e_changelog_replay.py` (new, in the gate). It applies
  a window and then rewinds the destination watermark to where the window began
  — the exact state a crash between the INSERT and `write_state` leaves — and
  drains again. 0.55.1: `log rows 10 -> 17`, ids 4 and 5 each holding two `I`
  records. Now: `10 -> 10`.

**Also closed in the same pass, from the older brief's §4 and A2:** the seven
sinks ANNOUNCE before they scan (`Artifact::Lock`), which closes the
start-instant window for the bulk lane, and the CDC drain now writes and reads
the SAME artifact — so the matrix's `log_based | anything | refused` row is true
for the first time, on Postgres, MySQL, ClickHouse and BigQuery destinations.

Three things that cost a measurement:

* a run refused by its own scan leaked its lock and poisoned the next run, which
  `e2e_failure_modes.py` leg 1 caught. `prepare` is the one step outside the
  error arm that covers everything else, so `pipeline::run` releases it
  (`Sink::release_lock`).
* a CDC BOOTSTRAP re-enters `transfer(mode="replace")`, which announces a *swap*
  lock — and `peer_blocks(swap, cdc)` would have it refuse the drain that
  started it. The drain hands the guard over before the load and the bulk lock
  covers it; the trade is written down in `docs/failure-modes.md`.
* Iceberg's drain is NOT guarded: a claim there lives in object storage and the
  CDC lane holds only a catalog connection. Its bootstrap rides the bulk sink,
  so the expensive half is covered. Stated in the README and failure-modes.

**The cost, and the follow-up it names.** A hard-killed drain now leaves its
lock, and the next run of that table is refused until someone drops it. That is
the same contract a killed bulk run has always had for its staging table, and it
comes from the same rule — nothing is collected on a guess. What is genuinely
new is that before 0.56.0 a killed drain left NOTHING and the next scheduled run
simply resumed, so for unattended CDC this converts a self-healing failure into a
manual one. Written down in `docs/failure-modes.md` in both the table and the
section, and in the README.

The clean fix is the one `naming::classify` already names: a liveness signal from
the engine instead of a timestamp — a lease the run renews, or an object whose
mtime advances while it writes. Two designs were considered and rejected here,
and the reasons are worth keeping:

* *Collect a lock whose replication slot is inactive.* Real liveness, and
  available: `run_group` holds the source pool and the slot name. But a drain
  that has announced and not yet attached its walsender reads as inactive, so a
  live peer could be collected — which reintroduces exactly the start-instant
  window the announcement closes.
* *A deterministic per-pipeline token, so a re-run recognises its predecessor's
  lock as its own.* Self-healing, and wrong: two CONCURRENT drains of one
  pipeline would recognise each other the same way and both proceed.

Verified: `benchmarks/e2e_cdc_guard.py` (new, in the gate). It plants a
live-looking peer of each kind and asks for the refusal, in both directions,
with two controls — a clean drain still works, and a prefix-sharing sibling's
lock is none of this table's business. Against the 0.56.0 build that had the
bulk half only, the three CDC-side assertions read `it was ALLOWED`.

| sev | finding | where |
|---|---|---|
| blocker | ClickHouse changelog: the window INSERT and its watermark are two round-trips, and replay re-appends every event under a DIFFERENT _apitap_lsn | `dest_ch.rs:651` |
| blocker | A TRUNCATE at a MySQL/MariaDB source is decoded, recognised as DDL, and then thrown away — the destination keeps every row the source dropped | `mysource.rs:644` |
| high | run_workers detaches every sibling worker on the first error — they keep writing into staging that discard() is deleting | `postgres.rs:510` |
| high | Every source ends its send path with `loader.send(...)?` — the Loader is dropped instead of aborted, so the sink commits or orphans the partial stream | `postgres.rs:919` |
| high | Iceberg merge retains every merge-key value for the whole run — memory grows with ROW COUNT, then is copied again at commit | `bqparquet.rs:322` |
| high | The parquet lane's per-pipe residency is a fixed 24 MiB row group, so the chunk-proportional pipe cap under-counts it — and the auto thin-chunk lever raises real memory while the model believes it lowers it | `mod.rs:265` |
| high | BigQuery changelog: the group chunker can split a table's INSERT from its watermark into two transactions, and cdc_script can resubmit a committed one | `dest_bq.rs:427` |
| high | MySQL destination hides another mode's state row behind `AND mode = 'log_based'`, so a CDC run silently full-replaces a table the cursor lane owns | `dest_my.rs:89` |
| high | changelog: a PK-changing UPDATE whose TOAST column is untouched can never be resolved, so the window dies with a 'torn window' error that re-bootstrapping does not fix | `changelog.rs:190` |
| high | Collapsed::deletes is documented as dedup'd but pushes the same key twice on INSERT/DELETE/INSERT/DELETE, which BigQuery's MERGE rejects | `collapse.rs:272` |
| high | dest_bq: the `landed` guard skips only non-Gone keys, so a key that is both a residue Delete/Rekey-old and a set-phase delete is staged as two 'D' rows | `dest_bq.rs:818` |
| high | changelog: `masked` is never set from a DELETE's or an UPDATE's OLD image, so unchanged-TOAST cells there bypass resolution and are written as NULL | `changelog.rs:94` |
| medium | The S3 multipart-abort sweep is dead code: `reapable` returns false for every classification | `s3.rs:771` |
| medium | Loaders already opened are dropped, not aborted, when a later sink.loader() fails | `mod.rs:464` |
| medium | The pinned-snapshot connection goes back to the sqlx pool inside an open REPEATABLE READ transaction on every error path | `postgres.rs:847` |
| medium | GcsLoader::finish leaks the resumable session when its final PUT fails | `gcs.rs:960` |
| medium | read().to_polars() sets batch_bytes to usize::MAX>>1, so the read residency model is bypassed and each worker holds its whole span set in RAM | `read_impl.rs:286` |
| medium | BatchBuilder's anti-preallocation clamp is per COLUMN, so a materialize read reserves ncols x 32 MiB per worker before the first row | `arrowcol.rs:382` |
| medium | APITAP_MEM_BUDGET silently ignores any value it cannot parse and falls back to the cgroup number it exists to override | `mod.rs:98` |
| medium | When cgroup detection finds nothing — no container, a non-Linux host, or a mount layout the walk cannot resolve — the pipe count is not capped at all | `mod.rs:256` |
| medium | MySQL source-identity marker is stamped only after a fully successful run, so a run that fails mid-drain leaves the table adoptable by a different server | `run.rs:1029` |
| low | MySQL sink registers the infile receiver before the two fallible calls that follow it | `mysql.rs:741` |
| low | BqLoader::finish detaches the remaining job-poll tasks on the first failing job | `bigquery.rs:1447` |
| low | Iceberg builds a fresh reqwest client and connection pool per table (twice per table in incremental mode) | `iceberg.rs:1046` |
| low | The Arrow batch seal gate counts data and offsets but not validity bitmaps, so a NULL-bearing batch overshoots its byte threshold | `arrowcol.rs:199` |

---

## [blocker] ClickHouse changelog: the window INSERT and its watermark are two round-trips, and replay re-appends every event under a DIFFERENT _apitap_lsn

**Where:** `crates/apitap-core/src/logbased/dest_ch.rs:651`

**Claim:** `apply_changelog` appends the window's events with `insert_stream` and then writes the watermark in a separate HTTP statement — `self.write_state(dest_table, source_id, outcome.end_lsn, c.count).await?;` (:651) directly after the `.await?` on the INSERT (:645-650). The module's replay argument for this gap is stated at :491-495: "Replay is safe because a re-drained window re-appends rows carrying the SAME `(lsn, seq)`, and `__current` picks one of them; the duplicate is inert." That premise is false. The stamp is the WINDOW BOUNDARY, not the event's own position: `let lsn = outcome.end_lsn;` (:610) and `seq` is the index within the window (`for (seq, ev) in c.events.iter().enumerate()`, :618). A re-drain computes a new boundary — `drain` breaks either at `if e >= stop_line` (drain.rs:414) with a stop_line re-read as `pg_current_wal_lsn()` per run (run.rs:1243-1247), or at the caught-up keepalive where `if wal_end > end_lsn { end_lsn = wal_end; }` (drain.rs:395-397). Both move between runs, so the replayed events are appended a second time with a different `_apitap_lsn` and a re-based `seq`, into a plain `MergeTree` (`CREATE TABLE {tmp} ENGINE = MergeTree PARTITION BY {part} ORDER BY ({order})`, :325) that de-duplicates nothing.

**Impact:** Two ordinary failure paths duplicate the audit trail silently, and the next run reports success over it. (a) Process death between the INSERT and `write_state`. (b) No crash at all: in a group, run.rs applies members serially inside one window (`for (i, (dest_table, qualified, pk_cols, source_id)) in actxs.iter().enumerate()`, run.rs:1325-1329); if member B's apply fails after member A's changelog INSERT and watermark landed, the run errors, the next run drains from the group MINIMUM (run.rs:812) and re-applies A's whole window. run.rs's own header calls this safe because "the apply paths are idempotent" — true for the replica shape, false for an append-only log. `<table>__current` still converges (later lsn wins), so nothing is visibly broken; the raw changelog — which is the entire product of changelog=True — permanently holds each event two or more times, under stamps that make (lsn, seq) useless as a de-duplication key. Anyone counting operations, computing per-window deltas, or driving downstream from the log double-counts.

**How to confirm:** On the QA rig (VPS): run a changelog=True pg→ClickHouse group of two tables, force member B's apply to fail after A's window lands (e.g. drop B's destination table mid-run, or kill the process between the insert_stream and write_state HTTP calls with APITAP_DEBUG on to see `window applied → watermark`). Re-run, then `SELECT _apitap_lsn, _apitap_seq, count() FROM t GROUP BY 1,2 HAVING count() > 1` and, more tellingly, `SELECT id, count() FROM t WHERE _apitap_op='I' GROUP BY id HAVING count() > 1` — the second query shows the same source INSERT recorded twice under two different _apitap_lsn values. Cannot be confirmed from the MacBook: needs a build and a live ClickHouse.

## [blocker] A TRUNCATE at a MySQL/MariaDB source is decoded, recognised as DDL, and then thrown away — the destination keeps every row the source dropped

**Where:** `crates/apitap-core/src/logbased/mysource.rs:644`

**Claim:** MySQL and MariaDB write TRUNCATE TABLE into the binlog as a QUERY event, not as a rows event. The reader parses that event, matches it with `is_ddl`, uses it only to drop cached schemas, and continues — it never calls `Collapser::truncate()` or `Changes::truncate()`. The collapsed window for a MySQL source therefore never carries `truncate: true`, so no destination ever empties the table, and the run reports success.

**Impact:** Silent, permanent divergence on every MySQL/MariaDB CDC pipeline whose source truncates a tracked table. Scenario (d): source does TRUNCATE t; INSERT 3 rows. Destination ends with (all old rows) + (3 new rows); source holds 3. The window is applied, the watermark advances, the run is green, and nothing in the output mentions the truncate — so the divergence is never noticed and never repaired. Affects both replica (`Collapsed::truncate`) and changelog (`ChangeOp::Truncate` records are never emitted, so `<table>__current`'s truncate cutoff at dest_ch.rs:415-419 never fires either).

**How to confirm:** On the VPS QA rig: MySQL source table with 1000 rows already replicated; run a window containing `TRUNCATE TABLE t; INSERT INTO t VALUES (...x3);` then drain+apply and ask the DESTINATION for `SELECT count(*)` — expect 3, predict 1003. Cannot be confirmed from the MacBook (read-only, no cargo/docker here). A unit-level check is also possible: feed a QUERY event body carrying "TRUNCATE TABLE t" through the mysource loop and assert `Collapsed::truncate` is true.

## [high] run_workers detaches every sibling worker on the first error — they keep writing into staging that discard() is deleting

**Where:** `crates/apitap-core/src/source/postgres.rs:510`

**Claim:** All four run_workers join loops are `for t in tasks { rows += t.await.map_err(...)??; }`. The `??` returns on the first failing worker, and the remaining `tokio::task::JoinHandle`s are dropped by the iterator — a dropped JoinHandle DETACHES the task, it does not abort it. Nothing else holds a handle, so the surviving workers run to completion outside the run's control, each still owning a source connection, a Loader (its own sink stream), and its chunk buffers.

**Impact:** discard() and the detached workers race, and the race is silent. S3/GCS/Iceberg: discard()/list-and-delete (s3.rs:900-911, gcs.rs:777-788, iceberg.rs:1095-1105) snapshots what exists now; a detached worker then completes its multipart upload and lands new part objects under this run's staging segment AFTER the sweep — leftovers carrying a foreign run token, which is exactly what the next run's reap_and_check_peers refuses with `locked:` (s3.rs:793-806). BigQuery: seal_file registers the worker staging table lazily (bigquery.rs:1325-1331), so a detached worker registers after discard() cloned the registry (bigquery.rs:2150) and its staging table survives the run. Postgres: `DROP TABLE IF EXISTS {staging}` (sink/postgres.rs:751) blocks behind the COPY the detached worker still holds, so the failing run hangs in cleanup instead of failing fast. In run_many the detached workers' connections and ~8×chunk of buffers outlive the Grant permits released when the table's future resolves (pipeline/mod.rs:600-593 doc: "peak memory is the single-table ceiling"), so one table's error breaks the memory ceiling for every sibling still running.

**How to confirm:** On the VPS QA rig: multi-table pg→s3 (or pg→pg) with ≥4 pipes, kill one span mid-flight (statement_timeout on one span, or drop one source connection). Assert with the SERVER, not the exit code: after transfer() returns Err, list the s3 staging prefix (or `SELECT relname FROM pg_class` for the pg staging name) 30s later and check for objects/tables created after the discard, and confirm the process still has the worker's source connections open (`SELECT count(*) FROM pg_stat_activity WHERE application_name='apitap'`). Fix shape: keep the handles, `abort()` (or await) the rest before returning.

## [high] Every source ends its send path with `loader.send(...)?` — the Loader is dropped instead of aborted, so the sink commits or orphans the partial stream

**Where:** `crates/apitap-core/src/source/postgres.rs:919`

**Claim:** The Loader contract says a source-side failure must go through `abort`: "Source-side failure: make the sink DISCARD the partial stream (a clean close could commit it)" (sink/mod.rs:62-64). Every other error arm in every worker honours it (`return Err(loader.abort(e).await)`), but the send calls themselves use a bare `?`, which drops the loader unaborted.

**Impact:** A send failure (sink refuses a buffer, HTTP 5xx on a part upload, LOAD DATA channel closed) leaves: a committed partial COPY in Postgres staging that only discard() saves; an S3/Iceberg multipart upload with all its parts stored and billed, invisible to discard() (which lists OBJECTS — an incomplete upload has none) and unreachable by the reap sweep (see the S3 reap finding); a GCS resumable session that nothing in the codebase ever cancels. One orphan per failed pipe, per table, forever.

**How to confirm:** Unit-level: a Loader stub whose send() returns Err after N buffers, driven through copy_out_worker, asserting abort() was called. Live: pg→s3 against MinIO with the part upload forced to 500 on the second part, then `aws s3api list-multipart-uploads --bucket ... --prefix <staging>` after the run returns — an upload id still listed is the leak.

## [high] Iceberg merge retains every merge-key value for the whole run — memory grows with ROW COUNT, then is copied again at commit

**Where:** `crates/apitap-core/src/wire/bqparquet.rs:322`

**Claim:** ParquetEncoder::keys accumulates one entry per row for the merge-key column and is never flushed, trimmed or bounded. flush_row_group() clears the column buffers and the def levels but not keys, so the 24 MiB row-group cap does not apply to it; at loader finish the whole vector is moved into the sink's done-list and survives until the commit, where every pipe's vector is concatenated into one more full-size vector.

**Impact:** On mode="merge" into Iceberg, peak memory carries a term proportional to the number of DELTA ROWS, not to parallel x chunk_bytes: ~8 B/row for an integer key, ~60 B/row for a uuid/text key (24 B String header + 36 B heap), summed across pipes, plus a second full copy of the same data at commit (and then the serialized delete parquet). A 20M-row incremental merge costs ~160 MB + 160 MB for int keys and ~1.2 GB + 1.2 GB for uuid keys — an unconditional OOM-kill in the 256 MB tier the product is benchmarked in, with no knob (chunk_bytes and parallel do not touch it). The engine's own comment calls it "Delta-proportional memory", which is exactly the property the headline denies, and the delta is unbounded (a backfill or a long-stopped pipeline).

**How to confirm:** On the VPS QA rig: seed a pg table with a uuid PK, land it into an Iceberg destination with mode="merge", then push a delta of ~5M rows and re-run the merge inside a 256 MB cage; watch cgroup memory.peak. Expect the peak to scale linearly with the delta row count and to be ~60 B/row above the same run with an integer key, while the same delta into a non-Iceberg sink stays flat.

## [high] The parquet lane's per-pipe residency is a fixed 24 MiB row group, so the chunk-proportional pipe cap under-counts it — and the auto thin-chunk lever raises real memory while the model believes it lowers it

**Where:** `crates/apitap-core/src/pipeline/mod.rs:265`

**Claim:** mem_capped models every pipe as costing 10 x chunk_bytes, but a ParquetEncoder pipe (GCS/S3/Iceberg/BigQuery parquet lanes) holds up to ROW_GROUP_BYTES = 24 MiB of decoded column buffers plus the writer and output buffer, a cost that does not shrink with chunk_bytes. Shrinking the chunk therefore buys pipes that each still cost ~24 MiB, and knobs() does exactly that automatically.

**Impact:** Peak on the parquet routes is ~parallel x 25 MiB + reserve, not parallel x 10 x chunk_bytes. Two concrete failures: (1) a 128 MB cage, pg→gcs (TO_GCS auto ask = (cores*2).clamp(2,8)) with no explicit chunk takes the thin branch — 2 pipes at 4 MiB become 4 pipes at 2 MiB, so the real row-group residency DOUBLES from ~50 MB to ~100 MB inside a 128 MB cap while the model records it as falling from 80 MB to 80 MB; (2) an explicit chunk_bytes=65536 (the documented floor, and what usage.md:1387 advises for size-capped destinations) makes allowed = (mem-40MiB)/640KiB — 137 pipes at 128 MB — so the CPU ask of 8 passes through unreduced and 8 x ~25 MiB = 200 MB of row-group builders land in a 128 MB container. Both end in a cgroup SIGKILL mid-run, and docs/usage.md:173-180 promises the opposite ("Each pipe budgets ~10 x chunk_bytes ... Memory use scales with parallel x chunk_bytes, never with table size").

**How to confirm:** On the VPS, run pg→gcs (format=parquet) on a 10M-row table in a 128 MB cage twice: once with default knobs (should pick 2 MiB x 4 after the thin lever) and once with APITAP-pinned chunk_bytes=4MiB (2 pipes), and compare cgroup memory.peak — the thin run should peak HIGHER. Then repeat with chunk_bytes=65536 and expect 8 pipes and an OOM-kill. A cheap unit-level proxy: assert mem_capped(8, 64*1024, 128<<20) == 8 while the parquet lane needs 8 x 24 MiB.

## [high] BigQuery changelog: the group chunker can split a table's INSERT from its watermark into two transactions, and cdc_script can resubmit a committed one

**Where:** `crates/apitap-core/src/logbased/dest_bq.rs:427`

**Claim:** `apply_group_changelog` documents its replay story as "the INSERT and the window's watermark row commit inside ONE transaction, so a window either landed whole or not at all" (:396-397). The code does not guarantee that. `stage_changelog` returns the pair as two statements — `vec![format!("INSERT INTO {t} ({into}) SELECT {sel} FROM {s};" …), format!("{};", state_sql(c.count))]` (:590-597) — and the committer chunks the flattened list by byte size: `if len + s.len() > CHUNK_BYTES && !batch.is_empty() { self.commit_batch(&batch).await?; … }` (:427-431), with `const CHUNK_BYTES: usize = 256 << 10;` (:424). The chunk boundary is size-driven, not pair-aware, so it can fall BETWEEN a table's INSERT and its own state row; `commit_batch` then wraps each side in its own `BEGIN TRANSACTION; … COMMIT TRANSACTION;` (:736-741). Separately, `cdc_script` retries the whole script text on a retryable message — `Err(Error::Transfer(m)) if attempt < 5 && retryable(&m)` (sink/bigquery.rs:702), where `retryable` matches "aborted", "backenderror", "internal error", "500 "/"502 "/"503 "/"504 " (sink/bigquery.rs:885-898) — and the job carries no client-supplied deterministic jobId (`cdc_script_once` posts `{"configuration": {"query": …}}` and reads the SERVER's `jobReference.jobId`, sink/bigquery.rs:712-731). A 503 on the `poll_job` GET after the transaction already committed resubmits the same INSERT.

**Impact:** Same silent duplication as the ClickHouse changelog, reached without any crash. A wide group (the fan-out this batching exists for) easily exceeds 256 KB of statement text — a 50-column table's `INSERT … SELECT` carries one cast expression per column, so ~30 such tables cross the threshold — and each boundary that lands between an INSERT and its state row makes that table's window non-atomic: the log rows commit, the watermark commits in the NEXT transaction, and any failure in between replays the window (with a different `outcome.end_lsn` stamp, `format!("CAST({} AS INT64)", outcome.end_lsn)`, :580) into an append-only table. The retry path duplicates even a correctly-paired chunk. The replica/MERGE path survives both mechanisms (key-idempotent MERGE, WRITE_TRUNCATE staging) — the comment at :692-694 that justifies chunking, "the window is replay-idempotent, so a crash between chunks converges", was written for that path and was carried over to the changelog path where it does not hold.

**How to confirm:** Needs a billing-enabled BigQuery project (CDC uses DML). On the VPS QA rig: run changelog=True with a group wide enough that the assembled statements exceed 256 KB (or temporarily lower CHUNK_BYTES in a scratch build) and log the chunk boundaries; confirm a table whose INSERT and state row land in different jobs, then kill the run between them and re-run. Query `SELECT _apitap_op, _apitap_lsn, COUNT(*) FROM t GROUP BY 1,2` and compare the per-event totals against the source. The retry half can be shown without BigQuery by unit-testing that `retryable("503 Service Unavailable")` is true while the script it guards is an INSERT.

## [high] MySQL destination hides another mode's state row behind `AND mode = 'log_based'`, so a CDC run silently full-replaces a table the cursor lane owns

**Where:** `crates/apitap-core/src/logbased/dest_my.rs:89`

**Claim:** `MyDest::read_state` filters the state row by mode: `"SELECT watermark, cursor_col FROM {} WHERE dest_table = ? AND source_id = ? AND mode = 'log_based'"` (:88-91). A row written by the bulk/cursor lane under the SAME key is therefore invisible and the function returns `Ok(None)` — not the loud refusal the other four destinations give. The bulk MySQL sink writes exactly that key: `INSERT INTO {state} (dest_table, source_id, cursor_col, watermark, mode, …) VALUES ('{dt}','{sid}',…,'{md}',…)` with `dt = sql_lit(&self.bare)` and `md = self.mode_str` (sink/mysql.rs:516-527), and its own read carries no mode filter (sink/mysql.rs:923). The keys collide for a Postgres source because both lanes derive `source_id` from the same `pipeline::source_identity` (run.rs:735 `source_id: crate::pipeline::source_identity(src_url, t)`). The Postgres destination removed this exact filter and documents why: "It used to be `AND mode = 'log_based'`, which reads like a safety check and is the opposite: a row written by the cursor lane was simply invisible, so a table that had been append-ed and was then pointed at log_based saw NO state, decided it was a fresh destination, and quietly ran a full bootstrap — discarding the incremental history … Read the row whatever wrote it, and refuse below if it is not ours; that is the same shape the bulk lane's read now has, and the two directions have to match or the guard only works when you approach it from one side" (dest_pg.rs:155-165). ClickHouse (dest_ch.rs:201-208), BigQuery (dest_bq.rs:94-100) and Iceberg (sink/iceberg.rs:1541-1546) all read the row unfiltered and then refuse. MySQL is the one destination still on the pre-fix shape.

**Impact:** Point an existing MySQL destination table that mode='append'/'incremental' has been maintaining at mode='log_based' (same source, same table) and the run reads NO state, takes the fresh-bootstrap branch (`if have.is_empty() { bootstrap_group(…) }`, run.rs:800-802), and runs the full load in `o2.mode = Mode::Replace` (run.rs:1097). Every row the append lane had accumulated that no longer exists at the source is destroyed, with no error and no prompt — while the identical mistake against a Postgres, ClickHouse, BigQuery or Iceberg destination is refused loudly with instructions. It also leaves the table with the cursor lane's history gone and a log_based watermark in its place, which the operator has no way to notice from the run's output.

**How to confirm:** On the QA rig: run `mode='append'` pg→mysql for a table, confirm `_apitap_state` holds a row with mode='append' for (bare, source_identity(...)); then run the same src/dest/table with `mode='log_based'` and observe that it bootstraps (full Replace) instead of erroring. Compare against the same two-step against a Postgres destination, which must fail with the "managed by mode='…'" message. Fix is one line: drop `AND mode = 'log_based'` from the SELECT and extend the existing cursor/mode check below to refuse a foreign row, matching dest_pg.rs.

## [high] changelog: a PK-changing UPDATE whose TOAST column is untouched can never be resolved, so the window dies with a 'torn window' error that re-bootstrapping does not fix

**Where:** `crates/apitap-core/src/logbased/changelog.rs:190`

**Claim:** `resolve_masked` looks the missing TOAST value up under the row's OWN key. For a PK-changing update the new image's key is the NEW key, but the value the source withheld lives at the destination under the OLD key. `mask_plan` only ever asks for the new key, `read_current` returns nothing for it, the in-window carry map has nothing for it either, and the function hard-errors. The replica path solves exactly this case with `ResidueOp::Rekey` (collapse.rs:181-208, which carries BOTH keys precisely so the old row can supply the value); the changelog accumulator has no equivalent.

**Impact:** Loud failure with no recovery on an ordinary source statement. `UPDATE t SET id = id + 1 WHERE id = 7` on a row with a >2KB text/bytea column that the statement does not touch aborts the whole window for every table in the run. The error's own remedy — clear _apitap_state and re-bootstrap — replays the same WAL and fails identically, so the pipeline is wedged until the source stops doing PK-changing updates on TOASTed rows. Scenario (c) on changelog=True.

**How to confirm:** VPS: pg source table (id int pk, title text, body text) with body >2KB so it is TOASTed; changelog=True to ClickHouse; bootstrap, then run `UPDATE t SET id=99, title='x' WHERE id=1` and drain. Predict Error::Transfer containing "the window is torn" naming column 'body'. Requires execution — cannot be run from the MacBook.

## [high] Collapsed::deletes is documented as dedup'd but pushes the same key twice on INSERT/DELETE/INSERT/DELETE, which BigQuery's MERGE rejects

**Where:** `crates/apitap-core/src/logbased/collapse.rs:272`

**Claim:** The `Slot::Upsert` arm of `delete()` pushes the key into `self.deletes` unconditionally. A key that cycles Delete -> Upsert -> Delete inside one window therefore lands in `deletes` twice, contradicting the field's stated contract. Every row-store destination streams the delete keys into a temp/key table where duplicates are inert, but BigQuery stages them as individual 'D' rows for a MERGE, and BigQuery refuses a MERGE in which two source rows match the same target row.

**Impact:** A window in which any pre-existing key is deleted, re-inserted and deleted again — a job/queue/session table reusing ids is the ordinary shape — hard-fails the BigQuery apply with 'UPDATE/MERGE must match at most one source row for each target row'. The watermark is not advanced, so every retry re-drains the same window and fails again: the pipeline stops until the operator manually intervenes. The dedup'd comment also means future consumers are entitled to assume uniqueness.

**How to confirm:** Pure unit check, no server needed, but must run on the VPS: add to collapse.rs tests — insert k, delete k, insert k, delete k, then `assert_eq!(out.deletes.len(), 1)`; predict 2. End-to-end confirmation of the BigQuery error needs the QA rig's BQ dataset.

## [high] dest_bq: the `landed` guard skips only non-Gone keys, so a key that is both a residue Delete/Rekey-old and a set-phase delete is staged as two 'D' rows

**Where:** `crates/apitap-core/src/logbased/dest_bq.rs:818`

**Claim:** `landed` is built by FILTERING OUT `Fin::Gone`, so it contains only keys that re-landed as upserts. But `resolve_window` already emits a 'D' staging row for every `Fin::Gone` key (dest_bq.rs:843-846). Any key that appears both as Gone in `finals` and in `c.deletes` is therefore pushed twice — independently of the collapse-side duplicate. The guard's own comment says this is exactly what must not happen.

**Impact:** Same terminal symptom as the previous finding — the BigQuery MERGE aborts with 'must match at most one source row for each target row' and the window can never be applied — but reached by a different, TOAST-flavoured route: delete a pre-existing key, re-insert it, issue an unchanged-TOAST update (key goes sticky), then delete it again. Or: delete key K, re-insert K, then UPDATE K -> K9 with an untouched TOAST column (Rekey makes K Gone while K is still in c.deletes). Both are legal source traffic.

**How to confirm:** Unit-testable on the VPS against resolve_window + the staging builder: construct a Collapsed with deletes=[k] and a residue ending in ResidueOp::Delete{k}, run the dest_bq staging loop, and count 'D' rows for k; predict 2, want 1. Fix direction: build `landed` from every key in `finals`, regardless of Fin variant.

## [high] changelog: `masked` is never set from a DELETE's or an UPDATE's OLD image, so unchanged-TOAST cells there bypass resolution and are written as NULL

**Where:** `crates/apitap-core/src/logbased/changelog.rs:94`

**Claim:** `Changes::delete` does not call `note_mask` at all, and `Changes::update` calls it only on the NEW image while pushing the OLD image as a `D` record. Under REPLICA IDENTITY FULL, Postgres does not detoast old tuples — an externally-stored unchanged column arrives as 'u' in the old image. If a window's only masked cells sit in old images, `self.masked` stays false, the destination skips `resolve_masked` entirely, and the renderer turns those cells into `\N`. The resolution machinery already handles these rows correctly (mask_plan and resolve_masked both iterate every event with a row) — only the flag that gates it is wrong.

**Impact:** Silent loss of source data into the destination on a green run: the `D` record of a REPLICA IDENTITY FULL table records NULL for every TOASTed column instead of the value the source shipped, with no error and no counter. The blast radius is bounded — `<table>__current` filters keys whose newest record is `D` (dest_ch.rs:400-402), so the reconstructed replica is unaffected — but the audit trail, which is the entire point of changelog=True, is wrong and unrecoverable once the WAL segment is recycled.

**How to confirm:** VPS: pg table with a >2KB text column, ALTER TABLE ... REPLICA IDENTITY FULL, changelog=True to ClickHouse. Bootstrap, then DELETE one row, drain+apply, and read the `D` record's TOASTed column from the destination — predict NULL, want the old value. The same run also settles the premise (whether pgoutput emits 'u' in an RI FULL old image) — if it does not, this finding is void. Cannot be checked from the MacBook.

