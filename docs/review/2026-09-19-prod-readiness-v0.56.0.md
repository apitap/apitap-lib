# Production-readiness audit of v0.56.0 — as released (2026-09-19)

**Read-only.** Nothing in this audit changed code, ran a build, or touched the rig;
every claim is from reading the tree at tag `v0.56.0` (`d45f932`).

**Method.** 29 agents in one workflow: three readers re-verified the ten `high`
findings the 2026-09-12 lenses never reproduced; four lenses reviewed the 0.56.0
diff (`acd9cd6..v0.56.0`) — lease/fence/collect correctness on all four engines,
failure paths, published promises vs code, and test-coverage honesty; one
adversarial skeptic per blocker/high finding; one writer for the verdict. The first
run lost 11 agents to a usage limit and was resumed from cache, so every finding
below carries a skeptic verdict.

**Calibration.** 21 findings went to skeptics and none was refuted. The last time
that happened it was flagged as a warning, so this time the eight findings that
decide the verdict were re-read by hand against the code before anything was
written down — and all eight hold:

| finding | evidence in the tree |
|---|---|
| ClickHouse keeper resurrects a collected lease (§3.2) | `dest_ch.rs:178-179` renews with `collected = 0` and a fresh `seq`; `sink/clickhouse.rs:109` reads `argMax(collected, seq)` — newest wins |
| BigQuery apply never reads the lease (§3.1) | every `lease`/`collected`/`expires_at` hit in `dest_bq.rs` is in lines 90–322; `apply*` at 649/680/924/951 carry none |
| MySQL TRUNCATE runs before the fence (§3.8) | `dest_my.rs:480-481` on the autocommit connection, `fence_tx` only inside the later transaction |
| MySQL `OR collected = 1` is dead (§3.9) | no `client_found_rows` anywhere in `crates/`; MySQL's default `affected_rows` counts changed rows |
| MySQL-source TRUNCATE-only window wedges (§3.4) | `mysource.rs:760-763` builds the collapser at window end; `wal_cols` is only filled by the rows path at `:602-603`; every apply requires it |
| `drain_group` returns past a live apply task (§3.10) | spawned `run.rs:1596`, joined only at `:1728` |
| Upgrade stamp collision (§3.6) | `acd9cd6 dest_ch.rs:610` stamped `end_lsn`; `dest_ch.rs:898` stamps `start_lsn` — the same value across the boundary |
| Shorter replay re-appends the tail (§3.14) | `dest_ch.rs:906` `skip = n.min(len)`, `:923-925` writes the replay window's own end as the watermark |

Three of those (§3.2, §3.4, §3.8) are defects in code written for 0.56.0 itself,
and §3.4 is a regression against 0.55.1 in one respect: 0.55.1 silently dropped a
MySQL-source TRUNCATE (divergence, no wedge); 0.56.0 applies it but a truncated table
left without rows for a window now wedges the group on every run.

The remaining findings were confirmed by the panel and not independently re-read.
They are labelled as such where it matters.

---


## 1. Verdict

**READY WITH CONDITIONS.** 0.56.0 delivered the guard it promised on Postgres: announce-then-check with no tie-break is sound on all four engines (naming.rs:379-384, run.rs:914-943), the Postgres lease is a real fence (dest_pg.rs:56-125, proven by e2e_cdc_fence.py), a killed drain's lock clears itself (e2e_cdc_lease.py), MySQL/MariaDB TRUNCATE now reaches the destination (mysource.rs:665-710), and a single drain's ClickHouse changelog replay appends nothing twice (dest_ch.rs:898-930). What stands against it is that the lease story is complete only on Postgres, and the docs claim more than the code does: BigQuery's apply never reads the lease at all (no lease reference in dest_bq.rs past line 322) while failure-modes.md:335-343 says it re-checks before every watermark; on ClickHouse the victim's own keeper overwrites the collector's claim (dest_ch.rs:173-183, clickhouse.rs:108-109); on MySQL the WAL TRUNCATE runs before the fence (dest_my.rs:480-482 vs :535), the claim-crash recovery is dead code (mysql.rs:220), and a cursor-lane state row is invisible so a mode switch silently full-replaces (dest_my.rs:281-285). Independent of the lease: a MySQL-source TRUNCATE with no rows in its window wedges the group (mysource.rs:760-764), BigQuery replica rejects a Delete→Insert→Delete window on one key (collapse.rs:269-274 → dest_bq.rs:1134-1140), the upgrade itself collides `_apitap_lsn` stamps for every existing changelog=True table (acd9cd6 dest_ch.rs:610 vs dest_ch.rs:898), and a bulk worker failure leaves its siblings writing into staging that `discard` is sweeping (source/postgres.rs:509-513). Decision by workload:

- **READY:** bulk `replace`/`append`/`merge` into Postgres, MySQL, ClickHouse, on a fleet that is entirely 0.56.0.
- **READY WITH CONDITIONS:** bulk into S3/GCS/BigQuery/Iceberg — pin `parallel` (or give the container ~33 MiB per pipe above the model), and expect one manual object cleanup after any worker error; Iceberg `merge` only for deltas of a few million rows. CDC replica (`changelog=False`) into Postgres — single-version fleet, no cross-lane mode switching. CDC replica into MySQL and ClickHouse **from a Postgres source** — on MySQL never point `log_based` at a table an `append` run manages and avoid source TRUNCATE on tracked tables; on ClickHouse a paused-then-resumed drain can land one extra window. CDC from a MySQL/MariaDB source only where a tracked table is never TRUNCATEd and then left without rows for a window.
- **NOT READY:** any CDC into BigQuery (unchecked lease; replica MERGE rejection on natural-key churn; changelog resubmit duplicates). MySQL/MariaDB-source CDC into ClickHouse where a drain can be paused or partitioned and resume (two winners, unbounded). Upgrading an existing 0.55.x `changelog=True` deployment before the seq-continuation fix. Any rolling upgrade that overlaps a 0.55.x bulk run with a 0.56.0 drain. Correct failure-modes.md:335-343 and stability.md:25 before anyone is pointed at them.

## 2. What 0.56.0 got right

- **Announce-then-check, no tie-break.** `lock_blocks` yields to any blocking peer (naming.rs:379-384); `blockers` asks every GUARDED kind about every listed name (naming.rs:448-483); both CDC groups announce every member before checking any (run.rs:914-937, :1147-1170) and spawn the keeper only after the scan (run.rs:945-953). Bulk sinks announce before scanning (sink/postgres.rs:841-842). Live: e2e_concurrent_runs.py leg 6, e2e_cdc_guard.py (pg→ClickHouse).
- **Lease-before-lock, lock-gone-before-lease-close, keeper stopped and joined first.** run.rs:910-926, :1032-1043 (and the MySQL twin :1143-1170, :1326-1337); `Keeper::stop` aborts and awaits (lease.rs:149-155). Reading only.
- **Postgres fence.** `fence_tx` is the first statement of both apply transactions, takes the row `FOR UPDATE` with `NOT collected AND expires_at > now()` and renews in-tx (dest_pg.rs:56-125; calls at :403, :420); collector claims `FOR UPDATE NOWAIT`, 55P03 → refuse (sink/postgres.rs:268-292); keeper renews `SKIP LOCKED` (sink/postgres.rs:204-224). Live: e2e_cdc_fence.py legs 1-3 (pg→pg).
- **Only a Lock is ever collectable, never with no lease.** naming.rs:498-503; every lane refuses on a missing lease row (sink/postgres.rs:84-99, dest_my.rs:231-253, dest_ch.rs:264-283, dest_bq.rs:296-312). Live: e2e_cdc_lease.py:177-188 (planted epoch-zero lock refused and left in place).
- **The clock is the destination's on every engine.** `now()` / `UTC_TIMESTAMP(6)` / `now64(6)` / `CURRENT_TIMESTAMP()` (sink/postgres.rs:171-199, sink/mysql.rs:128-151, sink/clickhouse.rs:77-98, dest_bq.rs:175-194); TTL clamped 30..3600, renewal a tenth (lease.rs:81-99). Reading only.
- **A refused bulk run takes its announcement back.** pipeline/mod.rs:440-446 → `Sink::release_lock` (sink/mod.rs:229-231). Live: e2e_failure_modes.py leg 1 via the widened `staging_names()` sweep in e2e_concurrent_runs.py.
- **MySQL/MariaDB TRUNCATE for the `TRUNCATE t; INSERT …` shape.** Strict parse, None → refusal (mysource.rs:853-880, :700-710); held until the accumulator exists (:612-621, :685-690). Live: e2e_mariadb_cdc.py:226-233 (gate leg).
- **Changelog stamp is the window START on both engines; ClickHouse single-drain replay is idempotent.** dest_ch.rs:898 (`outcome.start_lsn`), `_apitap_cdc_pending` + `appended_prefix` requiring `count == max(seq)+1` excluding baseline (dest_ch.rs:325-382, :904-930); BigQuery stamps `outcome.start_lsn` (dest_bq.rs:865) and packs a table's INSERT with its watermark whole (dest_bq.rs:53, :715; unit test with a real control). Live: e2e_changelog_replay.py (MariaDB→ClickHouse, rewinds `_apitap_state` in SQL and reads counts back from the server).
- **`__current` tie-break ranks a real change over a baseline at the same `(lsn, seq)`.** dest_ch.rs:672-682. Reading only.
- **Bookkeeping tables excluded from discovery.** `OWN_TABLES` (naming.rs:944-947) feeds `sql_exclusion` (naming.rs:860-866). Reading only.
- **Postgres `read_state` reads the other lane's row and refuses it.** dest_pg.rs:331-372. Live: e2e_state_contract.py leg 4 (pg→pg only).
- **`Dest::Ice` is genuinely unguarded and says so** (run.rs:151-158; README.md:200-203, stability.md:25, failure-modes.md:353-359). Reading only.

## 3. Findings that stand

Ordered: all are **high**; breadth first.

### 3.1 BigQuery: the apply path never checks the lease (three duplicate submissions merged)
- **What goes wrong.** An evicted BigQuery drain that resumes keeps applying windows and writing watermarks to its stop line. failure-modes.md:335-343 promises "the drain re-checks before it moves the watermark" and "never two winners for longer than one window" on BigQuery.
- **Trace.** `Dest::set_run` is a no-op for `Bq` with the comment "its check rides that script" (run.rs:163-171). The script is `BEGIN TRANSACTION; <MERGE|INSERT>; <state INSERT>; COMMIT TRANSACTION;` (dest_bq.rs:1008-1027, state SQL :1197-1207); `apply`/`apply_group`/`apply_changelog`/`apply_group_changelog`/`stage`/`stage_changelog` (dest_bq.rs:649-717, :924-1004, :781-890, :1031-1180) contain no lease predicate. Grep confirms every `lease|collected|expires_at|token` hit in dest_bq.rs is in lines 90-322 (the store and check_peers). Contrast dest_ch.rs:149-154 `check_still_mine` at :967, :982, :998, :1258. Run B collects A (dest_bq.rs:285-313) and proceeds; A's keeper `UPDATE … SET expires_at` (dest_bq.rs:196-210) keeps renewing; nothing on A's write path reads `collected`.
- **Bound in practice.** With a **Postgres source**, a paused A loses its walsender after `wal_sender_timeout`, so A dies at its next read and lands only its in-flight window (unchecked). With a **MySQL/MariaDB source** there is no slot exclusivity: A and B both apply until A's stop line.
- **Skeptic.** STANDS (all three verifiers: grep shows no lease read past line 340; `BqDest` has no `run_token` field).
- **Fix shape.** Give `BqDest` a run token in `set_run`, add `check_still_mine` (lease_get_self on own token → `Error::Locked` when `lapsed()`) at the top of `apply_group`/`apply_group_changelog` and again before `commit_batch` when staging exceeded `renew_secs()`; or prepend `IF NOT EXISTS (SELECT 1 FROM _apitap_lease WHERE token=… AND NOT collected AND expires_at > CURRENT_TIMESTAMP()) THEN RAISE …` to every script so the check is in the transaction. Until then, correct failure-modes.md:335-343.
- **Workloads.** All CDC into BigQuery; unbounded from MySQL/MariaDB sources.

### 3.2 ClickHouse: the victim's keeper resurrects a collected lease (two duplicate submissions merged)
- **What goes wrong.** A drain evicted while paused/partitioned passes `check_still_mine` for ever once its keeper ticks again — two winners with no bound.
- **Trace.** `ChDest::lease_renew` → `lease_write(…, ttl, 0)` on every tick (dest_ch.rs:173-183); `lease_write` appends a row-version with `seq = toUnixTimestamp64Micro(now64(6))` (sink/clickhouse.rs:77-98); `lease_get` reads `argMax(expires_at, seq)`, `argMax(collected, seq)` (sink/clickhouse.rs:101-133) so the newest row wins outright; the collector's claim is one row-version with `collected=1` (sink/clickhouse.rs:142-150). After the victim's next tick `lapsed()` (lease.rs:69-71) is false; `lease_still_mine` (dest_ch.rs:201-212) returns Ok at all four call sites; `write_state` moves the cursor. The apply never consults its own lock (dest_ch.rs `lock_name` used only at :233, :297). On Postgres/MySQL/BigQuery renewal updates `expires_at` only (sink/postgres.rs:204-224, sink/mysql.rs:153-175, dest_bq.rs:196-211) — ClickHouse is the one engine where renewal overwrites `collected`, and the one without a fence.
- **Bound in practice.** Postgres source: B is refused by `slot … is already active` (run.rs:1540-1544) while A's walsender lives; after `wal_sender_timeout` A dies at its next read, landing one window (unchecked because of the resurrection). MySQL/MariaDB source: unbounded.
- **Skeptic.** STANDS (both). e2e_cdc_lease.py kills the drain (`p.kill()` at :152) so the keeper is dead and never resurrects anything; the paused drain has no leg.
- **Fix shape.** Make `collected` sticky: `lease_get` uses `max(collected)` (or `argMax` over `(collected, seq)` with collected first), and `lease_renew` refuses to write when a claim exists; surface "collected" from the keeper to the drain via a shared `AtomicBool` the apply checks.
- **Workloads.** All CDC into ClickHouse; unbounded from MySQL/MariaDB sources.

### 3.3 Bulk: `run_workers` detaches every sibling on the first error (prior Finding 1)
- **What goes wrong.** Sibling workers keep draining the whole span queue and calling `loader.finish()` while `pipeline::run`'s error arm sweeps staging; on S3/GCS/BigQuery a late `complete_multipart`/load job re-creates a foreign-token object after the sweep, and the next run refuses with lease `None` ("nothing collects it") until a human deletes it.
- **Trace.** `tasks.push(tokio::spawn(copy_out_worker(…)))` (source/postgres.rs:499-507); `for t in tasks { rows += t.await…?? }` returns on the first Err and drops the remaining handles (:510-513) — detach, never cancel (no `.abort()`/`JoinSet`/cancel flag anywhere in source/*.rs, pipeline/mod.rs, sink/mod.rs). Same loop in source/mysql.rs:1188-1192, :1532-1536, source/clickhouse.rs:582-586. Workers loop `while let Some(sql) = pop(&queue)` (postgres.rs:666) over the shared queue (source/mod.rs:95-105) and end with `loader.finish()` (:939). The runtime is a process-wide `OnceLock<Runtime>` (py-apitap/src/lib.rs:50-54) so they outlive `transfer()`. `discard` lists-and-deletes now (sink/s3.rs:953-963); the next `prepare` classifies the survivor `Staged::Run{seg, peer}` with a foreign token and returns `locked_error(…, None)` (s3.rs:829-851). On Postgres the DROP waits behind the surviving COPYs — a slow failure, not a wedge.
- **Skeptic.** STANDS; only the object-store timing (worker finishing after the sweep) is not provable by reading.
- **Fix shape.** Shared cancel flag checked at each `pop`, cleared queue on first Err, exit via `loader.abort`; await every handle before returning so `discard` runs after all loaders are closed. Add a leg: `parallel≥4`, one span forced to fail, assert no `<token>/part-*` survives and the re-run is not refused.
- **Workloads.** Every bulk lane; wedge on S3/GCS/BigQuery/Iceberg, delayed error on Postgres/MySQL/ClickHouse.

### 3.4 MySQL/MariaDB source: a TRUNCATE with no rows event in its window fails every run
- **What goes wrong.** "log_based: missing WAL column list" on every apply; the watermark never moves; a multi-table group stops with it.
- **Trace.** A truncate for a tracked table with no collapser yet is parked (mysource.rs:690-696); at window end it becomes `Collapser::new(Vec::new()).truncate()` (mysource.rs:760-764). `wal_cols`/`wal_oids` are local to `drain_binlog` (:465-466) and written only by the `Relation` arm of the rows path (:597-603). Every replica apply does `outcome.tables.get(q)` → Some, then `wal_cols.get(q).ok_or_else(…"missing WAL column list")` (dest_ch.rs:1006-1009, dest_pg.rs:408-411, dest_my.rs:437-440). It unwedges only when the source writes rows to that table inside the re-drained window.
- **Skeptic.** STANDS. e2e_mariadb_cdc.py:230-233 tests only `TRUNCATE; INSERT` in one window.
- **Fix shape.** In each apply, when `c.truncate && c.upserts.is_empty() && c.deletes.is_empty()` do not require `wal_cols`: TRUNCATE + watermark and return. Add a leg with `TRUNCATE t` and no following rows (single table and a two-table group with sibling traffic).
- **Workloads.** All MySQL/MariaDB-source CDC (replica), every destination.

### 3.5 Rolling upgrade: the drain/bulk guard is one-directional against 0.55.x
- **What goes wrong.** A 0.55.1 bulk `replace` proceeds beside a live 0.56.0 drain; changes the drain applied between the replace's snapshot and its swap are gone and never re-drained. stability.md:25, README.md:200-202 say "both directions" with no version caveat.
- **Trace.** A 0.56.0 drain announces with a `__apitap_lock` table only (run.rs:915-925 → sink/postgres.rs:34-41) and creates no staging. `git log v0.55.1..acd9cd6` is three naming-only commits; at acd9cd6 `reap_and_check_peers` scans `artifact_match(&self.bare, Artifact::Staging, …)` only (acd9cd6 sink/postgres.rs:96-122) and no sink references `Artifact::Lock`; v0.55.1 naming.rs has no `Artifact::Lock` at all. Reverse on BigQuery: a 0.55.1 bulk run's only artifacts are `<bare>_<tok>__apitap_staging_N` worker tables; 0.56.0's CDC `check_peers` hands the listing to `blockers` unstripped (dest_bq.rs:293-296; only the bulk lane strips via `staging_base`, sink/bigquery.rs:1942).
- **Skeptic.** STANDS.
- **Fix shape.** Docs: the drain half is mutual only once every process is 0.56.0+; do not overlap a bulk run with a drain during a rolling upgrade. Optionally create an empty tokenized staging table for the drain's lifetime (0.55.1 classifies it `Live(Cdc)` and refuses Swap beside it); strip `_N` in dest_bq.rs `check_peers`.
- **Workloads.** Every destination during the upgrade window.

### 3.6 Upgrading a `changelog=True` destination from 0.55.x collides `_apitap_lsn` across the boundary
- **What goes wrong.** The last 0.55.x window and the first 0.56.0 window carry the same stamp with `_apitap_seq` restarting at 0; `__current` (ORDER BY lsn DESC, seq DESC — dest_ch.rs:672-682) shows the OLD value for any key whose old-window seq is higher than its new-window seq, until the key changes again. Silent, on the ordinary upgrade of every existing deployment, and again on any rollback-then-upgrade the stability page recommends via pinning.
- **Trace.** 0.55.x stamped `let lsn = outcome.end_lsn` and wrote the same value as the watermark (acd9cd6 dest_ch.rs:610, :651; acd9cd6 dest_bq.rs:580). 0.56.0 stamps `outcome.start_lsn` (dest_ch.rs:898, dest_bq.rs:865) = the watermark it was drained from (run.rs:1662-1670, drain.rs:398). `_apitap_cdc_pending` does not help — 0.55.x never wrote it, so `pending_window != Some(lsn)` and no skip runs.
- **Skeptic.** STANDS.
- **Fix shape.** When rows with `_apitap_lsn = start_lsn AND _apitap_op != 'B'` exist and the pending marker does not name this start, begin `_apitap_seq` at `max(seq)+1`. Document the meaning change on the stability page.
- **Workloads.** Every existing `changelog=True` table on ClickHouse and BigQuery.

### 3.7 MySQL destination `read_state` filters `AND mode = 'log_based'`; a mode switch silently full-replaces (prior Finding 6)
- **Trace.** Bulk `append` writes `(dest_table=self.bare, source_id, cursor_col, watermark, mode='append')` (sink/mysql.rs:706-719, mode_str at :1017). CDC reads with the same key (run.rs:852-861; `source_identity` shared with pipeline/mod.rs) but `WHERE … AND mode = 'log_based'` (dest_my.rs:281-285) → `None` (:297); the refusal at :299-305 is unreachable for the other lane's row. `have.is_empty()` → `bootstrap_group` → `Mode::Replace` (run.rs:1411) → `RENAME … DROP old` and `DELETE FROM _apitap_state WHERE dest_table=…` (sink/mysql.rs:1235-1255). dest_pg.rs:338-372 reads unfiltered and refuses; dest_my does not.
- **Skeptic.** STANDS.
- **Fix shape.** Mirror dest_pg.rs:349-373: select `mode`, refuse when `mode != "log_based" || cursor != STATE_CURSOR`; key by `bare(dest_table)`; add the reverse refusal in sink/mysql.rs `dest_state`; add a MySQL leg to e2e_state_contract.py (pg→pg only today).
- **Workloads.** MySQL-destination CDC pointed at a table an `append` run manages.

### 3.8 MySQL destination: the WAL-captured TRUNCATE runs before and outside the fence
- **Trace.** `TRUNCATE TABLE {ft}` on the autocommit connection (dest_my.rs:480-482) precedes `start_transaction` (:527-530) and `fence_tx` (:535). The module doc's "replay-safe" argument (dest_my.rs:4-6) holds for one drain, not for the loser of an eviction: B collects A (A holds no row lock yet), applies the TRUNCATE and the post-truncate inserts, advances; A resumes at :481 and truncates B's rows, then fails its fence — the rows are never replayed. Postgres carries the TRUNCATE inside the fenced tx (dest_pg.rs:420-423).
- **Skeptic.** STANDS; no MySQL-destination fence leg exists.
- **Fix shape.** Take the fence before the TRUNCATE (short tx with `fence_tx`, TRUNCATE under that row lock; MySQL's implicit commit is fine), or `DELETE FROM {ft}` inside the fenced tx.
- **Workloads.** MySQL-destination CDC under an eviction with a source TRUNCATE in the window.

### 3.9 MySQL: `lease_claim`'s `OR collected = 1` is dead — a collector crash between claim and DROP wedges the lock with "Nothing for you to do"
- **Trace.** `UPDATE … SET collected = 1 WHERE … AND (expires_at <= UTC_TIMESTAMP(6) OR collected = 1)` then `Ok(conn.affected_rows() > 0)` (sink/mysql.rs:211-220). MySQL/MariaDB report changed rows unless `CLIENT_FOUND_ROWS`; the pool is `Opts::from_url` + `OptsBuilder` with no `client_found_rows` (sink/mysql.rs:386-433; `grep found_rows` over crates/ and py-apitap/ is empty; mysql_async 0.37.0, Cargo.lock:2061-2063). dest_my.rs:239-242 explicitly relies on the disjunct. Next run: `lease=Some{collected:true}` → `locked_error` branch "it is applying a window right now … Nothing for you to do" (naming.rs:754-759), for ever.
- **Skeptic.** STANDS.
- **Fix shape.** Decide by re-reading the row (`SELECT collected … FOR UPDATE`), or set `client_found_rows(true)`, or `SET collected = 1, expires_at = expires_at` so the UPDATE always changes. Needs a MySQL leg planting an already-collected lease + lock.
- **Workloads.** MySQL-destination CDC after a collector crash (narrow, permanent, misleading message).

### 3.10 `drain_group` can return through `?` with the apply task mid-window; on the shared runtime it commits after lock and lease are gone
- **Trace.** Window k is sent (run.rs:1694, cap-1 channel), k-1 is awaited (:1702), `ws.standby_status(p, false).await?` (:1705) returns from `drain_group` without joining `apply_task` (join only at :1728; `drop(win_tx)` at :1715 skipped). `run_cdc` uses the shared runtime when the cpu quota is > 0.6 core (py-apitap/src/lib.rs:78-86) → a dropped JoinHandle detaches. `run_group` then stops the keeper, drops the lock and closes the lease (:1032-1043) while the orphan writes; `fence_tx` treats a missing row as "nothing to fence against" (dest_pg.rs:82-95) so the orphan's later tables are unfenced; the orphan's watermark can land below a later run's and trigger the BEHIND refusal (run.rs:1545-1553). The `?` predates 0.56.0 (acd9cd6 run.rs:1394) but 0.56.0 placed the release behind it. Below 0.6 core the `current_thread` runtime is dropped with the task, so the capped tier is not exposed.
- **Skeptic.** STANDS.
- **Fix shape.** Capture the loop's result in an inner `async {}`; always `drop(win_tx)` and await (or abort-then-await) `apply_task` before returning; make `fence_tx` treat "had a lease, row now gone" as fenced.
- **Workloads.** Postgres-source CDC on hosts above 0.6 core quota, every destination.

### 3.11 BigQuery replica: two 'D' rows for one key → MERGE rejected, window re-fails on every retry (prior Findings 8 + 9 merged)
- **Trace.** Delete→Insert→Delete on K in one window: `delete` on Vacant pushes K (collapse.rs:277-280); `insert` on `Slot::Delete` re-slots Upsert without recording the push (:158-163); `delete` on `Slot::Upsert` pushes K again (:269-274); `put_delete` has the same shape (:298-317); `finish` moves `deletes` verbatim (:319-327). The field's own doc says "(dedup'd)" (:32-33). Route via TOAST/rekey (Postgres source): `ResidueOp::Delete`/`Rekey` fold to `Fin::Gone` (resolve.rs:62, :107), `landed` is built from non-Gone finals only (dest_bq.rs:1102-1106), the finals loop emits 'D' for Gone (:1127-1130) and the deletes loop emits it again (:1134-1140). `merge_sql`: `WHEN MATCHED AND S._apitap_op = 'D' THEN DELETE` (dest_bq.rs:1305) with two source rows on one target → BigQuery's "must match at most one source row" error, not `retryable()` (sink/bigquery.rs:891-903). MERGE + state INSERT are one transaction (dest_bq.rs:1017-1027) → watermark unmoved → identical failure next run. Every other destination tolerates the repeat (`_ap_del` join on pg/my, key table on ch, `seen` on dest_ice.rs:144-148). MySQL sources reach only the first route: `precheck` refuses `binlog_row_image != FULL` (mysource.rs:262-275), though the decoder does emit `UnchangedToast` for absent columns (mybinlog.rs:336) if a session override slips past the global probe.
- **Skeptic.** STANDS (both).
- **Fix shape.** In dest_bq.rs `stage()`: build `emitted` from every key in `finals` and `insert` each `c.deletes` key, skipping on `false` (closes both routes at the one consumer that cannot tolerate a repeat); make the collapse.rs contract true or change the doc; unit test D/I/D asserting `deletes.len()==1` (red first); a BigQuery replica leg.
- **Workloads.** CDC replica into BigQuery, natural-key tables (sessions, queues, memberships) — likely during catch-up windows.

### 3.12 BigQuery changelog: a retryable 5xx on the poll (or a lost POST) re-runs a committed INSERT…SELECT (prior Finding 5, resubmit half)
- **Trace.** `stage_changelog` loads staging WRITE_TRUNCATE at load time only and returns `INSERT INTO t … SELECT … CAST(<start_lsn>) …, CAST(_apitap_seq) … FROM staging;` with no `NOT EXISTS`/pending guard (dest_bq.rs:845-887; the ClickHouse lane got `_apitap_cdc_pending`, BigQuery did not). `cdc_script_once` posts with a `jobReference` carrying no `jobId` (sink/bigquery.rs:718-724) and reads the server's; `poll_job` propagates any non-2xx from `api` (:519-541, :245-250); `cdc_script` retries on `"503 "` etc. by re-posting the identical script (:703-716). A poll that fails after DONE re-inserts the whole window.
- **Observable.** Duplicates carry the SAME `(lsn, seq)`, so `__current` and any pair-deduplicating consumer are unaffected; raw-log counts/sums are doubled for that window and `_apitap_state` gets two rows.
- **Skeptic.** STANDS.
- **Fix shape.** Client `jobId` per `cdc_script` call; on 409 Already Exists or a poll failure, poll THAT id rather than re-insert. Optionally the ClickHouse-style `NOT EXISTS` on `(start_lsn, seq)`. Add a BigQuery leg to e2e_changelog_replay.py.
- **Workloads.** `changelog=True` into BigQuery.

### 3.13 Changelog: a PK-changing UPDATE whose TOAST column is untouched is unresolvable — "torn window" on every retry (prior Finding 7)
- **Trace.** `Changes::update` pushes `D(old)` then `U(new)` when the key moved (changelog.rs:83-92); `Change` keeps only `{op,row}`. `mask_plan` keys only rows carrying `UnchangedToast` → the U row's NEW key (changelog.rs:101-122); `read_current` for the new key finds nothing; `resolve_masked` consults `carry[own key]` and `base[own key]` only (:163-197) → `Err("… the window is torn")` (:190-196) even under REPLICA IDENTITY FULL, where `carry[old key]` holds the value. Called from dest_ch.rs:876-885 and dest_bq.rs:825-833; no watermark → same failure next run; the group stops with it.
- **Skeptic.** STANDS.
- **Fix shape.** Carry `prev_key` on the U half of a moved-key update; add it to the readback list; resolve via `carry[own] → carry[prev] → base[prev] → base[own]`; unit test + a changelog leg of e2e_toast_rekey.py, red first.
- **Workloads.** `changelog=True` (ClickHouse/BigQuery) from Postgres sources with TOASTed columns and PK updates.

### 3.14 ClickHouse changelog: a SHORTER replay window re-appends the original's tail under a new stamp
- **Trace.** `appended_prefix` knows how many rows landed at the stamp, not where the original window ended (dest_ch.rs:359-382). `skip = n.min(len)`; when `skip >= len` the code writes the watermark at the REPLAY window's end and returns without re-marking pending (dest_ch.rs:923-926; `mark_pending` at :928-930 is below). Windows are cut by a 30 s clock and a byte budget (drain.rs:132, run.rs:1285), so a replay on a slower run/smaller container ends earlier; the next window's start no longer matches pending and appends events 61..100 again at a fresh stamp, seq 0... The log is plain `MergeTree` so the two never collapse. This is the duplicate-under-a-different-number the release notes say was removed.
- **Skeptic.** STANDS.
- **Fix shape.** Record the window END in `_apitap_cdc_pending`; never write a watermark below the recorded end (carry `already_appended` into the next window, or drain straight to the recorded end).
- **Workloads.** `changelog=True` into ClickHouse after a crash/replay with different timing.

### 3.15 Parquet lanes: per-pipe residency is a fixed ~33 MiB, the chunk-proportional cap under-counts it, and the thin-chunk lever raises real memory (prior Finding 4)
- **Trace.** Model: `reserve = 40 MiB; per_pipe = chunk*10` (pipeline/mod.rs:263-268), measured on pg→ch only (:247-249; benchmarks/profiling.md has no parquet/object-store cell). Reality: every ParquetEncoder pipe buffers up to `ROW_GROUP_BYTES = 24 MiB` (bqparquet.rs:33, sole flush trigger :482-484) plus `buf` 1 MiB (:409) and 8 MiB of encoded output before a send (s3.rs:1045-1052 and twins). Thin branch (:304-310): at 128 MiB and ≥2 cores → (2 MiB, 4 pipes) = 4×33 MiB before base; at 256 MiB and ≥4 cores → 8 pipes ≈ 264 MiB + 40. All four parquet sinks share `to_bq_parallel = (cores*2).clamp(2,8)` (dispatch.rs:53-55, :72-78). usage.md:172-180 says memory "scales with parallel × chunk_bytes". The 0.5-CPU headline tier is safe (`num_cpus` honours the quota → ask 2).
- **Skeptic.** STANDS; the OOM magnitude is the one thing a rig must show.
- **Fix shape.** `Sink::pipe_overhead()` (parquet sinks: `ROW_GROUP_BYTES + SEND_THRESHOLD`) folded into `per_pipe`; skip thinning when overhead dominates; or size the row group from the budget. One measured pg→gcs and pg→iceberg cell at 128/256 MiB in profiling.md; fix the usage.md sentence.
- **Workloads.** Bulk into GCS/S3/BigQuery/Iceberg in memory-capped containers on ≥3 cores with auto `parallel`.

### 3.16 Iceberg merge retains every merge-key value for the whole run, then copies it twice at commit (prior Finding 3)
- **Trace.** `capture`/`keys: KeyCap` "Delta-proportional memory" (bqparquet.rs:319-322); pushed per row (:551-583, uuid rendered to a 36-byte String); `group_bytes` sums `cols`+`defs` only (:590-593); `flush_row_group` clears `cols`/`defs` only (:659-664); `finish_file` neither (:670-677). Armed exactly on `Mode::Merge` (iceberg.rs:1057-1064, :1087); moved to `FileDone.keys` (:2050-2057); concatenated at commit while `files` is still alive (:1188-1197), then `write_delete_parquet` builds a third copy. ~8 B/row int, ~72 B/row uuid resident, ×2-3 at commit: a 2M-row uuid delta ≈ 145 MB + 290 MB at commit inside a 256 MB container; an OOM leaves the Iceberg claim marker for the next run to refuse.
- **Skeptic.** STANDS.
- **Fix shape.** Stream the equality-delete file per row group (second one-column encoder), one delete file per data file; move the `n == added_rows` check per file. Until then document the delta-proportional cost beside usage.md:179-180.
- **Workloads.** Iceberg `mode="merge"` with large deltas.

## 4. Findings the skeptics refuted

- **Prior Finding 10** (changelog writes NULL for unchanged-TOAST cells in DELETE/UPDATE old images) — NOT REAL: the shape is as described (changelog.rs:85, :94-97 never `note_mask` an old image) but no supported source delivers a masked cell in an old image that matters: Postgres flattens the old tuple before logging it (pgoutput 'u' only appears in the new tuple, wire/pgoutput.rs:296); on the MySQL lane absent columns do decode to `UnchangedToast` (wire/mybinlog.rs:336) only under MINIMAL/NOBLOB, which `precheck` refuses (mysource.rs:262-275), and a D record's non-key cells are defined as NULL anyway (changelog.rs:43-45). Nothing observable differs.

## 5. Unverified (rig experiments the skeptics asked for)

None lacked a skeptic verdict. These are the points where reading proves the mechanism but a rig is needed for the runtime fact or the magnitude:

- **3.3 object-store timing.** pg table with 4 spans > 60 s each, `transfer(pg, s3://…, parallel=4)`; `pg_terminate_backend` ONE COPY once all four are in `pg_stat_activity`; expect `transfer()` to raise within seconds, then `aws s3 ls --recursive` under the failed token for 5 min — confirmed if `part-*.parquet` appears after the exception and the re-run raises `LockedError` naming that segment, still after > `APITAP_LEASE_TTL_SECS`. Postgres-sink variant: measure wall time between the worker error and return; `pg_locks` shows the DROP waiting on AccessExclusiveLock.
- **3.15 OOM magnitude.** `docker run --memory=128m --cpus=4`, pg 10M-row seed with a text column → `s3://minio…` or `gcs://…?format=parquet`, no knobs; record `(chunk, parallel)` under `APITAP_DEBUG=1` (expect 2 MiB × 4) and cgroup `memory.peak`/exit 137; second cell `chunk_bytes=65536` (expect 8 pipes); third `--memory=256m --cpus=3`. A peak above the cap in any cell confirms; comfortably under in all three refutes the severity only. Drop dest objects after each run.
- **3.11 BigQuery-side rejection.** Unit: collapse.rs test D/I/D asserting `out.deletes.len()==1` (predicted 2). E2E on the e2e_bq_cdc.py dataset: bootstrap id=7, one window `DELETE 7; INSERT 7; DELETE 7`, drain — expect "UPDATE/MERGE must match at most one source row", watermark unchanged, identical failure on the second drain; control on the Postgres destination applies cleanly.
- **3.9 driver semantics.** QA MySQL: create the lease table per mysql.rs:116-124, insert `('db.t','tok', UTC_TIMESTAMP(6) - INTERVAL 1 SECOND, 1)`, run the exact claim UPDATE through mysql_async without `client_found_rows` and assert `affected_rows()==0` (and `==1` with `?client_found_rows=true`).
- **3.2 / 3.1 live confirmation (optional).** MariaDB → ClickHouse (or BigQuery), `APITAP_LEASE_TTL_SECS=30`: drain A, `kill -STOP` 45 s, drain B ("collected … Resuming"), `kill -CONT` A; on ClickHouse `SELECT argMax(collected, seq) FROM _apitap_lease WHERE token=<A>` flips back to 0 within 3 s and A keeps landing `_apitap_state` rows; on BigQuery A commits further script jobs after B's collection. Control: `kill` instead of `STOP` (the e2e_cdc_lease.py case) shows none of it.
- **`slots=N` with leases.** No gate leg uses `slots=` (grep of benchmarks/e2e_*.py is empty); run `tables=[a,b,c,d], slots=2` with one thread's process killed and assert the other group is unaffected.

## 6. Medium and low

- [medium] Postgres/MySQL: an apply transaction longer than the TTL commits with `expires_at` already past (in-tx renew stamps at tx start, dest_pg.rs:105-125; keeper skips the held row, sink/postgres.rs:204-224, sink/mysql.rs:153-175) — a healthy drain can be evicted between commit and the next tick; loud, no two winners.
- [medium] Three watermark writes sit outside the fence/check: MySQL no-traffic state upsert before the tx (dest_my.rs:430-434 vs :527-535); ClickHouse changelog no-event branches (dest_ch.rs:848, :868 with no `check_still_mine`); MySQL-source `stamp`/`write_marker` (run.rs:1302, :301-306).
- [medium] Two MySQL-source first runs of a multi-table group can both bootstrap: the handover releases the CDC lock (run.rs:1213-1224), the bulk guard is per-table and sequential (myrun.rs:148-171), and there is no slot `active` check as on Postgres (run.rs:1540-1544).
- [medium] Postgres guard is blind for unqualified tables when `search_path` does not start with `public`: the lock is created unqualified, the scan reads `nspname = 'public'` (sink/postgres.rs:332-341, :60-67).
- [medium] Prior Finding 2: 19 `loader.send(..).await?` sites drop the Loader instead of `abort`ing it (source/postgres.rs:746-937 and the others; contract at sink/mod.rs:59-62); costs an orphaned multipart/stream on the object-store sinks, over-reaches on the database sinks.
- [medium] e2e_concurrent_runs.py leg 7 passes vacuously when the runs do not overlap (`all([])` at :455-462).
- [medium] e2e_sigterm.py / e2e_sigterm_my.py call `clear_dead_lock()` before every leg (e2e_sigterm.py:109, :257, :290, :363, :377; _my:61, :177, :206), so "a graceful stop takes its lock back" has no live assertion.
- [low] Bootstrap handover: the unguarded gap spans the whole `bootstrap_finish` (PK ALTER + state write) after the bulk lock is gone (run.rs:990-1011, :1455-1477), not "the instant between the two".
- [low] ClickHouse CDC keys lease and lock on the unstripped `dest_table` (dest_ch.rs:156-158, :214-217) while the bulk lane uses the bare name; moot because the CDC lane cannot address a dotted destination.
- [low] ClickHouse `lease_get` evaluates two independent `argMax`es (sink/clickhouse.rs:108-109); a same-microsecond tie can mix row-versions — subsumed by 3.2.
- [low] BigQuery bulk collect step needs DML (`UPDATE _apitap_lease`, dest_bq.rs:125-131) which a sandbox project rejects — a Transfer error instead of a collection, loud.
- [low] `Keeper` has no `Drop`; a panic unwinding through `run_group` after the spawn (run.rs:945-953) detaches a renewer that keeps an orphan lock alive on the shared runtime.
- [low] The keeper quits on `shutdown::requested()` (lease.rs:127-131) before the wind-down finishes; a SIGTERM-ed ClickHouse drain can refuse its own final window at `check_still_mine` after 270-300 s.
- [low] `APITAP_CDC_APPLY_LANES ≥ 2` on a Postgres destination starves the keeper of the 2-connection pool (dest_pg.rs:203-206, run.rs:460-479); the resulting lapse blames a collector that does not exist.
- [low] Lease rows are never garbage-collected on Postgres/MySQL/BigQuery (only ClickHouse has a TTL, sink/clickhouse.rs:65); inert, one row per dead or collected run per table.
- [low] MySQL: a refused `SET SESSION innodb_lock_wait_timeout = 1` (`let _ =`, sink/mysql.rs:155, :208) silently falls back to the 50 s default per locked row.
- [low] usage.md:377-380 and :1284-1285 still carry 0.55-era sentences 0.56.0 contradicts; usage.md's CDC section never says Iceberg drains are unguarded.
- [low] A rollback to 0.55.1 replicates `_apitap_lease` and `_apitap_cdc_pending` as user tables under `tables="*"` (acd9cd6 naming excludes only `_apitap_state`).
- [low] e2e_failure_modes.py case 2 records PASS when the 1.2 s kill loses its race (:196-241); the lease assertions are simply skipped.
- [low] e2e_cdc_fence.py:180-182 `after == at_eviction or after < total` is a tautology given :175-176; the one-window-already-committed branch is never exercised.

## 7. The ten prior findings, resolved

| # | Finding | Verdict now | Severity now |
|---|---|---|---|
| 1 | `run_workers` detaches siblings on first error; staging swept under live writers | REAL (§3.3) | high |
| 2 | `loader.send(..)?` drops the Loader instead of aborting it | PARTLY REAL (object-store sinks only) | medium |
| 3 | Iceberg merge retains every key value; copied again at commit | REAL (§3.16) | high (Iceberg merge) |
| 4 | Parquet per-pipe residency under-counted; thin branch raises memory | REAL (§3.15); magnitude needs the rig | high (capped parquet lanes) |
| 5 | BigQuery changelog: chunker split (fixed by `pack_whole_groups`, dest_bq.rs:53/:715) / cdc_script resubmit | chunker half FIXED; resubmit half REAL (§3.12) | high (changelog into BQ) |
| 6 | MySQL `read_state` filters on mode; mode switch full-replaces | REAL (§3.7) | high |
| 7 | Changelog PK-changing UPDATE with untouched TOAST → torn window | REAL (§3.13) | high |
| 8 | `Collapsed::deletes` double push → BQ MERGE rejected | REAL (§3.11) | high |
| 9 | dest_bq `landed` excludes Gone → second 'D' | REAL (§3.11) | high |
| 10 | Old-image TOAST cells written as NULL | NOT REAL (§4) | none |

## 8. Coverage gaps

- **BigQuery lease/guard/collector: no live coverage at all.** The four BQ gate legs (gate.py:87-90) never plant a lock, kill a drain or collect; `pack_whole_groups` is unit-tested only. Reading suffices to show the check is absent (§3.1); a leg mirroring e2e_cdc_lease.py with a `bigquery://` dest is needed for the fix.
- **MySQL-destination fence, collector and already-collected plant.** e2e_cdc_fence.py is pg→pg, e2e_cdc_lease.py pg→ClickHouse; reading is NOT enough for §3.9 (driver semantics decide it).
- **ClickHouse paused-drain path** (§3.2) and the pre-watermark re-check that fires only when the INSERT took ≥ `renew_secs` (dest_ch.rs:981-983): no leg; a `docker pause` experiment is the proof.
- **Partition longer than TTL "ENDS a drain"** (failure-modes.md:346-350): true on pg/my by the fence predicate, false on ClickHouse after one keeper tick and on BigQuery always. Reading suffices.
- **TRUNCATE-only window on the MySQL lane and `changelog=True` TRUNCATE from MySQL**: unit tests only (§3.4). MySQL 8.4 TRUNCATE: the leg is MariaDB-only; the QUERY-event shape is identical, reading suffices.
- **MySQL keeper 1205-as-skip path** (sink/mysql.rs:153-175): no leg holding a lease row `FOR UPDATE` on a MySQL destination.
- **Cross-lane state contract on MySQL/ClickHouse/BigQuery**: e2e_state_contract.py is pg→pg only.
- **Bulk BigQuery meeting a Cdc lock**: whether a bulk-only `BqConn` without a billing project turns the `cdc_query` at sink/bigquery.rs:1727-1731 into a Transfer error was not verified.
- **`slots=N` with leases**: read-only trust (partition of the sorted list, run.rs:673-690); no leg.
- **Rolling-upgrade interleavings**: nothing in the gate runs two wheel versions against one destination.

## 9. What to do before the next release (in order)

1. **Docs first, same day:** failure-modes.md:335-343 (BigQuery is neither fenced nor checked; ClickHouse's one-window bound does not survive a keeper tick), stability.md:25 and README.md:200-202 (the drain/bulk guard is mutual only on a fleet that is entirely 0.56.0+; do not overlap a bulk run with a drain during a rolling upgrade), usage.md:172-180 and :179-180 (parquet lanes and Iceberg merge do not scale with `parallel × chunk_bytes`), usage.md:377-380 / :1284-1285 (0.55-era sentences).
2. **ClickHouse sticky `collected`** (§3.2): `lease_get` → `max(collected)`; `lease_renew` refuses to write when a claim exists; keeper surfaces "collected" to the drain. Leg: paused drain on MariaDB→ClickHouse.
3. **BigQuery `check_still_mine`** (§3.1): run token on `BqDest`, check at the top of `apply_group`/`apply_group_changelog` and before `commit_batch` when staging took ≥ `renew_secs()`. Leg: e2e_cdc_lease twin on `bigquery://`.
4. **dest_bq.rs `stage()` `emitted` set over all finals + deletes** (§3.11), plus the collapse.rs D/I/D unit test red-first and the doc at collapse.rs:32-33 made true.
5. **`changelog=True` seq continuation across the stamp change** (§3.6): start `_apitap_seq` at `max(seq)+1` when rows already exist at `start_lsn` and pending does not name it; note the meaning change on the stability page.
6. **TRUNCATE-only window on the MySQL lane** (§3.4): apply TRUNCATE + watermark without `wal_cols` when the collapser is empty; leg with no following rows, single table and 2-table group.
7. **MySQL destination trio** (§3.7-3.9): unfiltered `read_state` with the refusal; fence before the TRUNCATE; claim decided by re-read (or `client_found_rows(true)`). Legs: MySQL twin of e2e_cdc_fence leg 1, an already-collected plant, a MySQL leg in e2e_state_contract.py.
8. **`drain_group` unconditional join** (§3.10) and `fence_tx` treating "had a lease, row gone" as fenced.
9. **`run_workers` cancel flag + await-all** (§3.3) in the four join loops; leg with one forced span failure into S3 asserting no surviving `<token>/part-*`.
10. **BigQuery client `jobId` in `cdc_script_once`** (§3.12); BigQuery leg in e2e_changelog_replay.py.
11. **Changelog `prev_key` for moved-key masked updates** (§3.13); changelog leg of e2e_toast_rekey.py, red first.
12. **ClickHouse pending END** (§3.14).
13. **`Sink::pipe_overhead()` for the parquet lanes** (§3.15) with two measured `memory.peak` cells in profiling.md; **streamed equality-delete file** for Iceberg merge (§3.16) or, until then, the documented cost.
14. **Test hygiene:** make e2e_concurrent_runs.py leg 7 require an observed overlap; drop the pre-leg `clear_dead_lock()` after the graceful legs in both sigterm rigs and assert the lock is gone; fail e2e_failure_modes.py case 2 when the kill loses its race; replace the tautology at e2e_cdc_fence.py:180-182.