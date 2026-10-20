# apitap 0.61.0 — full-codebase Rust review (handoff)

**For the implementer.** This is a work queue, not an essay. It was produced on
2026-10-14 against `main` at `597f8e0` (workspace `0.61.0`), read-only on the MacBook,
with tool runs and repros on the bench VPS QA rig. Seven reviewers split the tree
(CDC core, CDC destinations, wire, bulk sinks, sources/pipeline, cross-cutting security,
style/scalability/robustness); every finding below was de-duplicated and then labelled
by how far it was verified.

**Evidence labels — trust them literally.**

| Label | Meaning |
|---|---|
| **REPRO** | Reproduced on the VPS (output quoted). |
| **READ** | The integrator re-read the cited lines by hand and the defect is there. |
| **AGENT** | One reviewer traced it; not re-read by the integrator. Re-read before fixing. |
| **PLAUSIBLE** | Mechanism is real, impact unmeasured or depends on server behaviour. Repro first. |

---

## 0. Rules for whoever picks this up

1. **No builds on the MacBook.** cargo build/check/test/clippy only on the VPS.
   - SSH: `ssh -i ~/.ssh/apitap_vps ubuntu@vps-f1d96aff.vps.ovh.ca`
   - Use the QA rig, not the release copy: repo mirror `~/apitap-qa`, cargo target
     `~/apitap-qa-target`, venv `~/qa-venv` (currently holds wheel 0.57.0 — rebuild),
     toolchain `~/.cargo/bin` (cargo 1.94, clippy, miri, cargo-audit).
   - Sync with an ABSOLUTE source, never a `cd` chained before it:
     ```
     rsync -az --delete --exclude '.git' --exclude 'target' --exclude 'dist' \
       --exclude 'benchmarks/wheels*' --exclude 'benchmarks/.env' \
       -e "ssh -i ~/.ssh/apitap_vps" \
       /Users/macbook/Documents/private/code/apitap-lib/ \
       ubuntu@vps-f1d96aff.vps.ovh.ca:~/apitap-qa/
     ```
   - Then `find crates py-apitap -name '*.rs' -exec touch {} +` before cargo, or you
     test stale code (rsync keeps mtimes). Check a new test shows in `cargo test -- --list`.
   - Long VPS jobs: `nohup … > ~/log &` and poll the log; verify by artifact, never `pgrep`.
2. **Every fix lands with a test seen RED without the fix.** Revert the one line on the
   VPS, run the single test, see it fail, restore. A test that passes both ways is worthless.
3. **e2e legs ask the server for the fact** (row count, `FINAL` count, state row), never
   trust an exit code.
4. **QA data budget: 1000 rows.** Drop every destination table a test creates; keep seeds.
5. Engine logic in Rust (`apitap-core`), never in the Python shim. One-line API: no new
   required knob — new behaviour is a safe default.
6. Commit messages carry **no** Claude/Co-Authored-By trailer.
7. Docker on the VPS is mixed prod + bench. Never prune. Only the `apitap-bench-*`
   containers are yours. **No MySQL bench container is running right now** — start
   one (see `benchmarks/run-server.sh`) before the MySQL items.

---

## 1. Verdict

**Not production-ready for every lane yet.** Bulk pg/my/ch and the default CDC
replica path are sound in the parts that were re-checked, but there are:

- 1 **SQL injection** (ClickHouse identifiers, REPRO),
- 2 **credential-exposure-by-default** paths (Postgres sqlx pools, MySQL CDC),
- 3 **silent data-loss** bugs (CH insert-only tie REPRO, parallel-apply first window,
  Iceberg sweep after an ambiguous commit),
- several **panics reachable from data or a hostile peer** (NUMERIC, handshake,
  error-body truncation),
- and process gaps: **CI runs no cargo test/clippy/fmt** at all.

"No bugs" cannot be guaranteed by any review; what this gives is a ranked, evidence-tagged
queue. Fix P0 → gate → P1 → gate.

---

## 2. P0 — fix before the next release

### P0-1 · ClickHouse identifier injection via backslash · **REPRO**
- `sink/clickhouse.rs:751-753` `ch_ident` escapes `` ` `` but not `\`. Same disease in
  `source/clickhouse.rs:36` (`q`). 45+ call sites: CREATE (`:1373`), INSERT column list
  (`:1579`), `max(cursor)` (`:1968`), CDC DDL (`logbased/dest_ch.rs:199,207,287-288`).
- A source column named `` x\` , 42 AS injected_col -- `` renders as
  `` `x\\`, 42 AS injected_col --` ``. On CH 24.8 (VPS):
  ```
  SELECT 1 AS `x\\`, 42 AS injected_col --` FROM system.one;
  → x\: 1   injected_col: 42
  ```
  Anyone who can create a column on the source can rewrite a destination statement
  (e.g. staging CREATE → `ENGINE=URL('http://attacker/…')`) with CH credentials.
- **Fix:** `name.replace('\\', "\\\\").replace('`', "\\`")` in both helpers (backslash
  first). Better: also refuse control chars/NUL like `bq_ident` (`sink/bigquery.rs:119`).
- **RED test:** unit test `ch_ident("a\\")`, `ch_ident("x\\`y")` round-tripped through a
  real CH `SELECT 1 AS {ident}` on the rig; expect exactly one column named as the input.

### P0-2 · CH insert-only: delete+reinsert of one key in one window hides the live row · **REPRO** (engine) + **READ** (code)
- `logbased/dest_ch.rs:678-727`. `collapse.rs:248-253` turns DELETE-then-INSERT of key K
  into `Slot::Upsert{del:true}`; `finish()` (`collapse.rs:404-406`) puts K in `deletes`
  too. The insert-only branch writes the upsert, then a tombstone, **both at
  `_apitap_ver = id.end()`**. ReplacingMergeTree keeps the last-inserted on a version tie.
- VPS, CH 24.8: live row v=5 then tombstone v=5 → `count() FINAL = 0`; control without
  tombstone → 1. ORM "replace", or `pk 1→2` then insert 1, loses the row silently.
- The delete-then-insert branch (`:648-675`) is safe; the comment at `:643-645` covers only
  the residue tail.
- **Fix:** in insert-only, skip tombstones for keys that also have a live upsert (tag in
  `finish()`, or filter `deletes` against the upsert key set before rendering).
- **RED test:** extend `benchmarks/e2e_ch_insert_only.py` (it only tests distinct keys,
  `:79`) with `DELETE id=1; INSERT id=1` in one source transaction; assert `FINAL` count.

### P0-3 · Postgres sqlx pools default to `sslmode=prefer` and answer cleartext/MD5 · **READ**
- `source/postgres.rs:40-55`, `sink/postgres.rs:457-462`, `logbased/run.rs:855-859`,
  `logbased/dest_pg.rs:475` all `.connect(url)` with the raw URL. sqlx default = prefer
  (no cert verification, plaintext fallback) and it sends the password on an
  AuthenticationCleartextPassword request. The 0.59.0 guard (`walsender.rs:1039-1056`)
  covers only the walsender socket, which connects *after* these pools.
- MITM answers SSLRequest `N` + cleartext auth → password captured.
- The MySQL source got the equivalent fix in 0.55.1 (`source/mysql.rs:745-762`); Postgres never did.
- **Fix:** one helper `pg_connect_options(url)` used by all four: off-loopback with no
  explicit sslmode → `VerifyFull`; any explicit mode is honoured (incl. `disable`).
  Mirror `dialect::mysql::{has_explicit_ssl_mode, host_is_loopback}`.
- **RED test:** a 4-case unit test of the mapping (loopback/no-mode, remote/no-mode,
  remote/`sslmode=disable`, remote/`require`). An e2e leg on a TLS-required server proves
  nothing (lesson from E1, see memory `control-the-test-first`).

### P0-4 · MySQL CDC connects without verified TLS by default · **READ** (control pool) / **AGENT** (binlog plane)
- `logbased/myrun.rs:121-131` `control_pool` passes the raw `src_url` to sqlx (PREFERRED);
  `myrun.rs:244` `MyWire::connect(src_url)` → `mywire.rs:139` default
  `SslPref::Preferred{explicit:false}`, which downgrades silently (warning only when
  `explicit`, `mywire.rs:506-522`).
- Consequences: caching_sha2 RSA exchange to an unauthenticated key (password), and an
  active attacker can inject binlog row events that get applied.
- **Fix:** route both through the same default-to-`VERIFY_IDENTITY` rewrite the bulk source
  uses (`source/mysql.rs:752-760`). Pair with P2-9 (`ssl-ca=`), otherwise RDS/Cloud SQL
  users can't pass verify_identity on the raw plane.
- **RED test:** unit test of the rewritten URL for the CDC entry points.

### P0-5 · Iceberg: a landed commit reported as failed → sweep deletes live files · **AGENT**
- `sink/iceberg.rs:384`, `:1475`, `:2088`; sweep `:1237`, `:1893-1900`.
  (a) 2xx path runs `body_ok` (JSON parse) — a reset mid-body or a proxy's empty 200 → Err.
  (b) Unknown path does `load_table(...).await?` — if the reload fails, Err without
  checking `snapshot_landed`. (c, PLAUSIBLE) Conflict path checks only the latest
  attempt's snapshot id → a late-landing 504 attempt gets committed twice (append dup).
- On Err, `finalize`/`cdc_commit` sweeps data/delete/manifest files of a live snapshot →
  table unreadable.
- **Fix:** any 2xx = Done regardless of body; remember every attempt's snapshot id and check
  all on each reload; if outcome can't be settled, return an "unknown outcome" error that
  **skips the sweep** (orphans are cheaper than a broken table).

### P0-6 · NUMERIC rescale: unchecked `10i128.pow` → divide-by-zero panic / garbage · **READ**
- `wire/pgcopy.rs:346-348` `numeric_to_scaled_i128`. `dscale` comes off the wire (≤16383).
  `Less` branch: `acc / 10i128.pow(k)`; for k=128, `10^128 mod 2^128 = 0` → **panic in
  release** (division by zero always panics). 39 ≤ k < 128 → wrong divisor, wrong value.
  The `Greater` branch's `pow` is also unchecked (the `checked_mul` doesn't cover it).
- Trigger: unconstrained `numeric` holding `round(0,200)` → CH `Decimal(18,4)`.
  Callers: `rowbinary.rs:401`, `arrowcol.rs:883`, bqparquet.
- **Fix:** `checked_pow`; `Less` + `None` → result 0 (|acc| < 10^39); `Greater` + `None` → error.
- **RED test:** unit test with a crafted binary numeric (ndigits=0, dscale=200) and one
  with a real digit at dscale=60.

### P0-7 · `APITAP_PARALLEL_APPLY=1` never applies the first window · **READ**
- `logbased/run.rs:1887` receives `o`; the branch at `:1979` starts its own
  `win_rx.recv()` loop (`:1994`) and never pushes `o`. Later windows' marks advance state
  and slot past it → **W1 lost**. With one window, `:2060` confirms `o.id.end()` with no
  watermark → every later run fails "watermark BEHIND the slot".
- Also: gated on `Dest::Ch` only, not insert-only (doc `:2345` says otherwise);
  concurrent windows share and TRUNCATE one key table (`dest_ch.rs:1787`); delete+insert
  windows are not commutative.
- The flag is undocumented, but it exists in a shipped wheel.
- **Fix (minimum, ship now):** refuse the flag unless the destination is proven
  insert-only/changelog, and push `o` into `inflight` before the inner loop. Per-window key
  table before enabling it anywhere else.
- **RED test:** e2e with exactly one window and with three windows; assert every row present.

---

## 3. P1 — correctness/robustness, next

| # | Label | Where | Defect → fix |
|---|---|---|---|
| P1-1 | AGENT | `logbased/dest_ch.rs:767`, `:835-836` | Insert-only TOAST readback `SELECT … WHERE pred` with no `FINAL`/order → reads an arbitrary version or a tombstone, then deletes all and writes the stale value. **Fix:** `FINAL … AND _apitap_deleted=0` (or `ORDER BY _apitap_ver DESC LIMIT 1`, tombstone = missing). |
| P1-2 | READ (mysql) / AGENT (ch, bq) | `sink/mysql.rs:1327-1337`; `sink/clickhouse.rs:2172-2182`, `:2147`; `sink/bigquery.rs:2856-2863`, `:2140-2178` | Append: data committed, then a fallible `DROP staging?`, *then* `write_state` → on error the batch is re-appended next run. CH refuses DROP above `max_table_size_to_drop` (50 GB default) → deterministic for big batches; the default replace branch also `?`s its old-table drop. BQ copy jobs lack a client job id and `poll` has no transient retry. **Fix:** state right after the data commit (MySQL: same transaction), cleanup best-effort, CH drops of own tables with `SETTINGS max_table_size_to_drop=0`, BQ job-id + adopt-on-409. |
| P1-3 | READ | `logbased/mysource.rs:650-672` vs `drain.rs:44-66,480-541` | MySQL CDC has **no** per-transaction cap; README:376-379 claims 256 MiB for both. A 2 GB UPDATE OOM-loops forever. **Fix:** port `tx_bytes`/`tx_buf_limit()`/`tx_too_big`. |
| P1-4 | READ | `logbased/run.rs:1734-1741`; `myrun.rs:281-296` | Follow mode never checks `shutdown::requested()` → after SIGTERM, a tight `pg_current_wal_lsn()`/`SHOW BINARY LOG STATUS` loop until the follow deadline. **Fix:** add `|| crate::shutdown::requested()` at loop top and the `!hit` branch, both lanes. |
| P1-5 | AGENT, repro first | `source/mysql.rs:945-949`, `:1087-1092` | `INT UNSIGNED` PK (Laravel `increments`) is an int cursor, but MIN/MAX decode as `Option<i64>`; sqlx-mysql rejects UNSIGNED → every `parallel>1` / `read()` fails. BIGINT UNSIGNED > i64::MAX also. **Fix:** `CAST(MIN(c) AS CHAR)`, parse to i128, split in i128 (with P1-6). |
| P1-6 | READ | `source/mod.rs:95-108` `spans()` | `hi - lo + 1 + n - 1` overflows i64 (`lo=-1, hi=i64::MAX`, hash ids). Release wraps → negative step → every span empty → **"successful" rows=0 run**. **Fix:** i128 arithmetic; test `(i64::MIN, i64::MAX)`. |
| P1-7 | READ (cdc side) / AGENT (effect) | `logbased/dest_pg.rs:534,582,706,818` vs `sink/postgres.rs:622-624` | CDC uses unqualified `_apitap_state` (search_path → `public`), bulk uses `"<schema>"._apitap_state`. For `sales.orders` a bulk replace never clears the CDC watermark → resume from a pre-replace LSN. **Fix:** CDC qualifies with the resolved schema; bare key only for the default schema. |
| P1-8 | AGENT ×2 | `wire/arrowcol.rs:950-962`, `seal()` `:249`; `py-apitap/src/capsule.rs:113,286` | Utf8 Arrow columns are never UTF-8 validated (old audit P1-7, still open). `SQL_ASCII` PG data → invalid `u` buffers into pyarrow/polars (UB in consumers). **Fix:** `simdutf8`/`from_utf8` per sealed buffer; error naming the column or demote to Binary. |
| P1-9 | READ | `wire/mywire.rs:318-346` `parse_handshake` | Guard `p.len() > pos+3` weaker than reads (needs pos+6; `&p[pos..end]` can start past len). Pre-TLS → on-path attacker crashes the job even with verify_identity. **Fix:** checked reader (as in `mybinlog` `R`), add to `torture.rs`. |
| P1-10 | READ (width) / AGENT (rest) | `wire/rowbinary.rs:103-114`, `:403-405`, `:370` | `Decimal(P>38)` mapped to width 16 (Decimal256 is 32) → stream misalignment; `v as i32/i64` wraps when source widened after bootstrap; `RbType::Bool => out.push(f[0])` panics on empty field. **Fix:** `None` for P>38 (TSV fallback), `try_from`, `f.first()`. |
| P1-11 | READ | `sink/s3.rs:399`, `sink/iceberg.rs:247` | `String::truncate(400/500)` panics off a char boundary (non-ASCII key/table names echoed in error bodies) — in Iceberg's Unknown arm this panics *instead of* running `snapshot_landed`. **Fix:** `chars().take(n)` as `gcs.rs:220` does. |
| P1-12 | AGENT | `source/clickhouse.rs:610-611,646-670` | CH source counts and streams in two queries with no shared snapshot → live tables / replicas behind an LB fail `got != expected`. **Fix:** drop the count; detect truncation in-band (trailing-bytes check `:637` + exception header). |
| P1-13 | AGENT | `logbased/run.rs:1242,1258`; `myrun.rs:298` | MySQL lane uses the full window budget but now overlaps apply → depth+2 full windows resident (~3× counter ≈ 260 MB in the 256 MB cage). **Fix:** `cdc_window_budget()` or the PG pending/applied gate. Measure RSS before/after. |

---

## 4. P2 — hardening

| # | Label | Where | Item |
|---|---|---|---|
| P2-1 | PLAUSIBLE | `logbased/drain.rs:300` | Open streamed (proto v2) tx blocks caught-up detection → one-shot drain waits `window_max_secs` (1 h). |
| P2-2 | AGENT | `shutdown.rs` `Guard::install`; `run.rs:1530,768-779` | With `slots=N`, a late group's guard install `clear()`s an earlier SIGTERM. Arm one guard in `run_sloted`. |
| P2-3 | PLAUSIBLE | `logbased/dest_my.rs:130-149` | `_ap_del` key twin built without CHARACTER SET/COLLATE → error 1267 or full scans on pre-existing tables. |
| P2-4 | AGENT | `dest_pg.rs:331,395`; `dest_my.rs:206,260` | Masked update / rekey don't check `rows_affected()`; CH and BQ fail on a missing row, pg/my succeed silently. |
| P2-5 | AGENT | `source/clickhouse.rs:350,506-509` | `engine` stored on the shared `ChSource`, raced by concurrent `probe`s in `run_many`. Move into `TablePlan`. |
| P2-6 | AGENT | `source/clickhouse.rs:219` | `count_rows` varint `pos + used + len` wraps → infinite loop on a hostile body. `checked_add`. |
| P2-7 | PLAUSIBLE | `sink/s3.rs:540-575` | `ListMultipartUploads` ignores `IsTruncated`, zips `<Key>`/`<UploadId>` by position, no XML entity decode. |
| P2-8 | PLAUSIBLE | `wire/walsender.rs:137`, `pgoutput.rs:330`, `drain.rs:152` | Retained frames pin whole 1 MiB scan windows; budget charges only `frame.len()` → sparse tracked traffic can pin GBs. Copy out small retained frames. |
| P2-9 | AGENT | `wire/mywire.rs:414-418` | `verify_identity` trusts only `webpki_roots`; no `ssl-ca=` → private-CA servers forced to `required`. |
| P2-10 | AGENT | `mybinlog.rs:812`, `:453-488`, CRC; `bqparquet.rs:303` | Old open items: `decode_decimal` p=s=0 panic / `precision-scale` underflow (R11), negative TIME2 fraction (R4), binlog CRC32 negotiated but never verified, unchecked `us + PG_EPOCH_MICROS`. |
| P2-11 | AGENT | `logbased/dest_ch.rs:1257-1262` | Failed engine-detection query swallowed into `""`. |
| P2-12 | AGENT | pipeline/mod.rs (~`:700`) | Staging discard/release only in the error arm; a dropped/cancelled future leaves the lock → next run `locked:`. Drop guard + per-call cancel token. |
| P2-13 | AGENT | sqlx lanes (`source/postgres.rs:41-55`, `source/mysql.rs:761-770`, `postgres.rs:1294`) | No TCP keepalive / statement timeout → half-open COPY can hang forever (raw planes have keepalive). |

---

## 5. Performance opportunities (measure each in the 0.5 cpu / 256 MB cage)

1. **Zero-copy upload bodies** — `s3.rs:381`, `gcs.rs:240,296`, `bigquery.rs:1556` clone
   every ≥8 MiB part per attempt: an extra memcpy of all output + 8–16 MiB unpriced RSS
   per pipe. Hand reqwest `bytes::Bytes`.
2. **PG/MySQL CDC upsert: load straight into the target** after the delete phase
   (`dest_pg.rs:242-275`, `dest_my.rs:151-186`) — removes a full second row copy; est.
   30–40 % of the upsert phase.
3. **CH residue tail batching** (`dest_ch.rs:754-867`) — ~3×N serial round trips per
   TOAST/rekey op → ~3 statements per window.
4. **MySQL binlog per-cell/per-event allocation** — `decode_cell` returns a fresh `Vec`
   (`mybinlog.rs:343`), `nullbm.to_vec()` per row (`:325`), `TableMap`/`Relation` cloned
   per rows event (`mybinlog.rs:964-1000`, `mysource.rs:640-672`). Cache
   `Arc<Relation>` per table id; decode into the frame. Est. 5–10 % my→ch drain CPU.
5. **CH source `count_rows` double parse + double copy** (`source/clickhouse.rs:236-256`)
   and the per-span `count()` (drop with P1-12).
6. **Buffer recycling for non-PG sinks** — `pipe.reclaim()` returns `None` except the PG
   loader; every 4 MiB chunk is a fresh zeroed alloc (the ~13 % page-zero cost profiled
   before).
7. Minor: `MyWire::read_packet` zero-fills via `resize` (`mywire.rs:686`); bqparquet UUID
   `format!("{:02x}")` ×16 (`bqparquet.rs:314`); BQ JSON per-cell `Map` + key clones
   (`dest_bq.rs:1386-1430`); `ChStore::types()` clones the map per insert; BQ `bearer()`
   tokio Mutex per chunk (`bigquery.rs:221`); split buffers instead of `drain().collect()`
   (`bigquery.rs:1435-1446`, `gcs.rs:870-900`); parallel S3/GCS finalize copies.

**Do not benchmark `APITAP_PARALLEL_APPLY` until P0-7 is fixed** — any A/B number from it
so far skipped window 1.

---

## 6. Style, scalability, robustness — scorecard

| Area | Grade | Short reason |
|---|---|---|
| Style | **B−** | Modern trait design (RPITIT + `routes!` monomorphization) and Rust-conventional naming; but a 4-variant all-`String` error enum, 23 fns > 150 lines, 9 files > 2 000 lines, rustfmt not applied, no lint config, and **no cargo in CI**. |
| Scalability | **A−** | Every channel bounded, fair FIFO grant on tables in flight, sink residency priced in the plan, 1000 tables = cheap futures. Ceiling is the deliberate one-decoder-per-slot CDC; MySQL CDC is one core. |
| Robustness | **B** | Restart-idempotent bulk (staging + swap + watermark in-tx), keepalive on raw planes. Weak: retry is hand-rolled 6 ways with no jitter and classified by **substring matching** on flattened errors; sqlx lanes have no timeouts; 35 `APITAP_*` knobs parsed ad hoc, junk silently ignored; errors rarely name the table/position. |

**Measured facts (2026-10-14):**
- `cargo clippy -p apitap-core --all-targets` (VPS): 51 unique warnings, 102 total. Real
  ones among the noise: never-read `t_trunc`/`t_ins`/`t_clear`, dead `VerifyCa` variant
  and `nul_str`, unused imports, `PgRead` more private than `FrameScanner::next_raw`.
  The rest is cosmetic (38 doc-indent, lifetimes, `div_ceil`, `is_multiple_of`).
- `cargo fmt --all --check`: **63 files, 1209 diff hunks** (top: `dest_ch.rs` 92,
  `dest_bq.rs` 82, `naming.rs` 79, `run.rs` 56). No `rustfmt.toml`.
- `cargo audit`: 5 advisories, all `quinn-proto 0.11.16` (RUSTSEC-2026-0338…0342). It is
  only in `Cargo.lock` via reqwest's optional `http3`; `cargo tree -i quinn-proto
  --target all` prints nothing → **not compiled in, not exploitable**. Still run
  `cargo update -p quinn-proto` to clear it. Yanked: `chacha20 0.10.1`, `spin 0.9.8`.
- Non-test code: ~119 `.unwrap()` + 135 `.expect(` (≈100 realistic panic sites after
  mutex-poison and fixed-width `try_into`), 485 numeric `as` casts vs 28 checked
  conversions, 41 `unsafe` lines under `#![deny(unsafe_op_in_unsafe_fn)]`, 447 tests.

**Style/robustness work, in order (each its own commit, no behaviour change unless stated):**
1. CI: add `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test` (needs a
   workflow file — **tell the owner first; the PAT cannot push `.github/workflows/*`**).
2. One `cargo fmt --all` commit, alone, nothing else in it. Then fix clippy to zero and
   add a `[lints]` table.
3. `Cargo.toml:9` comment says MSRV 1.75, `rust-version = 1.94` — fix the comment.
4. Typed error: `ErrorKind` + `table`/`position` context + `#[source]`; classify
   transience where the error is born (SQLSTATE, HTTP status, CH `Code: N`), then delete
   the substring matchers (`bigquery.rs:1080-1093,1044`, `dest_ch.rs:1454,1540,1898`,
   `clickhouse.rs:211`, `sink/postgres.rs:190,659`). Large; do after P0/P1.
5. One `crate::retry` (policy, classifier, jittered backoff, attempt count in the error).
6. `Knobs::from_env()` parsed once at entry; reject junk naming the variable; stop reading
   knobs mid-apply (`dest_ch.rs:1258,1300`).
7. Misplaced rustdoc: `transfer` (lib.rs:375) has none — its contract sits on private
   `reported` (lib.rs:352-357); error.rs:5-6 doc on the wrong variant.
8. Python mapping: `InvalidInput`/`Connect` → `ValueError`/`ConnectionError`, not
   `RuntimeError` (`py-apitap/src/lib.rs:44-45`).
9. Per-run `Arc<Progress>` instead of process-global counters (`progress.rs:37-49`).
10. Blocking `JoinHandle::join()` in async (`run.rs:767`) → `spawn_blocking` or drop it.

---

## 7. Checked and found sound (don't re-review)

- PG/MySQL CDC fences (lease row FOR UPDATE in the apply tx), CH pinned-deadline fence
  and group mark count check, BQ fence-first tx + `pack_whole_groups` + `bq_ident`,
  Iceberg window+watermark in one commit.
- Slot confirmed only up to `applied`, incl. keepalive replies; `wait_for_apply`
  cancel-safe; overlap shutdown joins before tenure release; collapse rules and key
  injectivity; replay seq overflow checks; signal handler async-signal-safe.
- Walsender framing, SCRAM, the walsender auth-downgrade guard, `MAX_FRAME`/`MAX_PAYLOAD`.
- All 17 `unsafe` unchecked reads in `arrowcol.rs:786-953` (bounds proven against the
  same slice); capsule release/double-release guard; `mallopt`; `set_keepalive`.
- PG/MySQL identifier quoting; pg residue binds; MySQL `sql_lit` escaping; watermark
  literal escaping per dialect; polars pushdown renderer rejects `\` and NUL.
- SigV4; no `Debug` on secret-holding structs; `urlerr::sanitized`; CH basic auth not in
  URL; GitHub token pinned to `api.github.com`.
- vendored `sqlx-core` differs from 0.8.6 by one constant (`DEFAULT_BUF_SIZE`).
- **Not** worth unifying: CDC `apply_unit` across engines (~20 % similar — same shape,
  not copy-paste). **Is** worth unifying: the `finalize → finalize_inner → release`
  wrapper (×6) and lease/announce stubs (×7) — drift risk on a correctness invariant.

---

## 8. Suggested commit order

1. P0-1, P0-6, P1-11, P1-9, P1-10 — small, local, unit-testable panics/injection.
2. P0-3, P0-4 — TLS defaults (one shared helper per dialect) + release note: "remote
   URLs with no sslmode now verify".
3. P0-7 (refuse the flag first), P0-2, P1-1 — CH insert-only correctness.
4. P0-5 — Iceberg commit outcome.
5. P1-2 … P1-8, P1-12, P1-13.
6. Gate (`benchmarks/gate.py`) on the wheel you ship; then P2 and §6.

After each group: sync → touch → `cargo test -p apitap-core` on the VPS, RED-before
evidence in the commit body, drop QA destination tables.
