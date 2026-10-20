# apitap — deep review, round 3 (2026-10-15)

**For the implementer.** Round 3 reviews the 20 fix commits `dc8bf4e..54a29fd` that landed
after `docs/review/2026-10-15-review-round2.md` ("round 2"). Read round 2 and the round-1
handoff (`docs/review/2026-10-14-handoff-review-rust.md`) first; their §0 rules still apply
(VPS-only builds, QA rig `~/apitap-qa`, touch before cargo, RED-before-GREEN, ask the
server, 1000-row budget, no Claude trailer, never prune docker).

Tree reviewed: `main` at `54a29fd`. Labels: **REPRO** reproduced on the VPS · **READ** the
lines were re-read · **PLAUSIBLE** mechanism real, impact needs a repro.

**Verdict: not production-ready.** Most round-2 items are now closed, and the closures
are verified on the VPS. But one fix (`ffb3338`) **breaks MySQL/MariaDB CDC for ordinary
tables** (REPRO on 8.0.46 and MariaDB 10.6.28). A new REPRO shows `read()` silently reads
the **wrong server** when the URL has `?host=`. The parallel-apply gate checks only
window 1.

---

## 0. Ship-blockers — do these first

| # | Label | Where | Defect | Fix |
|---|---|---|---|---|
| R3-1 | **REPRO** | `wire/mybinlog.rs:332-363` (`catalog_kind`), used at `logbased/mysource.rs:632-650` | `ffb3338` maps the catalog to the wrong binlog bytes. Live servers send DATE as `MT_DATE 0x0a` (not 0x0e), ENUM/SET as `MT_STRING 0xfe` (real type in the metadata; see the decoder's own comment `:567-571`), and every TINY/MEDIUM/LONG TEXT/BLOB as `MT_BLOB 0xfc` (size in the metadata). Every drain after bootstrap refuses tables with DATE, ENUM, SET, TINYTEXT, MEDIUMTEXT, LONGTEXT or LONGBLOB. Re-bootstrap does not help (refused again), so the lane is stuck for good. Hits the Paysera MariaDB trial. VPS: `bench.qa_r3_cdc_date column 1 (d) is type 0x0e in the source catalog, but the binlog events … describe it as 0x0a`. Control tables (INT/VARCHAR/TEXT/CHAR/DATETIME/DECIMAL/BIT) pass, 21 = 21. | date→`MT_DATE`; enum/set→`MT_STRING`; all text/blob sizes→`MT_BLOB`. For `MT_STRING`, compare the real type from the metadata. Add an **e2e test on a real server** with exactly these columns. The unit test pinned the wrong bytes. |
| R3-2 | **REPRO** | `source/postgres.rs:687`, `wire/walsender.rs` `parse_url` / `connect_sql` (`:684`) | The raw COPY plane and the replication connection dial the URL authority and ignore `?host=`, `?hostaddr=`, `?port=`. sqlx (planning) honours them. VPS: `postgres://…@localhost:5544/postgres?host=172.17.0.1&port=55432` → planning hit the intended server, but the **rows came from `localhost:5544`** (`SRC-wrong-host`) at parallel None/1/4. pg→ch Transcode rides the same plane. CDC slot/stream: same `parse_url` (READ). | Resolve the effective host/port once (the same last-wins rule as sqlx), and have every plane consume it. Alternatively, refuse URLs with `host=`/`hostaddr=`/`port=` query keys on the raw and replication planes. Add a test that compares the planes' targets. |
| R3-3 | READ | `logbased/run.rs:1905-1919` vs `:2061-2066` | The P0-7 gate (`parallel_ok` incl. `o.bodies.parallel_safe()`) is evaluated once for the **entry window**. Later windows from `win_rx.recv()` enter `inflight` with no check. Window 2 with residue (TOAST masked update) can read back before window 1 lands → stale value at a higher `_apitap_ver`. A truncate window's `clear_owned` can race window 1's inserts → pre-truncate rows survive. Silent. | Check `parallel_safe()` on every received window. On a non-safe window, drain `inflight`, then apply serially. Test at the arm level, not just `Bodies::parallel_safe`. |
| R3-4 | READ | `sink/bigquery.rs:2864-2881`, `dest_state` `:2702` | R2-2 is still open on BigQuery: `74caa8b` doesn't touch BQ. A flipped bootstrap still does `copy_stagings_into_final(.., "WRITE_TRUNCATE")` plus a `*` barrier. Two first-run fan-in appends → B truncates A's rows and erases A's watermark, and both report success. | Use `WRITE_EMPTY` (or refuse) when `flipped_bootstrap(..)`. |
| R3-5 | READ | `sink/clickhouse.rs:2002-2023` then `:2040-2058` / `:2066-2104` / `:2160-2175` | The CH R2-2 fix is check-then-act: probe `system.tables`, then `CREATE IF NOT EXISTS` + `EXCHANGE` + `DROP staging`. If a peer publishes in between, the EXCHANGE swaps the peer's table into staging and the DROP deletes it. In the engine branch it ATTACHes into the peer's table and `DELETE _apitap_state` wipes its watermark. ON CLUSTER (the Paysera shape) widens the window. | On a flipped bootstrap, publish with `RENAME TABLE staging TO final`, which fails atomically if the target exists. |
| R3-6 | READ | `logbased/dest_ch.rs:850-917` | R2-4 is only partly closed. `517a740` removes the new-key duplicate, but replay readback at a *reused* old key returns the later image. Case `Rekey 1→2; INSERT 1(X)`: key 2 gets X's TOAST value, silently. Chain `Rekey 1→2; Rekey 3→1`: the replay deletes C, then `continue`s on missing 3 → **row C lost**. A crash between the old-key delete (`:893`) and the insert → readback misses → TOAST row lost. The comment `:899-902` ("same image") is false here. PG/MySQL: R2-4 still open (`run.rs:1242`). | Make the readback version-aware (read at `_apitap_ver < window`), or refuse to replay a window whose Rekey old key has a later image. Test the chain. |
| R3-7 | READ | `sink/iceberg.rs:1347-1416` (bulk), `:2063-2112` (CDC), sweep at `:1265-1269`, `:1976-1979` | Once `ambiguous = true`, any `?` in a later attempt (S3 GET of the manifest list, `write_manifest_avro`, `put_object`) still exits as `Err` → sweep deletes the data files. If attempt 0 lands late, the table references deleted files. On CDC the LSN rode that commit. `59eb1b0` documents this as a residual. | Wrap the attempt body: after ambiguity, map every Err to Unknown. Add a fault-injection test (504, then 503 on GET). |

## 1. P1

| # | Label | Where | Defect / fix |
|---|---|---|---|
| R3-8 | READ | `wire/walsender.rs:689-756`, `:487` | Regression from `dc8bf4e`: the replication connection defaults to `VerifyFull` off-loopback but ignores `PGSSLMODE`, `PGSSLROOTCERT`, `?ssl-root-cert`/`?ssl-ca`, and trusts only webpki roots. RDS/self-signed + `PGSSLMODE=require` or `?ssl-ca=` → the pool works but CDC fails `UnknownIssuer`. `PGSSLMODE=disable` + plaintext server → CDC fails. The README:169 promise is false for CDC, and the CDC error has no opt-out hint. The raw COPY plane has the same blindness: VPS shows `PGSSLMODE=disable` still sends 3 SSLRequests then falls back to sqlx (perf loss). Fix: one shared TLS resolver (env + URL keys + root store) for pool, raw plane, walsender. |
| R3-9 | READ | `dialect/postgres.rs:18,27-31` | `effective_host` doesn't percent-decode the authority. `postgres://%2Fvar%2Flib%2Fpostgresql/db` (a sqlx-supported socket form) gets `sslmode=verify-full` → hard fail on the socket. |
| R3-10 | PLAUSIBLE | `source/clickhouse.rs:613-660`, `sink/clickhouse.rs:491-500` | `0320914` dropped the per-span `count()`. A CH user/profile with `limit`, `max_result_rows`/`max_rows_to_read` + `*_overflow_mode='break'`, or `max_execution_time` + `timeout_overflow_mode='break'` ends the stream at a block boundary with HTTP 200 → a short table is swapped in as success. Fix: pin `limit=0, offset=0`, every `*_overflow_mode=throw`, on `query_stream`. Update the stale docs `:12-16`. |
| R3-11 | READ | `source/clickhouse.rs:536-543` | UInt64 cursor min/max parsed as `i64` → parallel CH read of `cityHash64`-style ids fails outright. Parse i128 and fall back to one span, like MySQL (`9503105`). |
| R3-12 | READ | `sink/iceberg.rs:582-588`, retry `:1337` | R2-2 on Iceberg: a flipped bootstrap is an Overwrite, and the CAS loser's retry overwrites the winner (the module comment admits it). Refuse on a flipped bootstrap when the table now has a snapshot. |
| R3-13 | READ | `pipeline/mod.rs` ~`:729` (79d9bfe) | The new post-announce refusal and the `?` on the re-read `dest_state` run after `prepare` created staging, but call only `release_lock()`, not `discard()`. On PG the leftover staging makes every later run refuse as "locked" (the code's own comment). Route through the discard arm. |
| R2-7b | READ | `dest_bq.rs:2063,2087`, `bigquery.rs:856` | Still open: CDC staging is one name per (table, run) with WRITE_TRUNCATE per window → 1500 table-ops/day quota exhausted in hours in follow mode. (The 100-tables/tx half is fixed: 95-group cap, test RED-checked.) |

## 2. P2

- **R3-14** (`run.rs:1923`) `WARNED` is a process-global static, so a long-lived Python process warns once ever. Progress `add_rows` only runs after the arm exits: the counter is frozen for the whole parallel drain.
- **R3-15** `dialect/postgres.rs:13-20`: `effective_host` is first-wins and prefers `hostaddr`; sqlx is last-wins. `?host=localhost&host=prod` → judged local → `prefer`. `postgres:///db` + remote `PGHOST` → untouched → sqlx `prefer`. `postgres://u:p@/db?host=/sock` → `empty host` plus a wrong "not local" hint. The TLS hint is appended to every connect error, including a wrong password on `sslmode=disable` (`source/postgres.rs:59`, `sink/postgres.rs:466`, `run.rs:863`, `dest_pg.rs:485`).
- **R3-16** MySQL binlog plane (`wire/mywire.rs:131-180,402-403`): ignores `ssl-ca` (webpki only; P2-9 still open), `socket=` (PLAUSIBLE), and `verify_ca` (the README lists it). `myrun.rs:137` has no `tls_required_hint`, so MariaDB without TLS fails CDC with a bare error.
- **R3-17** polars pushdown (`py-apitap/python/apitap/__init__.py:387,494-496`): integer overflow (polars wraps; MySQL computes in BIGINT; PG errors) → lost rows; boolean operands in ARITH/CMP → PG type error; a Float32-typed literal without a Cast slips through; a server error on pushed SQL is raised instead of falling back.
- **R3-18** gsheets (`source/gsheets.rs:123-135,349-353`): `grid_rows` swallows every error as -1, so it silently falls back to the old stop-at-empty-page rule. Exact title match. 1M allocated rows ≈ 100 requests > 60/min quota, and there is no 429 retry (R2-29).
- **R3-19** fork: `naming.rs:345-353` `BASE`/`NEXT` are inherited across `fork()` → identical staging tokens in children. `lib.rs:57`: a fork while another thread holds the `RT` mutex deadlocks the child. `capsule.rs:384` `export_batch` runs outside `catch_unwind`. The GIL half of R2-10 is still open.
- **R3-20** CH stream marker: tables that store `DB::Exception` text are refused permanently (false positive). Exceptions > 512 bytes over fixed-width schemas could slip through (narrow).
- **R3-21** BQ `close_run` fence UPDATE runs under `cdc_script`'s 30-min deadline with `let _` → `table_delete` can run while a zombie is mid-tx (PLAUSIBLE, narrow).
- **R3-22** Iceberg: a spec of only `void` transforms is refused needlessly (`iceberg.rs:1139,1889`).
- **R3-23** Cosmetic: lost `\` line continuations leave 10–26-space gaps in errors at `dialect/postgres.rs:89`, `sink/iceberg.rs:1141`, `sink/mysql.rs:1297`, `bigquery.rs:560`, `pipeline/mod.rs:784`. `source/mysql.rs:268-276`: `le` lost `#[inline]`. `github_api.rs:11,806,815-818` still documents commits as incremental, and the error renders `cursor="?"`.

## 3. Test quality (VPS, `~/qa-r3-mutA.log`, `~/qa-r3-mutB.log`)

- Suite at HEAD: **463 passed**, 3 ignored (baseline 452). The workspace is green.
- **All 9 named helper tests go RED** when their helper logic is reverted (spans, unsigned bounds, catalog kinds, PG TLS ×2, parallel_safe, flipped_bootstrap, watermark_moved, 95-table cap).
- **Call sites are unpinned:** reverting only the call site keeps 463/463 green for 9503105 (MIN/MAX decode), ffb3338 (drain check), 29f52a9 (`&& parallel_safe()`), 74caa8b (all 3 sinks), 79d9bfe (re-read refusal) and dc8bf4e (4 sites). R3-1 and R3-3 are exactly the bugs that helper-only tests let through.
- **No test at all:** 59eb1b0 (iceberg ambiguous), 517a740 (rekey delete), 54a29fd cancel-and-settle.
- `cargo clippy --all-targets -- -D warnings`: **102 errors** (`~/qa-r3-clippy.log`), mostly older style lints. This round added "items after test module" at `dialect/postgres.rs:103` and in `source/mysql.rs` / `pipeline/mod.rs`, plus an unused import `sink/mysql.rs:1481`.
- `cargo fmt --check`: **1229 hunks** (the tree was not fmt-clean before either). Either adopt fmt in one mechanical commit, or drop the claim.

## 4. Closed and verified this round (don't re-review)

| Item | Evidence |
|---|---|
| P1-5 / R2 UNSIGNED PK `read()` | REPRO-fixed: INT UNSIGNED 1000 rows, `read()` default + parallel=4, transfer p=1/p=4 → 1000/1000. BIGINT UNSIGNED near 2^64 and across 2^63 → exact, `decimal128(20,0)`, single-pipe fallback. No `Option<i64>` unsigned decode left. |
| P1-6 spans | REPRO-fixed: pg {MIN,-1,0,1,MAX} → 5 rows at parallel None/4/16; pg→pg pipes=4 → 5 distinct; MySQL same. |
| R2-8 pushdown (3 shapes) | REPRO-fixed: Float32 ==, `&`, `%` → `pushed=None` and the lazy result equals eager; `id > 2` still pushed. |
| Round-1 CH identifier injection | REPRO-fixed: backtick+backslash column stays literal, no `injected_col`. |
| R2 §1.1 socket/`?host=` (pool) | REPRO-fixed through a logging relay: socket plain and `%2F` in `?host=` OK; `?host=remote` and `?hostaddr=remote` behind localhost → TLS required; `PGSSLMODE=disable` respected by the pool. |
| P0-6 numeric, P1-11 siblings | `dscale=200` test present; `dest_bq.rs:2226` uses `char_excerpt`; no byte-slicing of user strings left in `crates/`. |
| R2-3 | Holds (announce → check_peers → re-read), apart from R3-13's cleanup. |
| R2-5 | Partitioned tables refused in `prepare` before announce; `cdc_bind` re-checks per window. |
| R2-6 | Cancel + settle; unknown → no new-id retry; `close_run` fence claim queues behind any zombie tx. |
| R2-7 (100/tx), R2-9, R2-11, R2-10 catch_unwind/fork-runtime | Held up under review. |
| R2-2 on PG / MySQL | RENAME-without-DROP / plain RENAME are atomic. |

## 5. Still open from earlier rounds (untouched at HEAD)

R2-4 (PG/MySQL), R2-12, R2-13, R2-16, R2-17, R2-18, R2-19, R2-20, R2-21, R2-22, R2-23,
R2-24, R2-25, R2-26, R2-27, R2-28, R2-29, R2-30, P1-12 (now R3-10), P2-9 (now R3-16), and
the handoff/pass-2 items not listed as closed above.

## 6. Order of work

1. **R3-1** — fix `catalog_kind` before any MySQL/MariaDB release, plus a real-server e2e
   (DATE, ENUM, SET, LONGTEXT, LONGBLOB) on MySQL 8 and MariaDB 10.6. If time is short,
   revert the drain check in `ffb3338`: being stuck is worse than R2-1's narrow window.
2. **R3-2, R3-8** — one effective-host + TLS resolver shared by pool, raw COPY plane and
   walsender.
3. **R3-3** (gate every window), **R3-6** (rekey replay), **R3-7** (Iceberg Err→Unknown).
4. **R3-4, R3-5, R3-12** — R2-2 on BQ / CH / Iceberg; then the 2-concurrent-bootstrap e2e
   that "rides the next gate run" — run it.
5. **R3-13, R3-10, R3-11, R3-9, R2-7b**.
6. Call-site tests for every helper in §3, plus tests for 59eb1b0 / 517a740 / 54a29fd.
   Each must be seen RED with the call site reverted.
7. §2 P2s, §5 leftovers, clippy/fmt.

VPS artefacts: scripts `~/qa-r3-scripts/` (`repro.py`, `tls.py`, `cdc.py`, `proxy.py`),
logs `~/qa-r3-*.log`. All QA tables and state rows dropped; mirror restored. **Note:**
`~/qa-venv` is now an editable install of 0.61.0 whose `.so` lives untracked in
`~/apitap-qa/py-apitap/python/apitap/`, so the next `rsync --delete` removes it. Rebuild
after syncing (`maturin develop --release`, ~9 min).
