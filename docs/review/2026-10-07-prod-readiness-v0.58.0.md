# Production-readiness review of v0.58.0 — 2026-10-07

**Scope.** Read-only static review of the tree at `0.58.0` (HEAD `4813d18` at review
time; workspace `Cargo.toml:6`), cross-checked against the project's own dynamic
evidence: the release gate on the PGO wheel the tag ships (`benchmarks/cdc-steady-30t-0.58.md`
§B2.8/§B2.8b and the release commit `1a8622c`), the §6.5 pull-back verification
(PyPI `.so` md5 identical to the gated wheel), `gate.py --self-test` / `--matrix`,
the v0.56.0 audit (`2026-09-19-prod-readiness-v0.56.0.md`), the v0.57.0 handoff spec
(`2026-09-27-handoff-v0.57.0.md`), and `~/apitap-057-checkpoints.log` on the bench
VPS. **No build, no run**: execution stays on the rig, as before.

**Calibration.** This review re-verified every one of the 23 findings of the
2026-08-18 audit (0.42.0) against the current code, by reading the cited lines —
not by trusting the changelog. Where a finding is marked FIXED below, the fix was
read in place. Where a fix took a *different and better* shape than the audit
suggested, that is said explicitly.

---

## 1. Verdict

**READY.** The three defects that held the 0.42.0 verdict back — MySQL CDC type
corruption, transport security, and unattended operability — are closed, and the
structural work of 0.57.0/0.58.0 (ownership as a value, layouts that travel with
their windows, one replay rule, declared pipe residency) closed the 0.56.0 audit
in a way this review could verify in code, not just in release notes.

| Workload | Verdict | Notes |
|---|---|---|
| Bulk `replace`/`append`/`merge` → pg / my / ch | **READY** | One caveat, documented and measured: wide values (§5.3) |
| Bulk → S3 / GCS / BigQuery / Iceberg | **READY** | Residency is declared and priced (`Sink::pipe_residency`, `plan.rs`); sibling failure stops the crew (`settle_all`, `settle_partial`) |
| Iceberg `merge` | **READY** | Equality deletes stream per data file (0.57.0) |
| CDC replica → Postgres / MySQL / ClickHouse / BigQuery | **READY** | Every write goes through a `Tenure` unit; each engine's fence read in place (pg/my row lock, ch pinned predicate, bq per-run fence table) |
| CDC changelog → ClickHouse / BigQuery | **READY** | `replay_plan` is the one rule; the one-statement mark set is count-checked (`dest_ch.rs:1729-1733`) |
| MySQL/MariaDB-source CDC, TRUNCATE-only windows | **READY** | `TableWindow::seal` makes a body without its layout unrepresentable |
| Upgrading from 0.55.x / 0.56.0 | **READY, with the 0.57.0 conditions** | Upgrade drains before bulk jobs; stop 0.56.0 BigQuery drains before 0.57.0 runs; go straight past 0.56.0 for `changelog=True` (`stability.md:23-25`, `failure-modes.md` upgrade section) |
| Iceberg drains | **unguarded, by design** | `lease.rs:67`, `stability.md:25` — one drain per Iceberg table is the scheduler's job |

Dynamic evidence, for the record:

- **The gate that ships: 80/80, 0 skipped in 6993 s** (BigQuery and GTID legs
  included) on the PGO wheel whose installed `.so` md5
  `00ecb13647c6ce791b6ec5be38c9a030` is the PyPI artifact — the §6.5 pull-back
  installed `apitap==0.58.0` in a fresh venv and the md5 is **identical**. The
  re-land non-PGO wheel gated **80/80 in 6935 s** before it (`§B2.8b`).
- Earlier wheels, for the classification record: the QA wheel `07ce71c3` ran
  **79/80 in 7421 s**, its single failure `e2e_my_liveness.py` being the leg's
  rig precondition ("more than one binlog to work with: 1 files") which passes
  standalone on the same wheel, twice; the batch wheel `08106acf` ran 79/80
  with the `e2e_toast_rekey` regression this release fixed.
- 0.57.0, from `~/apitap-057-checkpoints.log`: CP5 **80/80 (6573 s)** and the
  released PGO wheel **80/80 (6610 s)**.
- `gate.py --self-test` PASSED; `--matrix` exit 0, 123 cells, 0 GAP.

## 2. The 0.42.0 audit's findings, resolved

All 23, re-read against 0.58.0:

| # | Status | Evidence |
|---|---|---|
| 1 MySQL CDC type corruption (ENUM/SET/JSON/BIT/TIME) | **FIXED** | Labels parsed from `COLUMN_TYPE` (`mybinlog.rs:139-169`); the live `MT_STRING` path resolves them (`:531-563`); no labels or an out-of-range index **refuses** rather than writing a number (`render_enum:598-611`, `render_set:616-641`); TIME2 sign + complement + 838 h range (`:453-488`); BIT travels as bytes, matching the bulk lane (`:500-520`); JSON is refused by precheck (`mysource.rs:350-364`) |
| 2 MySQL TLS: unverified default, downgrade, cleartext | **FIXED (redesigned)** | `SslPref::Required` = encrypt-no-verify — which is exactly MySQL's own `ssl-mode=required` semantics, stated in the module doc (`mywire.rs:8-9`); `verify_identity` is native, chain **and** hostname (`tls_upgrade:397-409`); `verify_ca` is refused by name with a message pointing at the two correct options (`:151-161`); an explicit `preferred` that cannot upgrade warns once (`:506-522`); default stays `preferred`, like the mainline client |
| 3 Postgres walsender plaintext | **FIXED** | Full TLS via tokio-rustls; `verify-full` checks chain against system roots and the hostname (`walsender.rs:318, 397`) |
| 4 Idle table pins WAL forever | **FIXED** | On caught-up keepalive the window's end moves to `wal_end` and travels the ordinary watermark path (`drain.rs:183-215`) — the comment records the measured trap (confirming without moving the watermark turned 7 gate legs red) |
| 5 MySQL watermark without server identity | **FIXED** | `@@server_uuid` read and carried (`mysource.rs:188-193`); GTID destination leg exists (`e2e_my_gtid_dest.py`, green) |
| 6 Live catalog vs historic TABLE_MAP, no arity check | **FIXED** | `sc.names.len() != map.cols.len()` → loud error naming both counts (`mysource.rs:583-596`) |
| 7 Relation mid-window relabels the window | **FIXED** | `relayout` cuts the window at the previous commit (`drain.rs:226-231, 255-258, 326`); `Layout` travels with each window (`window.rs`) |
| 8 Zero HTTP timeouts | **FIXED** | One shared builder: `connect_timeout` + a total-request deadline, with a doc explaining why `read_timeout` is the wrong tool (`http.rs:3-31, 69-70`); gate leg `e2e_http_deadline.py` green |
| 9 BigQuery identifier escaping | **FIXED** | `bq_ident` vets every part (`sink/bigquery.rs:119, 177-178, 604`) |
| 10 `sql_mode=''` on CDC apply | **FIXED** | `sql_mode='STRICT_ALL_TABLES'` with the reasoning inline (`dest_my.rs:683-692`) |
| 11 No per-value byte cap | **RESIDUAL, documented + measured** | `failure-modes.md:65-78` states it plainly and carries the measured table (a 64 MB value → 1037 MB peak RSS). A cap still does not exist (§5.3) |
| 12 Streamed-tx bytes escape the budget | **FIXED** | The charge lives in `DrainSession` and moves with the ops (`drain.rs:238, 253-269`) |
| 13 ClickHouse without retry | **FIXED (better shape)** | Transport-level retry (4 attempts, ~7 s backoff, `sink/clickhouse.rs:553-557`) — and a **deliberate refusal** to retry ClickHouse statement exceptions, because a half-committed INSERT has no rollback and a retry would double-append the changelog (`:522-530`). This is the correct answer; the audit's "retry TOO_MANY_PARTS" would have been wrong |
| 14 Unbounded wire allocation / unchecked slicing | **FIXED** | `MAX_FRAME = 1 GiB` (Postgres's own protocol cap) on both read paths (`walsender.rs:118-158, 1174-1209`); guarded header slices (`:1364-1397`); SCRAM iteration cap (`:1613-1617`); row capacity bounded by the message (`:903`) |
| 15 cgroup detection only at the root | **FIXED** | Full resolution through `/proc/self/cgroup` + `mountinfo`, smallest limit on the path (`pipeline/mod.rs:156-277`) |
| 16 Watermark literal escaping | **FIXED** | Per-source `cursor_literal` (`pipeline/mod.rs:624-627`; impls in `source/{mod,clickhouse,mysql}.rs`) |
| 17 GitHub Link-header SSRF | **FIXED** | Host **and** scheme pinned to the API host before the token rides again (`github_api.rs:515-527`) |
| 18 Docs claim universal atomicity | **FIXED** | `failure-modes.md` now says "written last, replay idempotent by construction", with the object-store exception spelled out and a "What is NOT covered yet" section |
| 19 Published wheel ≠ tested wheel | **PARTLY** | The process is now documented and the artifacts are hash-recorded (campaign `.so` md5 and the PyPI PGO md5 verified identical in the §6.5 pull-back, `cdc-steady-30t-0.58.md` §B2.8/§B2.8b and the release commit `1a8622c`), but `publish.yml` still uploads a committed `dist/` wheel without verifying it against any gate run, and no CI job builds it (§5.2) |
| 20 Stale `Cargo.lock` | **FIXED (this review's commit)** | The review caught `Cargo.lock:133-134, 170-171` at `0.57.0` against `Cargo.toml:6` `0.58.0` (`cargo build --locked` fails on a clean clone). The lock regenerated by the 0.58.0 release build was taken from the rig and committed with this review (§5.1) |
| 21 No lint hardening / fuzzing | **PARTLY** | `#![deny(unsafe_op_in_unsafe_fn)]` in both crates; `rust-version = "1.75"` declared. Still no fuzz targets, no Miri, no `cargo audit`/`deny`, no CI (§5.4) |
| 22 No tracing / metrics | **NOT FIXED (open)** | Diagnostics are still `eprintln` behind `APITAP_DEBUG`; `progress.rs` remains the only structured surface. Not a correctness item; it is the remaining operability gap (§5.5) |
| 23 Test coverage holes | **LARGELY CLOSED** | 409 unit tests (was 197), 7 in `logbased/run.rs` (was 0, the lane-pool and mark-set tests included), 2 in `sink/mysql.rs`; and above all the 80-leg gate with `--self-test`/`--matrix`. Remaining unit gaps: `drain.rs`, `lib.rs`, the PyO3 boundary, `_predicate_sql` — all exercised end-to-end by the gate, none by unit tests (§5.6) |

## 3. The 0.56.0 audit: closed structurally in 0.57.0/0.58.0

The 0.57.0 handoff claimed five structural changes. Spot-checked in code:

- **Ownership is a value.** `Tenure`/`Held` (`lease.rs:334-491`): every CDC write
  passes `open` → `close`, `release` cannot pass an open unit (the `Held` carries
  the read guard), `Drop` aborts the keeper and abandons announcements without an
  await, and a failed sweep keeps the claim collectable after the TTL instead of
  leaking it (`give_back:506-552`). `owner_verdict` (`:259-264`) fixes the 0.56.0
  "missing row = nothing to fence" hole.
- **Layouts travel with windows.** `TableWindow::seal` is the only constructor and
  takes the layout; mixed row widths inside one source transaction are refused
  with a remediation message (`window.rs:209-239`; tests `:330-373`). The 0.56.0
  TRUNCATE-only-window wedge is unrepresentable now.
- **Per-engine fences, read in place.** pg/my row lock; ClickHouse pinned
  predicate with a monotone deadline (`dest_ch.rs:798-826` — the doc explains why
  the live margin alone is not monotone: an `INSERT … WHERE false` succeeds, so a
  later statement could pass after an earlier was fenced out); BigQuery per-run
  `_apitap_fence<token>` as the script's first UPDATE with `RAISE` on a lost claim
  (`dest_bq.rs:1720-1764`).
- **One replay rule.** `replay_plan` (changelog) — the mark set is count-checked:
  the server's `written_rows` must equal the number of marks or the window refuses
  and replays (`dest_ch.rs:1729-1733`), with a read-back fallback when a proxy
  strips the summary header (`:1757-1770`).
- **Declared residency.** `Sink::pipe_residency` + planner pricing; the lane
  pools are memory-priced with measured OOM receipts (`run.rs:506-549`: 16 lanes
  at 2 CPU peaked 250.6 MB in a 256 MB cage, so 20 MiB/lane resolves 256 MB to 8).

## 4. The 0.58.0 campaign (lane pool + one-statement mark set) under review

This is the newest code and the one that was reverted once. Read end to end:

- A group window applies members through a **bounded** pool (`settle_partial`,
  `run.rs:1836-1856`): `buffer_unordered`, every result survives — a sibling's
  failure does not cancel in-flight members (the exact defect the first gate run
  caught, RED spec and all, `cdc-steady-30t-0.58.md:84`).
- Members close **without marks** and ride back their pin deadline
  (`apply_member_pending:1887-1903`); the group close opens **one** unit over the
  applied members at the **minimum** deadline (`run.rs:1747-1758`), and `repin`'s
  continuity check (`dest_ch.rs:836-840`) refuses the whole close if any member's
  deadline lapsed un-renewed — this is the fix for the revert's cause ("a fresh
  group pin had no continuity"), and it is a real proof, not a re-check.
- The mark set is one `INSERT` through `input()` with per-row lease predicates
  plus the pinned deadline (`state_batch_sql:927-942`); partial = not owner.
- The pin machinery distinguishes the ordinary open (cache reuse allowed, fresh
  read otherwise) from the group close (must continue `prev`) at one site
  (`open_unit_at:1674-1698`), and `repin_due` re-reads at 1/8 of the budget with
  the measured reason (a half-budget rule replayed a minute-long DELETE forever).

Residual risk after reading: the ClickHouse close path is now the most complex
single function in the crate; the compensating controls (count check, continuity,
`--self-test`, the CH gate legs) are all present and green. No new finding.

## 5. Remaining findings at 0.58.0

Ordered by fix cost, not severity — none is a correctness blocker.

### 5.1 [FIXED in this review's commit] `Cargo.lock` was stale (finding #20)
The review caught `Cargo.lock:133-134` (`apitap-core 0.57.0`) and `:170-171`
(`apitap-python 0.57.0`) against `Cargo.toml:6` (`0.58.0`), which makes
`cargo build --locked` fail on a clean clone and points `cargo audit` at a
different graph. The lock regenerated by the 0.58.0 release build (the only
diff: those two version lines) was taken from the rig and committed with this
review. The release commit itself should carry its own regenerated lock next
time; the wheel is unaffected either way.

### 5.2 [medium] The published wheel is still not verified against the gate in CI
`publish.yml` publishes whatever `dist/` holds; the only gates on it are the
py3.9 import smoke and the README scheme check (`publish.yml:1-3, 22, 34-46`).
The real gate runs manually on the rig, and its wheel md5s are recorded in the B2
record — good practice, but operator discipline, not a check. Minimum: pin the
gate-tested md5 in the tag commit and assert it in `publish.yml` before upload.

### 5.3 [medium, documented] Wide values still OOM a capped container
`failure-modes.md:65-78` documents it with a measured table; no cap exists. A
table with a 64 MB value still dies at ~1 GB RSS on a 256 MB worker. Acceptable
only because it is now written down; a per-value refusal with a named remedy
would be better than the OOM-kill.

### 5.4 [low] No fuzzing / Miri / dependency audit (finding #21 residue)
The wire decoders parse untrusted binary into pointer arithmetic; `cargo-fuzz` on
`walsender::read_message` and the binlog tuple builder is still absent, as are
`cargo audit`/`deny`. The caps from #14 make this cheaper to add, not less worth
adding.

### 5.5 [low, open] No tracing or scrapeable metrics (finding #22)
Unchanged since 0.42.0: `eprintln` behind `APITAP_DEBUG`, no spans, no counters.
The gate and the B2 records substitute during development; a fleet operator has
only logs. This is the last 0.42.0 item with no movement.

### 5.6 [low] Unit-test gaps under a strong e2e net (finding #23 residue)
`drain.rs`, `lib.rs`, `py-apitap/src/lib.rs`, `capsule.rs` (FFI) and
`_predicate_sql` still have no unit tests. The 80 gate legs exercise all of them
end-to-end (the gate *is* Python), so the risk is regression-detection latency,
not blindness.

### 5.7 [low, process] Rig preconditions still make some gate runs non-green
The runs inspected across 0.57.0/0.58.0 show a pattern: several non-green runs
whose only failures were rig preconditions (binlog rotation, BigQuery compaction
overlap, the toast-rekey replay scenario), every engine assertion green, the
failing legs passing standalone. The last two full gates on the final engine
(the re-land wheel 6935 s and the shipping PGO wheel 6993 s) were clean 80/80,
so the classification holds rather than the flakes — but the legs' preconditions
deserve deterministic setup (force a `FLUSH BINARY LOGS`, plant the overlap) so
a green gate does not depend on the rig's mood.

## 6. What is genuinely good

- **`lease.rs` reads like the spec it came from** (`:37-70`): ownership as a
  value, the clock that cannot lie (destination-stamped), why a lapse is not an
  eviction, and a per-engine fence paragraph. A reviewer can check the code
  against the doc in one pass, and they agree.
- **The pin's monotonicity argument** (`dest_ch.rs:798-826`) is the kind of
  subtle async-correctness reasoning that is usually missing: why a live margin
  is not a proof, and why a fixed deadline turns "fenced out" into "replays
  loudly".
- **`TableWindow::seal`** is type-driven design done right: the 0.56.0 wedge is
  not fixed, it is *unrepresentable*, and the refusal names the remedy.
- **Everywhere the answer is a refusal, the message names the cause and the
  fix** — ENUM index without labels, REPLICA IDENTITY NOTHING, key not in the
  WAL relation, lost claim. This was already true at 0.42.0; it held through two
  rewrites.
- **The ClickHouse no-retry decision** (`sink/clickhouse.rs:522-530`) is better
  than the audit that requested retries: it refuses to retry what cannot be
  retried safely, and retries what can.
- **The honesty infrastructure**: `gate.py --self-test` proves the gate can fail
  a leg; `--matrix` proves every claim has a leg and a sentence; B2 records carry
  wheel md5s and publish the misses (the settle_all defect, the revert, the
  harness bugs). This is what makes the green runs worth trusting.

## 7. Before the next tag

1. Keep the release commit carrying its own regenerated `Cargo.lock` (§5.1 — the
   0.58.0 one was fixed in this review's commit).
2. Record the gate-tested wheel md5 in the tag commit; assert it in `publish.yml`
   before upload (§5.2).
3. Make the three rig preconditions deterministic (§5.7).
4. Add `cargo-fuzz` targets for `walsender::read_message` and the binlog tuple
   builder; run Miri on the arrow column tests (§5.4).
5. Decide the fate of #22: adopt `tracing` + a metrics counter set, or write the
   "logs only" position into `stability.md` so it stops being a silent gap (§5.5).
6. If wide values matter to users, a precheck refusal naming the value and the
   row beats an OOM-kill (§5.3).

## 8. Method and limits

Static review only; every citation was read at the cited line. Dynamic claims are
quoted from the project's own artifacts (`cdc-steady-30t-0.58.md` §B2.8/§B2.8b,
`gate.py` self-test/matrix output, the VPS gate logs for 0.57.0) and are as good
as those artifacts are; I did not re-run them. The 0.57.0 handoff's §0 decisions
were treated as intent, and the code was checked against them — where the code
took a different shape, the code is what is reviewed. Two earlier audits'
verdicts (0.42.0, 0.56.0) were re-derived from code rather than trusted; where a
fix was better than what the audit asked for, that is recorded above.
