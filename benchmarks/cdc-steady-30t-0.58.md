# CDC steady state, 30 tables in one group — 0.58 campaign

Shape: PostgreSQL source (`apitap-bench-pg-src`, 30 × 1M-row `prof_pg_t01..t30`,
one publication, one replication slot) → ClickHouse destination, apitap in a
cgroup of **0.5 CPU / 256 MB** (`--cpus=0.5 --memory=256m --memory-swap=256m`)
unless a row says otherwise. The writer (`benchmarks/cdc-steady-profile/writer.py`)
offers a counted stream of UPDATE+INSERT+DELETE transactions over 30 tables
(round-robin per thread); its rate is witnessed by the server (WAL LSN +
`pg_stat_user_tables` counters), never by the client ledger alone. Every
measured shape ends with the per-table md5-prefix digest
(`bench-capped-pg-ch-validator.sh`); `MATCH` means 30/30 tables.

The destination version is named every time, because it decides the wall:
- `ch-24.8` = `apitap-bench-ch` (ClickHouse 24.8.14). No patch-part deletes:
  a lightweight `DELETE` scans and masks parts, and its service time grows
  with concurrency.
- `ch-25.8` = `apitap-bench-ch3` (ClickHouse 25.8.29). 0.57.0's `patch_ok`
  probe turns on `lightweight_delete_mode='lightweight_update'` for every
  applied table; deletes become patch-part writes.

Rig: `benchmarks/cdc-steady-profile/{converge,keepup,windowcost,scaling-ch3}.sh`.
`keepup.sh` measures an already-converged group from a clean start (the earlier
t30-pg-r1 run polluted pass 2 with the bootstrap + backlog; this harness
separates them). Raw logs and `system.query_log` censuses are on the bench VPS
under `~/bench-cdc-steady/logs/`.

## 1. Baseline confirmation (0.57.0)

### 1.1 Statement census — what the 30-table apply actually issues

One converged keep-up pass, ch-24.8, 1.04M changes offered at 35k/s for 30 s
(`windowcost.sh` over `system.query_log`):

| class | n | p50 ms | server s |
|---|---|---|---|
| dest DELETE (key-table subquery) | 409 | 61 | 26.5 |
| key-table INSERT | 409 | 14 | 5.9 |
| key-table TRUNCATE | 340 | 2 | 0.7 |
| dest INSERT | 405 | 29 | 11.7 |
| `_apitap_state` INSERT | 1,620 | 16 | 25.9 |
| lease SELECT (pin per member-window + keeper) | 2,452 | 20 | 48.4 |
| guard scans (`system.tables`/`mutations`/`columns`) | 1,887 | ~3 | 7.3 |

Two facts the earlier t30 log hid:

1. **The per-INSERT batch was never 139 changes.** The
   3.94M-changes/28,356-INSERT ratio in the handoff counts *every* statement
   class together. The real destination INSERT holds ~2,500 changes (405
   INSERTs for 1.04M changes) and the key-table DELETE ~2,550 keys. The wall
   is **statement count x latency**, not an undersized batch.
2. **The DELETE is the expensive statement** (61 ms p50 serial on 24.8), and
   under a lane pool its per-statement latency *rises*: 418 ms p50 at 8 lanes.
   That is why the lane pool plateaus on 24.8.

### 1.2 Rate vs lanes (ch-24.8, 0.57.0 + lane-pool wheel)

| lanes | 1 | 2 | 4 | 8 | 16 | 30 |
|---|---|---|---|---|---|---|
| applied changes/s | 8,222 | 11,319 | 10,290 | 10,671 | 10,696 | 11,228 |

Plateau at ~11k/s from 2 lanes on: the destination's total statement service
time, dominated by DELETE and the per-member pin/state chain.

### 1.3 Destination version decides the DELETE cost

Direct HTTP DELETE, real ownership predicate, 5,000 keys:

| form | destination | p50 (1 lane) | p50 (8 lanes) | throughput (8) |
|---|---|---|---|---|
| inline IN-list | ch-24.8 | 52 ms | 214 ms | 35/s |
| inline IN-list | ch-25.8, patch | 66 ms | 79 ms | 77/s |
| key-table subquery | ch-25.8, patch | **20 ms** | **34 ms** | **209/s** |

Conclusion: the key-table DELETE stays (the inlining experiment measured 3.6x
slower per statement and was reverted), and on 25.8 patch-part deletes remove
most of the 24.8 delete wall.

## 2. What shipped

| commit | what | RED evidence on the VPS |
|---|---|---|
| `1a24c7d` | lane pool for a ClickHouse group: `apply_lanes` -> `ch_apply_lanes` (one lane per 8 MiB over a 96 MiB base, cap 8, serial below; `APITAP_CDC_APPLY_LANES` still overrides). Pg/My/Ice stay serial. | `red_fix_lanes_spec.py`: arm -> 1 fails `row_stores_stay_serial_and_clickhouse_does_not`; default -> 1 lane fails `clickhouse_overlaps_members_by_default` |
| `6ec0f4f` | pin cache: `ChStore` keeps the last successful lease pin per key set and `open_unit` reuses it while < 1/8 of its budget has elapsed (the threshold `keep` already uses). The deadline is the same `e0`; every predicate, the re-pin continuity, the watermark and the claim rules are unchanged. | `red_fix_pincache_spec.py`: collapse the reuse window to the whole budget -> fails `pin_reuse_window_is_an_eighth_of_the_budget`; bypass the cache -> fails `pin_reads_are_amortized_across_units` |
| (follow-up) | `settle_all`: a lane-pool error waits for every member already in flight instead of cancelling it (the 0.57.0 `try_collect` shape). Found by the first gate run on this wheel: `e2e_toast_rekey.py` failed because cancelling the sibling changed the documented partial-landing shape (an earlier group member closing while a later one fails), which its replay constructs itself from. | `red_fix_settleall_spec.py`: restore `try_collect` -> fails `a_pool_error_does_not_cancel_in_flight_siblings`; the e2e leg itself was the RED control on the pre-fix wheel |

An inlined clear-phase DELETE was implemented, measured, and **reverted**
(`red_fix_clearkeys_spec.py` is its RED evidence). The shipped code keeps the
subquery; the census below is the final engine.

Measured deltas:

| shape | 0.57.0 | fix 1 (lane pool) | shipped engine (fix 1 + pin cache) |
|---|---|---|---|
| ch-24.8, 35k offered / 30 s | 8,348/s, 0.140 cap, 101.7 MB | 11,256/s, 0.211, 78.3 MB | — |
| ch-25.8, 35k offered / 30 s, 15 MiB window | 10,357/s, —, 71.9 MB | — | 22,147/s, 0.578, 78.6 MB (inline-DELETE experiment) |
| ch-25.8, 35.8k offered / 180 s, 64 MiB window | — | — | **28,430/s, 0.786, 169.8 MB** |
| lease pins per member-window (census) | 1.86 | 1.86 | **0.32** |

All rows 30/30 checksum MATCH. The 22,147 row measured the intermediate wheel
that also carried the inlined-DELETE experiment (since reverted); the shipped
wheel is the 28,430 row.

## 3. The 0.5 CPU / 256 MB target run (ch-25.8, patch deletes)

Writer offered **35,838 changes/s for 180.1 s** (6,453,000 changes, witnessed:
WAL LSN bracket + `pg_stat_user_tables` counters), 64 MiB window, default 8
lanes. The drain applied all 6,453,000 changes in **235.5 s** of busy wall =
**27,397/s** at the 32 MiB window and **28,430/s** at the 64 MiB window; it
did **not** keep up (it was still ~55 s behind when the writer stopped).
Both runs: 30/30 checksum MATCH, MEMPEAK 114.9 MB (32M) / 169.8 MB (64M),
cap_frac 0.787, avg 0.39 cores of the 0.5 quota.

Final 64M census (6.45M changes): delete 95.1 s / 1,289 st, insert 94.7 s,
key insert 38.6 s, state 71.7 s / 2,250 st, guard+other ~70 s; total server
time ~367 s over 226.8 s busy => ~1.6 concurrent statements. The engine is
latency-bound between windows, not CPU-starved (39 % of the quota used on
average; bursts reach 79 %).

Verdict: **33,333/s at 0.5 CPU is NOT met; the measured ceiling is 28,430
changes/s (85 % of target), 1.71M changes/min, checksum-exact.**

## 4. Scaling (ch-25.8, window 64 MiB)

Same 30-table keep-up shape; writer offered per row (server-witnessed);
converge between points; lanes set through `APITAP_CDC_APPLY_LANES`.

| CPU quota | lanes | offered | applied | cap_frac | avg cores | MEMPEAK |
|---|---|---|---|---|---|---|
| 0.5 | 8 | 35,838/s | **28,430/s** | 0.786 | 0.393 | 169.8 MB |
| 1 | 8 | 65,944/s | 25,554/s | 0.502 | 0.502 | 181.6 MB |
| 2 | 16 | 95,316/s | 27,208/s | 0.285 | 0.571 | **234.6 MB** |
| 4 | 24 | 96,457/s | **OOM-killed (137)** at 12.6 s | — | — | — |

The curve is **flat**: more CPU does not buy more rows. At 1 CPU the client
becomes CPU-bound at 23.3 µs/change (vs 14.5 µs/change at 0.5 CPU — concurrency
overhead), at 2 CPU it is back to latency-bound while memory is nearly at the
cap, and 24 lanes at 4 CPU exceeds 256 MB. The lane cap of 8 in the shipped
code is the measured-safe bound, not an arbitrary one.

The owner's requirement "resources up => rows/min up" is therefore **not
demonstrated** for this shape: the wall is the ClickHouse destination's
statement service time under concurrency plus the client's CPU cost per
change, and both are already at their 0.5-CPU/256-MB edges.

## 5. Memory and checksums

- Steady MEMPEAK: 78.6 MB (15 MiB window) → 114.9 MB (32 MiB) → 169.8 MB
  (64 MiB) on ch-25.8; all far under the 256 MB cap.
- The **fresh 30-table bootstrap** is the memory peak: 179 MB with the PGO
  0.57.0 wheel (fits 256 MB), 209 MB with the non-PGO release build (OOM at
  256 MB; the bootstraps for these runs used a 512 MB cap, stated here). The
  measured shape always starts converged; steady apply is unaffected.
- 30/30 checksum MATCH in every measured shape in this report.
- The 0.57.0 PyPI wheel under test has `.so` md5
  `41e9f7c252d3e1eb5403b70f87bf5435`; the campaign wheel's md5 is recorded in
  the gate section below.

## 6. Honest arithmetic

At 0.5 CPU the ceiling measured here is 28,430 changes/s (85 % of the
2M/min target). The engine spends 367 s of destination statement time per
6.45M changes (57 µs/change across the statement mix) and only reaches ~1.6
concurrent statements; the two biggest remaining per-window costs are the
state write (one 28 ms INSERT per member per window, 2,250 in the run) and
the pair of destination statements per active member (DELETE 66 ms p50 +
INSERT 56 ms p50, 1,747 each). Closing the last 15 % needs one of:

- **batched state writes** (one INSERT per window instead of one per member:
  measured headroom ~70 s of the ~367 s server time), which needs a group
  unit whose `owner_pred` is a single multi-key subquery;
- **a longer window cadence or a second apply consumer** so the ~1.6
  concurrency rises toward the client's CPU ceiling (~36k/s at 0.5 core by
  the per-change CPU measured here, 14.5 µs/change);
- or a CPU quota above 0.5 (the scaling table shows what the same engine does
  with more).

What is already delivered: the same shape moved from 8.3k/s (0.57.0, ch-24.8)
and 10.4k/s (0.57.0, ch-25.8) to 28.4k/s with exact checksums, 1.6-3.4x, with
the client at 39 % of its 0.5-core quota and 170 MB peak.

The numbers above were measured on the pre-`settle_all` wheel; the follow-up
changes failure handling only (a failing member no longer cancels its
in-flight siblings) and the full lib suite plus the gate ran on the rebuilt
wheel below. The steady rates are unaffected by that path (no failures in a
keep-up run); the final wheel's checksums in the gate include this shape's
engine under the destination legs.

## 7. Release gate on the final wheel

- wheel `.so` md5: `07ce71c3089101bfa1a1d36a78095126`
- `gate.py`: **79 passed, 1 failed, 0 skipped in 7421 s** (80 legs, BigQuery
  included; run from `~/apitap-058b` with `~/gate-venv`, stdin `/dev/null`)
- the one failure, `e2e_my_liveness.py` (8.1 s), is the leg's own rig
  precondition — "the server has more than one binlog to work with: 1 files"
  — a binlog-rotation state, not an engine assertion. It **passes standalone
  on the same wheel, twice** (RC 0, "MYSQL LIVENESS E2E: PASSED").
- `gate.py --self-test`: PASSED. `gate.py --matrix`: exit 0,
  123 cells, 0 GAP, 0 other problems.
- engines unaffected by this campaign passed unchanged (Postgres, MySQL,
  Iceberg, S3/GCS, BigQuery legs); the ClickHouse CDC legs
  (`e2e_logbased_dests ch`, `e2e_changelog_replay`, `e2e_toast_rekey`,
  `e2e_state_contract ch`, `e2e_cdc_apply_orphan`, `e2e_cdc_fence`,
  `e2e_guard_matrix ch`) all green.

The 24.8 path deserves one more sentence, because production LTS users may
still run it: there the ceiling is ~11k changes/s (the lane curve plateaus and
the delete's service time grows with concurrency). Moving the destination to
ClickHouse 25.7+ is worth more than any engine change this campaign could
make on 24.8.

Campaign wheel: `apitap-0.57.0-cp39-abi3` built from these commits, installed
`.so` md5 `07ce71c3089101bfa1a1d36a78095126` (non-PGO release build; the
0.57.0 PyPI PGO wheel is `41e9f7c252d3e1eb5403b70f87bf5435`).
