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

# B2 — levers 1–3, the census correction, and the final keep-up

Same shape as §1–§7 (30 × 1M-row `prof_pg_t01..t30`, one publication, one
slot, 0.5 CPU / 256 MB unless a row says otherwise). B2 starts from the B1
engine and lands two commits, measures a third and reverts it, then attacks
the wall the B1 census pointed at — and finds the census pointed at the wrong
wall. Everything below is on the wheel built from HEAD (`279c302`, `0463694`,
`2d34c67`), `.so` md5 `08106acfca97b8dce2fc5818d5ef96c9` — a fresh build from
the same tree on 2026-10-06 reproduced that md5 exactly, so the B2 baselines
and the QA wheel are the same binary.

## B2.1 What shipped

| commit | what | RED on the VPS | measured |
|---|---|---|---|
| `279c302` | a window's whole mark set is ONE `_apitap_state` INSERT (`state_batch_sql` + `render_state_batch_row`; the group unit closes with every mark) | `red_b2_state_spec.py` | state writes 2,250 st / 71.4 s -> 74 st / 2.47 s in one 6.4M-change run; wall inside drift (the apply side hides behind the drain-side windows) |
| `0463694` | `ch_apply_lanes` reads the cgroup CPU quota (`cpu_limit_cores`) and the measured per-lane cost (20 MiB/lane over a 96 MiB base, `round(16*c)` floored at 8, cap 16) | `red_b2_lanes_spec.py`, `red_b2_cpu_spec.py` | 4-CPU point survives 256 MB (24 lanes OOM-killed it before); 2-CPU MEMPEAK 250.6 -> 148.6 MB |

## B2.2 Lever 3 — group pin + lease-create memo: measured, no wall win, reverted

The WIP made two changes under A/B: (a) the pin cache's exact-key hit gained a
SUBSET hit, so the group unit's single lease read (all 30 member keys, `e0` =
the minimum over the set) serves every member unit of the same window, and the
group unit is opened before the lanes instead of at the close; (b) the
`CREATE TABLE IF NOT EXISTS _apitap_lease` round trip is memoized once per
process and destination (`ensure_lease_table`).

A/B/A on ch-25.8, 0.5 CPU / 256 MB, 64 MiB windows, writer ~35.6k/s for 60 s
(`b2-l3ab.out`; the l2 arm is HEAD `08106acf`, the l3 arms the WIP `aba58aae`):

| arm | changes | wall s | busy s | busy rate | cap_frac | MEMPEAK | checksum |
|---|---|---|---|---|---|---|---|
| l3-a (WIP) | 2,148,000 | 98.034 | 87.422 | 24,570.5/s | 0.6953 | 153.1 MB | 30/30 |
| l2-a (HEAD) | 2,151,000 | 100.611 | 88.219 | 24,382.5/s | 0.6963 | 169.9 MB | 30/30 |
| l3-b (WIP) | 2,135,000 | 101.128 | 89.446 | 23,869.2/s | 0.6977 | 158.9 MB | 30/30 |

The census is where the lever actually shows — same window, server-side
seconds (`windowcost.sh`):

| class | HEAD (l2-a) | WIP (l3-a) |
|---|---|---|
| dest DELETE | 37.57 | 33.51 |
| dest INSERT | 34.25 | 32.28 |
| other | 31.84 | 27.18 |
| key-table INSERT | 15.01 | 15.32 |
| staging/other | 10.84 | 10.70 |
| lease probe | **9.67 (852 st)** | **0.16 (12 st)** |
| lease write | 3.89 | 4.35 |
| state read | 1.84 | 1.60 |
| key TRUNCATE | 1.59 | 1.56 |
| DDL setup | 1.56 | 1.60 |
| state write | 0.74 | 0.73 |
| **total** | **~148.8 s** | **~129.0 s** |

`CREATE TABLE IF NOT EXISTS _apitap_lease` per interval: HEAD 540, WIP 1 each
(the memo works; the `lease_probe` class is that DDL plus the pin reads).
So the WIP removes ~840 statements and 13 % of the destination's server time —
and moves the busy rate by **−0.7 %** (two WIP legs vs one HEAD leg, all inside
the ±3 % host noise). The removed statements ran inside the 8 apply lanes, off
the critical path. Not committed: the working tree was restored to HEAD and
this is the negative result.

## B2.3 The census correction: the destination is not the wall at 0.5 CPU

Three measurements, each cheap, redirect the attack:

1. **The `windowcost.sh` window includes the harness's own validation.** The
   `other` class is dominated by `validate30.sh`'s per-table MD5 aggregate
   (29 queries, 20.7 s in one 2.14M-change run) and the `staging_other` class
   by guard machinery (`system.mutations` waits, key-table drops); the engine's
   own destination statements are DELETE 33.5 s + INSERT 32.3 s + key INSERT
   15.3 s. The B1 report read `other` as engine work; it is mostly the
   validator.
2. **The debug window log prices the critical path.** In the 180-s proof
   (73 windows): sum of drain windows **210.7 s** vs sum of apply windows
   **63.2 s** (avg 2.89 s vs 0.87 s). The drain (WAL read + decode + collapse)
   is 3.3x the apply; the drain chain alone bounds that run at ~29k/s even if
   every destination statement were free.
3. **The walsender alone has 2.5x headroom.** Two slots created before one
   unpaced 7,165,000-change generation (7.26 GB of WAL, 1,014 B/change), then
   drained with `pg_recvlogical` and no apitap in the loop (`b2r-wsprobe`):
   text 91.5 s = **78,306 changes/s** at 0.976 core; binary 85.0 s =
   **84,294/s** at 0.973 core. The source can feed far more than the pipeline
   takes.

The wall at 0.5 CPU is the client pipeline itself. The cleanest number is the
pure catch-up of that probe backlog: 7,165,000 changes in **226.2 s =
31,679/s at cap_frac 0.9494** (cpu 110.0 s = 15.35 µs/change, MEMPEAK
163.1 MB, 30/30). At 15 µs/change the 0.5-core ceiling is 33,333/s exactly:
the target needs the quota ~100 % used AND no per-change regression. The
measured 94.9 % utilization is the whole gap.

## B2.4 Binary pgoutput (opt-in): neutral on the wall, −9 % walsender CPU

`APITAP_PG_BINARY=1` on the same 30-table keep-up (64 MiB, 0.5 CPU; text arm
and binary arm in the same session):

| arm | changes | busy s | busy rate | MEMPEAK | walsender CPU |
|---|---|---|---|---|---|
| text | 2,144,000 | 90.462 | 23,700.6/s | 163.1 MB | 70.30 s |
| binary | 2,136,000 | 88.690 | 24,083.9/s | 176.9 MB | 64.27 s |

The binary wire is more compact (108k-event windows instead of 96k at the same
64 MiB budget) and cuts the walsender's own CPU by 9 %, but the end-to-end
rate moves +1.6 % — inside noise. Left opt-in (the renderer's type coverage
is a deliberate gate); it is not the lever for this shape.

## B2.5 Final keep-up proof — writer >= 33.3k/s for 180 s

25.8 (`apitap-bench-ch3`, `:8126`), 0.5 CPU / 256 MB, 64 MiB windows, default
lanes (8), wheel `08106acf`:

- writer offered **6,115,000 changes in 180.288 s = 33,918/s**, witnessed by
  WAL LSN bracket (`12B/D9113490 -> 12E/5AFE2920`) and `pg_stat_user_tables`
  counters (+6,115,000); 10.77 GB of WAL
- applied **all 6,115,000**; busy 244.653 s = **24,995/s**; cap_frac 0.7701;
  MEMPEAK **173.4 MB**; **30/30 checksum MATCH**
- the drain did not keep up live (24,995/s against the 33,918/s offer); it
  caught up ~60 s after the writer stopped, and the last three passes are rows=0

24.8 (`apitap-bench-ch`, `:8124`), same shape (fresh 1 GB bootstrap first:
30,000,000 rows in 75.6 s, MEMPEAK 766.3 MB — the bootstrap sizes buffers to
the 1 GB budget, by design):

- writer offered **6,125,000 changes in 180.065 s = 34,015/s** (witness
  +6,125,000)
- applied **all 6,125,000**; busy 245.653 s = **24,933/s**; cap_frac 0.7675;
  MEMPEAK **196.8 MB**; **30/30 checksum MATCH**

Both destinations now apply at the same rate at 0.5 CPU: the 24.8 DELETE wall
(B1: ~11k/s ceiling) is hidden behind the client-side drain once the lane pool
overlaps it. B1's "moving 24.8 to 25.7+ is worth more than any engine change"
was true for the serial engine; at B2's overlap it is no longer the deciding
factor at this quota.

## B2.6 Scaling (fresh wheel, 32 MiB windows)

Same shape as §4, converged between points, lanes default (`ch_apply_lanes`:
8 at this cage at every quota), writer offered per row (witnessed). The host
was loaded (loadavg 8–26, ClickHouse background merges from the campaign's own
writes), so the levels are 5–10 % below the quiet-host §4 table; the shape is
the same:

| CPU quota | lanes | offered | applied | cap_frac | avg cores | MEMPEAK |
|---|---|---|---|---|---|---|
| 0.5 | 8 | 35,436/s | 22,981/s | 0.659 | 0.329 | 122.9 MB |
| 1 | 8 | 53,736/s | 23,155/s | 0.499 | 0.499 | 124.4 MB |
| 2 | 8 | 56,611/s | 23,673/s | 0.252 | 0.505 | 157.9 MB |
| 4 | 8 | 56,648/s | 21,218/s | 0.114 | 0.457 | 210.4 MB |
| 0.5 (64 MiB) | 8 | 35,446/s | 20,297/s | 0.648 | 0.324 | 175.7 MB |

All points 30/30 MATCH. The curve is still flat and now for a measured reason:
more quota does not buy rows because the client is not CPU-bound above 0.5
cores (cap_frac 0.25 at 2 CPU, 0.11 at 4) and the destination is not the wall
either (apply 0.87 s vs drain 2.89 s per window). The engine waits on the
WAL stream and its own single-threaded pipeline, and the quota cannot shorten
that. The 4-CPU point no longer OOM-kills: 210.4 MB peak at 32 MiB windows.

## B2.7 Honest verdict on 2M/min

**Not reached, and the campaign says plainly why.** The measured ceiling on
this shape at 0.5 CPU is **31,679 changes/s = 1.90M/min** (pure catch-up, cap
0.949, 30/30); the 3-minute paced proof applies at 24,995/s (25.8) and
24,933/s (24.8) because the writer outruns the pipeline and the difference is
drained after it stops. The target needs 33,333/s at 0.5 cores, i.e.
15.0 µs/change with the quota 100 % busy; the engine measures 15.35 µs/change
at 94.9 % busy. Both halves are marginal; neither the source (78–84k/s alone)
nor the destination (apply = 30 % of the drain's critical path) is the
binding constraint. The remaining gap is the client's per-change CPU, ~53 % of
which is kernel TCP receive for a one-packet-per-change WAL stream — a cost a
same-host Unix socket would largely remove, and one that a dedicated host (the
B1 28.4k was measured on a quiet box; today's quietest run hit 31.7k) would
lower. Neither is an engine change this campaign could land.

What B2 delivered: the state batch and the quota-aware lanes (committed,
RED-tested), the census corrected, lever 3 measured and rejected on evidence,
binary pgoutput measured and left opt-in, and a final proof that is honest
about the gap: 1.90M/min pure / ~1.50M/min paced, checksum-exact, under 200 MB
on both destination versions.

## B2.8 QA wheel and gate

- QA wheel: built from HEAD (`py-apitap/Cargo.toml`), installed `.so` md5
  `08106acfca97b8dce2fc5818d5ef96c9` — identical to the wheel's and to the
  wheel every B2 run used; installed into `~/qa-venv` (md5 re-verified after
  install).
- `gate.py` on that venv (80 legs, BigQuery and GTID legs enabled):
  **80 passed, 0 failed, 0 skipped in 7,352 s** (RC=0), including
  `e2e_toast_rekey.py` in both variants.
- The first full gate on the pre-fix B2 wheel (`08106acf`) came back 79/80:
  `e2e_toast_rekey`'s CH replay case failed because the one-statement mark set
  (`279c302`) dropped a succeeded member's watermark when a sibling failed.
  That regression is fixed by `c8d14fd` (`settle_partial` + a group close over
  the succeeded subset), whose wheel is `.so 5e00a8ad…`; the 80/80 above is
  the gate on that fixed wheel, and the leg proves the per-member commit again
  ("the attempt landed RP's re-key (D, U) and failed on RPB after it").
- harness changes that go with this section: `keepup.sh` gained a `DB_S`
  override (a 180-s writer needs more than WS+120 of tail) and an
  `APITAP_PG_BINARY` passthrough; `leg.sh` prints the binary mode it runs
  under; `pgbin_sample.py` filters its own `docker exec` shells out of the
  walsender count (they carry the pattern in their own cmdline — the artifact
  trap, again).

## B2b — the adversarial review, the revert, and the safe re-land

Two independent adversarial reviews of the six engine commits (B1
`1a24c7d`/`6ec0f4f`/`fa56b6c`; B2 `279c302`/`0463694`/`c8d14fd`) confirmed two
HIGH defects:

1. **Bootstrap fan-out (from `1a24c7d`).** `apply_lanes()` was also driving
   `bootstrap_group`: up to 8 full loads ran at once on a 30-table ClickHouse
   bootstrap, and every `transfer_within` plans its pipes against the WHOLE
   cgroup budget, so N loads each claimed the entire cage — the B2 wheel
   OOM-killed the 30-table bootstrap in 256 MB, a load 0.57.0 ran serially in
   179 MB, while the code comments still said "stay serial". Fixed in
   `118bb05`: `bootstrap_lanes()` is separate (BigQuery's server-side jobs fan
   out; every row store bootstraps serially).
2. **Mark-set continuity (from `279c302`).** The group close opened a FRESH pin
   (`prev = None`) and the member units closed empty, so `repin`'s continuity
   check never ran for a member: one whose owned statement had started inside
   its deadline and lost parts to a stall could have its watermark advanced
   over data that never landed, with no replay. The commit's claim that the
   group `e0` is "the minimum over every member's key" was not what the code
   did.

**Revert, then re-land safely.** The batch's own commit had measured it as
wall-neutral, so it was reverted (`8d37853`) — but the revert then measured
17,360/s on this report's keep-up protocol versus 24,995/s recorded for the
buggy build (B2.5), so the batch is not free: the optimization is worth
keeping SAFELY. `8919bd3` re-lands it: `Fence::open_unit_at` / `Tenure::open_at`
carry a continuation deadline into the unit open, `apply_member_pending`
returns each member's `pin_deadline()`, and the group close opens ONE unit over
every carried member's key at `min(deadlines)` — `repin` then refuses exactly
when any member's continuity broke. The per-member proof is intact; only the
statement count drops.

Measured after the re-land (30 tables, 0.5 CPU / 256 MB, 64 MiB windows,
ch-25.8, writer 34,985 changes/s for 180 s, converged start):

| arm | applied | wall | rate | cap_frac | MEMPEAK | checksum |
|---|---|---|---|---|---|---|
| re-land `69cc2ec8` | 5,550,000 | 307.1 s | 18,075/s | 0.43 | 203.5 MB | 30/30 after converge |
| revert `aceb934e` | 6,300,000 | 362.9 s | 17,360/s | 0.41 | 196.2 MB | 30/30 |

The earlier 24,995/s reading for the buggy build did not reproduce on the same
box state; it stands only with its date and wheel (B2.5). What the re-land
verifies: `e2e_toast_rekey.py` PASSED on its wheel (including "the attempt
landed RP's re-key (D, U) and failed on RPB after it"), unit suite 409 passed,
and the 30-table destination converged to 30/30.

**A note on the measurement chain itself:** the "30/30 MISMATCH" readings
during this work were validation-call artifacts, not engine faults — the table
list was passed with its `public.` prefix, so the validator built
`public."public.prof_pg_t01"` and returned an empty digest, and one converge
ran against the default ClickHouse. The validators are parameterized (`CH_C`,
bare table names); with those set correctly every converge validated 30/30.
