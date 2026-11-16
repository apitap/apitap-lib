# apitap.sql(): memory bounded by construction, then speed that scales with cores

Status 2026-11-12: architecture + work orders. Written by the architect session
for the engineer session. Follows `2026-11-09-sql-sf50-gcp-bench.md` (W1, W2).
Nothing here is measured yet unless it cites a number from an earlier document.

## 0. The target, and what physics allows

Owner's target: any data size answers in < 10 s at 0.5 vCPU / 256 MB, and gets
proportionally faster as cores are added.

What the existing receipts say about a **cold** query (nothing remembered, the
servers only scan, filter by the user's WHERE, and send):

- Wall = max(server scan, rows this side must receive ÷ this side's rate).
- This side's rate at 0.5 core is ~4M rows/s of wall across TPC-H shapes (the
  10g read floors in memory `tpch-066-scope`: Q9's inputs alone take 48.1 s,
  Q6's 3.0 s). The whole 22-query run uses 351 s of CPU in 935 s of wall, which
  is 75% of a half-core quota: the engine is mostly CPU-bound already, and a
  large share of that CPU is the kernel's socket receive (Q8: 49%).
- So 10 s at 0.5 core is roughly 40M received rows. A cold query that must
  receive more than that cannot finish in 10 s at half a core whatever the
  engine does, unless rows are not received. The no-heavy-pushdown rule (HARD)
  forbids making the server drop them by a join filter.

Therefore the target splits into three regimes, and each has its own lever:

| regime | what bounds it | lever | this document |
|---|---|---|---|
| R1 cold, 0.5 core | rows received × CPU per row | less CPU per row, no idle wall (phases overlapped) | §3, phases C–D |
| R2 cold, N cores | serial fold, source scan rate | fold inside the pipe workers, key-range partitions in parallel | §3, phase D |
| R3 any size < 10 s | must not re-read what did not change | incremental state, O(changes) per run (views; exists for 1–2 tables: 26K changes on 20 GB in 2.4 s) | §5, later round |

R3 is the only regime where "any size < 10 s" is literally reachable. It is not
this round. This round makes R1/R2 correct at any size (W1) and faster.

## 1. Hard rules (unchanged, all from memory)

Engine work in Rust in `crates/apitap-core`, never the Python shim. Cargo, tests,
benches only on the VPS. Servers only scan + own-table WHERE + send. Measure ALL
queries first, design general fixes, one build, retest all. A test counts only
after it was seen RED without the fix. No fix may raise wall, CPU or peak on the
10g TPC-H suite (baseline in memory `prod-ready-no-regression`; README table for
0.67.0). Every VPS container gets `--memory` and `--cpus`. No GCP (it costs
money; ask the owner first). No commit, no push, no publish.

## 2. W1: memory bounded by construction

### 2.1 What is known

- SF50 Q9 was OOM-killed at 256 MB after 43 s; the same query peaks 189 MB on
  the 10g seed.
- The 10g seed is NOT TPC-H-shaped on the join key: `lineitem_10g`'s
  `l_orderkey` covers roughly [1 .. 60M] while `orders_10g` has 69.9M orders
  over a ~280M key range. The intersected join range is therefore ~60M keys and
  ~15M orders. At SF50 it is 300M keys and 75M orders: a build side 5× wider
  than anything the 256 MB cage has ever been measured on. "Memory grows with
  lineitem rows" is most likely "the join range, and everything sized by it,
  grew 5×, and the multi-pass regime for a star plan was entered for the first
  time".
- The engine sizes itself with absolute constants calibrated on that seed:
  `DIRECT_MAX` 64 MiB, `PROBE_HEADROOM` 64 MiB, `GATE_MAX_BYTES` 16 MiB (134M
  keys: a 300M-key range gets no gate at all), `probe_pipe_cap`'s 32/40/48/96/
  112 MiB thresholds, `pre_room` ≥ 1 GiB. Each is a cliff at another scale.
- Not everything held is priced: each dimension is checked alone against the
  whole build budget (`star::load_dim`, `dim_cap = build_budget()`), their sum
  is not; kernel socket memory is priced only through the constant headroom
  (measured once: five pipes = 40 MiB of kernel memory the cgroup charges);
  key sets, gates, `cgroups`, stage tables and the build each have their own
  fraction with no shared account.

### 2.2 Hypotheses to confirm or kill by measurement (phase A)

- H1 unpriced sum: build map + dimensions + probe-side groups + gates each
  inside its own share, together past the cage.
- H2 constant cliffs: no gate on a > 134M-key range → ungated probe → heavier
  decode and more bytes in flight; a pass count the heuristics were never
  tuned for.
- H3 kernel socket memory: `sock`/`kernel` in `memory.stat` grows with pipes
  and with a slow reader; worse over a WAN (the SF50 run crossed the internet).
- H4 allocator retention between passes (`trim_heap` not reached on a path).
- H5 a structure that really is O(rows): e.g. pairs staged before the map, a
  key set, `Early`, a spill writer's buffers per range.

### 2.3 Design: one ledger, one rule

**Rule: nothing is allocated in proportion to data without asking the ledger,
and every asker has a degrade path that is not the OOM killer.**

`sql::ledger` (new, small): one `Ledger` per statement.

- `limit` = `pipeline::mem_limit_bytes()`; `floor` = what the process holds
  before the statement's own structures (runtime, pinned snapshot connections,
  allocator slack), measured, not guessed.
- Classes: `Build` (key map + pairs), `Dims`, `Groups` (groups, accumulators,
  `cgroups`), `Keys` (semi-join sets, gates), `Stage`, `Pipes`.
- `reserve(class, bytes) -> Result<Reservation, Over>`; a `Reservation` is a
  guard that can `grow`/`shrink` and releases on drop. `Over` carries the class,
  the ask and what is free.
- `Pipes` is priced per pipe as user buffers (batch bytes × batches in flight)
  **plus the kernel receive buffer**. If phase A confirms H3, the read sockets
  set `SO_RCVBUF` explicitly so that term is a known number instead of TCP
  autotuning's; chosen from the limit, with a floor that keeps WAN throughput
  (measure with the netem recipe in `benchmarks/wan-latency.md`).
- Pipes get what is left: `pipes = min(cpu cap, free / pipe price)`. Under one
  pipe's price the batch shrinks before the read is refused.
- `APITAP_DEBUG` prints one `[mem]` line per phase: the ledger by class next to
  the process RSS and the cgroup's `anon`/`sock`/`kernel`/`file`. The harness
  asserts `actual ≤ ledger + slack`. That assertion is the RED test of W1: on
  the 0.67.2 wheel there is no ledger and the SF50-shaped run dies.

Degrade paths, one per class (most exist; the ledger only routes to them):

| class over | path | state |
|---|---|---|
| Build | split the key range into passes (`join_pass` returns `false`) | exists |
| Groups | parts by a GROUP BY integer column (`partitioned`), or key-range passes for `key_groups` | exists |
| Stage | chunks on disk (`mem::Writer`) | exists |
| Keys | growing bitmap → hash set; past its share: parts by the key (`partitioned` on that column) | partly |
| Dims (sum) | first shrink: load order so a dimension is gated by the dimensions that narrow it; then parts by the largest dimension's key on both the dimension and the fact (`partitioned`); a single dimension row set that cannot fit one part is a named refusal | new |
| Pipes | fewer pipes → smaller batches → named refusal below one | new |

**Constants become functions of the limit, pinned at 256 MiB.** Every absolute
threshold listed in §2.1 is replaced by `f(limit)` such that `f(256 MiB)` is
exactly today's value (64 MiB = limit/4, 16 MiB = limit/16, 96 MiB = 3·limit/8,
and so on), with a floor for tiny cages. A unit test pins the 256 MiB values.
This keeps every 256 MB plan on the 10g seed bit-identical (no regression by
construction) and removes the cliffs at other sizes. Where phase A shows a
constant is simply wrong at SF50 shape (for example the gate cap that leaves a
wide range ungated), fix the rule, not the number: a pass over part of the range
gates that part.

### 2.4 Reproducing SF50's shape without GCP

Seed `lineitem_w` in the bench Postgres: `lineitem_10g` with `l_orderkey`
remapped monotonically onto the whole key domain of `orders_10g` (so the
intersected range is ~280M keys and every order can match), written in key
order so `ctid` page spans still work. Same 60.3M rows and ~10 GB. `l_partkey`,
`l_suppkey` untouched, so the `partsupp`/`part`/`supplier` joins stay valid.
Truth = DuckDB in a capped container, once per query, kept.

Second axis, no new data: the 10g suite in a 128 MB cage. Bounded by
construction means every query there finishes exact or refuses by name; an
OOM-kill is a W1 failure.

### 2.5 Done when

1. `lineitem_w` suite (all 22 TPC-H texts with `lineitem_w` for lineitem) at
   `--cpus=0.5 --memory=256m`: 22/22 exact, no OOM, cgroup peak ≤ 240 MB.
2. 10g suite at 128 MB: every query exact or a named refusal; zero OOM-kills.
3. 10g suite at 256 MB, interleaved old/new n ≥ 2: per query no worse than the
   run-to-run spread on wall, CPU, peak; answers identical.
4. A leg, RED on the 0.67.2 wheel and GREEN on the build, registered in
   `benchmarks/gate.py`, `gate.py --matrix` clean; `e2e_sql_tpch.py` (4 configs)
   green.
5. `[mem]` ledger vs actual table for the worst five queries in the report.

## 3. Speed (phases C–D): designed now, tuned from phase A's profile

Phase A also records, for every query on both seeds: wall, CPU, cores used
(CPU ÷ wall), and a `perf` split of CPU into kernel receive / wire decode +
Arrow build / fold (build + probe) / other, plus the wall spent with CPU idle
per phase. The levers below are ordered by what the existing receipts suggest;
the profile decides which are built.

**S1. Fold where the rows are decoded (R1 and R2).** Today each pipe's worker
decodes into an `ArrowBatch`, sends it through one bounded channel, and ONE task
folds every batch (`join_pass` → `fold_probe`). That is a serial stage behind N
decoders: it caps the engine near one core (measured 0.9–1.2 of 8) and puts a
queue of batches in memory. Change: the read path takes an optional per-batch
sink run inside the worker (`ReadOptions` gains it; `read_impl::ArrowLoader`
calls it instead of `tx.send`). The probe's sink is `fold_probe` over the shared
read-only map (`Arc`) into that worker's own accumulators, `hit` and `cgroups`;
workers' partials merge at the end with the existing `merge_acc` /
`merge_fields` / `combine`. The batch is reused in place (no allocation, stays
in cache), and `Pipes` in the ledger loses its queue term. The build side does
the same for a direct-address map: spans are disjoint key ranges, so workers
write disjoint indices; group ids come from one small shared dictionary.
Worker threads and pipes come from the CPU quota, never a constant.

**S2. No idle wall: overlap phases under the ledger (R1).** Dimensions, build
and probe wait on different servers. `Pre` (the probe read started beside the
build) is switched off below 1 GiB by a cliff; under the ledger it is "start
the next phase's read when one pipe of it fits", at any size. Same for a
pass's successor: pass i+1's build reads while pass i's probe folds.

**S3. Per-batch group ids in the build (R1).** The build encodes a group key
and hashes it per row (`groups.id(kb)`). Resolve group ids per batch from the
group columns (a one-entry cache for runs, a small direct table for a single
integer/date column), then write the map in one loop.

**S4. Key-range partitions in parallel (R2).** A pass is an independent unit
(its own map, shared-nothing accumulators). With W = f(cores) passes in flight,
each with budget/W, the join scales with cores and each map shrinks toward the
cache. At 0.5 core W = 1 and S2 covers the overlap.

**S5. Fused wire → fold (R1), only if the profile says so.** If Arrow build +
cell reads are a large share of user CPU on the probe, fold straight from the
COPY frame: key (gate) → map lookup → argument columns read in place.

**Kernel receive.** If it is the largest single term, measure larger reads per
wakeup (the replication reader's `APITAP_READ_COALESCE_US` knee was −6% CPU per
change) on the bulk read plane before designing more.

Done when (phase D): 10g suite at 0.5 core no slower on any query and faster in
total; at 2 and 8 cores (2 GB / 8 GB cages) cores used rises toward the cage
until the source servers are the limit, shown with their own CPU/iowait; peaks
inside the cage; answers identical.

## 4. Phases and gates

| phase | work | ends with |
|---|---|---|
| A | rig check; `lineitem_w` + truth; `[mem]` attribution (may be instrumentation only); measure all on both seeds + 128 MB axis; CPU split | tables to the architect — **gate G1** |
| B | the ledger, constants as functions, the confirmed W1 fixes, the leg | §2.5 — **gate G2** |
| C | S1 at 0.5 core + S2 + S3 (as the profile ranks them) | suite table — gate G3 |
| D | S4, multi-core tiers | §3 done-when |

The engineer may go from A into B without waiting **only if** the cause is one
of H1–H5 and the fix is the mechanism §2.3 names for it. Anything else stops at
G1. Phases C and D start only on the architect's word.

## 4b. DuckDB comparison leg (after gate G2, not before)

Owner asked 2026-11-12 for speed and memory against DuckDB on the current
engine. It runs once, after the W1 fix is green, so the numbers describe the
engine that ships and the rig is not shared with phase A/B timings.

- Same cage for both: `--cpus=0.5 --memory=256m`, then 2c/2 GB and 8c/8 GB.
  DuckDB = latest release with the postgres and mysql extensions, ATTACH on the
  same two servers, same query text, `memory_limit` and `temp_directory` set to
  what the cage allows (a spill disk with room; state both in the table).
- All 22 queries on the 10g seeds and on `lineitem_w`; n >= 3, interleaved
  apitap/DuckDB per query; wall, CPU seconds, peak (non-file), answer exact
  against the truth. An OOM-kill or an engine error is a recorded result.
- Fairness per `benchmark-fairness-rules`: defaults vs defaults in one table,
  each side's best documented tuning in a second; check
  `information_schema.processlist` / `pg_stat_activity` for joins or aggregates
  pushed into a server and say so per query; versions pinned; cold/warm stated.
- Output goes to `reports/` (never committed). No README or docs change from it
  without the owner's word.

## 5. Later: R3, the literal "< 10 s at any size"

Incremental views already move a 20 GB two-table answer by its changes in
seconds at 256 MB. Extending them to star plans (`2026-11-08-sql-live-star.md`,
designed, not built) is the route to a query whose cost is its changes, not its
size. It needs local state (`APITAP_STATE_DIR`), so it is a product decision as
well as an engine one. Not started in this round.

## 6. Gate G1 decision (2026-11-12): what phase A found, what phase B is now

### 6.1 Findings that change the plan

Phase A (HEAD = a0, n = 1, logs in `~/w1/logs/` on the VPS):

- **No query OOMs at HEAD** on `lineitem_w` at 0.5c/256 MiB (22/22 exact) nor on
  10g at 128 MiB (21 exact, Q9 a named refusal). 0.66.0 does die on `lineitem_w`
  (Q10, anon 254 MiB); the SF50 OOM was seen on 0.66.0. Whether 0.67.2 passes
  the real SF50 run is **not established** (300M-row probe, TLS, a real NIC
  were not reproduced) and can only be settled on GCP, on the owner's word.
- The RED test of §2.3 ("the SF50-shaped run dies on 0.67.2") is void.
- Memory is nevertheless unpriced, in three places that grow with something
  other than the limit:
  1. **Kernel receive buffers (H3).** Every raw-plane Postgres socket asks for
     4 MiB (`walsender.rs` `RECV_BUF_BYTES`), the kernel doubles it: 8 pipes =
     63 MiB of `sock`. MySQL sockets autotune to 6 MiB. At 256 KiB Q1 peaks at
     137 instead of 217 MiB at the same wall on loopback, but Q9 over a 67 ms
     path takes 353.7 s instead of 124.2 s.
  2. **Key sets sized by key range (H5).** Q21's bitmaps: 62.9 → 164.8 MiB when
     the range widens 4.6× with the same ~700K keys. This is O(key range): the
     one structure found that still grows without bound with data scale.
  3. **Spill writers (H5/H1).** Q13: 16 MiB write-out per writer, unpriced
     across writers, plus the spill's dirty page cache (file 115–120 MiB): the
     cgroup sits at 257–261 MiB, over `memory.max`.
- The counted classes are a minority of every peak (Q13 28.7 of 259). Pipes'
  user buffers are the biggest uncounted anon term.
- `lineitem_w` costs 1.7× the 10g wall (1697 vs 974 s) at the same memory: 24
  passes instead of 5 on Q9, discarded first builds of 13–24 s, Q10 119 of
  201 s idle. That is phase C material (§6.4), not W1.

### 6.2 Phase B, in this order (each step: RED first, then the fix, then the 10g A/B)

**B1. Receive buffers by law, priced.** One function (shared; `sql()`/`read()`
adopt it now, the transfer planes keep today's 4 MiB until the transfer round
measures them):

    need     = cores × RATE × rtt          bytes the cage can consume in one round trip
    per_pipe = clamp(2 × SKEW × need / pipes, FLOOR, CEIL)

- `rtt` measured per connection after the handshake (`TCP_INFO` `tcpi_rtt`, or
  the timed startup round trip where TCP_INFO is not available); `RATE` = the
  received bytes per CPU-second of the krecv-heaviest query in the phase A logs
  (a named constant with its source in a comment); `SKEW` = 2 (pipes are not
  equally loaded); `FLOOR` = 256 KiB; `CEIL` = 4 MiB (today's value).
- Same call on the MySQL read sockets (an explicit `SO_RCVBUF` ends autotuning,
  so the number is known).
- The ledger reserves `pipes × effective buffer` (read back with `getsockopt`)
  in a new class `Kernel`. Over its share the answer is **fewer pipes, never a
  window under the law's value**.
- `APITAP_RECV_BUF_BYTES` keeps overriding, as today.
- RED/GREEN: (a) Q1 and Q17 max `sock` on 10g at 256 MiB ≤ the reservation
  (today 63 and 58 MiB, unpriced); (b) Q9 at netem 50 ms within the spread of
  today's 124.2 s (n ≥ 3 each side); (c) 2c/2 GB and 8c/8 GB loopback Q1, Q9,
  Q17 no slower (the law must not cap a bigger cage).

**B2. Key sets sized by keys, not by range.** `KeySet` picks its form from
density: dense → today's bitmap; sparse → a two-level set (64K-key chunks, each
a sorted `u16` array under 4096 members, a bitmap above) or equivalent, so the
size is ≈ min(2 B × keys, range / 8). Membership stays O(1)-ish and
branch-light; the decoder gates built from a set keep working.
- RED/GREEN: unit test, 700K keys spread over a 280M range ≤ 4 MiB (today
  ~35 MiB); Q21 on `lineitem_w` `keys` class back near its 10g value; Q21, Q18,
  Q20, Q22, Q4 CPU on 10g no worse (these fold through key sets).
- A set that is over its share even by count goes to the existing path
  (`partitioned` by that key). No new path.

**B3. Spill writers under one budget, and their page cache released.**
- Writers share one write-out budget reserved in `Stage` (the per-writer buffer
  shrinks as writers multiply, with a floor; under the floor, fewer writers at
  a time).
- After each flushed chunk: start writeback for it, and drop the chunk before
  it from the page cache once written (`sync_file_range` + `posix_fadvise
  DONTNEED`, Linux only, a no-op elsewhere). Same `DONTNEED` behind the reader
  when a stage is read back. Keep only if Q13's wall is not worse.
- RED/GREEN: Q13 cgroup peak ≤ 240 MiB on both seeds (today 257–261).

**B4. The ledger of §2.3, now with measured prices.** Classes Build / Dims /
Groups / Keys / Stage / Pipes / Kernel; pipes' user buffers priced from what
phase A measured per pipe (Postgres raw plane and MySQL separately); dimensions
reserved as a sum; constants as `f(limit)` pinned at 256 MiB by the unit test.
The `[mem]` line prints reserved next to actual; the harness asserts
`actual non-file ≤ reserved + slack` at every mark. Extend the marks to the
HAVING key-set read so Q18's peak phase is attributed (its 175/211/201 MiB
spread across runs is unexplained: say what it is).

Not in B: making Q9 answer at 128 MiB (a dimension larger than the build
budget stays a named refusal for now).

### 6.3 Done when (replaces §2.5 items 1 and 4)

1. `lineitem_w` and 10g suites at 0.5c/256 MiB: 22/22 exact, **cgroup peak
   ≤ 240 MiB on every query**, and at every `[mem]` mark actual non-file ≤
   reserved + slack. RED today: Q13 (peak), Q1/Q17 (sock unreserved), Q21 on
   `lineitem_w` (224 MiB, keys 165).
2. 10g at 128 MiB: unchanged rule (exact or named refusal, zero OOM-kills).
3. 10g at 256 MiB interleaved a0/new, n ≥ 2: no query worse than the spread on
   wall, CPU, peak. Totals stated.
4. The WAN leg of B1(b) and the cage legs of B1(c).
5. One gate leg (`gate.py`, `--matrix` clean) that fails on the 0.67.2 wheel and
   passes on the build: Q13 peak ≤ 240 MiB and Q1 `sock` ≤ reserved, on the 10g
   seeds (no new seed needed in the gate). `e2e_sql_tpch.py` 4 configs green.
6. Report: per step kept / neutral-reverted with numbers, the reserved-vs-actual
   table for the worst five, and what was not verified.

Stop and hand back at G2. Do not start phase C.

### 6.4 Measurement to bring to G2 for phase C (no code change)

For Q9, Q10, Q3, Q5 on `lineitem_w` with `APITAP_DEBUG`: per pass, the source
statements issued (from `pg_stat_activity` / `processlist`), each one's wall
and rows, and the client's CPU in that pass; and how the engine decided the
pass count (what it knew before the first, discarded build). The question for
C is whether each table can be scanned **once** (range-partitioned to local
stages when the build does not fit) instead of once per pass, and whether the
pass count can be known before the first build from cheap statistics.

## 7. Gate G2 decision (2026-11-13): B1–B3 kept, phase B is not closed

### 7.1 Standing (build b4, `.so` 71bfe314, uncommitted)

- Kept: B1 (receive buffers by the law, floor 1 MiB), B2 (key sets by count),
  B3 (spill writers under one budget, page cache released).
- Met: 22/22 exact on both seeds at 0.5c/256 MiB, highest peak 222 MiB (was
  259–261); 128 MiB axis; the leg RED on 0.67.2 and GREEN on the build;
  `--matrix` clean; `e2e_sql_tpch.py` green.
- **Not met, and both block "done":**
  1. **No-regression (§6.3 item 3).** 10g, a0 → b4, n = 2: wall +2.0 %, CPU
     +3.6 %. Q19's CPU comes from B1's floor; Q4, Q7, Q20 are high even with a
     fixed 4 MiB buffer and are not attributed.
  2. **Reserved ≥ actual.** 75 of 335 marks (10g) and 84 of 486 (`w`) are over
     their reservation. Memory is bounded by measurement on two seeds, not yet
     by construction.
- New facts: allocator retention is real (H4): 99–101 MiB of anon held at
  `finalize` with ~0 counted (Q2, Q7, Q13). Read queues are bounded by count,
  not by bytes (Q18 swings 175–211 MiB with queue fill; reads at ~24 MiB per
  pipe against a 16 MiB price).

### 7.2 Phase B′ (gate G2b), in this order

**B5. Find the CPU, give it back.** On a quiet box (no build running): a0, b1,
b3, b4 interleaved, n ≥ 5, CPU-seconds first and wall second, on Q4, Q7, Q8,
Q18, Q19, Q20 and Q1; then the fix for each attributed cause, or the revert of
that piece. Suspects, cheapest check first:
- work done without `APITAP_DEBUG`: the ledger sampler started by
  `ledger::begin()`, mark arguments computed at every phase boundary, per-batch
  reservation updates. Without debug the ledger must cost a handful of atomics
  per phase and no thread.
- `KeySet` dispatch in the fold loop (flat vs sparse decided per lookup): hoist
  the form out of the loop / monomorphize the two paths.
- B3's `DONTNEED` behind readers on stages that are read more than once.
- B1 on short queries: the law is a **lower** bound. Give each socket
  `min(CEIL, Kernel share ÷ pipes)` when that is above the law, so a cage with
  room keeps today's 4 MiB and only a tight one goes down to the law.
Done when: per query, mean CPU and mean wall inside a0's range at n ≥ 5; totals
of the 22-query 10g suite a0/new interleaved n ≥ 3 not above a0's spread.

**B6. Read queues bounded by bytes.** One byte semaphore per read, sized to its
`Pipes` reservation: a decoder acquires a batch's bytes before it sends, the
fold releases them after it has consumed the batch (a batch larger than the
whole permit passes alone). Reads on planes that never registered a pipe
register. Then the pipes' actual memory is at most its reservation by
construction, not by an observed price.

**B7. Allocator retention.** Print `mallinfo2` (in use / free in arenas /
mmapped) in the `[mem]` line to split "held by the engine" from "held by the
allocator". Then, glibc only: `malloc_trim(0)` at phase boundaries (a few calls
per statement), and/or a fixed `M_MMAP_THRESHOLD` so large buffers go back to
the kernel on free. Keep what lowers the next phase's peak with CPU inside the
spread; mimalloc stays rejected.

**B8. The assertion.** With B6 and B7 in, compare each interval against the max
of its two ends' reservations (the engineer's `ledger2.py` rule), slack
≤ 16 MiB: zero marks over on both seeds, enforced by the leg (today the leg
prints "marks over their reservation" and passes: it must fail on it).

Stop at G2b. If B6 or B7 cannot meet its condition after a real attempt, report
the numbers; do not widen further.

### B8 decision (architect, 2026-11-14)

The assertion counts what the engine holds, not what glibc keeps on its free
lists. Per mark, `held = anon − min(glibc free, anon − live)` (live = mallinfo2
in use + mmapped), the same `held_bytes()` rule `free_pipes` now prices its
pipes by, plus sock, is compared with the interval's reservation (the larger
of its two ends) plus a slack of at most 16 MiB: one definition for planning
and for the check. glibc free is not a reserved term: on build b6 every mark
over raw anon on both seeds was covered by glibc free at that mark (none had
another cause), and its bound is the cage itself, guarded separately by the
peak condition (cgroup peak ≤ 240 MiB on every query, both seeds) and reported
as the most glibc free seen per seed. The leg `e2e_sql_bounded.py` fails on any
mark over under this rule and on the peak; checked against raw anon it must
fail on Q13.

## 8. Phase C design: every source table is read once per statement

From the §6.4 measurement on `lineitem_w` (a2, 0.5c/256 MiB):

| query | wall | passes | what is read more than once |
|---|---|---|---|
| Q9 | 195 s | 24 | each `lineitem_w` page part by ~3 passes (ctid spans are coarser than passes); a first build of the whole range is thrown away; 8 MIN/MAX statements of 2.7 s each |
| Q3 | 102 s | 14 | `lineitem_w` ~2× |
| Q10 | 212 s | 4 GROUP BY parts | everything 4×: dimensions, the orders build, all of `lineitem_w` |
| Q5 | 97 s | 2 | first build discarded (15 s) |

The pass count is learned by building once and throwing the build away, and
passes re-ask the servers for rows the engine has already received. Both are
the same defect: **the engine cannot keep what does not fit, so it re-reads.**

**C1. Hybrid grace, generalised.** When a build (or a group set, for
`partitioned`) passes its reservation mid-read, nothing is discarded: the key
range is split, the ranges that fit stay in memory, the rows of the others go
to local stages (`mem::Writer`, one writer per range under B3's shared budget),
and the read continues to its end. The probe side is read once the same way:
rows of the in-memory range fold immediately, the rest are staged by range.
Each later range is then joined from its two stages, no server statement. A
range whose stage is still over the reservation splits again from its stage
(never from the server).
- Pass count needs no estimate and no discarded build; catalog estimates only
  pick the initial fan-out.
- `partitioned` (Q10's GROUP BY parts) replays its inputs from stages instead
  of re-reading dimensions, build and probe from the servers per part.
- The page-part MIN/MAX statements go away on this path (no span pruning is
  needed when a table is read once).
- Disk is the new budget: staged bytes are reserved in `Stage` against the
  free space of the stage directory; over it, the existing key-range passes
  remain the fallback, named in the debug line.
- No server does more than scan and send (hard rule kept).

Risk to measure before building all of it: staging costs client CPU (encode +
write + read back) that a second server scan does not. Prototype on Q10 and Q9
only, behind an env switch, and compare CPU-seconds and wall on both seeds; the
10g Q9 (5 passes, 77 s) is the control that must not get slower.

**C2. Then S1–S5 of §3** as the phase A CPU split ranks them (fold in pipe
workers first: at 2 cores Q9 uses 1.0 core).

Phase C starts only on the architect's word after G2b. The DuckDB leg (§4b)
runs between G2b and C, on the build that closes B′.

## 9. Gate G2b decision (2026-11-14): phase B accepted for release work, three gaps open

Build bf (uncommitted). Against a0 on 10g, no debug, n = 3: wall 957.8 → 925.0 s,
CPU 370.0 → 368.8 s, highest peak 260–266 → 217–219 MiB, 22/22 exact;
`lineitem_w` 22/22 exact at 217 MiB; 128 MiB axis 21 exact + Q9 refused, no OOM;
leg RED (raw anon) / GREEN (held rule); `--matrix` clean.

Accepted: B1, B2, B3, B5, B6, the statement-start trim, and B8's rule. The
memory guarantee for this release is **measured**, not yet by construction: 21 of
959 debug marks are over their reservation by a few MiB, all live engine memory
(no glibc free). The assertion stays debug-only; the leg (Q13, Q1, peak ≤ 240)
is the gate.

Open, for the next round (not this release):
- **B9** `free_pipes` prices from the ledger's reservations, not from measured
  process memory (also removes the Q21 pipe/peak interaction, 188 → 208 MiB).
- **B10** the large mmapped read/build blocks (Q18 build: 66 MiB mmapped beside a
  72.5 MiB reservation) and the block held across Q16's statements (14 MiB) get
  a class.
- **B11** Q21 stage/key memory past its reservation (3–5 MiB beyond slack).

Before release (release-readiness round, §10): the glibc fix below, the
independent review's findings, Q14 settled at n ≥ 5 (+0.7 s wall in two n = 3
sets), the full gate (read, transfer, CDC, TLS, WAN), the PGO wheel A/B against
the released 0.67.2 PGO wheel, cages 2c/2 GB and 8c/8 GB, and the held rule once
at 128 MiB with debug.

**Release blocker found by the architect:** `ledger::malloc_split` calls
`libc::mallinfo2` directly under `cfg(linux, gnu)`. `mallinfo2` exists from glibc
2.33; the wheels are manylinux2014 (glibc 2.17). Resolve it at runtime
(`dlsym`, once, cached); when it is absent, `held_bytes()` falls back to RSS
(the 0.67.2 behaviour) and the debug line says so. Test: the fallback path is
exercised by a unit test that forces "absent".

## 10. Release-readiness round R (2026-11-14): review findings + gates

Independent review of the bf diff (read-only, no build). Findings 2, 3, 6 were
spot-checked by the architect against the code and hold. Each item: fix with a
test seen RED first, unless marked otherwise.

| # | finding | disposition |
|---|---|---|
| R1 | `mallinfo2` linked directly: `import apitap` fails on glibc < 2.33 (wheels are manylinux2014; `-z now`) | **blocker.** `dlsym(RTLD_DEFAULT, "mallinfo2")` once, cached; absent → `None`; `held_bytes` then = RSS. Unit test forces "absent". Prove the `.so` has no undefined `mallinfo2` (`nm -D --undefined-only`). |
| R2 | sparse key set at low density costs 3–6× the old hash set (4M keys over 2^36 → ~230 MiB vs ~64 MiB) | **blocker.** Choose the form by cost: when occupied chunks × ~48 B (+ directory) passes keys × 16 B, go to the hash set. Test: 4M keys over 2^36 ≤ old hash size. |
| R3 | a list chunk near 4096 unique keys re-sorts 4097 elements on every duplicate | **blocker.** Promote on unique count; compact only on capacity doubling; a duplicate never triggers a full sort. Test: 10M inserts of ≤ 4096 unique offsets into one chunk bounded in time (count sorts, not wall). |
| R4 | `held_bytes` subtracts glibc `fordblks` that `malloc_trim` already returned → under-counts, too many pipes when the process holds non-glibc memory (polars, pymalloc, stacks) | Prefer: after a trim, held = RSS (the trim already returned what can be returned). Must keep Q13 inside a0's wall range at n ≥ 5. Add a leg: a 60 MiB polars frame held in the process, then Q13: no more pipes than 0.67.2, no OOM. If neither rule satisfies both, report. |
| R5 | MySQL raw lane: `SO_RCVBUF` is capped by `net.core.rmem_max` and locks autotuning → ~416 KiB on default hosts (0.67.2 autotuned to 6 MiB) | Read `rmem_max`; if the law's size is above it, do not set the option (stay on autotune) and reserve `tcp_rmem` max. Same rule on the Postgres read lane only if it never asked more than 0.67.2 did. |
| R6 | star dimensions: the sum check now refuses plans 0.67.2 answered; sum uses `bytes()`, per-dim uses `bytes() + payloads × 24` | **blocker (no-regression rule).** Never refuse what 0.67.2 answered: over the sum, fall back to 0.67.2's per-dimension behaviour (and say so in the debug line); one byte metric for both checks. Leg: `e2e_sql_tpch.py` tiny-budget config + a two-60-MiB-dimension case. |
| R7 | RTT kept as a process-wide max; `pipes_within` silently lowers an explicit `parallel=` | RTT per endpoint (host:port), latest value, not max; an explicit `parallel=` lowered by the law gets the same `progress::note` as the memory clamp. |
| R8 | each read sizes its sockets as if alone (8 one-pipe `Early` parts → ≥ 64 MiB sock) | Size from the process-wide Kernel share left (`RESERVED`), not this read's share; test with 8 concurrent one-pipe reads. |
| R9 | `apitap.read()` with 1–2 pipes: batches 12.5 → 1.5 MiB, 4–8× more Python iterations | Measure first: `read()` of 10M rows at 1 and 2 pipes, 0.5c/256m, a0 vs new, n ≥ 3. If slower: batch floor = the old size whenever the gate has room. Remove `APITAP_READ_GATE`. |
| R10 | junk `APITAP_RECV_BUF_BYTES` means autotune on read lanes, 4 MiB on transfer planes | One parser, one meaning (the transfer planes' existing behaviour). |
| R11 | `rcvbuf::reserved()` dead outside tests; two tests depend on the host cgroup | Fix dead code (`-D warnings` clean); tests take the budget as a parameter. |

Also in R: Q14 at n ≥ 5 (a0 / bf / bf without B6) — attribute or fix; the held
rule once at 128 MiB with debug.

**Then the gates, on the final R build, in this order:** (1) unit tests +
clippy `-D warnings`; (2) full `gate.py` (all legs, not `--matrix`) incl. read,
transfer, CDC pg/my, MySQL TLS, WAN Q9 at 50 ms; (3) the PGO wheel built the
release way, A/B against the released 0.67.2 PGO wheel, 22 queries 10g,
interleaved, no debug, n ≥ 3, plus `lineitem_w` once; (4) cages 2c/2 GB and
8c/8 GB, Q1 Q9 Q13 Q17 Q21, n ≥ 2. Stop at gate R with one report; no commit,
no tag, no publish.

## 11. SF50 on GCP (2026-11-15): Q9 still OOMs, on 0.67.2 and on wheel H

Run: tpch-pg + tpch-my c3-standard-8-lssd (SF50, 300M lineitem, TLS, VPC), test VM
c3-standard-8, each query in `--memory=256m --cpus=0.5`, truth = Postgres natively.

| query | 0.67.2 (PyPI) | wheel H (.so 9959489c) |
|---|---|---|
| Q6 | exact 32.1 s, 68 MB | exact 31.3 s, 70 MB |
| Q12 | exact 76.5 s, 118 MB | not run |
| Q14 | exact 57.4 s, 162 MB | not run |
| Q3 | exact 488.5 s, 142 MB | not run |
| Q5 | exact 141.7 s, 114 MB | not run |
| Q10 | exact 324.7 s, 162 MB | not run |
| Q18 | exact 1063.1 s, 206 MB | not run |
| **Q9** | **OOM after 39.5 s** | **OOM after 41.0 s** |

Q9 diagnostics (wheel H unless noted; host-side cgroup sampling every 100 ms, debug logs
in `~/w1/gcp/dbg/` on the VPS):

| cage | result | anon at death |
|---|---|---|
| 256 MB, debug | OOM 38.8 s | 234 MiB |
| 256 MB, debug, 0.67.2 | OOM 41.0 s | 228 MiB |
| 512 MB | OOM 41.0 s | 507 MiB |
| 1 GB | OOM 41.2 s | 987 MiB |

The death is not a slow climb: anon sits at ~129 MiB from 25 s to 38 s (dimensions
loaded: part 544K rows, partsupp 2.18M rows, dims = 49 MiB), then jumps to the cage in
under a second, **at the moment the probe side's range arrives** (last line in every
run: `probe range known at ~39 s: build narrowed to [1..300000000]`). The same jump
fills 512 MB and 1 GB, so it is one allocation proportional to what was built so far,
not to the cage. The lineitem_w seed never showed it: its probe range (8 page-part
MIN/MAX at ~2.7 s each on 60M rows) arrives early; on 300M rows over a disk-bound
server it arrives at ~39 s, after the build has filled a large part of the range.

**Architect's reading of the code (to be confirmed by the engineer):** `sql.rs`
~5249–5256, in the build loop after the probe range is known. With the range final,
`need` = the whole remaining range; when `need > c.dmax || need > c.budget`, the direct
array is converted in one step: `pairs.extend(a.iter().enumerate().filter(..).map(..))`
— 16 B per filled slot (i64 + u32), allocated by `extend` growth, unpriced, while the
direct array is still alive. Nothing checks it against the build budget; `join_pass`
only returns `false` (key-range passes) after the fact. Also printed before it:
`[plan] pipes=7 model_peak=320MiB mem_limit=256MiB` for the partsupp read, and three
concurrent 2-pipe reads at model_peak 120 MiB each: reads are planned one at a time.

### W1b work order (engineer; QA verifies)

1. **Reproduce on the VPS, RED first.** The trigger is latency of the probe side's
   range. A test-only knob (cfg(test) or a debug-only env, never on by default) that
   delays the probe range future by N seconds reproduces it on `lineitem_w`. RED on the
   0.67.2 wheel and on H: Q9 at 256 MB OOM with the delay; the pass without the delay
   stays as it is. Show the jump in the cgroup samples.
2. **Fix by construction:** the direct → pairs conversion is reserved in the ledger
   (`Build`) before it allocates; if it does not fit, stop the build here and go to the
   existing key-range passes (`join_pass` → `false`) with the direct part already built
   kept as the first pass, instead of converting. No conversion may allocate more than
   the build budget left; `pairs` grows under the same check. Look for other one-shot
   conversions of the same shape (direct → pairs, bitmap → hash) on the build path.
3. **Concurrent reads priced together:** the `[plan]` model peak of reads that run at
   the same time (dimension reads, the build read, `pre`/probe read) must sum within the
   limit; a read whose model peak alone passes the limit takes fewer pipes. This is the
   pipe side of B9.
4. Done when: the delayed-range repro is GREEN at 256 MB and 128 MB (exact or named
   refusal, never OOM); no regression on the 10g suite and `lineitem_w` against the
   released 0.67.2 wheel; then SF50 on GCP (owner's word; `up.sh full` resumes the paused
   VMs with their data): all 8 queries exact at 256 MB / 0.5 vCPU, Q9 included.

### 11.1 QA finding (2026-11-15): W1 made the bigger cages worse — blocker

QA, own trigger (a view on lineitem whose statements answer 40 s late), Q9, 0.5 core:

| trigger | cage | 0.67.2 (c4a41758) | H (9959489c) |
|---|---|---|---|
| range 280M (`lineitem_w`) | 512m | exact, 1876 s, 475 MiB | OOM ~66 s (anon 308 → 493 MiB in 0.6 s) |
| range 280M | 1g | exact, 987 s, 652 MiB | OOM ~69 s (846 → 1017 MiB in 0.5 s) |
| range 60M (10g) | 512m / 1g | exact | exact |
| range 60M | 256m | OOM | OOM |

Cause: W1 turned `direct_max` from a flat 64M keys into limit/4, so the build fills a
larger direct array while the probe range is pending, and the unpriced conversion to
pairs grows with the cage. Requirement added to W1b step 2: **while the probe range is
pending, the direct array may grow only as far as its conversion would still fit**
(direct bytes + 16 B × filled entries ≤ build budget left); the conversion itself is
reserved; past either, key-range passes. Acceptance adds: the wide-range trigger at 256m,
512m and 1g, never OOM, no worse than 0.67.2 anywhere it answered.

### 11.2 Backlog from QA (2026-11-15), pre-existing in 0.67.2, not this release

- **L1 plain LIMIT refused:** `SELECT l_orderkey, l_quantity FROM pg.lineitem_10g LIMIT 5`
  is refused after ~11 s with "groups past their memory budget (32 MiB)" (no GROUP BY in
  the query) on 0.67.2 and wheel I, 3/3. A projection with LIMIT must stream and stop
  early, or be refused up front by name.
- **L2 LIMIT not pushed through a join:** lineitem × orders `LIMIT 5` reads everything:
  wheel I 937–986 s (3/3 answered), 0.67.2 1155–1251 s with one OOM-kill in 3. Stop the
  probe once LIMIT rows are produced when there is no ORDER BY/aggregate above the join.
- **L3 concurrent statements share no memory budget** (QA p6, 10 `sql()` + 2 `read()` in
  one process): at 256m both 0.67.2 and wheel I are OOM-killed 2/2 (each statement budgets
  as if it owned the cage); at 1g / 2 CPU 0.67.2 is OOM-killed 2/2 while wheel I answers
  (11/12 and 12/12). Pre-existing; fix = one process-wide ledger across statements.
- **L4 pool timeout under concurrency** (wheel I, 1g, once in 2 runs): Q14 "key range of
  lineitem_10g: pool timed out while waiting for an open connection". Not comparable on
  0.67.2 (it OOMs first). Connection pool size vs concurrent statements; a statement must
  wait or fail by name, not time out on its own key-range query.
- **L5 the "≤ 240 MiB" bound holds on the 22 TPC-H texts, not on every Q13-shaped query**
  (QA6, Q13 with another LIKE: 0.67.2 267 MiB, wheel I 261 MiB, no OOM). Release notes say
  so.
- **L6 CDC → ClickHouse (insert-only/Replacing) applies unchanged-TOAST updates key by
  key** (QA p9, engineer's diagnosis 2026-11-16): per key a `SELECT … FINAL WHERE id=N
  LIMIT 1` readback of the unchanged TOAST value, an INSERT, a residue-journal INSERT and
  a `DELETE … WHERE id=N` — ~0.5 s per key, so a burst with ~10K unchanged-TOAST updates
  takes over an hour while the reader stops reading by design. Fix: set-based readback
  (one `SELECT … WHERE id IN (…)` per window), no per-key DELETE. Also: progress shows
  `changes=0` until a window commits, so a slow window looks like a hang — progress must
  count applied keys inside a window. Measured (QA p12, fresh tables, interleaved r/i/r/i, 600 s each): 0.67.2 1192/1236 keys,
  wheel I 1199/1288 keys — the same ~2 keys/s; per-key DELETE (with the lease subquery)
  averages 400–440 ms and is ~500 of the 600 s; SELECT readback 32 ms, INSERT 16 ms.
  Pre-existing, not this release. First fix: no per-key DELETE (tombstones as a batched
  INSERT into the Replacing table), then batched readback.
- **L7 incremental views share a replication slot by query fingerprint → silent wrong
  answer** (QA p10, 2026-11-16; pre-existing, `sql/live.rs` ~184 and ~1160): the slot is
  `apitap_v_{fp16}_{side}` from the query + sources only. Two views of the same query into
  two destinations (or the same view from two hosts / two state dirs) use one slot; the
  one that drains second sees only part of the changes and lands a wrong answer with no
  error. Fix (small): on every drain, check that the slot's `confirmed_flush_lsn` equals
  the position this view's state file recorded; if the slot moved past it, refuse by name
  ("another consumer advanced slot …; rebuild this view"). Also make the slot name include
  the destination (URL + table), so two declared views never collide. MySQL side: same
  check against the stored binlog position is not needed (binlog is not consumed), but the
  state identity check applies to the Postgres side of a pair.

## 12. Production round P (2026-11-16): owner's bar — "until users can use it in production"

The owner set the release bar: no release until it is clean for production use, not only
"no worse than 0.67.2". Classification of every open finding:

| item | class | in this round |
|---|---|---|
| L7 incremental views share a slot → silent wrong answer | wrong answer | **fix** |
| L6 CDC → ClickHouse unchanged-TOAST apply ~2 keys/s, progress shows 0 | looks hung / hours | **fix** (no per-key DELETE, batched readback, progress counts applied keys) |
| L1 plain `SELECT … LIMIT n` refused ("groups past their memory budget") | ordinary query refused | **fix** (projection + LIMIT streams and stops early) |
| L4 pool timeout under concurrent statements | ordinary use errors | **fix** (wait for a connection or fail by name; no timeout on the engine's own key-range query) |
| L2 LIMIT through a join reads everything | slow, answers | fix if small after L1, else next |
| L3 many `sql()` statements in one 256 MB process OOM | niche limit (also 0.67.2) | document; process-wide ledger next round |
| L5 peak > 240 MiB is page cache | wording | release notes state the non-file bound |
| B9–B11 ledger gaps, Q13 beside a large non-glibc frame | known limits | document; next round |
| Q21 CPU +2.6 % (wall better) | trade | accept, stated |

Each fix RED first (a test or leg that fails on the current wheel), then QA verifies on
the final md5 with its own harness. After round P: the engineer's release checks run once
on the final wheel (full gate, cages 2c/8c, WAN, CDC A/B incl. a TOAST burst, Q7/Q21),
QA's remaining CDC checklist (my→ch, pg→my, kill/rerun + lease timing, `APITAP_RECV_BUF_BYTES`
variants, largest-transaction sweep), and SF50 on GCP on the owner's word.

### 12.1 Architect decisions during round P (2026-11-16)

- **L7 upgrade (orphaned slot):** a re-tagged view must not leave its old slot holding WAL.
  Built in p2 (`sql/live.rs` adopt_old_state): new-tag state absent + `{fp16}.state` in
  this state dir + every PG side's old slot exists, inactive, `confirmed_flush_lsn` = the
  old state's position, old publication exists → adopt (alias file, state renamed to the
  new tag, slot and publication keep their old names). The old state never recorded its
  destination; accepted, because the state is the query's aggregate and `land()` replaces
  the whole destination, and adoption happens once. Any check fails → new-tag bootstrap +
  WARNING naming each old slot with its `pg_drop_replication_slot` statement. Never
  auto-drops. Leg `e2e_sql_live_upgrade.py`: RED OLD=pgoI NEW=p1, GREEN NEW=p2.
- **L1 early stop scope:** kept as built (wraps only the final rewritten SELECT's
  `run_one`; CTE/derived/IN are answered before it; semi key sets load through their own
  reader; stop needs one side, no star). Invariant to state in a code comment. Leg
  `e2e_sql_limit.py`, 12 cases on fixtures where a cut inner read returns 0 rows; QA
  checks adversarially on its own fixtures. Explicit plumbing only if either finds a hole.
- **L4 RED baseline** = wheel I (5c27ae7e); 0.67.2 has no delay knob, not comparable.
- **CDC lease after a kill** (QA p13): reruns refused ~320 s, same as 0.67.2, message names
  it, data exact after. Accepted; release notes say a killed CDC run holds its table ~5 min.
- **QA p13 (CDC remainder, wheel I vs 0.67.2):** all routes exact, kill/rerun exact, no OOM.
  Two follow-ups: `APITAP_RECV_BUF_BYTES=junk` is silently ignored → p2 refuses an
  unparseable value by name (a typo must not look applied); one transaction of 20k × 5 KB
  peaks at 234–239 MiB on my→ch in a 256m cage (same on 0.67.2) → release notes state the
  largest-transaction bound with this number.
- **Collation (p3), all class a:** `SELECT DISTINCT` without aggregates answered, folding
  like GROUP BY (MySQL _ci GROUP BY was already right on 0.67.2); PG `citext` GROUP BY /
  DISTINCT folded (pre-existing: 3 groups vs PG's 2); PG nondeterministic ICU collations
  refused by name (pg_attribute/pg_collation probe). **ORDER BY text** on a non-C / _ci
  collation sorted by bytes (pre-existing, e.g. glibc en_US: a < B, AC < A-D → ORDER BY …
  LIMIT returns other rows). Chosen: byte sort + conservative adjacent-pair guard over the
  sorted list, refuse by name (column, collation, first offending pair ≤16 chars each,
  remedy COLLATE "C"/_bin) when byte order may disagree; ties among collation-equal values
  are not refused. Rejected: refuse all non-C text ORDER BY (would refuse TPC-H on any
  non-C database); real collator (icu4x, 3–5 days, still not bit-identical to glibc/DUCET).
  QA judges against the server's own ORDER BY, not DuckDB. Bench PG is Alpine/musl (its
  en_US sorts by bytes): RED needs an ICU column or a Debian PG.
- **p2 results (2e44345b):** L7 rel0672 RED → p2 GREEN; upgrade no-adopt RED → GREEN from
  OLD pgoI and OLD rel0672. L1 unit leg passes everywhere host-uncapped; the L1 RED is
  `l1.sh` in the 256m cage (rerun on rel0672 in chainP3). **L4 NOT green on p2**: both
  key-range probes ask 8 connections each; under a role/server limit sqlx retries forever
  while parts hold the rest. p3: probe halves 8→4→2→1, PG connect timeout named. Architect
  requirement: bounded outcome for N statements vs a role CONNECTION LIMIT (answer exactly
  or fail by name), acquire-all-or-release if halving can livelock.
- **p3 additions:** MySQL NULL placement per source (explicit NULLS FIRST/LAST was ignored
  on every source before — pre-existing, fixed); legacy MySQL _ci (general_ci, unicode_ci,
  unicode_520_ci, latin1_swedish_ci, MariaDB uca1400): ASCII-only fold, refuse by name on
  non-ASCII in GROUP BY/DISTINCT/deciding ORDER BY pairs (exact weights need WEIGHT_STRING
  lookups, ~1–1.5 days, not now; no GPL tables). Stage-table WHERE on legacy _ci: sticky
  flag → refuse by name, not documented as a gap.
- **L8 (new, backlog):** plain `SELECT … ORDER BY text LIMIT n` over 60M rows refused past
  the group budget on every version (GROUP BY-every-column rewrite). L1-class; next round.
- **L8 moved INTO round P (p4):** chainP2 data point — `SELECT 3 cols FROM lineitem_10g
  ORDER BY l_orderkey LIMIT 2` refused at 471,491 groups (37 MB > 32 MiB) even on an
  integer key: ordinary query refused (class c). p4 = streaming top-N heap for plain
  projection ORDER BY + LIMIT (priced n+offset rows; too large → refused up front by name);
  guarded text keys keep full sort + pair guard while within the group budget, refused by
  name past it. p2 L1: plain LIMIT 5 answers in 1.9 s, WHERE+LIMIT 3.1 s; LEAK=5 RED shown.
- **L9 (backlog):** incremental views with a WHERE compare on a pre-0900 MySQL _ci column are
  refused at plan time (conservative; no per-statement scope in a view plan).
- **Legacy _ci mark:** per statement (task-local Arc in each condition), not process-wide.
- **L8 folded into p3** (chainP3 had not started): single-table plain projection ORDER BY +
  LIMIT n keeps n rows (cut at 2n / half budget); n rows past budget → refused up front by
  name; guarded text key → full sort + pair guard, parts path may still answer, else named.
  Joins unchanged (L2 stays backlog). Legs `e2e_sql_topn.py` (5 cases, 256m/0.5, vs PG) and
  the l8.sh sweep, RED rel0672.
- **QA collation suite on p3 (62c6fc61):** all ten silent wrong answers of 0.67.2/I/p2 gone
  (exact or named). **Regression (class c):** ORDER BY text on the PG default collation
  (C1, C17) now refused on musl en_US where byte order was right. p4: comparison oracle —
  undecided adjacent pairs batched to the source (PG unnest … COLLATE, MySQL STRCMP …
  COLLATE), one round-trip per statement per column, capped (~4096 pairs / 1 MiB); server
  agrees → answer, disagrees or over cap → named refusal with the first disagreeing pair.
  ORDER BY only; GROUP BY equality keeps refusing.
- **L4 after p3:** probe fixed; two more raw-timeout sites (pin acquire / sqlx plane, the
  parallel read's snapshot holder on SQLSTATE 53300) → bounded waits + named in p4. Reads of
  one statement CAN wait on each other (build read awaiting the probe range while holding
  pipes; prefetch at ≥1 GiB), so a single join on a role with 1 free connection refuses after
  300 s — class c. **p5:** per-source process-wide connection budget (headroom read at first
  contact, re-read on a named refusal); each statement reserves its plan's minimum
  concurrent connections all-or-nothing, FIFO, bounded; planner falls back to a serial plan
  (needs 1) when the overlapped minimum doesn't fit; extra pipes opportunistic. Deadlock-free
  within a process by construction; bounded waits remain across processes.
- **p5 scope revised:** per-statement headroom probe → serial mode when small (sql.rs only,
  ~3 h); a single join on a role with 1–2 free connections answers. The process-wide
  connection budget (5 connection sites: PG snapshot holder, raw COPY/walsender reads +
  sqlx fallback, key-range pool, one-shot schema/estimate/collation/oracle/semi pools, MySQL
  equivalents; ~800 lines / 8 files, touches transfer/read/CDC) is **L3b, next round**,
  designed together with L3 (one process-wide ledger for memory and connections).
  Concurrent statements on a tight role: bounded wait then named (class d, documented).
- **p5 final shape:** overlapped_need = 1 + 8 + 2×max(2·cores, 2, 8) (17 at 0.5 core);
  serial only below it, pipes ≤ free−1; star dims read one at a time under the cap; < 2 free
  and a source read 2+ times → refused by name (no unpinned answer); read once → unpinned
  is fine (one span = one snapshot). Leg e2e_sql_pool_low: PG limit 1 one-table exact,
  limit 1 join named, limit 2 join exact, limit 2 star exact, MySQL MAX_USER_CONNECTIONS 1
  join exact.
- **L11 (release notes + backlog):** a statement reading one MySQL server more than once
  sees each read at its own moment (no snapshot export in MySQL), at any limit,
  pre-existing. Notes say so; optional single-connection consistent mode with L3b.
- **p6 (86feec28) QA:** collation suite passes except one class-a defect: text ORDER BY …
  DESC under a non-byte collation answers in byte order (oracle asked the DESC pair in the
  ASC direction; pre-existing on 0.67.2, but p6 claims to guard it). C1/C17/C4o exact again
  on musl via the oracle; ASC refusals cover the whole candidate set. p7: DESC flip + audit of
  every direction-dependent path (NULLS under DESC, multi-key tie-break, order_limit,
  pair_ok), RED legs; MIN/MAX(text) answered on byte-ordered collations, else an accurate
  named refusal (today: misleading "arithmetic on a Utf8 column"). **L12 (backlog):**
  MIN/MAX text under linguistic collations.
- **p6 perf (chainP6, 10g 256m/0.5):** 22/22 exact, p3 ≈ p6. n=3 interleaved medians vs
  pgoI: **Q1 77.3 s vs 61.5 s (+26%)**, **Q13 170.9 vs 150.1 (+14%)**, **Q4 15.1 vs 13.3
  (+13%), peak 114 vs 84 MB**; Q20/Q22 noise. Entered between pgoI and p3. Rule applied:
  perf profile (no debug, --symfs), cause before code, fix in p8; gate p8 within 3% of pgoI
  at n=3 on flagged queries. topn p6 5/5 incl. LIMIT 3,000,000 refused in 0 s by name.
- **p7 (b2d5ce9b) order_locale:** DESC defect fixed — my/pg DESC, DESC LIMIT 1, DESC + WHERE
  now refused by name ("x-y"/"x_y"); default-collation ASC/DESC exact via oracle; general_ci
  on MySQL and MariaDB exact. Leg's "mixed ASC/DESC" case BAD only because it groups by
  `id % 2` ("GROUP BY on the expression … not supported (apitap.sql() v1)") — leg to use a
  column key. **L13 (to classify):** GROUP BY on an expression refused by name (v1 limit).
- **pool_low p7 2/5:** cause = PgSource pool ≥2 + sqlx returns dropped connections from a
  spawned task, so the PK probe dials a second connection (53300 → 30 s acquire timeout).
  p8: probe/planning queries on one connection with eager return_to_pool; pgtrace proof.
- **L13 GROUP BY expressions:** tier (a) EXTRACT / date_trunc keys IN round P as p9 (~0.5 d;
  timestamptz only when the source default TimeZone is UTC, else refused by name with a
  remedy). Tiers (b) arithmetic keys (~1 d) and (c) CASE keys (~1–1.5 d) next round
  (L13b/L13c); docs/sql.md lists them as refused, with a tested CTE workaround if one exists.
- **P6b verdict (2026-11-16 11:23): the Q1/Q13/Q4 gap is PGO, not code.** Non-PGO hf (wheel
  I source) vs p6, n=2–3 medians: Q1 78.0 vs 79.6 s, Q4 15.8 vs 15.2 s (peaks 113 vs 118 MB),
  Q13 ~169 vs ~173 s; PGO pgoI Q1 62.5, Q4 14.7, Q13 ~154. Gate stays like-for-like; the
  release check is the PGO build of the final wheel vs pgoI. P8.go created.
- **QA on p7 (b2d5ce9b):** L4 concurrency mix 17/17 exact ×2 (wheel I 9–10/17 bad); Q14,Q6×12
  25/26, 24/26 (named refusals + the known PK-probe site); MySQL MAX_USER_CONNECTIONS 1
  exact; pl4lim PG PK-probe FAILs = RED for p8. QA's own L7/L6/P13 run invalid (harness:
  reused dest names → named "not the table … bootstrapped from" refusals; unlocked L7 beside
  its own L4). **New (L14, must-fix candidate, pre-existing):** incremental-view drain
  OOM-killed at 256m with ~332K change images on wheel I and p7 — drains must apply images in
  ledger-priced windows. **L15:** live-view path gives raw sqlx pool timeout under
  connection exhaustion (fold into p9). Docs: dropping a destination does not reset its
  _apitap_state row.
