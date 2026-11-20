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
- **QA on p8 MIN/MAX:** class a NEW in p8 — PG char(n) compared padded (MIN '' vs E'\t'
  wrong). p10: Pad per source (PG bpchar strip-trailing-spaces; MySQL PAD SPACE pad-shorter)
  through MIN/MAX, ORDER BY/top-N, parts order_limit, WHERE, GROUP BY/DISTINCT fold. Audit
  found a pre-existing wrong answer: the equality fold trimmed all trailing ASCII whitespace,
  grouping 'a\t' with 'a' (both servers) — fixed to spaces only. Leg e2e_sql_pad_order.py.
  Refusal text shows collations by their human name. **L16 (next round):** aggregates over
  both sides of a join (two-sided aggregation, 1–2 d); today refused with an accurate message
  and a CTE workaround (QA to confirm the workaround answers).
- **p10 (ed59b2de) = release candidate (2026-11-16 16:36).** chainP10: every leg PASSED —
  pad (RED p8 12/16 wrong → 14/14), live drain p10 peaks 47/87/107 MB (p7 73/204/161; p7 did
  not OOM in this leg — QA's case M is the L14 proof), date buckets incl. a non-UTC role
  refused, minmax incl. the M9 remedy shape, pool_low 5/5, collation/nulls/order_locale/live.
  Q4/Q12 n=5 vs p7: +2% / −8% (walls inflated by a concurrent QA scan, interleaved). Q12
  "creep" = noise: perf CPU p7 4.5 s vs p9 4.6 s, same symbols. Next: QA verdict on ed59b2de
  and release chain R (PGO wheel vs pgoI, full gate, cages, WAN, CDC A/B, Q7/Q21).
- **p11 (2026-11-16 ~17:00):** MIN/MAX on date/timestamp/timestamptz (PG) and
  DATE/DATETIME/TIMESTAMP (MySQL) was refused on 0.67.2 and p10 (class c, pre-existing).
  Folded as the integer (days / µs) through the Int path, answered in the column's own kind;
  refused inside arithmetic/HAVING. Leg e2e_sql_minmax_dates.py (RED p10). Release chain
  (chainRel1, PGO) moved to the p11 source, gated on p11's legs; the p10 PGO attempt was
  stopped on purpose (log ~/w1/chainRel1.p10-stopped.log). docs/sql.md "Refused today" table
  written (uncommitted).
- **QA regression on p10 (class a, introduced by p10's Pad change):** PAD SPACE keys got their
  own sort key K::P, and the pair guard only matched (K::B, K::B), so MySQL utf8mb4_general_ci
  (and 0900 _ci PAD SPACE) ORDER BY skipped the guard and answered byte order silently (p8
  refused correctly). p11: guard matches K::P pairs too, padding applied inside the compare.
  Leg e2e_sql_order_locale.py case 10 (QA's 4-row case, all rows, DESC LIMIT 5), RED p10.
  Lesson: a new sort-key variant must be added to every match on the old one — audit by grep.
- **QA case M on p10 (L14 NOT closed):** QA's view shape (qa_li, one ~166K-row UPDATE) at
  256m/0.5: exit 137 OOM on wheel I and on p10 (anon 246 / 254 MiB); 202K+ rows → named
  refusal at ~200 MiB RSS. Cause: APITAP_TX_BUF_BYTES defaults to the whole cage and counts
  less than resident bytes, so the named refusal races the OOM killer. Class b. **p12:** tx
  buffer default from the ledger's drain budget (cage minus planned non-buffer memory),
  accounting in resident bytes of decoded images, same rule for log_based CDC transfers;
  RED legs = QA's case-M shape and a pg→ch single-transaction ladder at 256m; never exit 137.
- **p12 (building 2026-11-18 19:2x):** tx cap default = 7/8 × (cage − RssAnon at drain start
  − planned 2× window − 4 MiB), env wins, above cage refused by name; tx charge in resident
  bytes at commit; changelog key compare without alloc, moved_from kept only for masked rows,
  old image moved; view masked key move now takes the unchanged-TOAST cell from its D's old
  image (refused "no old image" through p11 — found while writing the leg); musl + libc → byte
  order; top-N up-front refusal from n_distinct/index cardinality (no WHERE only); human
  refusal text. Legs: e2e_txcap_cage.py, e2e_changelog_key_move.py, order_locale case 11;
  control wheel chainP12x with masked moves broken must turn key_move RED.
- **p12 (1ed47f4b) early results:** txcap case-M shape at 256m: p11 50K exact 131 MB, 100K
  exact at 247 MB (unsafe), 166K exit 137; p12 50K exact 85 MB, 100K refused by name at 92 MB
  ("cap 153 MiB = 7/8 of (256 − 12 held − 68 planned)"). Never 137, conservative; real peak
  past the cap measured to tighten in p13. key_move RED p11 → 4/4 exact p12; QA masked PK
  move via view RED p11 → exact p12. order_locale case 11c refused before reading on p12.
- **Slot ownership (decision):** test legs dropped every apitap_v_% slot (8 legs fixed for
  p13: only slots publishing their own tables). Product: a first run that finds an inactive
  slot with its own derived name and no state of its own will REFUSE by name (another state
  dir/host may own it) instead of dropping it — p13, CDC lane and view lane; RED/GREEN leg
  with two state dirs. Full gate runs only with QA's view/CDC work idle (19 old CDC legs drop
  apitap_% slots).
- **p13 scope:** above + 2-table view refusal message + tx-cap charge tuned toward measured
  resident bytes (goal: largest exact with zero 137s).
- **Top-N pricing undercounts (class b, found via QA L8j2):** p12 LIMIT 200K → exact with
  anon 217 MiB (not page cache) in QA's runner; a lean polars runner with debug → exit 137 at
  anon 223 MiB. Priced against a 32 MiB group budget; real ≈ 2n groups before a cut + the
  cut's copy + pipes. p13: up-front price counts resident bytes of the worst concurrent set,
  so such a LIMIT is refused up front at 256m (or answered within budget if the cut gets
  cheaper). Also p13: view first-run refusal of an own-name slot it has no record of;
  remedy texts name the slot; e2e_sql_live case 8 (RED p12).
- **p12 (1ed47f4b) final, 2026-11-18 21:49 — not a candidate, six items open.** Closed: every
  exit 137 in the ladders (QA case M p11 137 → p12 named refusal at 88/90 MB with/without
  debug; pg→ch 1M-row transaction p11 137 → p12 plain lane exact, changelog 200K exact, 400K
  named), key_move, musl top-N (QA L8d/d2 exact). Open → p13: (1) tx charge over-counts
  1.7–2× (100K drains exactly at a real 158 MB under an explicit 240M cap; default refuses);
  (2) env above cage not refused on a bootstrap run; (3) top-N up-front refusal over-refuses
  the shape the parts path answered on p11; (4) top-N LIMIT 200K anon 217 MiB / 137 with
  debug — pipes 128 MiB reserved beside 30 MiB of groups; plan must price both; (5) Q1
  82.2 → 91.4 s p11→p12 (CPU +11%), profile before code; (6) key-move control build not run.
  Team from p13 on: engineer and QA on Opus effort medium, architect on Fable 5.2.
- **p13 (2fc1e7df, cargo test 690/0/4), 2026-11-18 23:0x.** Tx charge calibrated to measured
  anon (view ~1450 B/row real → 1672 charged; changelog 470 → 578; plain 600 → 612; p12 had
  under-charged the plain lane), pinned by a unit test to [measured, 1.3×]. env cap above the
  cage refused at every log_based entry. Top-N: up-front refusal uses the parts path's own
  predicate + column statistics; guarded refusal falls to the parts path again (p12 had
  dropped the marker); pipes + state priced together. Key-move control wheel RED, p13 GREEN.
  All regression legs PASSED. Q1 p11→p12 "regression" = host variance (identical symbol
  profiles; rerun p12 faster).
- **Case M closed on evidence:** the 166K-row view drain really peaks at 245 MiB anon (QA's
  "113–118" was anon at a page-cache peak), so it cannot fit 256m; named refusal is correct
  (p13 6/6, no 137). Release-note bound from the p14 ladder (est. ~110K rows).
- **QA on p13:** L8j2 LIMIT 200K exact ×3 + debug, max anon 166–167 MiB (GREEN). Finding:
  top-N price ~2.5–3× high — refuses `ORDER BY tc, id LIMIT 200000` that p12 answered at
  162 MiB. **p14:** topn_price rebuilt from the real key encoding (610/512 B per kept row vs
  measured 469/414), unit-pinned; guard refusal says the rows had to be read to know.
  Ladders n=3 for release notes run on p14.
- **p13 gates (chainP13 done 2026-11-19 01:17):** F gate n=3 interleaved non-PGO — Q1
  81.8 / 83.0 / 80.6 s (p11/p12/p13), Q9 80.0/78.9/79.5, Q10 25.7/25.2/25.4, Q20
  19.0/18.8/19.2: no regression. QA on p13: own-name slot case (two state dirs) and
  TX_BUF above cage both RED p12 → GREEN p13; case M 6/6 named refusal, max anon 118–123.
- **p14 (086dbb59) not the candidate:** top-N price calibrated (text LIMIT 200K–300K, int to
  400K answer at 256m; larger refused up front; no 137 in ~25 runs) but the plan fills the
  cage to the brim (state 116 + pipes 139 of 256 MiB; anon max 228–230). **p15:** top-N
  state + pipes ≤ 7/8 × (cage − measured anon) (`topn_room`), ladders at 256m/512m/1g n=3
  with/without debug reporting memory.peak per rung; gate: peak ≤ ~90% of cage, zero 137,
  scales with the cage. Release chains repointed to p15; go-files stay with the architect.
- **p15 (10081e73, test 691/0/4), chainP15 done 2026-11-19 05:21.** topn_room: kept rows +
  pipes ≤ 7/8 × (cage − measured anon). 18 regression legs PASSED; L8j2 exact ×6 at 88–90%;
  10g p13 vs p15 n=3 flat (Q17 30.6 → 33.1 overlapping spreads; recheck on the PGO wheel).
  **Release-note bounds (n=3, p15; CDC/view code unchanged after):** largest single UPDATE
  exact at 256m — view 110K (120K named; QA independently: 107K exact, 130K named),
  pg→ch changelog 350K (400K named), pg→ch plain 1M, my→ch changelog 200K (250K named),
  my→ch plain 1M. env cap above cage: RED p12 → GREEN p15.
- **QA FINDING 3 on p15 (blocker for top-N):** N1 `ORDER BY k2 DESC, id LIMIT 500K` at 256m
  exact 6/6 but bimodal peak 216 vs 241 MiB (94%). Cause (engineer, from QA's tsv + mallinfo):
  identical in-use and mmap; +37 MiB of freed heap glibc keeps, from the kept set's doubling
  reallocs. **p16:** reserve the kept set for 2·n_eff up front, refill a reused n-row spare
  at each cut (no realloc after start), per-row cut check, n_eff = min(n, 1.25 × row
  estimate + 1024); price = that layout, unit test asserts no realloc. Gate: QA's boundary
  sizes n=6 ≤ 90% of cage at 256m and 512m.
- **90% rule, precise form:** counts memory that cannot be reclaimed (anon + kernel + sock);
  spill-file page cache on the plain CDC lanes is excluded (reclaimable), so those lanes are
  judged on max anon.
- **Backlog L17:** guarded (ICU) top-N through the parts path could ask the oracle about
  cross-part pairs instead of refusing after the read at small cages.
- **p16 (d931fa1a, test 692/0/4):** FINDING 3 fixed — kept set reserved up front, peaks
  deterministic (N1 256m: 300K 62%, 400K 73%, 6/6, one population; 512m N1 1M 78%, N5 600K
  75%). State prices re-measured on the new layout: text 568 B/row (price 628, 1.11×), int
  456 (532, 1.17×), N1 351 (380, 1.08×). Over-refusal (N1 500K, N5 300K at 256m) came from
  the plan charging each read pipe 38 MiB = process reserve counted twice.
- **p17 (building 2026-11-19 06:16):** one pipe = 22 MiB beside the kept rows; pins re-set to
  p16's measurements. Predicted largest exact at 256m: N5 ≈ 320K, N1 ≈ 530K. Gate: boundary
  rungs n=6 at 256m/512m ≤ 90% in every run, else lower the boundary. chainP16 cut short at
  a leg boundary (its remaining legs rerun on p17); 10g compare is p15 vs p17. Release
  chains point to p17; go-files with the architect.
- **p17 (c9083082):** pipe charge 22 MiB. QA gate 84 runs (14 sizes × 6): exact or refused up
  front, no 137, ≤ 87% of cage, one population per size. Largest exact: 256m N1 500K (82%),
  N5 320K (87%); 512m N1 1.0M (80%), N5 690K (70%).
- **QA FINDING 4 on p16:** a grossly stale row estimate (reltuples 1000 vs 4M rows; also
  never-ANALYZEd tables) shrank the top-N budget to 1 MiB and refused an answerable query
  with "ask for fewer rows". **p18 (2bdfc42d, test 693/0/4):** unknown estimate → n;
  admission also prices the full n and keeps its pipes; one priced growth when rows pass the
  estimate; a refusal caused by the estimate names it and says ANALYZE. Engineer: p17 RED →
  p18 6/6 answered. QA: 4000×-stale PG, never-ANALYZEd PG, MySQL TABLE_ROWS 1000/4M — all
  6/6 exact at 256m and 512m, ≤ 83%, one population; boundary gate 84 runs ≤ 86%.
- **chainP18 done 2026-11-19 10:42:** 11 regression legs PASSED; L8j2 exact ×6 at 78–81%;
  10g n=3 p15 vs p18 flat except Q20 18.8 → 20.2 s (overlapping spreads; recheck on PGO).
  **Rel.go created 10:42:52** (architect): chainRel1 = PGO wheel of p18 + 22 queries vs
  pgoI. Rel2.go (full gate, cages, WAN, CDC A/B) waits for Rel1's table and QA's final
  verdict (slot/CDC items on p13 2fc1e7df, SQL items on p18 2bdfc42d).
- **Release chain part 1 (chainRel1, done 2026-11-19 12:52):** PGO wheel of p18 = pgoP18
  .so 20bfbc41 (PGO_EXIT 0, 0 profile errors, one wheel, md5 differs from pgoI and p18).
  22/22 ok vs pgoI. n=3 medians: Q1 65.1 → 66.3, Q2 33.1 → 33.1, Q3 39.3 → 39.9, Q4
  14.0 → 14.3, Q13 166.6 → 170.8, Q17 30.5 → 30.0, Q19 12.4 → 12.6, Q22 18.6 → 18.4,
  Q20 18.7 → 19.3, Q12 13.1 → 14.2. Follow-up n=5: Q20 18.2 → 18.4 (+1.1%); Q12 12.7 → 13.1
  (+3.1%, CPU 3.4 → 3.4, pgoP18's spread inside pgoI's 12.6–13.8); perf by symbol identical
  (decode 22/23%, probe 19/20%; Coll::fold 16 → 12 samples). **Architect: speed gate
  passed** — Q12's 0.4 s is wall without CPU, inside the old wheel's own spread; no code
  cause. Wheel still labelled 0.67.2: version bump + rebuild + short recheck before publish.
- **Known, accepted for this release (next round):** top-N pipe plan charge 22 MiB vs
  measured 26–37 MiB at the boundary (absorbed by the 7/8 share; worst run 88%); ICU-guarded
  top-N at small cages reads before refusing (L17).
- **QA final verdict (2026-11-19 ~13:30, reports/round-p/qa-verdict-final-p18.md):** p18
  passes. Own build md5-identical (693/0/4). SQL items GREEN on 2bdfc42d (estimates, top-N
  boundary 84 runs ≤ 86%, full sets 256m/512m/1g, L8, L1, collation, MIN/MAX, dates);
  slot/CDC items GREEN on 2fc1e7df. Open: FINDING 2 only (ICU top-N late named refusal,
  accepted, L17). **Rel2.go created 13:25** — full gate, 2c/8c cages, WAN, CDC A/B, Q7/Q21
  on pgoP18; QA idle for its duration.
- **Owner decisions 2026-11-19:** version = **0.68.0**; GROUP BY on arithmetic/CASE (L13b/c)
  deferred to the next round, documented as a named refusal. BigQuery is NOT untested: the
  key is on the VPS (~/apitap.json); the gate skipped bq legs only because BQ_SA was unset.
  chainUp reruns every skipped leg (bq + old-wheel upgrade 0.55.1/0.56.0) on pgoP18 after
  chainRel2. BigQuery cleanup: only new tables in apitap_cdc_e2e with a leg-owned name
  prefix are dropped; never a dataset; anything else new is reported.
- **Release chain part 2 (chainRel2 + chainUp, 2026-11-19):** full gate on pgoP18 228/1/33,
  then the 33 skipped legs with BQ_SA and the 0.55.1/0.56.0/0.62.x wheels 32/1/0. Both reds
  closed without an engine change: e2e_sql_join = leg check bug (`"1 pass" in` matched 11/21;
  fixed, ×3 green; case-5 pass counts are random on every wheel, n=5); e2e_guard_matrix bq =
  one BigQuery `backendError` on the control replace (×3 green on rerun, pgoP18 and pgoI).
  **Every one of the 262 legs has a pass on pgoP18.** Cages 2c/8c flat (Q1@2c, Q17@8c
  rechecked n=3); pg→ch CDC flat; Q7/Q21 pass. Open speed flags: WAN Q6 +0.7 s (gate 0.3),
  my→ch +7.6% at n=3.
- **Pre-existing, found by the gate:** bulk load/copy jobs to BigQuery are not retried on a
  retryable errorResult (bigquery.rs:2038, :2643; `retryable()` covers only state rows,
  session start, CDC scripts). Architect: not a blocker (named failure, nothing committed,
  rerun succeeds); include the 40–60 line fix only if another build is needed anyway, else
  release note + first item next round.
- **Release checks on pgoP18 complete (2026-11-19 ~18:30):** my→ch n=5 flat (32.56 vs
  32.65 s, CPU 15.35 vs 15.37) — the n=3 +7.6% was noise, flag closed. WAN Q6 +0.6 s is real
  and explained from the server log: 3 extra Postgres round trips before the first COPY
  (160 → 163 statements; first statement → first COPY 2.64 → 3.27 s; the read 3.88 vs 3.89 s):
  `headroom()` asked twice per statement (p5) and `order::pg_collations` for every table
  even with no text column named (p3). Disk: bench-my binlogs before 2026-11-14 purged
  (66 → 34 GB), 70 GB free.
- **Architect decision — build p19 = the 0.68.0 candidate**, one build, version bumped in it:
  (1) cache the headroom answer per statement+URL; (2) gate the collation probe on "the
  statement names a text column of this table" (conservative: any reference anywhere,
  incl. join keys, GROUP BY, DISTINCT, MIN/MAX, ORDER BY through an alias, `*`) — QA attacks
  this gate, a wrong skip is class (a); (3) BigQuery bulk load/copy job retried under a fresh
  job id on a retryable errorResult (same backoff as load_rows), fault-injected leg;
  (4) e2e_pg_hardening.py:156 exact-number check; (5) version 0.68.0. Gates: WAN Q6 ≤ pgoI
  + 0.3 s n=3; collation/order/pad/minmax/nulls/pool legs green with their REDs intact;
  then PGO wheel labelled 0.68.0, full gate ONCE on that wheel with BQ_SA and the four old
  wheels from the start, 22 queries vs pgoI, CDC A/B, QA verdict on the final md5.
- **p19 (fc23a380, version 0.68.0, test 700/0/4):** headroom once per statement; collation
  probe gated by `collation_probe_needed` (word match on possibly-text columns; always on
  `*`, NATURAL, column-list alias, U&, unparsable, quoted-quote names); BigQuery
  `job_with_retry` for the copy job and single-PUT load files (≤ 8 MiB) — multi-part
  streamed files stay un-retried (architect: accepted for 0.68.0, release note). RED p18 →
  GREEN p19 on e2e_sql_probe_count and e2e_bq_job_retry; WAN Q6 statements 163 → 161,
  first statement → first COPY 3.30 → 2.90 s. **RelF.go created 2026-11-19 ~19:30**: PGO
  wheel pgoP19 → 22 queries → WAN → one full gate (BQ + old wheels from the start) → CDC
  A/B, cages. QA (own build, then attack on the collation gate) runs in parallel.
- **QA FINDING 5 on p19 (class a, 2026-11-19 ~19:40; chainRelF stopped before it started):**
  source types that arrive as `(col)::text` but do not order/compare like text are ordered,
  MIN/MAXed and in part grouped by the bytes of their text form. Postgres: 19 types wrong
  (enum, inet/cidr, interval, timetz, arrays, unconstrained numeric 1.0/1.00, float NaN/-0,
  jsonb, money, pg_lsn, oid, domain over int, ranges, composite, tsvector; json/xml/point
  answered where the server refuses). MySQL: ENUM, SET, TIME, JSON. MIN/MAX and DISTINCT on
  them are NEW in round P (0.67.2 refused); ORDER BY and some GROUP BY wrongs exist in
  0.67.2. Also engine-evaluated predicates over a CTE column (`e > 'alpha'` 3 vs 1).
  The p19 probe gate itself passed QA (39 cases).
- **p20 design (architect, approved plan of engineer #5):** `ArrowField.ord` by allowlist at
  schema time — Collated / ByteExact / EqOnly / Opaque, default Opaque; ORDER BY, top-N,
  MIN/MAX, engine `< >` need Collated or ByteExact; GROUP BY, DISTINCT, COUNT(DISTINCT)
  need EqOnly; refusals up front by one shared helper (column, type, use, remedy);
  projection always works. Native fixes: domains → base type (domain over int becomes Int;
  release note), unconstrained numeric decimal-text comparator + normalised keys (not a
  refusal — ordinary data), float -0/NaN per source. Deferred with a named refusal: native
  enum order, oid unsigned decode, MySQL TIME unless the native decode is small. PG enum
  `=`/`IN` against a literal stays allowed. inet '/32' rendering unchanged (same in 0.67.2).
  Size ~600–700 lines + leg e2e_sql_type_order.py (RED p19 / GREEN p20).
- **QA on p19 (2026-11-19 ~21:00):** item B headroom-once GREEN (L4 mix 17/17 and 26/26
  twice, wheel I RED; role limits 1–3 with held and mid-statement sessions exact or named;
  observation: at limit 2 a star query is refused at 1 free but waits and answers at 0 free).
  Item C BigQuery retry GREEN (append/replace with injected backendError exact, 0
  duplicates; non-retryable refused by name; exhausted → named, target untouched; kill -9
  at 3/6/9/12 s then rerun exact, 0 duplicates; Ctrl-C releases the lease). Two findings
  outside the retry: **C1** SIGTERM on a bulk run exits 143 without cleanup, lock + staging
  stay, next run refused by name (pre-existing by design, shutdown.rs — the signal
  Airflow/K8s/systemd/docker stop send; same gap the Cloud Run study found); **C2** the
  remedy names one residue table per failed run (lock, staging_0, staging_1 → three runs).
  Architect: neither blocks 0.68.0 (named refusal with a working remedy, no wrong data);
  C2 goes into the next p-build if one is needed anyway, else release note; C1 = first item
  next round (bulk SIGTERM → discard + release_lock), also the Cloud Run prerequisite.
- **p20 (f4b6352f, test 704/0/4):** FINDING 5 fix as designed; e2e_sql_type_order RED p19
  (120 XX) → GREEN p20 (371/0); 26/26 SQL regression legs. QA on p20: all type sweeps incl.
  types off the list and 18 hostile numerics exact or refused by name, binary/NULL siblings
  exact, B and C verdicts carry (code identical). **One defect, class b:** behind a CTE the
  class refusal comes after the stage was read (ORDER BY enum; `e > 'alpha'`,
  `ip = '…'`, `iv = '1 day'`). p20 will not ship.
- **p21 = p20 + (1) every class check before any read** — engineer's design: a dry pass
  plans the whole stage chain over empty stage answers (task-local SQL_DRY, stops after
  plan_probed, per-statement schema/estimate cache so no extra round trips) and returns
  only class refusals; architect's conditions: no side effects (dest DDL, slots, lease,
  spill, state), no connection the real run then lacks, run-time checks stay as a second
  line with a debug assertion, a dry-pass failure can only fall through, planning overhead
  measured; **(2) QA C2:** one refusal lists all residue of a dead bulk run
  (naming.rs blocker_error_all). chainRelF1 on p20 cut after the PGO build so p21 is not
  delayed; timed checks run on pgoP21 only.
- **QA D1/D2 on p20 (pre-existing, same on p18; 2026-11-19 ~22:10):** **D1 class a** — an
  incremental view whose Postgres source table was dropped and recreated keeps answering
  "ok, 0 change image(s)" forever (publication silently lost the table and is reused), even
  after the documented reset. **D2 class b** — a view grouped by inet dies on the first
  UPDATE/DELETE ("state out of step"): bootstrap key is COPY `::text` '10.0.0.2/32', stream
  key is the output text '10.0.0.2'. Architect: **p22 = p21 + dry-pass contract (B1/B4) +
  D1 + D2.** D1: every run verifies publication coverage and table identity (pg_class oid
  kept in state; old state adopted); first run adds the table before the snapshot; a drain
  with a hole takes the lost-position path (named refusal or on_lost="rebootstrap"), never
  "ok". D2: one shared normalisation for bootstrap and stream spellings; unprovable types
  refused as view keys. QA is extending both to log_based CDC and MySQL (drop+recreate,
  rename swap, inet as PK, role-level DateStyle/IntervalStyle/bytea_output).
- **p21 (b932b14e, test 705/0/4; 2026-11-19 ~23:20):** type_order RED p20 (13 XX) → GREEN
  (14 shapes refused with 0 reads, PG log + MySQL general_log); residue list RED → GREEN on
  pg/my/ch; QA: dry pass refuses with 0 reads on its own harness, no side effects (16
  refusals, full catalog diff), C2 GREEN on pg/my/ch/bq, plim3 22/22 as p19, COL/C10/MMP/DT/
  L1 verdicts as p20, statement counts identical (one extra collation probe on a refused
  nondeterministic GROUP BY — class c, next round). **Defect (engineer's leg):** pool_many
  623 s vs 360 s bound — the dry pass waited ACQUIRE_WAIT for a planning connection, then
  the run waited again. D1 confirmed views-only (CDC already guards oid + publication;
  QA: 8 DDL variants pg, and MySQL/MariaDB drop+recreate/rename swap, all exact or named).
  D2 confirmed pre-existing on 0.67.2/p18/p20: inet PK (any role) and interval PK under
  IntervalStyle ≠ postgres diverge pg→ch (2 source rows → 4).
- **p22 = release candidate:** p21 + dry pass never waits for a connection and is skipped
  without stages + views use CDC's ensure_publication and carry `#oid=` in the position
  (old positions adopted) + catalog-driven output-function rendering (pg_cast castmethod
  'f') + read connections pin intervalstyle/datestyle/extra_float_digits/bytea_output in
  the existing SET + TRUNCATE-under-view message. Behaviour changes for the notes: inet
  prints as Postgres displays it; log_based tables with inet/cidr (or interval under a
  non-default IntervalStyle) bootstrapped before 0.68.0 → re-bootstrap once; domain over
  int reads as Int. Known next-round items: bulk SIGTERM cleanup (C1), schema probe sent
  twice per table, pool_many 300 s wait at limit 4, MySQL TIME native decode, native enum
  order, limit-2 refuse-at-1-free vs wait-at-0-free.
- **p22 (fcbd9c51, test 707/0/4; 2026-11-20 ~00:20):** QA own build identical; D2 keys 68/68
  (17 PK types × pg→ch/pg→pg × 2 roles) exact, plain columns one spelling, changelog lane
  consistent; views D1/D2 GREEN (drop+recreate / rename swap / unpublished → named; reset
  re-adds the table; 0.67.2 and p18 views adopted; inet group EQUAL); output change
  confined to inet host values; refusal-before-read 27/27, side effects clean. Engineer:
  live_recreate RED p21 → GREEN p22, 31/32 legs (1 leg bug), pool_many 329 s (p21: 623 s),
  BYTEDIFF identical over 28 non-inet types, trivial single-stage statements flat.
  **Not shipping:** timed m23 — Q21 opens 30 vs 28 Postgres connections; WAN 50 ms 2-stage
  pg×my CTE 6.5 → 6.8 s every round (first statement → first COPY 2.85 → 3.46 s): the dry
  pass opens/closes its own planning connections and the run reconnects. Q6/Q9 equal, Q15
  faster on wall with a later first COPY.
- **p23 = p22 + dry pass hands its planning connections to the run + nondeterministic
  refusal returned from the dry pass + APITAP_TEST_DRY_NO_CONN lever.** Gates: connections
  per statement ≤ p20 on both sources; WAN CTE2/Q9/Q15/Q21 ≤ p20 + 0.1 s; refused
  statement leaves zero connections; role-limit legs and QA's shortage runs unchanged.
- **p23 (0872ff5a): wrong by construction** — the dry pass's kept connections went into
  stage 1's planning scope and its close_planning closed them all, so stage 2 reconnected
  (WAN CTE2 still +0.3 s, Q21 +2.4 s). Never shipped, never QA'd.
- **p23b (db70fe0e, test 715/0/4, sql.rs only) = release candidate (2026-11-20 ~04:00):**
  kept connections live in STMT_PLAN.conns, a stage takes its source's connection on first
  use, execute_scoped closes only its own sources, statement_scope closes the rest; 8 unit
  tests count connects per statement. m23c (dedicated user, quiet rig): connections per
  statement ≤ p20 on Postgres everywhere; MySQL equal except Q21 55 → 58 (in p22 too;
  timeline pending, not blocking unless a connection is held or left). WAN 50 ms n=3 walls
  p20 → p23b: CTE2 6.5 → 6.5, Q9 119.8 → 119.4, Q15 44.2 → 42.2, Q21 155.8 → 155.7, Q6
  same. First COPY later on staged statements (CTE2 +0.6 s, Q15 +0.75 s, Q21 +2.3 s:
  the dry pass plans every stage first) at equal or better wall — accepted, documented.
  Rig rule learned: counts and WAN walls only with a dedicated user and QA fully idle
  (two contaminated measurements this night).
- **p23b fails QA (2026-11-20 ~05:40): hold-and-wait under connection shortage.** Role limit
  2, 10 concurrent statements in one process: max wall 48 s (p22) → 342 s (p23b), 3
  answerable statements refused "no connection within 30 s" (p22: 1). Each statement keeps
  its dry pass's planning connections while waiting for more. No wrong answers; everything
  else clean (own build identical 715/0/4, plim3 22/22, connections per statement ≤ p20,
  zero sessions left, Q21's +2–3 MySQL connections short-lived: none idle > 0.21 s →
  Q21 question closed, not blocking). Engineer's 32 legs 31/32: e2e_sql_live case 7 failed
  once in four (stale copy refused correctly; the failing condition — owner drain exit or
  destination ≠ Postgres — was not printed). Not accepted as "flaky": N=20 p23b / 20 p20 /
  10 rel0672 with all four conditions printed, before any release chain.
- **p23c rule (architect): a statement never holds a kept planning connection while it
  waits for another connection** — every waiting acquire first releases all kept
  connections; kept connections count as held in headroom; none idle across a long read.
  Unit test with a pool hook (RED on p23b's behaviour), registered shortage leg (limit 2/3,
  10 concurrent, bound 120 s), m23c again for the WAN gain. Fallback if the rule kills the
  gain: p22 behaviour (close after the dry pass) and accept +1 connect per extra source on
  staged statements — decided with numbers.
- **p23c decision (architect, 2026-11-20 ~06:10):** engineer's "never hold while waiting"
  design (process-wide registry, 2 s timers, bounded quick attempts, three more files)
  REJECTED as too much machinery for 0.3 s. My replacement premise (make the run's planning
  connection lazy) was wrong: planning is already lazy; p22's extra connect is the LIVE
  headroom question of each source whose first stage is a later one (CTE2: MySQL +1; Q21:
  Postgres +2). Chosen: **p23c = p22 + nondeterministic refusal from the dry pass +
  APITAP_TEST_DRY_NO_CONN lever + shortage leg e2e_sql_pool_staged.py + connect-count unit
  tests; nothing kept, all p23/p23b handover code removed.** Gate relaxed knowingly:
  connections ≤ p20 + one per later-stage source; WAN CTE2 ≤ p20 + 0.4 s, Q21 ≤ +2%,
  Q9/Q15 ≤ +0.1 s, Q6 unchanged. Release note: refusing before any read costs one planning
  connect per source first read by a later stage. Next round: the dry pass asks headroom
  on its open connection (stale-answer trade-off, own shortage test).
- **QA stall-frequency study (2026-11-20 06:13–07:22, limit 2, 10 concurrent, N=10 per
  wheel interleaved, fresh role per round, pg_stat_activity sampled):** stalled rounds
  (wall > 120 s) p20 3/10, p22 4/10, **p23b 1/10**; rel0672 0/10 because it never waits
  (30 s, raw "pool timed out" on 61/100 statements). All 8 stalls on every wheel: both role
  connections idle in transaction 294–340 s (snapshot holders: each statement pins, then
  waits for a read connection); no persistent plain-idle session → kept planning
  connections not implicated. 39 rounds: no wrong answer, no raw pool error. My earlier
  "p23b regressed, hold-and-wait on kept connections" was WRONG (one round, no control);
  QA's back-to-back control on p22 (334.5 s) overturned it.
- **FINAL DECISION: p23b (db70fe0e) is the 0.68.0 candidate, unchanged.** p23c shelved
  (reports/round-p/p23c-work/). e2e_sql_pool_staged.py unregistered for this release
  (would fail at random on the pre-existing stall); matrix back to 310. Next round, with
  that leg as RED: (b) snapshot holder + first read connection all-or-none, (a) release
  kept connections before the per-stage pin, (c) no pin for a read-once source below
  headroom 3; plus the dry pass asking headroom on its open connection. Documented limit
  for 0.68.0: role limit 2 with ~10 concurrent statements may wait ~5 min before a named
  refusal or an exact late answer (0.67.2: raw failure at 30 s).
- **REVERSAL (2026-11-20 ~09:00): p23b is OUT.** QA insisted on limit 3 after the
  favourable limit-2 table: at role limit 3 (N=15 per wheel, interleaved, fresh roles)
  p22 stalled 0/15 (max 95 s), p23b 8/15 (max 359 s, median 314 s; 19 answerable
  statements refused vs 4). All stalls = three snapshot holders idle in transaction; no
  plain-idle kept session visible at 0.25 s sampling — the handover shifts WHEN statements
  pin (limit 2: 1/10 vs 4/10; limit 3: 8/15 vs 0/15). Rule fixed before the data (≥ p22+4
  → handover implicated) applied. My "pooled 4/15 vs 4/15, one common rate" reading was
  wrong. No wrong answers anywhere.
- **Candidate: p23c = p22 behaviour (nothing kept) + nondeterministic refusal from the dry
  pass + APITAP_TEST_DRY_NO_CONN + connect-count unit tests.** Cost accepted and to be
  documented: +1 planning connect per source first read by a later stage (WAN 50 ms: 2-stage
  pg×my CTE 6.5 → 6.8 s). Gates: QA stall study p22 vs p23c (limit 3 N=15, limit 2 N=10:
  ≤ p22 + 2), m23c with revised bounds, 32 legs. Handover (p23/p23b) rejected on
  evidence; not to be retried before the snapshot-holder all-or-none fix.
- **Open correctness question (blocks the chain on any wheel):** e2e_sql_live case 8 failed
  once in 20 on p20 with the owner's drain exiting 0 (either a silent re-bootstrap or
  destination ≠ Postgres); case 7 clean 40/40. Code reading finds no gap in the drain's
  upper bound nor any way a refused run touches the owner's slot. Instrumented loop running.
- **SIMPLIFICATION (2026-11-20 ~09:15, after the owner asked why it got longer): the
  candidate is p22 (fcbd9c51) AS BUILT — no p23c build.** p23c = p22 + one saved round trip
  on a refused nondeterministic statement behind a CTE + a test lever; p22 is built, QA's
  own build is identical, and it is verified almost end to end (D2 68/68, views, spelling,
  refusal-before-read, side effects, 31/32 legs, stall study 0/15 at limit 3). Its only
  reason for rejection was the +0.3 s WAN cost on staged multi-source statements, now
  accepted. Next round: the doubled collation probe behind a CTE, the dry-pass lever
  (code in reports/round-p/p23c-work/), and everything listed above. Remaining before the
  chain: the case-8 question; QA's gaps on p22 (plim3, quiet statement counts, pstale).
- **QA FINAL VERDICT on p22 (2026-11-20 ~09:25): shippable** — 13 items, no wrong answer, no
  unnamed refusal (reports/round-p/qa-verdict-final-p22.md). Limit 4: p22 0/5, p23b 0/5.
- **Case 7/8 status:** one failure captured with full evidence (p20, 09:04 UTC): the view's
  slot was dropped by our own benchmarks/pgo-build.sh (lines 77/102 sweep every inactive
  apitap_% slot); the owner re-bootstrapped honestly; destination = Postgres. Two earlier
  failures (gate on p23b ~04:40, p20-16 at 08:09) stay UNEXPLAINED: QA dropped no slot
  then, no cron/timer exists, the leg did not yet print the slot state. No evidence of
  wrong data anywhere; QA's independent harness 20/20 clean on p22. Carried to the owner
  as the one open item at publish time. Next round: pgo-build.sh drops only its own slot;
  leg gets an independent bootstrap signal.
- **Release chain on p22 (pgoP22):** first PGO build failed at training on residue of the
  PGO build killed 2026-11-19 22:04 (staging table on my-dst → LockedError; = QA's C1
  biting our own tooling); dropped by exact name, destinations swept, rebuilt from 09:01.
