# `apitap.read()` → Polars — the DataFrame race

One line against the two ways people load Postgres into DataFrames today:
connectorx (the Rust→Arrow incumbent behind `pl.read_database_uri`) and
`pandas.read_sql` (the default everyone suffers). Same box, same 15-column
10M-row table (`bench_data_10m_cap`), all counts verified. Harness:
[`bench-read.sh`](bench-read.sh), e2e value-level suite:
[`e2e_read.py`](e2e_read.py).

```python
df = apitap.read("postgres://…/db", table="public.orders").to_polars()
```

## Full box (16 cores), 10M rows → materialized DataFrame

| reader | wall | ratio |
|---|---|---|
| `apitap.read().to_polars()` | **14.7 s** | — |
| connectorx → polars | 55.9 s | 3.8× |
| `pandas.read_sql` | 295.4 s | 20× |

## Materializing is a RAM problem, not a CPU one

The 15-column 10M frame measures a **5.14 GB peak RSS** to materialize
(the ~2 GB frame plus the Arrow→polars conversion transient) — so
"10M → DataFrame in 256 MB" is physics, not a benchmark, and no reader
passes it. With the RAM honest and the CPU capped instead:

| 10M → polars @ 0.5 vCPU / 6 GB | wall | peak |
|---|---|---|
| apitap | **57.3 s** | 4.8 GB |
| connectorx | **OOM-killed** (its transient tops 6 GB) | — |

## The home tier: 0.5 vCPU / 256 MB

| leg | apitap | connectorx |
|---|---|---|
| 1M rows → polars (narrow table) | **2.3 s, 94 MB peak** | 4.9 s, 112 MB peak |
| **10M rows, streaming aggregation** | **52.8 s, 130 MB peak, sum verified** | **OOM-killed** |

The streaming leg is the structural difference, not a tuning delta:
connectorx materializes the whole result by design, so a 10M-row table
simply does not fit a 256 MB container no matter how long you wait. apitap
exports a PULL-based Arrow C stream — batches decode when the consumer
asks, memory holds the batches in flight (sized off the cgroup limit), and
a plain `pyarrow.RecordBatchReader.from_stream(reader)` loop analyzes ten
million rows on half a core with a flat ~130 MB curve.

## How it works

The same parallel binary-COPY range pipes every transfer route uses feed
Rust-side Arrow columnar builders (the parquet lane's bounds-first tuple
walk, reused); the batches cross into Python through a hand-rolled Arrow
C Data Interface + PyCapsule stream — no arrow-rs, no pyarrow dependency
in the wheel, zero copies at the boundary. polars/pyarrow/duckdb all
consume it natively. Typed end to end: int16/32/64, float32/64, bool,
decimal128(p,s), date32, timestamp µs (naive + UTC), utf8, binary;
uuid/jsonb and exotic types ride `::text` — every table reads.

One honest note found (and fixed) by this bench: the first FFI cut leaked
every batch through a no-op child release callback — the 256 MB probe
caught it in minutes (+40 MB per batch, flat after the fix). Small tiers
are not just a market: they are a leak detector.

## The speed campaign (branch read-speed)

A cpu-clock flamegraph at the 0.5-core regime (perf attach starves on
this VPS — fork-mode inside the CPUQuota scope is the recipe) split the
budget two ways: `BatchBuilder::push` 26.5% self, and ~30-35% under
sqlx's copy stream — one refcounted `Bytes` per CopyData message ≈ per
ROW, through four future layers. 10M rows = a 10M-poll storm.

Four knives, each measured at 0.5 vCPU / 256 MB / 10M rows, each kept
only if the number moved:

| state | wall | peak |
|---|---|---|
| campaign start (auto-parallel floor fix) | 26.7 s (p=5) | 194 MB |
| p=6 sweet spot (sweep) | 24.8 s | 211 MB |
| + raw COPY plane, NUMERIC fast path, adaptive reserve | 20.7 s | 197 MB |
| + FrameRaw (spans verbatim → builder, no strip/accumulator) | **14.6 s** | **102 MB** |

- **Raw COPY plane**: the walsender stack grew `connect_sql` +
  `copy_out_start/next` — frames coalesce out of a 1 MiB read buffer
  into one reused Vec. No per-row Bytes, no poll storm. Falls back to
  sqlx on TLS URLs; `APITAP_RAW_COPY=0/1` forces either plane.
- **NUMERIC fast path**: ndigits ≤ 3 short-circuits (u64 header load,
  pow-10 table) — 2.0-2.1× on money-shaped numerics, bit-identical by a
  3,400-case oracle grid against the general path.
- **Adaptive pre-reserve**: each seal re-reserves per column from that
  column's actual final size (+1/8), so varlen columns stop paying
  geometric-growth reallocs against a uniform split.
- **FrameRaw**: the read lane ships span payloads verbatim; the builder
  walks headers/trailers natively (multi-span), chunks decode IN PLACE
  (buffered tail = one straddling tuple, not the stream). Two full-stream
  memcpys and six 4 MiB accumulators vanished — that is the RAM halving.
- **auto-parallel**: read's own memory model lands AUTO on 6 pipes at
  256 MB — 14.6 s with no knobs.

Tried and rejected by measurement (so nobody retries them without new
data): fusing the bounds+decode passes (16.0-16.3 s vs 14.6-15.2 s —
fusing re-imposes per-field bounds checks and breaks the pure-scan loop
LLVM optimizes); mallopt arena tuning at tight tiers (+20-30 MB peak,
zero speed); pipes > 6 at 0.5 core (CPU-bound, 7 measured slower); the
raw plane as default for the TRANSFER FrameStrip lane (pg→pg COPY-in
paced ~1 s slower by the new flush pattern — modes must not regress, so
transfer keeps sqlx until that flush is reworked).

Wheel-level (Python, streaming AUTO, same tier): 57.3 s at campaign
start → 18.8 s mid-campaign → tracks the engine at ~15 s with the final
build. pg→ch and the other transfer modes: unchanged (regression matrix
vs the released 0.20.0 wheel, twice, inside noise).

## Heavy-query boundary map (50M rows, branch read-10s, 2026-08-03)

The lazy() plugin raced on bench_data_50m (22 GB, 15 cols, no PK — TID
ranges) at 0.5 vCPU. What fits where, measured not guessed:

| query shape | 256 MB | 1 GB |
|---|---|---|
| filter + small group_by (2/15 cols) | **9.2s** / 137MB | — |
| 6 aggregations over 5 numeric cols | **17.6s** / 159MB | — |
| join vs a local Python DataFrame + agg | **16.2s** / 212MB | — |
| full streaming drain, all 15 cols | **59.1s** / 133MB | — |
| group_by of 1M buckets | OOM | **17.4s** / 1004MB |
| exact median over 50M floats | OOM | **12.6s** / 931MB |
| text filter + group on string cols | OOM | OOM — needs 2GB: **28.3s** / 1848MB |

The engine's share stays flat (~100-130MB) in every leg — the growth is
polars' COMPUTE state (group tables, full columns for exact quantiles),
which is proportional to the intermediate result, not the table. That is
the honest sentence for the docs: memory follows the ANSWER's working
set, never the table.

Context ties: the simple-query leg TIES raw SQL-in-postgres (9.2s both)
— the polars API costs nothing over not transferring. plain polars
(read_database_uri, eager AND .lazy()-after) is OOM-killed on every leg
at 256MB. And plus-cores does NOT speed the thin-column legs (user
measured 3 cpus: 10.2s vs 9.2s): postgres's 22 GB scan is the floor —
which is exactly the frugality thesis, a half core saturates what the
database can serve.

KNOWN UPSTREAM BUG (polars 1.43.2, latest): sort()/top_k() over a
register_io_source plugin panics polars' streaming engine
(expr_to_ir.rs:619 unreachable; io_sources/batch.rs:107 unwrap). A
10-row pure-polars repro does NOT trigger it — narrowing in progress
before filing upstream. Until fixed: aggregate first, or
to_parquet() -> scan_parquet() for sorted outputs.

## Control: pure polars scan_parquet (no apitap) — same 50M, same caps

Dump once via apitap.to_parquet (73.7s uncapped; 22 GB row-store → 1.00 GB
zstd columnar), then the same battery on pl.scan_parquet:

| query | apitap lazy (live pg) | scan_parquet (local file) |
|---|---|---|
| filter + small group @256MB | 9.2s / 137MB | **2.8s / 44MB** |
| multi-agg @256MB | 17.6s / 159MB | 9.7s / 59MB |
| join-local @256MB | 16.2s / 212MB | 10.0s / 49MB |
| 1M-group agg @256MB → @1GB | OOM → 17.4s | OOM → 12.6s |
| exact median @256MB → @1GB | OOM → 12.6s | OOM → 6.9s |
| top_k @256MB | polars PANIC (plugin bug) | OOM |

Verdicts the control settles:
- The 256MB/1GB boundaries are polars' COMPUTE state — pure scan_parquet
  OOMs on exactly the same legs. Not a plugin defect.
- The row-store tax is real and quantified: the same query is 9.2s from
  live Postgres (22 GB of pages must be read to extract 2 columns; raw
  SQL ties at 9.2s; +cores don't help) vs 2.8s from a columnar file that
  reads only the touched columns.
- top_k at 256MB fails EVERYWHERE (native: OOM) — our plugin's panic is
  still upstream-report-worthy (a panic is not a clean error), but no
  path fits a 50M sort in 256MB today.
- Product guidance this yields: LIVE data → lazy() direct (9.2s, no
  staging); REPEATED analytics → to_parquet once (74s), then scan at
  file speed (2.8s/query). Both are one-liners.

## Control 2: plain polars + connectorx ladder — same infra, 50M @0.5cpu

Same containers, same table, same query (filter + small group_by via
read_database_uri then .lazy().collect(streaming) — the eager read must
complete before any query runs):

| tier | result |
|---|---|
| 256MB / 1GB / 2GB | OOM-killed |
| 16GB | OOM-killed (~8 min of reading first) |
| 24GB | OOM-killed (~12 min of reading first) |

96x our working tier and it still cannot START the query — the
materialize-first architecture needs the whole 50M x 15-col frame plus
conversion transients in RAM. Higher tiers not attempted (production
shares the host). The ladder sentence: apitap answers this query in a
256MB container in 9.2s; plain polars+connectorx has no answer at any
tier up to 24GB.

Also measured, to_parquet compression @0.5cpu/256MB (10M): zstd 80.5s /
0.44GB beats snappy 89.9s / 0.84GB — the encoder path, not the
compressor, is the cost; the zstd default stands.

## Round 3 research (2026-08-08): no speed gold, one silent-corruption bug

Ten internet-wide sweeps (Arrow view/dictionary/REE layouts, polars ingest,
connectorx/ADBC, DuckDB scanners, pg egress economics, SIMD parsing, columnar
build, io_uring/socket syscalls, MySQL+ClickHouse lanes, streaming memory) →
33 findings → top 6 adversarially verified → **1 kept, 5 refuted**. The one
survivor was a CORRECTNESS fix, not a speed knife (shipped: i32 offset guard,
`wire/arrowcol.rs::check_offsets`).

The whole StringView / "German strings" family (4 independent proposals) died
on three walls worth recording so nobody re-digs them:
1. **The headline tier never touches polars.** The 52.8 s / 130 MB streaming
   leg is a pure `RecordBatchReader.from_stream` loop, so every polars-boundary
   idea can only land on a big box, by definition.
2. **Polars' cast is already zero-copy on the data buffer** — it slices, it does
   not memcpy — so the "save a memcpy" story is false; only a metadata pass
   remains.
3. **`ColB::bytes` is the seal gate**, so any layout that adds bytes/row shrinks
   the batch inside a 256 MB cage — a layout tax paid exactly where we can least
   afford it.

Also corrected by the arithmetic: the 5.14 GB "materialization transient" is
mostly the frame itself (~499 B/row × 10M ≈ 5.0 GB), not conversion overhead.
The transient is therefore much smaller than it looked, and attacking it means
attacking row width, not the conversion.

Unverified near-misses, gated on a profiling run (do NOT build first): typed
Arrow buffer pooling closed by the Arrow C release callback (kill the
page-zeroing tax — `clear_page_erms` was 4.8% of samples in profiling.md);
`SET max_parallel_workers_per_gather = 0` on read connections; bulk validity
bitmaps via SSE2 compare+movemask; SIMD prefix-sum for varlen offsets.

## Auto-parallel re-calibration (2026-08-08): the small tiers were running at half speed

The read path picks its pipe count from the cgroup budget. That model was fitted
in 2026-07 against an engine that no longer exists — its own comment recorded
6 pipes peaking at **211 MB**, and the same 6 pipes now peak at **133 MB**.
Buffer recycling and the frame-native rows freed that headroom and nothing ever
spent it, so every tier was running short of pipes.

Re-swept on the 10M × 15-col read leg at 0.5 core, peak RSS from cgroup
`memory.peak`, 2-3 interleaved rounds per point:

| cage | pipes | wall | peak |
|---|---|---|---|
| 256 MB | 2 / 4 / 6 / **8** / 12 / 16 | 38.6 / 23.3 / 13.5 / **12.4** / 12.4 / 12.7 s | 92 / 107 / 133 / **190** / 222 / 252 MB |
| 128 MB | 2 / 3 / 4 / **5** / 6 | 37.5 / 24.2 / 22.9 / **15.3** / 13.4 s | 59 / 68 / 72 / **85** / 106 MB |
| 64 MB | 1 / **2** | 48.5 / **41.2** s | 31 / **44** MB |

New model: 16 MB reserve + 22 MB/pipe, cap 8 → **8 / 5 / 2**, each measured,
none past 74% of its cage. Past the knee memory buys nothing (256 MB: 12 pipes
ties 8, 16 is slower at 99% of the cage). Pinned by a unit test
(`read_impl::tests::auto_pipes_matches_the_swept_calibration`).

End-to-end through the AUTO path, old binary vs new, installs md5-verified:

| cage | before | after |
|---|---|---|
| 64 MB | 48.5 s / 31 MB | **41.7 s / 42 MB** (−14%) |
| 128 MB | 37.5 s / 56 MB | **16.4 s / 82 MB** (−56%) |
| 256 MB | 13.5 s / 133 MB | **12.4 s / 184 MB** (−8%) |

### ST_K: swept, TIE, left at 64
64 vs 128 with `parallel=8` pinned, 3 rounds, binary md5-verified per leg:
medians 13.6 s / 7.3 s CPU vs 13.5 s / 7.2 s — 0.7%, under the lane wall, and
round 3 flipped sign.

### Harness bug that invalidated three conclusions (read this before benching)
`pip install /w/wheel-<variant>.whl` fails with **"is not a valid wheel
filename"** — pip enforces PEP 427 naming — and the harness sent pip's output to
/dev/null. Every leg silently ran ONE binary installed hours earlier. It faked a
"3/3 win" for ST_K=128 and made an env-var lever look inert (the running binary
predated it). Fix, now standard: keep each variant's wheel at its original PEP
427 name in its own directory, and md5 the installed `_apitap.abi3.so` against
the .so inside the wheel before every leg, aborting on mismatch. Runtime levers
(API args) are immune and stayed valid — which is why the worker-count sweep
survived.

### Kernel profile of the read leg (z15 re-read with `--kallsyms`, no `--symfs`)

`--symfs` redirects kernel symbol lookup too, which is why the first pass showed
only raw hex for kernel frames. Re-read of the SAME perf data with
`--kallsyms=/proc/kallsyms`:

| symbol | share |
|---|---|
| `entry_SYSCALL_64` (all syscalls, cumulative) | 34.7% |
| `recvfrom` path (cumulative) | 27.7% |
| `rep_movs_alternative` — socket→userspace copy | **12.8% self** |
| `tcp_send_ack` + `tcp_cleanup_rbuf` | ~15% cumulative |
| `clear_page_erms` | **0.75% self** |

**Buffer pooling / page-zeroing: REFUTED by measurement.** `tune_allocator`'s
comment cites `clear_page_erms` at ~13% of a 0.5-core run; on THIS path it is
0.75%. The candidate is closed — an earlier claim that it was "refuted because
the symbol is absent" was wrong for the right conclusion: the symbol was absent
because kernel symbols were unresolved, not because the cost was zero.

**Next knife, data-led:** we set NO socket options anywhere (no `SO_RCVBUF`, no
`TCP_NODELAY`, no `TCP_QUICKACK`). 12,527 `recvfrom` calls per run, each drain
triggering `tcp_cleanup_rbuf` → an ACK. A larger receive buffer means fewer
calls, fewer ACKs, less syscall entry cost. Honest estimate 3-7%, with a real
risk to test rather than assume: setting `SO_RCVBUF` explicitly DISABLES Linux
receive-buffer autotuning, which can lose on a real network even when it wins on
loopback. A/B it in the cage against a remote-ish path, not only on loopback.

### Regression check on the PUBLIC lazy-query workload (50M rows, column pushdown)

The pipe re-calibration was fitted on the 15-column FULL-SCAN leg. The published
lazy-query workload has a different shape — `filter` + `group_by` with column
pushdown, so only 2 of 15 columns ever leave Postgres — so it needed its own
check before the calibration could ship.

`apitap.read(URI, table="bench_data_50m").lazy().filter(pl.col("regular_int") % 3 == 0)
.group_by("bool_val").agg(pl.len()).collect(engine="streaming")` @0.5cpu/256MB,
3 interleaved rounds, installs md5-verified, old calibration vs new:

| round | old (36 MB/pipe) | new (22 MB/pipe) |
|---|---|---|
| r1 (cold page cache) | 21.1 s / 164 MB | 10.7 s / 169 MB |
| r2 (warm) | 8.4 s / 135 MB | **6.8 s / 131 MB** |
| r3 (warm) | 8.7 s / 146 MB | **6.6 s / 136 MB** |

**No regression — a 21% win, at slightly LOWER peak RSS.** The group_by result is
byte-identical on every leg and matches the published figure (false 8,332,935 /
true 8,333,870). Warm, the leg is CPU-saturated (3.5 s CPU over a 6.6 s wall = 0.53 of the half
core); the cold round is the source-bound one (4.8 s CPU over 21.1 s = 0.23),
which is why r1 must never be compared across sides.

## The source is the floor: apitap beats MySQL at MySQL's own aggregation (2026-08-09)

Prompted by a "why isn't this 5 seconds, it only pulls 2 columns" challenge on the
100M-row cross-engine article. Measured the same per-day aggregation done by the
SERVER ITSELF, with apitap nowhere in the picture, 2 rounds each:

| engine | server does it alone | apitap @0.5 core |
|---|---|---|
| MySQL, 50M rows | **190.3 / 190.4 s** | **162.9 s** |
| Postgres, 50M rows | 12.3 / 11.5 s (16 cores) | 10.4 - 13.6 s |

**apitap extracts 50M rows across the wire and aggregates them on HALF A CORE
faster than MySQL aggregates them in place** — and matches a 16-core Postgres
while using ~1/50th of its CPU. Both lanes sit at or under their source's own
floor, so no client-side knife can reach 5 s: that would require the databases
to scan faster than they can.

Why MySQL is 15× slower than Postgres on identical row counts:

- `innodb_buffer_pool_size` = **128 MB** against a **28,807 MB** table (1:225).
  Every scan re-reads the whole table.
- `EXPLAIN` shows `type=ALL, key=NULL, rows=48,899,707, Using temporary` — a full
  table scan with no covering index, so InnoDB reads every full row to project
  2 columns. Row stores cannot skip columns.

The levers are all server-side, none in our engine: a covering index on
`(d, amount)` (turns 28.8 GB of scan into an index-only pass), a sane buffer
pool, or — the article's own answer — land to Parquet once and join the lake in
10.4 s forever.

This also VERIFIES (and understates) the article's line that "170 seconds is the
server scanning 28 GB of InnoDB, a floor every client pays": the floor measures
190 s.

## MySQL read lane: where apitap's own time actually goes (2026-08-09)

Asked directly — not "is the server slow" but "what can WE cut". Measured the
MySQL lane alone in the cage, with container CPU from `cpu.stat`:

**`bench_wide_50m` (48.9M rows, 28.8 GB, NO primary key)**

| pipes | wall | our CPU | quota used | peak |
|---|---|---|---|---|
| auto | 159.3 s | 14.9 s | **19%** | 99 MB |
| 1 | 156.3 s | 14.8 s | 19% | 96 MB |
| 4 | 155.0 s | 14.7 s | 19% | 98 MB |

Identical across pipe counts because `span_stmts` for MySQL range-splits ONLY
when an explicit integer `cursor=` is given, else it emits one
`SELECT … WHERE true`. And that is CORRECT here: the table has no primary key,
so N ranges would be N full scans. We idle 81% of the wall waiting — our decode
of 48.9M rows costs 14.9 s of CPU total.

**`orders` (45.8M rows, 6.2 GB, PK `o_orderkey`)** — the table where splitting IS
possible, warm round:

| mode | wall | our CPU | quota used |
|---|---|---|---|
| single stream (today's default) | 39.1 s | 18.9 s | **97%** |
| PK ranges × 4 | 38.2 s | 18.2 s | 95% |
| PK ranges × 8 | 41.1 s | 19.1 s | 93% |

**Splitting buys nothing warm** (38.2 vs 39.1 s is noise; 8 pipes is worse):
one stream already saturates the half core. Our decode floor is ~19 s of CPU →
38 s of wall at 0.5 core, and we land on 39.1 s. The 56.1 s cold first leg at
69% quota is the only place ranges helped (56 → 36 s), i.e. exactly when we are
WAIT-bound.

**The gap, honestly scoped:** Postgres has a third span strategy MySQL lacks —
CTID page ranges (PG 14+, no index needed), which is why pg parallelises with no
cursor while MySQL never does. Auto-detecting an integer PK on the MySQL side
would close it, but it would NOT have helped either case measured here (one
table cannot be split, the other is already CPU-saturated). It is worth building
for the case this rig cannot produce: a PK-bearing table on a remote or slow
server, where the client waits.

### Splitting an UNINDEXED column: neutral, not catastrophic (prediction was wrong)

`bench_wide_50m` has zero indexes; `EXPLAIN` on a slice predicate returns
`type=ALL, key=NULL, rows=48,899,707, filtered=11.11`, i.e. every slice is a full
table scan. Predicted 2-4× slowdown. Measured:

| mode | wall | our CPU |
|---|---|---|
| single stream | 155-159 s | 14.7-14.9 s |
| `cursor="id"`, 2 slices | 160.9 s | 16.2 s |
| `cursor="id"`, 4 slices | 161.3 s | 15.1 s |

**Flat.** N concurrent full scans share the OS page cache — the server does N×
the logical work but pays one pass of physical I/O, and it has 16 cores to do the
filtering. So the prediction of a 4× blowup was wrong; splitting an unindexed
column is a no-op here, not a trap. It is also not a win: the wall is one pass
over 28.8 GB no matter how the client asks for it.
