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
