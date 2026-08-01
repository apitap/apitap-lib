# Hot-path profiling with hotpath (2026-07-30)

How the transfer hot path was measured function-by-function, what the profile
said, and which optimizations it did (and did not) justify. Raw profiler output:
[hotpath-profile-raw.log](hotpath-profile-raw.log).

## Harness

[hotpath](https://github.com/pawurb/hotpath-rs) is wired into `apitap-core`
behind an off-by-default feature — zero overhead (and no dependency) unless
enabled:

```bash
SRC=postgres://… DST=postgres://… TABLE=public.bench_data_10m \
    cargo run --release --example profile -p apitap-core --features hotpath
# add ,hotpath-alloc for the allocation report
```

Seven measure points cover the path: `pipeline::run` (umbrella),
`copy_out_worker` (per span), `SpanStrip::push` / `Transcoder::push` (per COPY
message ≈ per row), `PgCopyLoader::send` / `ChLoader::send` (per chunk,
backpressure), `ChConn::insert_stream` (per span HTTP). All via
`#[cfg_attr(feature = "hotpath", hotpath::measure)]`.

## What the profile said (10M rows, 16 cores, uncapped)

**pg→pg (23.5 s, 8 pipes)** — sink-bound. `send` totals 152 s of the workers'
177 s (**86% of worker time is waiting on the destination's COPY-IN**, i.e. the
WAL write wall the README already documents). The relay CPU (`pgcopy::push`,
10M × 456 ns) is ~2.6%.

**pg→ch (11.8 s, 32 pipes)** — source-bound. `send` totals 10 ms (**no
backpressure at all**); workers spend their lives in `stream.try_next()`
waiting for the source's COPY. Transcode (`rowbinary::push`, 10M × 1.6 µs) is
~4% per worker.

**Allocation report, both routes: 4.4 GB per run, 99.9% exclusive to
`copy_out_worker`.** That is the fresh `Vec::with_capacity(chunk + 64 KiB)`
per chunk (~1,100 per run) plus a 1 MiB carry buffer per span (spans =
parallel × 6). The transcode itself allocates **0 bytes** — capacity headroom
works as designed. Net leak: none (RSS diff ≈ 0); the cost is pure churn.

## What was changed (and why)

1. **Chunk-buffer recycling on the Postgres sink** (`Loader::reclaim`).
   The COPY-IN sender task now borrows the buffer for `sqlx`'s send (which
   memcpys into its write buffer either way), then hands the emptied `Vec`
   back through a 4-slot channel; `copy_out_worker` reuses it instead of
   allocating. At steady state the chunk loop allocates nothing. Sinks that
   genuinely consume the buffer (ClickHouse's zero-copy HTTP body) keep the
   default `reclaim() → None`.
2. **Lazy `Transcoder` carry buffer.** The 1 MiB preallocation per span was
   for a buffer that only ever holds a header prefix or a partial-tuple tail —
   both rare and small. It now grows on demand: spans × 1 MiB of churn gone
   (30 MiB at the 256 MB tier's 5 pipes, 192 MiB at 32 pipes).

**Rejected on the same data:** micro-optimizing `transcode_field` — the alloc
report shows the transcode allocates nothing and its bswap arms are already
shaped for the optimizer; and any mimalloc/jemalloc swap — see
[mimalloc-ab.md](mimalloc-ab.md) for why the allocator is not the problem.

## Did it help? (same harness as every number here, checksum-validated)

Alloc re-profile after the change (pg→pg, same run shape): `copy_out_worker`
exclusive allocations collapsed from **4.4 GB to 205 MB (−95%)**.

10M rows, mean of 2, same-day same-box as the stock numbers, all 8 runs
checksum-matched:

| route, box | stock 0.13.2 | optimized | Δ |
|---|---|---|---|
| pg→pg, 0.5 cpu / 256 MB | 65.2 s / 49.7 MB | 62.9 s / **41.6 MB** | −3.5% time, **−16% RSS** |
| pg→pg, 16 cpu / 4 GB | 23.2 s / 495 MB | 22.3 s / **285 MB** | −3.9% time, **−42% RSS** |
| pg→ch, 0.5 cpu / 256 MB | 23.7 s / 160 MB | 23.6 s / **147 MB** | ≈ time, **−8% RSS** |
| pg→ch, 16 cpu / 4 GB | 10.9 s / 370 MB | 11.3 s / 351 MB | +3.5% time (within rep spread), −5% RSS |

As predicted, both routes stay database-bound so wall time barely moves (the
pg→ch 16-cpu delta is inside the run-to-run spread of both wheels). The real
wins are structural: allocator traffic gone (−95%), and peak RSS down
everywhere — most dramatically −42% on the multi-thread pg→pg tier, where the
constant stream of freed-and-reallocated 4 MiB chunks was what kept glibc's
per-thread arenas fat. The 256 MB flagship box now runs pg→pg in 41.6 MB peak.

## How low can the box go? (pg→pg, 10M rows, 0.5 vCPU, optimized wheel)

Shrinking the container against that 41.6 MB peak — every run checksum-matched,
pipe count auto-sized to 1 by the memory heuristic:

| memory cap | result | peak |
|---|---|---|
| 128 MB | 65.0–66.6 s ✓ | 41.5–41.7 MB |
| 64 MB | 61.3–62.9 s ✓ | 41.6 MB |
| 48 MB | 62.7 s ✓ | 41.7 MB |
| **44 MB** | **64.4 s ✓** | **41.7 MB (2.3 MB slack)** |

Wall time is flat from 256 MB down to 44 MB — memory is genuinely not the
limiting resource at any of these sizes, exactly as the bounded-memory design
intends. 44 MB is a probe, not a support statement; the honest README-able
claim is: **10M rows, pg→pg, in a 64 MB / 0.5 vCPU container, with headroom.**
(For scale: the 256 MB tier is where dlt was OOM-killed at 1M rows.)

Same probe on the transcode route — **pg→ch, 10M rows, 0.5 vCPU** (at 256 MB
this route runs 5 pipes / 23.6 s; below ~128 MB the memory heuristic drops to
1 pipe and the transcode serializes onto the half core):

| memory cap | result | peak |
|---|---|---|
| 128 MB | 58.7–61.8 s ✓ | 41.1–41.3 MB |
| 96 MB | 57.5 s ✓ | 41.3 MB |
| 64 MB | 58.4 s ✓ | 37.3 MB |
| 48 MB | 58.3 s ✓ | 41.2 MB |
| **44 MB** | **60.3 s ✓** | **41.1 MB** |

So the trade below 128 MB on this route is pipes, not correctness: ~2.5× the
wall time of the 256 MB tier, every run checksum-matched. Both flagship routes
land 10M rows inside 44 MB.

## The 100 GB ladder (232M rows, pg→ch, 0.5 vCPU)

Same schema scaled 23×: 232,000,000 rows = 101 GB in Postgres. Source checksum
is computed once and cached (`run-server.sh` now does this — a full-scan
`md5(string_agg…)` also had to be replaced with an order-independent per-row
hash sum first, because the concatenation crosses Postgres's 1 GiB buffer limit
around ~110M rows: at this scale the *validator* broke before the engine).

| memory cap | pipes | transfer | peak RSS | verdict |
|---|---|---|---|---|
| 256 MB | 5 (auto) | **536.7 s (8m57s)** | 170.8 MB | match |
| 128 MB | 3 (forced) | 638.8 s (10m39s) | 113.6 MB | match |
| 80 MB | 2 (forced) | 875.0 s (14m35s) | 72.4 MB | match |
| 64 MB | 2 (forced) | OOM-killed mid-run | needs 72.4 | predicted ✗ |
| 48 MB | 1 (auto) | 1552.7 s (25m53s) | 48.0 MB | match |
| 44 MB | 1 (auto) | 1423.1 s (23m43s) | 44.1 MB | match |

Readings:

- **8m57s for 100 GB on half a core in 256 MB** — ~432K rows/s, ~11 GB/min,
  ending in an atomic swap, all 232M rows checksum-verified.
- **The forced-pipe rows are the heuristic's indictment.** Auto picks 1 pipe at
  every cap below 256 MB; forcing 3 pipes at 128 MB is 2.2× faster and still
  11% under the cap. The marginal cost is ~28-41 MB/pipe (5→3: 28.6, 3→2: 41.2)
  with a ~45-56 MB base — `mem_capped_parallel` (pipeline/mod.rs) budgets far
  more than that. Retune queued (2026-08-01), including a chunk_bytes dimension
  (3 thin pipes may beat 2 fat ones at the same cap).
- **The 64 MB OOM was predicted before it ran** (2-pipe peak measured 72.4 MB) —
  the failure bracket is as informative as the passes.
- Wall time at 1 pipe (~24-26 min) is transcode serialized on half a core, not
  memory pressure: 48 MB and 44 MB run at the same speed as each other.

## The same 100 GB, latest ingestr and dlt (raw: [100gb-ladder-raw.log](100gb-ladder-raw.log))

Per the ground rules: latest releases (ingestr v1.1.14, dlt 1.29.1), dlt on its
pyarrow backend, the identical 0.5 vCPU / 256 MB container, a 30-minute kill
cap, destination wiped between runs.

| tool | outcome |
|---|---|
| apitap (this repo) | **536.7 s, peak 170.8 MB, 232M rows checksum-matched** |
| ingestr v1.1.14 | **OOM-killed at ~21 s** (exit 137, `OOMKilled=true`), 0 rows landed — reproduced ×2 |
| dlt 1.29.1 + pyarrow | **OOM-killed at ~21 s** (exit 137, `OOMKilled=true`), 0 rows landed — reproduced ×2 |

The 30-minute cap never came into play: neither tool survived the first minute
at this table size, in the container where apitap finishes with 85 MB to spare.
(At 10M rows ingestr *does* complete in this box — 428 s in our earlier
measurement; the 23× table is what pushes its working set over.) Both failures
were reproduced on a second run before being recorded here.

## Session 2 (2026-07-31): the retune the ladder paid for

Three engine changes, each with the measurement that justified it
(raw: [session2-raw.log](session2-raw.log)):

1. **MySQL 8.4 hang → fixed.** `mysql_async` 0.34 silently hung forever against
   MySQL 8.4 servers (connections parked in Sleep, no error — reproduced on
   published 0.13.2 too). Upgrading to 0.37 fixes it outright (1M my→my in
   6.2 s vs an infinite hang), and every sink connection now carries a 30 s
   deadline that names the failure instead of freezing.
2. **The memory→pipes budget now matches the measured ladder.** Old formula:
   96 MiB reserve + 8×chunk per pipe (forced 1 pipe at every cap ≤128 MB). New:
   40 MiB reserve + 10×chunk, fitted to the 100 GB ladder's whole-container
   peaks and locked by a unit test (`mem_budget_matches_the_measured_ladder`).
3. **Auto thin pipes.** When memory (not CPU) caps the pipe count and
   `chunk_bytes` wasn't pinned, the engine now trades buffer depth for pipes
   (2 MiB chunks). `chunk_bytes` became `Option` end-to-end so an explicit
   value is never touched.

What auto now does on the same 10M pg→ch run (was: 1 pipe / ~58 s at every cap
below 256 MB):

| cap | auto config | time | peak | vs before session 2 |
|---|---|---|---|---|
| 256 MB | 8 × 2 MiB | 21.7 s | 119 MB (46%) | same speed, −30 MB peak |
| 128 MB | 4 × 2 MiB | **23.4 s** | 80.7 MB (63%) | **2.5× faster** |
| 80 MB | 2 × 2 MiB | **29.1 s** | 49.0 MB (61%) | **2× faster** |
| 44 MB | 1 pipe | 58.9 s | 40.7 MB | unchanged, still completes |

The 128 MB container now nearly matches the 256 MB one — parallelism, not
memory, was always the thing being bought. mimalloc was also re-tried on the
new code and rejected a second, final time: [mimalloc-ab.md](mimalloc-ab.md).


## Session 3 — the MySQL source (2026-08-02)

Same discipline as the first campaign, pointed at `mysql://` sources: hotpath
for the wait-structure, `perf` on software `cpu-clock` (this VPS blocks PMU
counters), and the worker's own fetch/encode/send split. Rig: 11.8M rows
(the ingestr schema), MySQL 8.0 source, 16-core host.

**Finding 1 — the TLS tax.** The top `perf` symbol on my→pg was
`_aesni_ctr32_ghash_6x`: MySQL 8 negotiates TLS by default and sqlx obliges,
so the entire wire stream pays AES-GCM. `?ssl-mode=disabled` (trusted
networks): my→pg 34.6 s → 27.6 s (−20%), my→ch 14.8 s → 12.4 s (−16%).
Zero code — documented in the usage guide. (Postgres benches never paid this
because postgres:16-alpine ships without TLS; a TLS-enabled pg would.)

**Finding 2 — the source was never the wall.** With TLS off, the worker
split reads fetch ≈ 2 s, encode ≈ 1 s, send-wait ≈ 21 s of a 28 s my→pg run,
on every one of 16 workers — and my→ch moves the same rows in 12.4 s. The
MySQL source sustains ~950K rows/s; the pg *destination* saturates at
~450K rows/s regardless of pipe count.

**Fixes landed.** (1) The mysql row_worker now recycles chunk buffers from
the pg sink's back-channel instead of allocating a fresh multi-MB Vec per
chunk (`clear_page_erms` was 4.8% of samples — the same churn the pg COPY
worker shed in session 1), and my→pg gets the overlapped COPY loader.
(2) `my_pg_parallel` caps at the measured dest saturation: 8 pipes = 26.0 s
where 16 = 28.4 s — same speed, half the peak memory, less lock contention.

| my→pg, 11.8M rows | wall | peak | notes |
|---|---|---|---|
| 0.16.0, defaults (host) | 34.6 s | — | TLS on, 16 pipes |
| 0.16.0, `ssl-mode=disabled` (host) | 27.6 s | — | knob only |
| **this session** (host, auto 8 pipes) | **26.0 s** | — | −25% end-to-end |
| 0.16.0 in 2 vCPU / 1 GB | 26.7 s | 246 MB | ssl off |
| **this session** in 2 vCPU / 1 GB | **24.5 s** | 303 MB | −8% engine-only; +57 MB is the overlap pipeline's in-flight buffers, inside every tier budget |

pg→pg on the same dest instance: 25.3 s — the my→pg gap is now noise. What
remains of my→pg is the **destination's** ~450K rows/s COPY ceiling, which is
the next session's target (with `sqlx BinaryRow::decode_with` at 6.3% +
async-stream yielding at ~8.5%, a raw-protocol MySQL reader is the source-side
endgame if it ever becomes the wall again).
