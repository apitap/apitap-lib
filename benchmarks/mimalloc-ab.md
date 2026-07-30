# mimalloc A/B — evaluated and rejected (2026-07-30)

Prompted by [Kerkour's heap-fragmentation article](https://kerkour.com/rust-high-performance-memory-fragmentation-allocations)
("swap in jemalloc/mimalloc, memory more than halved"), we measured whether
`#[global_allocator] mimalloc` helps apitap. **It doesn't — the change was reverted.**
This file records why, so the idea isn't re-tried without new evidence.

## Setup

- Two wheels built on the bench server from the *same* 0.13.2 source, differing only in
  the allocator (`mimalloc = "0.1"` + `#[global_allocator]` in `py-apitap/src/lib.rs`);
  sanity-checked via `mi_malloc` symbol presence/absence in each `.so`.
- Same harness as every other number in this directory: `run-server.sh`, 10M rows,
  ingestr's schema, checksum-validated (16/16 runs matched), timed inside the container.
- New instrumentation (kept): the harness now prints `PEAK_MB` — the container cgroup's
  `memory.peak` — after every run.
- 2 reps per cell; raw output in [mimalloc-ab-raw.log](mimalloc-ab-raw.log).

## Results (mean of 2, glibc = stock wheel)

| route, box | glibc | mimalloc default | Δ |
|---|---|---|---|
| pg→pg, 0.5 cpu / 256 MB | 65.2 s / **49.7 MB** | 64.0 s / 70.8 MB | time − , RSS **+42%** |
| pg→pg, 16 cpu / 4 GB | 23.2 s / 495 MB | 22.8 s / **420 MB** | time − , RSS **−15%** |
| pg→ch, 0.5 cpu / 256 MB | 23.7 s / **160 MB** | 23.0 s / 192 MB | time − , RSS **+20%** |
| pg→ch, 16 cpu / 4 GB | 10.9 s / **370 MB** | 11.5 s / 427 MB | time + , RSS **+15%** |

Wall time is a wash everywhere (±5%, inside run-to-run noise at n=2). Peak RSS is
*worse* in 3 of 4 cells — including the 256 MB flagship box — and the one win
(pg→pg multi-thread arena spread) is cancelled by the pg→ch loss on the same tier.

## Can tunables fix the RSS? Yes — by paying with time (pg→ch @ 256 MB)

| config | time | peak RSS |
|---|---|---|
| glibc (stock) | 23.7 s | 160 MB |
| mimalloc default | 23.0 s | 192 MB |
| `MIMALLOC_ARENA_EAGER_COMMIT=0` | 25.0 s | 161 MB |
| `MIMALLOC_PURGE_DELAY=0` | 27.1 s | 152 MB |
| both | 30.0 s | 125 MB |

Eager arena commit is what inflates default-mimalloc RSS; disabling it just gets back
to glibc parity while running slower. Full eager-purge reaches 125 MB — genuinely below
glibc — but at +27% wall time. glibc already sits at the knee of this pareto curve.

## Why the article's premise doesn't transfer

Kerkour's fragmentation story is *many small allocations at a high rate* (thousands of
DNS/HTTP/JSON objects per second). apitap's hot path is the opposite: a small number of
large, fixed-size chunk buffers streaming with backpressure. glibc serves those via
`mmap` directly and returns them on free — there is no small-object churn to fragment,
so a fragmentation-resistant allocator has nothing to win and its segment caching only
adds standing overhead.

The allocation-*reduction* half of the article (heapless / bytes / smallvec) is the
half that applies here — `bytes` is already load-bearing in apitap-core.

## Verdict

- **Not merged.** Allocator stays the system default.
- **Kept:** `PEAK_MB` reporting in `run-server.sh` — peak RSS is now a first-class
  benchmark output alongside wall time.
- Revisit only if a future workload profile shows small-alloc churn (e.g. a row-decode
  path for a new source type), and then re-run this exact A/B.
