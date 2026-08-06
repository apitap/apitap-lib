# Profiling the CDC apply path (2026-08-06)

Session 5 of the CDC speed work. Two knives shipped to the branch before any
of this (key-table reuse −9.4%, DDL memoization −9.0%), and then the campaign
was stopped and the path was *measured* instead of guessed at. Six research
angles produced a plan; the measurements overturned most of it, including two
numbers this file's author had been quoting.

Rig: OVH VPS, 16 vCPU / 61 GB. Ten wide tables (15 columns, ~607 B/row),
300k rows each, burst of 150k mixed changes per table (60% insert / 30%
update / 10% delete) in 10k-row transactions = 1.5M changes. One container at
`--cpus=0.5 --memory=256m --memory-swap=256m`, PyPI wheel 0.27.0.

## What was wrong in our own model

**The window count was off by 4.75×.** Estimates for both shipped knives were
built on "38 windows". The instrumented run says **71**, and the ClickHouse
`query_log` for the same run shows 73 DELETEs and 64 row INSERTs — because the
burst writes one table at a time, so a window typically carries changes for
*one* table, not ten. The knives still won their A/Bs; the model that predicted
them did not deserve to.

**Two phase timers were added together as if they were CPU.** `run.rs` prints a
per-window apply time and a per-window drain time. Summed over the run:

| | seconds |
|---|---|
| Σ apply | 44.3 |
| Σ drain | 32.3 |
| serial sum | 76.6 |
| actual wall | 56.5 |

They cannot be added. The drain and the apply are two tokio tasks on **one**
half core, so each timer counts wall in which the other task was running. The
20.1 s gap is overlap, not slack.

What the split *does* say: apply is the critical path, and 12.2 s of the drain
is exposed while 20.1 s is hidden behind it. So a drain-side knife pays roughly
38 cents on the dollar; an apply-side knife pays in full. The research plan's
closing recommendation — "the next campaign is a drain campaign" — is the
opposite of what the numbers support.

*(This run measured 55.5 s rather than the 44.6 s baseline because
`APITAP_DEBUG=1` prints per window. Ratios hold; the absolute is inflated.)*

## CPU-bound, not wait-bound

cgroup `cpu.stat` does not have the overlapping-timer problem — `usage_usec` is
real CPU consumed by the whole container.

| round | wall | CPU | avg cores | saturation of the 0.5 quota |
|---|---|---|---|---|
| 1 | 45.3 s | 19.3 s | 0.426 | 85% |
| 2 | 42.8 s | 18.9 s | 0.442 | 88% |
| 3 | 46.3 s | 17.3 s | 0.375 | 75% |

**~83% saturated.** We are burning CPU, not waiting on ClickHouse. Two
consequences: knives that remove *round trips* attack a small share, and every
CPU-second removed is worth **two seconds of wall** at half a core.

And there is a lot to remove: 18.5 CPU-s for 1.35M changes is **14.3 µs per
change**, ~71 cycles per byte moved, for work that is fundamentally copy,
escape, send.

## Where the CPU goes

`perf record -F 999` attached to the capped container from the host. First
report was garbage — the host is Ubuntu, the container is Debian, so offsets
resolved against the wrong symbol table and produced impossible names
(`pthread_create` at 21.9%, `getsgnam_r`). Rebuilt with a `--symfs` assembled
from the container image.

| object | share |
|---|---|
| libc.so.6 | 56.8% |
| `_apitap.abi3.so` | 28.7% |
| kernel | 13.7% |

Our own hot symbols:

| symbol | % |
|---|---|
| `core::hash::BuildHasher::hash_one` | 5.81 |
| `logbased::drain::drain` closure | 4.89 |
| `Collapser::key_of_row` | 4.56 |
| `wire::pgoutput::Reader::tuple` | 3.12 |
| `hashbrown::RawTableInner::drop_elements` | 1.35 |
| `Collapser::finish` / `put_upsert` / `update` | 3.17 |
| `hashbrown::reserve_rehash` | 0.80 |

Mapping the libc offsets to their nearest exported symbol puts ~38-42% of all
samples in the **malloc/free region**, with `__lll_lock_wait_private` (the
glibc arena lock) visible at 4.4%.

The sources are identifiable in code:

1. `pgoutput::Reader::tuple` copies every cell with `to_vec()` — 15 columns ×
   1.5M changes = **22.5M short-lived Vec allocations**.
2. `Collapser::key_of_row` builds a `Vec<Vec<u8>>` per change (`Key` is
   `Vec<Vec<u8>>`), so one outer plus one inner allocation each, and updates
   call it twice.
3. The collapse map is keyed on that nested Vec, so hashing chases pointers
   and every evicted entry drags its allocations with it.

## Hypotheses killed, with receipts

**16 tokio worker threads on half a core.** `py-apitap/src/lib.rs:17` builds
the runtime with `Runtime::new()`, whose worker count is
`available_parallelism()`. That honours cpuset but not the `cpu.max` quota, and
the container confirms the mismatch: `os.cpu_count()` = 16 while `cpu.max` reads
`50000 100000`. The theory fit the profile perfectly — futex wakeups, context
switches, kernel spin locks, allocator arena contention.

It is wrong. A real 10-table drain has **3 OS threads** (main, our probe, one
tokio worker). Tokio spawns workers lazily and half a core never generates
enough parallel work to wake a second. `TOKIO_WORKER_THREADS=1` is measurably
identical to the default (52.2 s vs 53.3 s), and the variable *is* honoured
(setting it to 4 produces 6 threads). Cost of killing this: ~20 minutes, no
Rust written.

**RowBinary for the CDC lane.** The apply sends `FORMAT TabSeparated`
(`dest_ch.rs:200`, `:219`), unlike the bulk lane which sends RowBinary. That
looks like an oversight and is not one: pgoutput hands us values as **text**, so
TSV is nearly a copy-with-escaping, while RowBinary would mean parsing every
int/date/numeric on our half core — moving work from ClickHouse's 16 cores onto
our 0.5. ClickHouse's own format benchmarks also put TabSeparated *ahead* of
RowBinary on the read side. Settled both ways; do not revisit.

## The allocator probe

Not a proposal — an instrument. Same 0.27.0 wheel, allocator swapped by
`LD_PRELOAD`, three interleaved rounds:

| allocator | r1 | r2 | r3 | median | peak RSS |
|---|---|---|---|---|---|
| glibc | 52.4 | 45.4 | 83.7 | **52.4 s** | 104-108 MB |
| tcmalloc | 42.8 | 43.0 | 42.3 | **42.8 s** | 112-124 MB |
| jemalloc | 42.9 | 42.9 | 45.8 | **42.9 s** | 106-113 MB |

**−18% on the median.** The spread matters more than the median: glibc ranged
45-84 s on identical work, and its worst round ran at 40% CPU saturation while
tcmalloc, minutes later on the same box, hit 90%. For a per-minute CDC job that
unpredictability costs more than the average does.

Two independent allocators agreeing to within 0.1 s is not noise.

This does **not** reopen `benchmarks/mimalloc-ab.md`. That rejection stands and
its own root-cause note explains why this is consistent: the bulk transfer path
is "few large fixed chunk buffers", which glibc serves fine via mmap, while the
CDC apply path is exactly the small-allocation churn that rejection said such
allocators *do* fix. Different workload, different answer. Shipping a global
allocator would change the bulk path too, which is where mimalloc lost, so the
preferred fix remains **allocating less**.
