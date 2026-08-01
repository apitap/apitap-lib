# Drepper's cpumemory (2007), read for apitap — notes & experiment backlog (2026-08-01)

The full 114-page paper was read section-by-section and synthesized against
apitap's measured profile. Verbatim synthesis below; then the status of the
gate experiment.

(A) EXECUTIVE SUMMARY — Drepper 2007 for apitap, 2026

1. The paper's hardware specifics (FSB/Northbridge, DDR2/FB-DIMM, inclusive L3, 64-entry TLBs, absolute cycle counts, Xen shadow paging, IA-64 speculation, TSX prediction, oprofile) are all obsolete; its mechanisms — locality, RFO, MESI, TLB, plateaus — remain load-bearing.
2. Biggest live claim for apitap: working-set plateaus at cache boundaries. 2 MiB chunks fitting Sapphire Rapids' 2 MiB private L2 (and mapping to exactly one huge page) is the paper's best candidate mechanism for the measured "2 MiB beats 4 MiB" — but this is a hypothesis to confirm, not a fact.
3. Second live claim: every streaming store costs a hidden read-for-ownership of the destination line, so chunk-append silently doubles its bandwidth; mitigation is NT stores, but modern RFO-elision (SpecI2M) and the warm-recycled-buffer effect may already neutralize it — and NT stores punish the downstream consumer.
4. Third live claim: MESI dirty-line transfer and false sharing (channel atomics, per-pipe counters, Arc refcounts, recycle free-list) still cost 4–11x per contended line; at ~1.6K chunks/sec the rate is low, so expect real-but-immaterial — `perf c2c` decides in an hour.
5. TLB claims survive with a twist: linear access + huge STLBs make 4 KiB pages mostly fine on bare metal, but EPT nested walks on VMs (apitap's only habitat) multiply every walk's cost — the paper's virtualization remark aged best of anything in it. THP via `madvise(MADV_HUGEPAGE)` on recycled buffers is a one-line test.
6. Software prefetch for linear streams is deader than in 2007 (Drepper already measured 94.5% of SW prefetches wasted); modern HW prefetchers own this workload. Do not add `_mm_prefetch` without a demand-miss finding first.
7. NUMA sections are moot: gate with `lscpu | grep NUMA` — the 16-core VPS and typical C3 shapes are single-node; also vCPU pinning inside a VPS does not control physical placement.
8. Overriding skepticism: at ~1.3 µs/row ≈ 4,000+ cycles/row, apitap is plausibly compute/frontend-bound (branchy per-field OID dispatch), not memory-bound — in which case every memory optimization in the paper is second-order. One top-down `perf stat` run settles this and must run first.
9. The paper retro-validates existing apitap decisions: contiguous chunk streaming (open-row/prefetch reward), buffer recycling (minor-fault + warm-line + warm-TLB elimination — the 42% RSS win has a mechanistic story), and distrust of generic swaps (mimalloc losing twice matches "generic tricks lose to well-behaved linear workloads").
10. Enduring methodology: use counter *ratios* per instruction, never absolutes; measure before optimizing; the profiling chapter's method (not its tools) is the part to keep.

(B) RANKED EXPERIMENTS — 16-core cloud VPS, docker + perf, ranked by expected-information-per-hour

E1. Top-down bound classification of the transcode loop (GATE for all others)
- Hypothesis: at ~4,000 cycles/row the loop is core/frontend-bound, not memory-bound; memory tuning is second-order.
- Method: steady-state 10 GB transfer, one pipe pinned (`taskset -c 2`). Run: `perf stat -M tma_frontend_bound,tma_backend_bound,tma_memory_bound,tma_core_bound -p <pid> -- sleep 30`, then `perf stat -e cycles,instructions,branches,branch-misses,L1-dcache-load-misses,l2_rqsts.miss,longest_lat_cache.miss,cycle_activity.stalls_mem_any -p <pid> -- sleep 30`. Compute per-row: instructions/row, branch-misses/row, MPKI. Vary nothing — classification run. (If TMA metrics unavailable under the VPS's virtualized PMU, fall back to `cycle_activity.stalls_mem_any / cycles`.)
- Decision rule: memory_bound < ~15% and IPC > 2 → deprioritize E4–E6 entirely, spawn compute-side work instead (branchless field dispatch, PGO/BOLT — branch-misses/row > ~10 confirms). memory_bound > 25% → proceed down this list.
- Effort: 1–2 hours. Highest information density of any experiment here.

E2. Chunk-size sweep with L2/LLC counters — find the mechanism behind "2 MiB beats 4 MiB"
- Hypothesis: 2 MiB source+dest working set stays L2-resident (2 MiB private L2/core); 4 MiB spills to shared L3, and with 6 pipes contends there.
- Method: fixed 10 GB dataset, sweep chunk size {512K, 1M, 2M, 4M, 8M}, at 1 pipe and 6 pipes. Per run: rows/s + `perf stat -e l2_rqsts.references,l2_rqsts.miss,LLC-loads,LLC-load-misses,instructions`. Plot L2 miss ratio and LLC-MPKI vs chunk size.
- Decision rule: L2 miss ratio inflects between 2M and 4M and throughput tracks it → mechanism confirmed; make chunk size derive from detected L2 size (`/sys/devices/system/cpu/cpu0/cache/index2/size`) instead of a hardcoded 2 MiB. Flat curves → the 2-vs-4 result was allocator/RSS behavior, not caches; keep 2 MiB for memory reasons only and stop cache-tuning chunk size. 6-pipe inflecting earlier than 1-pipe → cap pipes per host class.
- Effort: half a day (sweep automation + runs).

E3. `perf c2c` false-sharing scan of channels, counters, and the recycle pool
- Hypothesis: cross-core dirty-line traffic exists on channel internals/metrics/Arc refcounts but is immaterial at 1.6K chunks/sec — unless a per-row shared counter is hiding.
- Method: full-rate 6-pipe run; `perf c2c record -a -- sleep 60; perf c2c report --stdio`. Look for load-HITM lines with >1% of samples mapping to apitap/tokio/channel symbols; supplement with `perf stat -e mem_load_l3_hit_retired.xsnp_hitm`.
- Decision rule: hot HITM line found in apitap structs → pad to 128 B (`#[repr(align(128))]`) or make counters per-worker, re-run; improvement should appear at 6 pipes only (the false-sharing fingerprint). Clean report → permanently close the false-sharing/atomics topic and reject future padding PRs as cargo cult.
- Effort: 2–3 hours. One command to a possible genuine surprise; cheap rule-out otherwise.

E4. THP on the recycled buffer pool — dTLB walk cost under EPT
- Hypothesis: 100 GB streamed through 4 KiB pages costs measurable page-walk cycles, amplified 2–6x by the VPS's nested (EPT) walks; 2 MiB-aligned recycled buffers become single TLB entries with THP.
- Method: baseline: `perf stat -e dTLB-load-misses,dTLB-store-misses,dtlb_load_misses.walk_completed,dtlb_load_misses.walk_active,cycles` over a fixed 10 GB transfer with THP `never`. Then add `madvise(MADV_HUGEPAGE)` in the buffer-pool allocator (one line — buffers are long-lived and 2 MiB-sized already), THP `madvise`, verify AnonHugePages in `/proc/<pid>/smaps_rollup`, re-measure. Bonus diagnostic: walk_active/walk_completed = cycles/walk; >100 confirms the EPT tax.
- Decision rule: walk_active < 1% of cycles at baseline → claim true-but-not-binding, revert, done. > 2–3% and THP removes it with any throughput gain → ship the madvise (near-zero risk given recycling).
- Effort: half a day including the code change.

E5. Pipe-count scaling curve — shared-bandwidth ceiling on this VPS
- Hypothesis: on a 16-core VPS with unknown host contention, aggregate throughput goes sub-linear before core count runs out; 3.3 GB/s logical implies plausibly 10–20 GB/s of DRAM traffic (RFO doubling included).
- Method: run 1, 2, 4, 6, 8, 12 pipes on fixed data; record aggregate and per-pipe rows/s plus `perf stat -e cycle_activity.stalls_l3_miss,longest_lat_cache.miss,offcore_requests.demand_data_rd` (uncore/IMC counters will not exist on the VPS — approximate DRAM bytes as offcore demand reads x 64 B). Optionally baseline the machine with a `stream`-like memory benchmark in the same docker image.
- Decision rule: per-pipe throughput drops >15% before 8 pipes with rising L3-miss stalls → set a per-host-class pipe cap in config; near-linear → memory bandwidth is a non-issue here, close §6.4.3/bandwidth topics.
- Effort: half a day (mostly run time; reuses E2 automation).

E6. RFO cost of chunk-append, and an NT-store prototype (only if E1 said memory-bound)
- Hypothesis: filling fresh output chunks costs a hidden RFO read per 64 B line (~+50% memory traffic for the write path); recycled-warm buffers already suppress most of it, which is part of why recycling won.
- Method: baseline `perf stat -e offcore_requests.demand_rfo,l2_rqsts.all_rfo,l2_rqsts.rfo_miss` comparing (a) recycled buffers vs (b) fresh-mmap-per-chunk build. If rfo_miss bytes ≈ bytes written in (b) but not (a), recycling's mechanism is confirmed — document it as a design invariant. Only then prototype NT stores: 64 B staging buffer + `_mm256_stream_si256` flush + `_mm_sfence` before channel send, feature-flagged; measure end-to-end GB/s at 6 pipes.
- Decision rule: keep NT stores only on an end-to-end win at full pipe count; a producer-side counter win with end-to-end loss confirms Drepper's consumer-reload caveat — revert and record the negative result. Low or absent RFO traffic even in (b) → SpecI2M elision is active on this CPU; mark the whole claim obsolete for this hardware.
- Effort: 1 day (staging-buffer engineering is real work); ranked last because the expected outcome is a null or a regression, and it is gated on E1.

Excluded as untestable/irrelevant on available hardware: NUMA placement (single-node VPS — verify once with `lscpu`), IMC/uncore bandwidth counters (virtualized PMU), MSR prefetcher toggles, DDIO/DCA, SW prefetch of linear streams (pre-falsified by the paper itself; only revisit if E1 shows demand-miss stalls), physical-core pinning semantics inside the VPS (vCPU pinning does not control placement).
## Status: the gate experiment (E1) is blocked by cloud PMUs

E1 (top-down bound classification) was attempted three times on 2026-08-01:

- OVH VPS: hypervisor exposes NO hardware counters (`cycles` = <not supported>).
- GCE c3-standard-8: same — PMU requires the `--performance-monitoring-unit`
  flag, which the API rejects for c3-standard-8 AND c3-standard-22
  ("not supported for <shape> on API version v1"); it appears to need much
  larger / metal shapes.

Per house rules (no optimization ships without a measurement that justifies
it), the paper's remaining micro-optimizations (E3 c2c, E4 THP, E6 NT stores)
stay on the backlog until we have PMU access — options: a C3 metal instance,
or bare metal from a provider that exposes PMCs (e.g. Hetzner AX — which
dovetails with the vendor-outreach thread).

What the read DID settle without counters:

- Mechanistic story for measured wins: thin 2 MiB chunks ↔ L2 residency;
  buffer recycling ↔ warm lines + warm TLB + no page-zeroing; mimalloc's two
  rejections are consistent with a well-behaved linear workload.
- Expectation setting: remaining upside from this class of optimization is
  ~5-15% on transcode-bound (small-box) tiers only, ~0% on rigs where the
  databases are the wall. The big Drepper-flavored win was already shipped as
  auto-thin pipes in v0.15.0.
