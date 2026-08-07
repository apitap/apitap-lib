# Round 3 — mysql→ch & postgres→ch, flamegraph-driven (2026-08-07)

Branch `ch-ingest-r3`, on top of 0.28.0. Method unchanged: fresh four-lane
profile first (Z8, perf+symfs+cgroup cpu.stat), an eight-angle internet sweep
plus the full Rust toolbox judged adversarially against it, zero-build probes
before any Rust, then knives one at a time — each measured INCREMENTALLY over
the previous knife's wheel, plain-release vs plain-release, rig reset per
side, exact-comparator verification every time.

## What shipped (five knives)

| knife | cdc-pg | cdc-my | note |
|---|---|---|---|
| escape: SWAR run-copy + OID-gated skip | **−9.1%** (3/3) | −7% | copy_escape was 12.0% of samples; safe Rust, differential oracle test |
| digit tables replace core::fmt (mybinlog) | — | **−16%** (3/3) | fmt was ~11% inclusive; landed above prediction |
| owned-Bytes binlog events, uninit read | — | −3.1% (3/3) | kills copy-per-event + memset; read_buf-with-limit idiom |
| collapse: one entry() per event + foldhash | −5% (2/3) | **−15%** (2/2) | dead key clone gone; unreachable!() arms gone structurally |
| current_thread runtime for log_based | flat (−1.9%) | flat | **memory knife**: peak RSS −10MB; the atomic ping-pong hypothesis was WRONG (tokio only ever wakes one worker) — kept under the memory-wins rule |

Cumulative, session-local measurements at 0.5cpu/256MB:

- **cdc-my: 11.9s → ~7.4s for 1M changes (~135K changes/s, +40%)**
- **cdc-pg: ~47s → ~42s for 1.5M wide changes (~36K changes/s, +11%)**
- bulk lanes untouched this round (already 88-89% saturated; profiles thin)

PGO (~12% historically) applies on top at release time and is not in these
numbers.

## Probes that paid for themselves (zero Rust)

- tcmalloc/jemalloc LD_PRELOAD ceiling on cdc-my: **−25%** → confirmed the
  allocation diagnosis before the arena work.
- Schema A/B (DECIMAL/DATETIME → BIGINT): temporal+decimal cell cost
  **~2.3-2.7s of an ~11.8s lane** — the ceiling the digit-table knife mined.
- strace census on cdc-pg: 158K futex + 97K epoll_wait + 71K recvfrom per
  drain; writev only 972 (bodies already coalesced).
- Wide-parts receipt: destination parts are already overwhelmingly Wide
  (94MB wide vs 3.4MB compact per table) — closed that family with physics,
  not just arithmetic.

## Dead or parked this round, with receipts

- async_insert (single-writer shape; dedup off by default), max_insert_threads
  (INSERT SELECT only), optimize_on_insert (plain MergeTree no-op).
- Compressed insert bodies: **+2.3s SLOWER** on loopback — lz4 spends the
  scarcest resource (client CPU). Documented for WAN users only.
- FORMAT Native for bulk: capped win ≈ 0 (client-bound); parked as an
  uncapped-headline candidate.
- wide-parts table settings (physics receipt above), before-image skip (the
  rig has no update-heavy my workload), collapser reuse (<1%),
  status-update cadence (~0.0007%).
- Single-thread-as-speed-knife: measured flat; survives only as memory win.
- **fat LTO: UNRESOLVABLE on the drifted box** (pg within-round median −1.1s
  vs absolute median +0.6s, one +5.7s round against an outlier-fast base;
  my flat). Reverted to thin; re-test on a fresh box. Receipt in Cargo.toml.

## The 25-second target (user), honestly

Post-knives cdc-pg sits ~42s; the wall decomposes as ~20s client CPU (forced
wall ≈ CPU/0.5 ≈ 40s) overlapping a ~17s server tail (INSERT ~9s, DELETE
~8.3s). To reach 25s the client CPU must drop to ~12s AND the server tail
must stay hidden. PGO takes the projection to ~34-37s. The remaining ~10-12s
has no sized knife yet: the next flamegraph iteration on a FRESH box (this
one drifted 40.4-47.7s on identical work by night's end) has to find it, and
the leading unexplored candidates are the pump/decode task boundary
(futex census receipt), Reader::tuple + drain closure (~13% combined), and
the exposed server tail once client CPU shrinks. If the lane plateaus above
25s, that gets said with this decomposition, not fudged.

## Box-drift lesson (again)

Interleaved same-round comparisons stayed valid all night; absolute numbers
did not (identical work: 40.4→47.7s across hours). Every number above is a
within-round or same-block comparison. The bench harness needs a per-round
full reset baked in before the next campaign.
