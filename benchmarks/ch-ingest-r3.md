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

## Addendum — the 25s hunt, fresh-cage evidence (2026-08-07 morning)

Fresh cage (all three bench containers restarted, binlogs purged): 0.29.0 PGO
baseline 42.3/44.2/44.1 (median 44.1s, ~20.2 CPU-s, 93% saturated).

The current_thread knife erased the futex pool entirely (158K calls → zero);
the syscall census is now recvfrom 168K + epoll_wait 176K (~1 wall-s total).
Fresh flat profile: bytes refcount 7.5% (down from 17.3% — uncontended now),
Reader::tuple+drain 5.2%, key_of_row 2.0%, copy_escape 1.85% (was 12.0%),
pump+read_frame 2.2%. Server r3: INSERT 8.17s + DELETE 6.76s, hidden.

**The honest plateau: ~32-33s** for this 15-column workload — that is the
client-CPU-forced wall if every visible our-code symbol were removed
perfectly. 25s = 60K wide changes/s on half a core; nothing we have measured
does that on a full core.

**Next knife, specified (build in a fresh session):** frame-native rows.
`Tuple` becomes `{ frame: Bytes, cells: Vec<CellR> }`,
`CellR = Null | Range(u32,u32) | UnchangedToast` (Copy, 12B vs 40B):
- kills the per-cell slice_ref/clone/drop entirely (7.5% + the 1.5M
  KIND_VEC→Arc promotion allocs hiding in malloc);
- Reader::tuple builds ranges, not Bytes (its 2.7% shrinks with it);
- the MySQL lane gets its row-arena for free: decode_cell renders into ONE
  per-row buffer that becomes the frame (15 allocs/row → 1);
- Key stays Vec<Vec<u8>> (the retention rule from 0.28.0 holds).
Predicted: cdc-pg 44 → ~39s, cdc-my 7.6 → ~6s. Abort under 2s on pg.
Touches: pgoutput, mybinlog, collapse, drain, rowtext, dest_ch/pg/my/ice,
mysource (~300-400 lines). After it lands, the road below ~35s is
architectural (narrower rows already do 132K/s; more CPU; or the parked
CH≥25.7 patch-parts) — a product decision, not a knife.

## Addendum 2 — the 150K/s directive and the trade arcs (2026-08-07)

User target restated: 150K changes/s on the WIDE 15-col pg rig at 0.5cpu/256MB
= 1.5M in 10s. Measured walls against it: client 18.5 CPU-s forces ~37s;
server apply sums ~15s. Ordinary knives are exhausted (frame-native measured
FLAT on wall PGO-vs-PGO — it does what PGO does, not additive; kept as a
memory knife, peak now 81-89MB, −15%).

Arc A — patch-part deletes, UNBLOCKED: CH 25.8.29 stood up as
apitap-bench-ch3 (:8126); the #87265 probe with OUR predicate shape
(DELETE ... IN (SELECT) under lightweight_delete_mode='lightweight_update',
block-number/offset columns on) returns CORRECT results (99,500 / 0).
Next: the ~10-line dest_ch gate (server-version probe + provenance check +
SETTINGS clause), then a full A/B against the 25.8 destination. Expected:
server DELETE 6.8s -> ~0.5s; version-gated (24.8 LTS unchanged).

Arc B — pull-apply design (kills the render+outbound half of the client
wall): decode WAL for keys/windows, let ClickHouse pull row bodies itself.
Changes window semantics (converges to final state; not per-window image
replay) — a DESIGN decision needing its own correctness argument before any
code. Sketch only; do not build without sign-off.

Honest projection if Arc A lands: ~36-38s -> ~31-33s. 150K/s wide remains
outside both arcs combined; the receipts will say where it stops.

## Addendum 3 — patch-part deletes (Arc A): verdict

A/B on CH 25.8.29 dest (:8126), same wheel both sides, lever = `APITAP_PATCH_DELETE=0`,
3 interleaved rounds, full reset+reseed per leg, cdc-pg wide 1.5M:

| side | r1 | r2 | r3 | median |
|---|---|---|---|---|
| rewrite (mode off) | 42.5s | 41.3s | 39.7s | 41.3s |
| patch (lightweight_update) | 40.1s | 40.7s | 42.0s | 40.7s |

**Wall: TIE** (−0.6s median, under the 2% lane wall). Client is 95–97% quota-saturated
on both sides — DELETE wait was never on the critical path at 0.5cpu.

**Server: CONFIRMED WIN.** query_log split: delete avg 89.9ms → 16ms (−82%), ~9.0s →
~1.3s per round server-side. MutatePart: all 1020 from the rewrite side; patch side
rewrote ZERO parts — merge debt and dest disk churn eliminated. Verification exact
(t01/t05/t10 count+sum(id) MATCH).

**Decision: gate stays** (committed). Free on <25.7, strictly reduces destination load
on ≥25.7; pays wall only when the dest is the bottleneck (weak CH, bigger delete share,
shared box). Not a 25s card at 0.5cpu — the wall is client CPU. Remaining 25s/150K
paths: Arc B (pull-apply, needs sign-off), or accept the recorded plateau.

## Addendum 4 — pull-apply (Arc B): REJECTED, with receipt

Built behind env gates (`APITAP_PULL_APPLY=1` + `APITAP_PULL_SRC`), full A/B on CH
25.8 dest, same wheel, 3 interleaved rounds, cdc-pg wide 1.5M:

| side | r1 | r2 | r3 | median | client cpu |
|---|---|---|---|---|---|
| render (engine path) | 40.6s | 40.4s | 40.4s | 40.4s | 19.5s |
| pull (CH pulls from pg) | 45.2s | 44.8s | 43.6s | 44.8s | ~8.0s |

Fidelity was EXACT (count+sum 3 tables MATCH; full-row 15-col EXCEPT both
directions vs live pg = 0|0). Client CPU −59%. Wall +11%: the 256MB cage yields
~14.5MB windows → ~86 windows, and per-window fixed round trips ate the win.
A keys-only collapse (rows 830B → 68B, ~7 windows, projected ~13-16s) was coded
and NOT shipped.

**Rejected on identity, per user directive: apitap must BE the engine.** In pull
mode the row bodies move pg→CH between uncaged servers; the 0.5cpu/256MB headline
would no longer describe the thing doing the work. Numbers additionally negative
at step 1. Code reverted (never committed); receipts stay here.

Standing physics from this A/B: wall 40.4s at avg 0.484 cores = the wall IS
client CPU (19.5s / 0.5 core ≈ 39s). The 25s target = client cpu ≤12.5s (−36%);
150K/s wide = ≤5.0s (−74%), with us as the engine.
