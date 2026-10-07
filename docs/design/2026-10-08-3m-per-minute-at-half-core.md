# Design: 3M changes/min at 0.5 core / 256 MB — executing wave 0.61

Status: execution plan for `docs/review/2026-10-07-system-review-3jt-per-menit.md` §5.
Target: **50,000 changes/s (3M/min)** pg→ClickHouse, both *catch-up* and *keep-up*,
inside the **0.5 CPU / 256 MB** cage, checksum 30/30, and **scaling up when
resources scale up**.

## 0. Where we stand (numbers, not vibes)

| metric | measured | source |
|---|---|---|
| CPU per change (6.45M-change census) | **57 µs/change** | 0.58 report §5 |
| 30-table group, catch-up @0.5 CPU | **31,679 ch/s = 1.90M/min** | `cdc-steady-30t-0.58.md:376` |
| 30-table group, paced/keep-up | ~**1.50M/min** | `:398` |
| single-table 0.59 (release wheel) | 2.19M/min writer kept up with @0.40 core, 101 MB | leg b59-single2 |

Target math: 50,000/s × 0.5 core = **10 µs/change** total budget. From 57 µs that is
**5.7×**; the review's lever path (L1a…L6, §2 below) lands at 7.3–13.3 µs =
42–65k/s = **2.5–3.9M/min** — enough with margin.

## 1. Hot-path map (real code, one change)

```
socket ──read_frame (walsender.rs:163)──> pump_frames (:98) ──mpsc+permit(G0.2)──>
drain loop (drain.rs:166) ──pgoutput::decode (pgoutput.rs:381)──> tx_buf/streams ──>
Collapser (collapse.rs) | Changes (changelog.rs) ──seal (window.rs)──>
apply lanes (dest_ch.rs) ──RowBinary──> HTTP ──> CH
```

0.58 profile (fully grouped, review §5):

| component | share | note |
|---|---|---|
| kernel: loopback network + memcg accounting + copy | ±29% | the peer's ACK processing is billed to our quota |
| libc malloc/calloc/free | ±10.6% | 6 alloc/free pairs per change |
| pgoutput framing + decode | 6.2% | `pump_frames`, `read_frame`, `decode` |
| tokio scheduler/timer + mpsc | ±6.0% | 1 mpsc send/recv per change; 0.33 recvfrom + 0.36 epoll/change at keep-up |
| collapse + hashing | 4.8% | 4 SipHash lookups |
| `Bytes` refcount | 2.1% | `slice(25..)` |
| render + HTTP to CH (client side) | 0.7% | client apply is cheap; the expensive part is the STATEMENT in CH (below) |

Six alloc/free pairs per change (review §5, checked):
1. frame body via `BytesMut::zeroed` (calloc + zero-fill + memcpy) `walsender.rs:158`;
2. refcount box from `slice(25..)` `:1381`;
3. cell `Vec` `pgoutput.rs:288`;
4–5. key `Vec<Vec<u8>>` (two allocations) `collapse.rs:29,172`;
6. `Vec<&[u8]>` in key-table render `dest_ch.rs:611`.

Invisible-in-client-profile CH cost: **DELETE p50 61 ms (24.8) / 418 ms across 8
lanes** plus the key table — the most expensive statements; `changelog=True`
(insert-only) lifted MySQL to **113.8k/s MEASURED**. That is the key fact for the
30-table group.

## 2. Budget and levers (review §5.3, kept as-is)

| lever | what | saving | confidence |
|---|---|---|---|
| L1a | `read_buf` into `BytesMut::with_capacity` (no zero-fill), `advance(25)` instead of `slice`, payload *moved* into the Tuple | 0.3–0.6 µs | medium-high |
| L1b | **drop the pump task on current_thread**: the drain waits for one refill then scans every complete frame synchronously from its own window; the tested `co_win`/`co_refill` COPY-plane code (`walsender.rs:1106-1233`) is the blueprint; also closes G0.2 | 0.8–1.4 µs | medium-high |
| L2 | read coalescing: refill <32–64 KB → wait ±1 ms (one timer per refill); alternative `SO_RCVLOWAT`=64 KB + 2–5 ms timer | 2–4 µs at keep-up | medium |
| L3 | flat allocation-free collapse key: ±24 B inline, length-prefixed segments; `u64` fast path for single-int keys | 0.6–1.2 µs | medium-high |
| L3b | **range-key (fully zero-copy)**: key = frame handle + segment ranges, hashed/compared over the ranges; hybrid selection rule below | 0.2–0.4 µs over L3 on wide keys; zero copy, zero alloc | medium |
| L4 | dense table index instead of `HashMap<String, Collapser>` + SipHash; last-relid cache; `Instant::now()` per 256 events | 0.15–0.3 µs | high |
| L5 | recycle per-window containers (maps, Vecs, 1+4 MiB render buffers) through a bounded return channel | 0.1–0.3 µs | medium |
| L6 | per-window arena for cell `Vec`s; key render without `Vec<&[u8]>` | 0.15–0.3 µs | medium |

Projection: without L2 **11.3–13.3 µs** (35.7–42k/s); with L2 **7.3–11.3 µs**
(42–65k/s). 50k/s sits inside the range — provided L2 also lands at catch-up;
**measure §5.7 first**: catch-up recv-size decides whether L2 pays there.

## 3. Low-level design per component

### 3.1 Read path — no task, one buffer owned by the window
- **current_thread** (already chosen up to ±2 cores, G1.5). At 0.5 core a pump
  task is pure wakeup+channel cost; its premise ("syscall on our own core") is
  impossible inside this quota.
- The read buffer is **owned by the window** and recycled through a bounded
  return channel (L5): a fixed `Vec<BytesMut>` (e.g. 4×256 KB); `read_buf`/
  `advance` replace `zeroed` + `slice`.
- **L2 coalescing**: after a refill yields <64 KB, wait on readiness + a 1 ms
  timer; cancelling a readiness wait is safe (we never cancel a read). Raise
  `SO_RCVLOWAT` to 64 KB via `socket2` (already a dependency since G0.5).
- Frames are scanned synchronously: 5 B header → `advance`; payload `Bytes`
  *moved* (no `slice_ref`, no refcount promotion for REPLICA IDENTITY DEFAULT —
  `Cellv` already stores ranges into the frame; keep it that way).

### 3.2 pgoutput decode
- Keep the existing **zero-copy range** path (`CellR`/`Cellv`).
- Cell `Vec` → `SmallVec<[Cell; 16]>` or a per-window arena (L6); 15 columns =
  zero allocations.
- `cstr()` allocates a `String` per identifier — move to slices interned per
  `Relation` (registry is per-session; once per DDL is enough).
- `rel_oids` lookup only when a binary cell is present (L4).

### 3.3 Collapse (the core)
- **L3 — flat inline key**: one `SmallVec<[u8; 32]>` holding `u32 len | bytes`
  segments; hash over the slice; `HashMap` stays foldhash. 88 B key → 32 B, two
  allocations → zero.
- **L3b — range key (fully zero-copy)**: `KeyRange { frame: Bytes, segs:
  SmallVec<[(u32, u32); 4]> }` with a manual `Hash`/`Eq` over the ranges
  (foldhash over borrowed segments). Zero copies, zero allocations; the key
  pins its frame.
  - **Hybrid selection rule** (the tradeoff made explicit):
    `key_len ≤ 32 B` → inline flat (L3);
    else if `frame.len() ≤ 4 × key_len` → range key (L3b);
    else → flat spill (one heap buffer).
    Rationale: a range key pins the whole frame, so a 4-byte key inside a 32 KB
    frame would pin 32 KB per distinct key; the rule only chooses range keys
    when the pinning overhead is bounded relative to the key itself.
  - Correctness: frames are `Bytes` (refcounted); last-write-wins map holds
    ≤ one entry per live key; `clear()`/drop releases frames; the accounting in
    `cells_bytes` (frame-pinning) already models this residency.
  - Risk: extra indirection per hash/compare (one branch + bounds check);
    measured target is a net 0.2–0.4 µs over L3 only on wide-text keys, so L3b
    ships **behind the same RED/A-B discipline** and may be dropped if the A/B
    says otherwise.
- **Single-int fast path**: one int2/4/8 PK column → key is a `u64`; separate
  `HashMap<u64, Slot>` (the most common production shape).
- Slot: `enum Slot { Insert, Update{..}, Delete }` — preserve the existing
  last-write-wins semantics; do not touch `DeleteSet` (dedup by construction).
- `Instant::now()` per 256 events (L4); cache the last relid.

### 3.4 Window & memory (256 MB)
- Default window **32 MiB** (measured: 27,397/s @115 MB; on a busy host 32 MiB
  beats 64 MiB — G1.1), plus an adaptive controller targeting 2–5 s windows.
- 256 MB budget: window ≤32 MiB × (drain+overlap ≤2) + pump ≤32 MiB (G0.2) +
  256 MiB tx cap (G0.1, refusal) + arena ≤8 MiB + tokio/pg baseline ~10 MiB —
  under 256 with headroom; MEMPEAK is tracked by the leg (101 MB measured).
- Containers recycled across windows (L5): `HashMap::clear()` keeps capacity; a
  bounded 2-window return channel.

### 3.5 ClickHouse apply (where the expensive statements live)
- **Insert-only replica mode (optional, product decision)**: one
  `ReplacingMergeTree(version, is_deleted)` table; DELETE and the key table
  disappear — p50 61 ms/statement and 418 ms @8 lanes vanish. Readers use
  `FINAL` or an `argMax` view; document the between-DELETE/INSERT visibility
  gap. This is the realistic way a **30-table group with all tables active**
  crosses 50k/s.
- Without that mode: key-table delete stays; render keys without
  `Vec<&[u8]>` (L6), one `INSERT` per window per table (already), body cap
  (already).
- Lanes: `ch_apply_lanes` already reads the cgroup quota (0463694) — keep
  `round(16*c)` floor 8 cap 16; lanes only pay off after CPU per change drops
  (otherwise they just lock the core).

### 3.6 Pipeline & scaling (resources up → throughput up)
- **Scaling laws, formalized** (and tested by a leg):
  - lanes_apply = clamp(round(K·quota_cores), 1..16) (already);
  - window_bytes = clamp(mem_limit/8, 8..64 MiB) (G1.1);
  - `slots=N` divides **all** budgets (drain, bootstrap, lanes) — G0.8 done.
  - Outcome rule: 0.5 core → 50k/s; 1 core → ≥90k/s; 2 cores → ≥150k/s
    (the drain stays single-threaded; the gain comes from apply lanes + slots).
- **Follow mode** (G1.4): one walsender session + one tenure; removes
  3.6–5.7 s setup/teardown per pass (+6–16% keep-up). Health/metrics mandatory
  (long-running process).
- Drain/apply overlap already exists; ensure `wait_for_apply` (G0.4) adds no
  per-event timer (10 s interval, zero hot cost).

## 4. Verification (RED-first, A/B, production-ready)

- Every lever = one commit: RED spec under `~/apitap-057/redlib.py` (the pattern
  used 20+ times this session), green suite, gate leg where a seam exists.
- A/B in the 0.5/256 cage: **n≥3 interleaved rounds**, checksum 30/30, `.so`
  md5 recorded, `results.tsv`; `perf -g` for the unresolved libc symbol 0x9a72e.
- New gate legs:
  - `e2e_three_million.py`: paced 50k/s keep-up for 120 s, drain never falls
    behind, checksum exact, MEMPEAK <256 MB;
  - `e2e_scaling.py`: 0.5 vs 1 core and 256 vs 512 MB — **throughput must rise
    monotonically** (a scaling regression is a FAIL).
- Order: **measure §5.7 first** → L1a → L1b → L3 → L3b (A/B, drop if flat) →
  L4 → L5 → L6 → L2 A/B → MySQL C–G (G1.2 first) → CH insert-only.
- Definition of done for 0.61: catch-up **≤11 µs/change MEASURED**, keep-up
  50k/s paced checksum-exact at 0.5/256, both legs above green, full gate
  green, 0.61.0 released with numbers (not estimates).

## 5. Deliberately not doing

- **mimalloc/jemalloc**: rejected twice (worse RSS); L3/L5/L6 remove most of
  the reason; last priority.
- **Unix socket / transport**: not a lever on loopback (measured); L2
  coalescing attacks the per-packet-arrival cost instead.
- **PGO retrain**: done at release time, not while iterating; A/B uses one
  build for both sides.
- **Changing the Python API**: stays one line; every lever lives in Rust.
