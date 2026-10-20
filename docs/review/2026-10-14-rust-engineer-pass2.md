# apitap 0.61.0 — rust-engineer review, pass 2 (companion)

Companion to `docs/review/2026-10-14-handoff-review-rust.md` (the handoff queue).
Same commit (`main` at `597f8e0`, workspace `0.61.0`), same day. This pass ran the
rust-engineer skill workflow: architecture read of the core (`lib.rs`, `error.rs`,
`pipe.rs`, `source/mod.rs`, `sink/mod.rs`, `shutdown.rs`, `pipeline/`), parallel
agent passes over `sink/`, `logbased/`, `wire/`, `source/`, `py-apitap/`, then
validation on the bench VPS. Findings already in the handoff queue are
**referenced, not repeated** — everything below is either NEW or corroborating
evidence. Labels: **VERIFIED** (integrator re-read the cited lines),
**AGENT** (one reviewer traced it — re-read before fixing), **SYSTEMIC**.

---

## 0. Validation results (VPS, corroborates handoff §6)

| Check | Result |
|---|---|
| `cargo build` (Linux, dev) | ok, 1m35s |
| `cargo test` | **443 passed, 0 failed, 3 ignored** |
| `cargo clippy --all-targets --all-features` | apitap-core lib 56 + lib-test 102 (51 dup) / apitap-python 5 / examples 2. Real ones: dead `VerifyCa` variant (`mywire.rs:70`), `PgRead` visibility (`walsender.rs:207`), **unreachable pattern `py-apitap/src/lib.rs:99`**, **unnecessary `unsafe` `py-apitap/src/lib.rs:368`**, never-read `t_trunc`/`t_clear`/`t_ins` (`dest_ch.rs:597-599`), unused imports. Rest cosmetic (38 doc-indent, lifetimes, `div_ceil`). |
| `cargo fmt --all --check` | 1209 hunks / 63 files — identical to handoff §6. |
| macOS build | **E0425 `libc::TCP_KEEPIDLE` at `mywire.rs:480`** — Linux-only constant, not cfg-gated. Not a production defect (handoff rule 1 already forbids MacBook builds), but a `#[cfg(target_os)]` + `TCP_KEEPALIVE` fallback costs 4 lines and unblocks laptop dev. |

---

## 1. NEW findings — not in the handoff queue

### 1a. Correctness / silent degradation

**T1 · `logbased/mysource.rs:640` — panic on short rows-event body · VERIFIED**
`b[..6].copy_from_slice(&body[..6])` — `body` is the raw remainder of a network
binlog event; `split_event` only guarantees `HEADER_LEN`. Truncated/mangled rows
event = process panic. The only unguarded raw slice in this decode lane.
Fix: length check (or route through the bounds-checked `R`). Unit test with a
crafted short body.

**T2 · `logbased/mysource.rs:928` — `rest[..5]` cuts a multi-byte char · VERIFIED**
`truncate_target` slices `rest[..5]` after a length check but no char-boundary
check: `TRUNCATE 表名` panics. This exact bug class was already fixed in
`starts_with_word` (`:953-959`) and documented by the test at `:1048-1050` — this
site was missed. Fix: `rest.get(..5)` / `is_char_boundary` guard, same as the twin.

**T3 · `sink/iceberg.rs:926-931` — footer `flen` slice underflow · VERIFIED**
`tail[tail.len() - 8 - flen as usize .. tail.len() - 8]` — `flen` is read from the
remote object's footer, `size` from the (remote) manifest. A truncated object or a
lying `file_size_in_bytes` underflows the index → slice panic. Also
`size - flen - 8` at `:929` can underflow when the footer claims to extend past
EOF. Fix: validate `flen + 8 <= tail.len()` and `flen + 8 <= size` before slicing;
error loudly otherwise. Pairs with handoff P1-11 (same file, different site).

**T4 · `wire/pgmytsv.rs:364-384` — negative wire `ndigits`/`weight` → release panic · VERIFIED**
`i16::from_be_bytes(..) as usize` for `ndigits` (−1 → huge) wraps the length guard
at `:368`; `weight as usize` negative makes `0..=(weight as usize)` at `:384` slice
`f[8 + 2·gi..]` past the buffer → **panics in release**, per NUMERIC cell on the
pg→MySQL lane. Fix: read both as `i32`, reject negatives (the safe reference
pattern is `pgbindec.rs:201-207`). Add to `torture.rs`.

**T5 · `wire/pgcopy.rs:417,428` — negative `ndigits` → silent wrong value in release · VERIFIED**
Distinct from handoff P0-6 (that is the `10i128.pow`): here `(ndigits as usize)*2`
with `ndigits = −1` wraps the guard, the digit loop is skipped, and the general
numeric decoder returns `Ok((0, dscale))` — a corrupt NUMERIC decodes to a WRONG
value with no error, feeding the RowBinary/Arrow/parquet lanes. Fix: `ndigits >= 0`
check before the cast (fast path at `:374-409` is fine).

**T6 · `sink/clickhouse.rs:1601-1611` — unparseable count → staging dropped, run "succeeds" · AGENT**
`rows_staged` parses `SELECT count()` with `.parse().unwrap_or(0)`. A non-numeric
body (proxy garbage, truncated response) reads as 0 → the 0-row guard drops
staging and reports a successful 0-row run after streaming everything. The
sibling `dest_state` parses the same class of query with `map_err` (`:1657`). Fix:
`map_err`, match the sibling. **Severity: silent data non-publication — treat as
P1 in the handoff's terms.**

**T7 · `sink/bigquery.rs:243-257` — failed body read on HTTP 200 becomes `Ok(Value::Null)` · AGENT**
`resp.text().await.unwrap_or_default()` then empty → `Ok(Null)`. Via `table_get`
→ `cdc_state_table_exists` (`:616`) answers "exists", `ensure_state_table`
(`:2013`) skips creation, `ensure_dataset` (`:350`) says "exists" — wrong
existence/state decisions, no error. Fix: distinguish body-read failure from an
honestly empty body; propagate the read error.

**T8 · `sink/mysql.rs:1199-1208` (+`:1089,1164,1184,1288`) — COUNT parse failure disables the fan-in guard · AGENT**
`.parse::<u64>().ok().unwrap_or(0) > 0` — unparseable → `siblings = false` → the
anti-silent-skip refusal (comment at `:1153-1156`) is bypassed. All sibling sinks
parse with `map_err`. Fix: match them.

**T9 · `source/postgres.rs:209-220` — PK probe error swallowed · AGENT**
`fetch_all(..).unwrap_or_default()` on the `pg_index` probe: a transient failure
silently becomes "no PK / no int cursor", changing span strategy and merge-key
eligibility with no error. Fix: `?` (or at minimum a debug log under
`APITAP_DEBUG`, like `source/mysql.rs:1159`).

**T10 · `source/mysql.rs:691-713` — `write_datetime` misses the length check its twin has · VERIFIED (inconsistency)**
Twin `bin_datetime_micros` (`:373`) validates `b.len() < 1 + b[0]`; `write_datetime`
reads `b[5..8]`/`b[8..12]` guarded only by `len >= 7/11` vs the CLAIMED `b[0]`, not
the actual buffer. Callers currently guarantee the slice — one refactor away from
a release panic on truncated wire bytes. Fix: same check as the twin.

**T11 · `logbased/drain.rs:494-497, 522-525, 549-552, 569-574` — `.expect("stream open")` on a protocol deviation · AGENT**
`StreamAbort` (`:440-455`) removes `sess.streams[xid]` without clearing
`in_stream`; a whole-tx ABORT inside an open block makes the next row event panic.
Current PG emits ABORT between blocks, so this needs a deviation — but the lane
already defends both abort shapes elsewhere, and `.ok_or_else` is free. Fix:
replace the four expects with a loud protocol error.

**T12 · `logbased/drain.rs:239 + 261` — mid-block deadline break orphans `in_stream` · AGENT**
The window deadline can fire while `in_stream == Some(xid)`; follow mode
(`run.rs:1782-1795`) keeps the session mid-block, and the next window decodes
in-stream DML (which carries a leading xid byte, `pgoutput.rs:396`) as a
non-stream message → spurious "protocol desync" death. No data loss (stored
watermark is behind), but the run dies and slot confirmations stall. Fix: on the
deadline break inside a block, end the session (or resync), don't reuse it.

**T13 · `logbased/dest_ch.rs:1217-1229` — `engine_of` swallows IO errors as "no engine" · AGENT**
(= handoff P2-11, confirming) every transport error reads as classic MergeTree —
silent strategy downgrade and a missed Replicated refusal. Fix: log + treat as
unknown, refuse the ambiguous case.

**T14 · `logbased/dest_ch.rs:597-599` — clippy: timers assigned then overwritten** (also in
handoff §6 measured facts — confirming). Trivial cleanup.

**T15 · `drain.rs:83,96` — `rel_oids`/`rels` never pruned · AGENT**
Keyed by relation oid; a long follow session with table drop/create churn grows
them monotonically. Small entries; LOW. Fix: prune on Relation close, or cap.

**T16 · pagination loops trust the server forever · AGENT**
`bigquery.rs:321-342`, `gcs.rs:345-372`, `s3.rs:509-529` — a misbehaving endpoint
that always mints a `nextPageToken`/continuation-token loops and accumulates
unboundedly. Fix: iteration cap (e.g. 10k pages) or total-count cap.

**T17 · `wire/mybinlog.rs:215` + `source/mysql.rs:1579` — unchecked `pos + n` in `take` · AGENT**
Handoff P1-9 cites mybinlog's `R` as the *checked* reference; in fact `R::take`
uses `self.b.get(self.p..self.p + n)` — a huge lenenc `n` overflow-panics under
overflow-checks (release: wraps, `get` → clean error). Lesser than the pgmytsv
items but the same one-line fix as `pgoutput`: `checked_add`.

**T18 · `py-apitap/src/lib.rs:99` unreachable pattern, `:368` unnecessary `unsafe`, `:359/:382` two
`unsafe` blocks without SAFETY comments (house rule, `lib.rs:6-8`) · VERIFIED via clippy.**
Trivial; bundle with the clippy cleanup commit.

**T19 · SYSTEMIC: no total deadline on the control plane — broader than handoff P2-13.**
P2-13 covers sqlx lanes. Additionally unbounded: TOAST refetch `resolve.rs:250-263`,
Iceberg catalog REST `iceberg.rs:261-337`, CH `insert_stream_summary`
`clickhouse.rs:674-690`, `walsender.rs:866-964` `connect_with` (the whole
connect→TLS→auth chain; `MyWire::connect` has 10s — the walsender does not), and
the control queries `drain.rs:310`, `run.rs:1785`, `myrun.rs:289/317`. Net: dead
peers are detected everywhere (TCP keepalive), a **live-but-stalled server is
detected nowhere** — a stalling source parks apply and slot confirmations forever
while keepalives keep the connection nominally alive. Fix:
`tokio::time::timeout` on short control-plane calls only (the `http.rs:1-32`
argument against total read timeouts applies to the data streams, not these).

### 1b. Performance — NEW items (handoff §5 already queues the rest)

| # | Label | Where | Item |
|---|---|---|---|
| P-a | **VERIFIED** | `logbased/collapse.rs:57-61` | `v.to_string()` **per event** on the single-int-key fast path (the hottest loop in the repo) just to guard the round trip. `itoa::Buffer` on the stack removes a malloc per event. |
| P-b | AGENT | `logbased/changelog.rs:211-223` | One TOAST-bearing event copies EVERY cell of EVERY event in the window (`Vec::with_capacity` + `Bytes::copy_from_slice` per cell); only the carry needs owned values — feed non-masked rows from views (`frame.slice()` is a refcount bump). |
| P-c | AGENT | `logbased/resolve.rs:120-121,163` | Per upsert: `to_cells()` materializes `Vec<Cell>` AND the `Key` is deep-cloned for the index. Paid by the BQ/Iceberg apply paths every window. |
| P-d | AGENT | `source/gsheets.rs:130-137`, `source/github_api.rs:925-926` | Whole-page (and in gsheets, per-row) deep `Value` clones before read-only iteration. Borrow `&Value`. |
| P-e | AGENT | `sink/bigquery.rs:1688-1689` | `jobs.remove(0)` loop — O(files²) shifting in `await_jobs`. `VecDeque`. |
| P-f | AGENT | `logbased/dest_pg.rs:210-211,221` (+`dest_ch.rs:652/708/716`, `dest_my.rs:159-163`, `rowtext.rs:108`) | Per-key `Vec<&[u8]>` allocation in the COPY/delete loops — hoist outside the loop. |
| P-g | AGENT | `source/clickhouse.rs:622-627` | Every RowBinary byte memcpy'd twice (+drain shift) for row alignment; `split_at` handoff halves it. |
| P-h | AGENT | `logbased/dest_ch.rs:568` | `seq.to_string()` per appended changelog event — write into `buf` (itoa/`write!`); stamp/`at` are already hoisted. |
| P-i | AGENT | `logbased/dest_ch.rs:617-626,1428-1432` | `types()` clones the whole column→type HashMap per window, twice, on the binary path. Cache the `Arc`. |
| P-j | AGENT | `wire/pgbindec.rs:122-141` | 1-3 heap `String`s per float4/float8 cell to normalize ryu output; all branches could work on `&str`. |
| P-k | AGENT | `logbased/mysource.rs:889-904` | `contains_key` + `get_mut` per buffered op (entry API does one walk); same shape for `collapsers`. |

Corroborated in this pass (already queued in handoff §5, not repeated): upload
body clones (§5.1), mybinlog per-cell/per-event allocs (§5.4), BQ NDJSON per-cell
Map+key clones (§5.7), UUID `format!` ×16 (§5.7), `MyWire::read_packet` zero-fill
resize (§5.7).

---

## 2. Second opinion on the handoff scorecard (§6)

Agreed, with colour:

- **Docs/tests are the strongest parts of this codebase** — trait contracts in
  `sink/mod.rs`/`source/mod.rs`, `shutdown.rs`'s signal-handling rationale, and
  the RED-mutation test comments are exemplary; 443 green tests on Linux.
  py-apitap releases the GIL on every long entry point (`detach` at `lib.rs:201,
  279, 418, 438`) — one caveat: `capsule.rs:370-377` `stream_get_next` does a
  blocking pull inside the C callback with no GIL-release guard; consumer-dependent
  (fine for polars' Rust path, a stall if a consumer calls it holding the GIL).
- **Style B−** confirmed by the numbers above; the fastest path to "standard" is
  one `cargo fmt` commit + CI gate + clippy-to-zero, exactly as handoff §6.1-2
  orders it.
- **Robustness**: the failure-mode design (latch cancellation in `pipe.rs`,
  watermark-after-data, discard/release_lock per run token) is genuinely good —
  the residual risk concentrates in the older numeric-header decoders and the
  silent-`unwrap_or_*` decision sites listed in §1a here.

---

## 3. Production readiness — second opinion

Handoff verdict: *"Not production-ready for every lane yet."* This pass
**agrees**, and adds:

1. The safety NET is strong — atomic swap, 0-row guard, watermark-after-data,
   idempotent replay mean every failure mode found in either review fails
   CLOSED (crash/refuse/replay), not corrupting. That is the right foundation.
2. The blockers are the handoff P0s (CH identifier injection REPRO, two
   default-TLS credential paths, three silent data-loss bugs, parallel-apply
   W1) plus this pass's T5 (silent wrong-value NUMERIC) and T6 (silent 0-row
   "success"). None of these are hypothetical in a long-running CDC tool.
3. No CI gates at all (no cargo test/clippy/fmt) — process risk more than code
   risk, and the cheapest to fix.

Practical answer for the owner: **production-viable today only for the
well-trodden lanes (bulk pg/my→pg/ch, default CDC replica) on networks you
control, with the P0s fixed before any lane that touches untrusted schema names
(P0-1), the public internet (P0-3/4), or CH insert-only / parallel-apply /
Iceberg (P0-2/5/7).** Treat the P0 queue as the release gate; everything in §1
here slots in behind it (T1-T5 with the P0 panic batch, T6-T8 with the
silent-degradation batch, T19 with P2-13).

---

## 4. Suggested integration into the handoff queue

- Add T1, T2, T3, T4, T5 to the handoff's commit-order step 1 (small, local,
  unit-testable panics) — T4/T5 want `torture.rs` entries.
- Add T6, T7, T8, T9, T13 as a new P1 batch "silent degradation on decision
  paths" (all `unwrap_or_*` → `map_err`, one mechanical pass).
- Add T11, T12 to the drain/protocol P2 batch; T19 widens P2-13's scope.
- Perf §1b slots into handoff §5 alongside its items (P-a first — hottest loop).
- T18 rides the clippy-cleanup commit (handoff §6.2).
