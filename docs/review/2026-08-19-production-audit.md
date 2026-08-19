# apitap production audit — 2026-08-19

Reviewer: fresh pair of eyes, first pass over the whole tree at `v0.51.0`
(`89808f7`) plus the uncommitted working tree.

**Verification policy for this document.** Nothing below is taken from a code
comment. Every claim is either (a) reproduced on the bench VPS in
`~/apitap-qa` — a plain rsync copy of this repo, built with the native
toolchain (`cargo 1.94.1`) into `~/apitap-qa-target` — or (b) marked
explicitly as *reasoned, not yet measured*. Where a claim is reproduced, the
command and the output are quoted so the next agent can re-run it rather than
trust it.

## What the gate says today

| gate | result |
|---|---|
| `cargo test --workspace` | **pass** — 206 passed, 0 failed, 3 ignored |
| `cargo clippy --workspace --all-targets` | **FAIL** — 1 deny-level error, 73 warnings |
| `cargo audit` | **FAIL** — 1 vulnerability, 2 unsound, 1 yanked |
| CI running any of the above | **none exists** (`.github/workflows/` holds only `publish.yml`) |

The engineering in the hot paths is genuinely strong — the cgroup walk in
`pipeline/mod.rs`, the identifier vetting in `sink/bigquery.rs`, the
bounds-first staging in `wire/arrowcol.rs`, the progress module's refusal to
report a number it cannot stand behind. The findings below are not about
craft. They are about the gap between that craft and the automation that
would keep it true on a Tuesday when nobody is looking.

## Work queue

Ordered for pickup. "Evidence" says whether the finding was reproduced on the
rig or argued from the source — take the reasoned ones as leads to confirm,
not as facts to fix blind.

| # | finding | file | evidence |
|---|---|---|---|
| P0-1 | frame cap disabled by a leftover canary | `wire/walsender.rs:149` | in the diff |
| P0-2 | `pow` overflow → silent decimal corruption | `wire/pgcopy.rs:342,347` | **measured** |
| P1-1 | no CI; clippy fails | `.github/workflows/` | **measured** |
| P1-2 | MSRV declared 1.75, code needs 1.82 | `Cargo.toml`, `source/mysql.rs:799` | **measured** |
| P1-3 | panic across `extern "C"` aborts the process | `py-apitap/src/capsule.rs` | reasoned |
| P1-4 | GIL held across the read; polars loses 65% | `py-apitap/src/capsule.rs` | **measured** |
| P1-5 | Ctrl-C cannot stop a transfer; slot leaks | `py-apitap/src/lib.rs:130` | reasoned |
| P1-6 | no TCP keepalive on DB sockets | `mywire.rs:454`, `walsender.rs:629` | reasoned |
| P1-7 | Arrow UTF-8 exported unvalidated | `wire/arrowcol.rs:940` | reasoned |
| P1-8 | `bytea` → ClickHouse becomes text | `sink/clickhouse.rs:473` | **measured** |
| P1-9 | `ch_ident` misses the backslash escape | `sink/clickhouse.rs:295` | **measured** |
| P2-1 | staging table leaks on failure | `sink/postgres.rs` | reasoned |
| P2-2 | `o as u32` truncation; unbounded straddle buffer | `wire/arrowcol.rs:667,749` | reasoned |
| P2-3 | 1 vuln / 2 unsound / 1 yanked dependency | `Cargo.lock` | **measured** |
| P2-4 | parquet compiled twice | `Cargo.toml` | **measured** |
| P2-5 | decoders not fuzzed | — | — |
| P2-6 | `arrowcol` unsafe sound but unenforced | `wire/arrowcol.rs` | audited |
| P2-7 | progress state is process-global | `progress.rs:37` | reasoned |
| P2-8 | diagnostics are `eprintln!` | 30 sites | **measured** |
| P2-9 | 1000-row table opens 32 pipes | `pipeline/mod.rs:415` | **measured** |

If only three things get done: **P0-2** (it corrupts money columns silently),
**P1-1** (nothing else stays fixed without it), **P1-8** (it is wrong today, on
a route people use).

---

## P0 — must fix before the next release

### P0-1 · A disabled safety cap is sitting in the working tree

`crates/apitap-core/src/wire/walsender.rs:149`

```rust
-    if len > MAX_FRAME {
+    if false { // CANARY
```

The 1 GB Postgres protocol ceiling is switched off. `MAX_FRAME` is the only
thing standing between a wire-supplied `u32` length and
`BytesMut::zeroed(len)`, which touches every page it reserves. This is
uncommitted, so it has not shipped — but it is one `git commit -a` from
shipping, and the diff that carries it is the same diff that adds the test
meant to prove the cap works.

**Do:** restore the comparison, then re-run
`cargo test -p apitap-core walsender_frames_survive_hostile_lengths` and
confirm it still passes with the guard *on* (the harness in `torture.rs` was
authored against the canary, so it must be re-validated in the other
direction).

**Do also:** the canary is a good technique — mutation testing by hand. It
should not be able to escape into a commit. Add a `pre-commit`/CI grep for
`CANARY`, `if false`, `return Ok(()); //` and friends.

---

### P0-2 · `10i128.pow()` on a wire-derived shift — panics in debug, corrupts in release

`crates/apitap-core/src/wire/pgcopy.rs:342` and `:347`

```rust
let diff = scale as i32 - dscale;          // scale = declared, dscale = OFF THE WIRE
Ordering::Greater => acc.checked_mul(10i128.pow(diff as u32))   // pow is unchecked
Ordering::Less    => acc / 10i128.pow((-diff) as u32)           // pow is unchecked
```

The `checked_mul` guards the multiply but **not the `pow` that feeds it**.
`10i128.pow(39)` already exceeds `i128::MAX`.

Reproduced on the VPS with a crafted numeric field (`ndigits=1, weight=0,
sign=+, dscale=0, digit=1`) decoded against a range of declared scales:

```
$ cargo test -p apitap-core qa_ -- --nocapture
scale=0  -> Ok(Ok(1))
scale=18 -> Ok(Ok(1000000000000000000))
scale=38 -> Ok(Ok(100000000000000000000000000000000000000))
scale=39 -> Err(Any { .. })   <- panicked: attempt to multiply with overflow
scale=40 -> Err(Any { .. })   <- panicked
scale=76 -> Err(Any { .. })   <- panicked
```

`38` is the last scale that survives.

The release profile sets no `overflow-checks`, and **release is worse than the
panic**. Same probe, `cargo test --release`:

```
scale=0  -> Ok(Ok(1))
scale=18 -> Ok(Ok(1000000000000000000))
scale=38 -> Ok(Ok(100000000000000000000000000000000000000))
scale=39 -> Ok(Ok(-20847100762815390390123822295304634368))
scale=40 -> Ok(Ok(131811359292784559562136384478721867776))
scale=76 -> Ok(Ok(158788995957577343786214718011688878080))
```

The value **1** decodes to a large **negative** number at scale 39 — wrong
magnitude, wrong sign, wrapped `pow` fed straight through `checked_mul`, and
returned as `Ok`. No panic, no error, nothing in a log. In the wheel that ships,
this is silent decimal corruption.

Reachability: `scale` comes from the column's `atttypmod`, `dscale` comes from
the peer. A well-behaved PostgreSQL sends `dscale == scale` for a constrained
`numeric`, which is why this has not bitten. Anything else speaking the
Postgres wire protocol — CockroachDB, a pooler that rewrites, a hostile peer —
is not bound by that. This crate's own stated bar is
*"survive anything a server can send"* (`wire/torture.rs`), and this does not.

**Do:** `10i128.checked_pow(k).ok_or_else(|| bad("numeric rescale out of range"))?`
on both arms, and reject `diff` outside `-38..=38` with a real error. Then add
the crafted field above to `torture.rs` so it stays fixed. The probe used for
this finding is appended to `pgcopy.rs` in `~/apitap-qa` on the VPS as
`mod qa_probe` — lift it, do not re-derive it.

---

## P1 — production correctness and availability

### P1-1 · There is no CI, and clippy currently fails

`.github/workflows/` contains exactly one file, `publish.yml`, which fires on
a `v*` tag and does two smoke checks. **Nothing builds, tests, lints or audits
this repo on a push or a PR.** 206 good tests run only when a human remembers.

Current clippy state on `main` + working tree:

```
error: this loop never actually loops
  --> crates/apitap-core/src/wire/walsender.rs:1133:9
   = note: `#[deny(clippy::never_loop)]` on by default
error: could not compile `apitap-core` (lib) due to 1 previous error; 35 warnings emitted
```

The `never_loop` itself is benign — every arm of `co_control` returns, so the
outer `loop` is dead structure, not a bug. That is exactly the problem: it is
a deny-level error nobody has seen, sitting in front of the ones that will
matter.

Warning census (73 total, deduped by kind): 4 × doc list indentation, 3 ×
too-many-arguments, 3 × `div_ceil`, 3 × items-after-test-module, 3 ×
`as_bytes` after slicing, 2 × simplifiable boolean, plus singletons including
one MSRV violation (P1-2) and several never-read fields.

**Do:** add `.github/workflows/ci.yml` running, on push and PR:
`cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`, `cargo audit --deny warnings`, and a build on the
declared MSRV toolchain. Fix `never_loop` by unwrapping the `loop` in
`co_control` (or documenting why it stays with an `#[allow]` that names the
reason).

---

### P1-2 · The declared MSRV is not true

`Cargo.toml` declares `rust-version = "1.75"`, with a comment saying it exists
so *"`cargo build` says which toolchain is too old instead of failing
somewhere in the middle of a dependency."*

```
warning: current MSRV (Minimum Supported Rust Version) is `1.75.0` but this item is stable since `1.82.0`
   --> crates/apitap-core/src/source/mysql.rs:799:18
799 |  &std::iter::repeat_n("(TABLE_SCHEMA = ? AND TABLE_NAME = ?)", pairs.len())
```

Anyone on 1.75–1.81 gets precisely the failure the declaration was written to
prevent. Nothing catches it because nothing builds on the declared floor.

**Do:** either raise `rust-version` to `1.82` (honest, and costs nothing
given the wheel is built in a pinned container), or replace `repeat_n` with
`std::iter::repeat(..).take(n)`. Then add an MSRV job to CI so the number
stays a fact.

---

### P1-3 · A panic in the decoder aborts the user's Python process

`py-apitap/src/capsule.rs` — `stream_get_next`, `stream_get_schema`,
`stream_get_last_error`, `release_schema`, `release_array`, `stream_release`.

All six are `unsafe extern "C"` and **none wraps its body in
`catch_unwind`**. `stream_get_next` calls straight into
`ReadHandle::next_batch()`, i.e. the whole decode stack. A panic there unwinds
into a C frame, which modern rustc turns into an immediate `abort()`.

The consumer's experience: `pl.read_database(...)` does not raise — the
interpreter dies with `SIGABRT`, no traceback, no `except`, no `finally`,
nothing flushed. For a library distributed on PyPI and dropped into Airflow
workers, that is the difference between a failed task and a dead worker.

P0-2 proves panics are reachable from wire data, so this is not theoretical.

**Do:** wrap every `extern "C"` body in `std::panic::catch_unwind`, map a
caught panic to `st.last_error` plus a non-zero return (the interface already
has `get_last_error` wired for exactly this), and make the release callbacks
swallow-and-continue. `grep -c catch_unwind` over the repo currently returns 0.

---

### P1-4 · The GIL is held for the whole Arrow read — the docs say otherwise

Two places state the opposite of what the code does:

- `py-apitap/src/lib.rs:2` — *"The GIL is released for the whole transfer
  (`allow_threads`)"*
- `crates/apitap-core/src/read.rs:68` — *"Blocking pull (called from an
  arbitrary consumer thread, GIL released)"*

`transfer()` and `transfer_many()` do detach (`py.detach(...)`,
`py-apitap/src/lib.rs:130`). **The read path does not.** `capsule.rs` contains
no `detach`, no `allow_threads`, no `Python` token at all — the consumer
(polars/pyarrow/duckdb) calls `stream_get_next` while holding the GIL, and
`next_batch()` then does `rx.blocking_recv()` — a blocking wait on network
I/O — with the GIL still held.

Consequence: for the entire duration of `apitap.read(...)`, no other Python
thread runs. Any threaded host — a web service, an Airflow worker with a
heartbeat thread, a notebook with a progress spinner — stalls completely.

**Measured** on the VPS — a background Python thread counting `ticks += 1`
while the main thread reads a 1000-row / 125 MB table, best of 3:

```
control: time.sleep(1) [GIL free]      other-thread ticks/s = 10,222,286
pyarrow  pa.table(apitap.read())       other-thread ticks/s =  9,301,470   (91%)
polars   pl.from_arrow(apitap.read())  other-thread ticks/s =  3,572,012   (35%)
```

So the damage is **consumer-dependent**, which is the interesting part.
`pyarrow` wraps its own read loop in `with nogil:` and accidentally covers for
us — 91% of the GIL survives. **`polars` does not, and 65% of another thread's
throughput disappears.** polars is the consumer this read path exists for.

And this is loopback, where the blocking wait is short. The worse the link, the
more of the wall clock is spent inside `blocking_recv` holding the GIL, so a
remote source moves polars' number toward zero.

**Do:** in `stream_get_next` (and `stream_get_schema`), take the GIL token via
`Python::attach` and wrap the `next_batch()` call in `py.detach(...)` so the
blocking recv happens with the GIL released. Then fix the two doc comments so
they describe the code either way — today they promise a guarantee the code
does not provide, and the one measurement that looks fine is luck borrowed from
pyarrow.

### P1-5 · Ctrl-C cannot stop a running transfer

*Reasoned from the source; not reproduced on the rig.*

No `Python::check_signals`, no `tokio::signal`, no `ctrl_c` anywhere in the
tree (`grep -rn "check_signals\|ctrl_c\|signal::" crates py-apitap` → nothing
but unrelated prose).

`transfer()` blocks the calling thread inside `py.detach()` for the whole run.
CPython's SIGINT handler sets a flag that only the main eval loop can act on,
and the main eval loop is inside that call. So `KeyboardInterrupt` is deferred
until the transfer returns — on a multi-hour table, indefinitely.

The operator's only exit is `SIGKILL`, which leaves behind:
- the staging table (see P2-1);
- for `mode='log_based'`, the **replication slot** — which keeps pinning WAL
  on the source until someone drops it by hand. An unattended slot is how a
  production Postgres runs out of disk.

**Do:** poll `Python::check_signals()` from the progress reporter tick (it
already runs on a cadence), and on `Err` trigger a cooperative cancel —
abort the workers, drop the staging table, release the slot, and return
`KeyboardInterrupt`. Document the slot-cleanup contract either way.

---

### P1-6 · No TCP keepalive on any database socket

*Reasoned from the source; not reproduced on the rig.*

`http.rs:63` gets it right for HTTP — `.tcp_keepalive(Duration::from_secs(60))`,
with a comment explaining that *"TCP keepalive is what actually detects a peer
that vanished mid-stream."* That lesson never made it to the database sockets:

- `wire/mywire.rs:454` — `tcp.set_nodelay(true)` only
- `wire/walsender.rs:629` — `stream.set_nodelay(true).ok()` only
- `source/postgres.rs:39` / `source/mysql.rs:745` — `PoolOptions` set
  `max_connections` and an `after_connect`; no keepalive, no `acquire_timeout`
  override

Behind a cloud NAT (AWS NAT Gateway drops idle flows at 350 s, Azure at 4 min)
a long COPY that goes quiet, or a CDC stream on a low-traffic database, has its
conntrack entry silently dropped. The client then blocks in `read()` **forever**
— no RST, no timeout, no error. This is one of the most common shapes of
"my ETL job hung and I had to kill it".

Partial mitigation exists for one lane only: the Postgres walsender sends
keepalives at `wal_sender_timeout/2` (`logbased/drain.rs:146`), which keeps
that socket warm from the server side. Bulk COPY and every MySQL lane have
nothing.

**Do:** build the `TcpStream`s in `mywire.rs` / `walsender.rs` through
`socket2` with `SO_KEEPALIVE` + `TCP_KEEPIDLE ≈ 60 s`, and set
`acquire_timeout` on both pools. sqlx does not expose keepalive, so document
that gap for the sqlx-backed lanes or move them onto the raw planes.

---

### P1-7 · Text columns are exported as Arrow UTF-8 without ever being validated

*Reasoned from the source; not reproduced on the rig.*

`wire/arrowcol.rs:940-960` (decode) and `:216` (`seal`).

The `ColB::Utf8` arm does `d.extend_from_slice(f)` on raw wire bytes and
`seal()` hands the buffer out as `FinishedCol::Utf8`. There is **no
`from_utf8` on this path** — `grep -n "from_utf8" wire/arrowcol.rs` returns
nothing. The bytes cross the C Data Interface declared as `u`, and every
Arrow consumer treats that declaration as a promise it need not check
(`polars` builds `&str` from the buffer unchecked).

The connection asks for UTF-8 (`walsender.rs:717`, `client_encoding=UTF8`), and
for most databases Postgres transcodes and the promise holds. It does not hold
for a **`SQL_ASCII`** database: that encoding means *no conversion*, so
whatever bytes were inserted come back verbatim, `client_encoding` or not.
Legacy Latin-1 content in a `SQL_ASCII` database is common enough to matter.

Result: invalid UTF-8 inside an Arrow `Utf8` buffer, handed to a consumer that
will not check it — undefined behaviour in the *user's* process, caused by
data, with no error from apitap.

**Do:** validate once per batch at `seal()` — `std::str::from_utf8(&d)` over
the whole concatenated buffer is a single SIMD pass, negligible against the
wire, and Arrow requires that buffer to be valid anyway. On failure, either
error with the column name or fall back to `Binary`. Refuse `SQL_ASCII`
sources explicitly if that is cheaper.

### P1-8 · `bytea` lands in ClickHouse as Postgres *text*, not bytes

Verified end-to-end with a 1000-row, two-column table (`id int`, `b bytea`,
each value 4 raw bytes) copied to all three destinations from the same source:

| destination | `hex(b)` for `id=3` | `length(b)` | correct? |
|---|---|---|---|
| source (pg) | `00000003` | 4 | — |
| postgres | `00000003` | 4 | yes |
| mysql | `00000003` | 4 | yes |
| **clickhouse** | `5C783030303030303033` | **10** | **no** |

`5C78…` is ASCII for `\x00000003` — the ClickHouse column holds the *string*
Postgres prints for that `bytea`, not the four bytes it represents. The column
is typed `Nullable(String)` (`sink/clickhouse.rs:473`,
`Delivered::Bytes => "String"`), the pg-text lane relays the rendered form
verbatim, and nothing converts it back.

Consequences: `length()` and every size calculation on that column are wrong by
2.5×, a ch→anywhere round trip yields text where bytes were, and a
`\x`-prefixed string is indistinguishable from a genuine string that happens to
start that way. All of it silent — no warning, no error, the transfer reports
success.

This is a gap in coverage, not in understanding: `dialect/mysql.rs::is_binary_udt`
lists `bytea` explicitly, with a comment noting *"a postgres → mysql plan flows
through the same LOAD DATA column list and arrives HEX-encoded too."* The same
thought never reached the ClickHouse sink.

**Do:** decide what a `bytea` means in ClickHouse and make it true on every
lane. `String` holding raw bytes is the natural answer (ClickHouse `String` is
byte-safe); then the pg-text lane must `decode(…, 'hex')` on the way in, the
same way the MySQL lane `UNHEX`es. Add a bytes column to the 1000-row route
matrix so a lane can never quietly disagree with its siblings again.

---

### P1-9 · `ch_ident` does not escape backslashes — an identifier can swallow the statement

`sink/clickhouse.rs:295`

```rust
pub(crate) fn ch_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "\\`"))     // escapes the backtick...
}
```

...but not the backslash that does the escaping. The sibling function two
screens down gets it right — `ch_str` (`:368`) does
`s.replace('\\', "\\\\").replace('\'', "\\'")`, backslash first. `ch_ident` never
got the same treatment.

So a name ending in a backslash renders as `` `a\` ``, where ClickHouse reads
the `\`` as an *escaped* backtick and the identifier never closes. Confirmed
against the bench ClickHouse (24.8.14.39):

```
$ curl … --data-binary "SELECT 1 AS \`a\\\`"
Code: 62. DB::Exception: Syntax error: failed at position 13 ('`a\`'):
  Back quoted string is not closed: '`a\`'.

$ curl … --data-binary "SELECT 1 AS \`a\\\`, 2 AS \`b\`, 3 AS \`c\`"
Code: 62. … failed at position 36 …
```

The second one is the shape that matters: position 36 is well past the first
column, so the parser swallowed `` a\`, 2 AS `` into a single identifier and
kept going. Here it ends in an error; a name crafted to re-balance the quoting
ends in a statement that means something the operator never wrote.

Likelihood is low — it needs a source column or table whose name contains a
backslash. Severity is not, and this codebase already decided how it feels
about this class: `sink/bigquery.rs:74` *refuses* names it cannot render
faithfully rather than emitting something that might parse.

**Do:** `name.replace('\\', "\\\\").replace('`', "\\`")` — backslash first, exactly
as `ch_str` does. Better, follow the BigQuery precedent and reject control
characters outright. Unit-test both functions against the same hostile-name
list.

---

## P2 — robustness, hygiene, supply chain

### P2-1 · Staging tables leak on any mid-run failure

`pipeline/mod.rs:421-443`: `sink.prepare()` creates the staging table, then
`run_workers` / `rows_staged` / `finalize` all propagate with `?`. Only two
paths drop it — the zero-row guard and the successful swap
(`sink/postgres.rs:604`, `:619`). Any error in between leaves it on disk.

It self-heals: `prepare()` opens with `DROP TABLE IF EXISTS`
(`sink/postgres.rs:429`), so the next run reclaims the space. But between a
failure and the next run, a full table's worth of disk is held by a name the
user never chose, and nothing says so. **Do:** best-effort drop on the error
path, and name the leftover in the error message.

### P2-2 · `o as u32` truncates, and the straddle buffer is unbounded

`wire/arrowcol.rs:667` and `:749`: `self.st_off[..] = o as u32`, where `o` is a
`usize` offset into the buffer being walked. Above 4 GiB this truncates
silently; `decode_staged` then reads a valid-but-wrong address — in bounds,
so not UB, but wrong data with no error.

It is reachable only through the same door as the real memory concern: `push()`
buffers *one straddling tuple* in `self.buf` (`:551-578`), and a Postgres tuple
can hold many fields of up to 1 GB each. So a single very wide row both
(a) blows past the bounded-memory contract this engine is sold on, and
(b) can push offsets past `u32`.

**Do:** cap the straddle buffer explicitly (a tuple larger than, say, 256 MB
is a refusal, not an allocation) and `debug_assert!(o <= u32::MAX as usize)`.
The cap is the load-bearing half — it turns "bounded memory" from a property
of typical data into a property of the code.

### P2-3 · Advisories, all verified present in the real build graph

`cargo audit` → **error: 1 vulnerability found; 4 allowed warnings found.**
Confirmed with `cargo tree -p apitap-core -i <crate>` that each is genuinely
compiled (`sqlx-macros` and `sqlx-sqlite` appear in the audit output but are
**not** in the graph — lockfile-only noise, ignore them):

| crate | id | class | path |
|---|---|---|---|
| `rsa 0.9.10` | RUSTSEC-2023-0071 | vuln, 5.9, **no fix available** | `sqlx-mysql` |
| `event-listener 5.4.1` | RUSTSEC-2026-0221 | unsound | `moka` ← `iceberg` |
| `lru 0.18.1` | RUSTSEC-2026-0253 | unsound | `mysql_async` |
| `spin 0.9.8` | — | **yanked** | `rsa`, `flume` |
| `paste 1.0.15` | RUSTSEC-2024-0436 | unmaintained | `parquet` ×2 |

`rsa` is the one with teeth: it is a Marvin timing side-channel, it is reached
through `sqlx-mysql`'s `caching_sha2_password` on **non-TLS** MySQL
connections, and upstream has no fixed release. **Do:** decide and write down
a position — either require TLS for MySQL sources (which takes `rsa` off the
auth path) or accept it in a `deny.toml` with the reasoning. Right now
`cargo audit` is red and no one is looking, which is the worst of both.

### P2-4 · Two full Parquet implementations in one wheel

`cargo tree -p apitap-core -d` shows `parquet v54.3.1` (direct dependency) and
`parquet v58.4.0` (via `iceberg 0.10`) both compiled, plus `base64` 0.22 and
0.23, `cpufeatures` 0.2 and 0.3, `darling` 0.20 and 0.23. Parquet is the
expensive one — it is a large crate and it is in there twice, in a wheel whose
whole pitch is "small". **Do:** try aligning the direct pin to `parquet 58` so
the graph unifies; measure `.so` size and cold build time before and after.

### P2-5 · The decoders are not fuzzed

`wire/torture.rs` is a good hand-written adversarial harness and the
instinct behind it is right. But there is no `fuzz/` directory and no
`cargo-fuzz`/`arbitrary` anywhere in the tree. The single largest risk surface
in this codebase is *"bytes arrive from a peer and are decoded with `unsafe`
pointer arithmetic"* — `pgbindec`, `pgoutput`, `mybinlog`, `arrowcol`,
`rowbinary`. That is the textbook shape for coverage-guided fuzzing, and P0-2
is exactly the kind of finding a fuzzer surfaces in minutes.

**Do:** add `cargo-fuzz` targets for the five decoders above. Run them in CI
on a short time budget and on a nightly for longer. This is the highest
leverage item in this document that is not already a bug.

### P2-6 · The `unsafe` in `arrowcol` is sound — and its proof is one refactor from breaking

I audited the 17 `unsafe` reads in `decode_staged` against the invariant they
cite. **The invariant holds.** `stage_tuples` (`:631`) and `stage_framed`
(`:686`) both bounds-check every `(offset, len)` against the buffer before
recording it, `o` only ever advances behind a check, and `pos ≤ data.len()` is
maintained on every path including the early returns. Nothing to fix.

What is missing is enforcement. The safety of `decode_staged` depends entirely
on a *different function*, and nothing in the type system, the tests, or CI
would notice if `stage_tuples` were refactored to stop proving what it proves.

**Do:** run the existing `arrowcol` tests under Miri (`cargo miri` is already
installed on the VPS) and add that to CI. Miri will not catch an in-bounds
wrong read, but it pins the pointer arithmetic, and it is the only automated
statement anyone has about this module's soundness.

### P2-7 · Progress state is process-global

`progress.rs:37-56` keeps `ROWS`, `BYTES`, `PIPES`, `EST_ROWS` and friends as
`static` atomics, with the design note *"a transfer is one logical operation
per process."* For the engine's own CLI-ish usage that is true. For a library
on PyPI it is an API constraint: two `apitap.transfer()` calls from two Python
threads silently merge their counters, and `set_total_estimate`'s
first-writer-wins makes the percentage meaningless for both. **Do:** document
it in the Python docstring as a known limitation, or thread a run id through.

### P2-8 · Diagnostics are `eprintln!`, not a log facade

30 `eprintln!`/`println!` calls in `crates/apitap-core/src` outside tests —
including operationally interesting ones like `source/mysql.rs:1131`
*"mysql raw transfer plane declined"*, which tells you the fast lane silently
fell back to the slow one. An operator cannot set a level, cannot route these
to a collector, and cannot correlate them with the progress JSON. **Do:** move
them behind `tracing` (or `log`) with the progress module as a subscriber, so
the fallback notices land in the same stream as everything else.

---

### P2-9 · A 1000-row table opens 32 pipes

The engine's own progress line, transferring the 1000-row QA table:

```
pg->pg  … rows=1000 … pipes=8
pg->ch  … rows=1000 … pipes=32
pg->my  … rows=1000 … pipes=16
```

32 connections to move 1000 rows is ~31 rows per pipe. The heuristic that
would prevent this exists — `ROWS_PER_PIPE = 32 * 1024` in `pipeline/mod.rs:463`
— but it lives in `desired_pipes`, which only the **multi-table** path calls.
The single-table `transfer()` sizes pipes from the CPU/memory model alone and
never consults the row estimate it already fetched for the progress readout
(`pipeline/mod.rs:415`).

Harmless on a bench box, not on a production Postgres with `max_connections=100`
and a scheduler firing many small hourly tables. **Do:** apply the same
`est_rows`-aware clamp on the single-table path.

---

## What I checked and found healthy

Worth recording so the next pass does not redo it:

- **Identifier quoting.** `dialect/postgres.rs` and `dialect/mysql.rs` escape
  correctly and are applied consistently; `sink/bigquery.rs:74` goes further
  and *refuses* names it cannot render, rather than pretending — the right
  call, since BigQuery has no backtick escape.
- **Watermark handling.** `pipeline/mod.rs:340-357` parses a non-quoted
  watermark as `i128` before embedding it, with a comment naming the reason.
  Destination data is treated as untrusted. Correct.
- **cgroup detection.** `pipeline/mod.rs:69-215` walks `/proc/self/cgroup` and
  `/proc/self/mountinfo`, takes the *smallest* limit up the tree, and falls
  through v2→v1 instead of returning early on the hybrid-host case. This is
  the most carefully-written code in the repo and it is right.
- **Arrow C Data Interface ownership.** `capsule.rs` implements the release
  contract properly — `release = None` sentinels, `private_data` ownership,
  idempotent double-release. The only gap is panic safety (P1-3).
- **The `unsafe` in `arrowcol::decode_staged`.** Sound; see P2-6.
- **HTTP client hardening.** `http.rs` is exemplary — every timeout named,
  every default justified, keepalive on.
- **The MySQL TSV lane's escaping.** I chased a hash mismatch here and it was
  my instrument, not the code: `group_concat` truncates at 1024 bytes by
  default. With `group_concat_max_len` raised, 1000 rows of text containing
  embedded tabs and newlines hash **identically** to the source
  (`8cc5f8405f3c9a44a1af854f586c13d3`, 19493 bytes both sides), and `id=3`
  round-trips byte-for-byte including `09` and `0A`. The TSV lane is correct —
  recorded here so nobody re-opens it.
- **Decimal fidelity on the happy path.** `numeric(18,4)` and `numeric(38,10)`
  sum bit-exactly across pg→pg, pg→ClickHouse and pg→MySQL for the 1000-row
  probe. P0-2 is about the *rescale* branch, not the decoder.
- **CDC durability design.** `logbased/run.rs` commits each table's watermark
  with its data and advances the slot only after every group member commits;
  `dest_ch.rs` correctly swaps that for window-level idempotence, since
  ClickHouse has no multi-statement transactions. Both are right. One doc nit:
  `lib.rs`'s `Mode::LogBased` text states the same-transaction guarantee
  unconditionally, which is not true for the ClickHouse and BigQuery sinks —
  they are idempotent-replay instead. Worth a sentence.


---

## The QA rig this was measured on

Everything above ran against `~/apitap-qa` on the bench VPS, with a wheel built
from that tree (`apitap-0.51.0-cp39-abi3-manylinux_2_17_x86_64.whl`, 10.5 MB,
`11m45s`) installed into `~/qa-venv`. Per the QA data budget, every fixture is
**1000 rows**:

- `qa_probe` — 1000 rows, 9 columns: `numeric(18,4)`, `numeric(38,10)`, text
  with embedded tab/newline/NULLs, `bytea`, `timestamptz`, `date`, `boolean`,
  `float8`.
- `qa_bin` — 1000 rows, `id int` + `bytea`, to isolate P1-8.
- `qa_wide` — 1000 rows × 125 KB text (125 MB), so a read lasts long enough to
  time the GIL.

Routes exercised: pg→pg, pg→ClickHouse, pg→MySQL. All three moved 1000 rows and
reported success; the defects are in what landed, not whether it landed.

## How to reproduce any of this

```bash
ssh -i ~/.ssh/apitap_vps ubuntu@vps-f1d96aff.vps.ovh.ca
cd ~/apitap-qa
export PATH=$HOME/.cargo/bin:$PATH CARGO_TARGET_DIR=$HOME/apitap-qa-target
cargo clippy --workspace --all-targets    # P1-1, P1-2
cargo test --workspace                    # baseline: 206 pass
cargo audit                               # P2-3
cargo tree -p apitap-core -d              # P2-4
```

`~/apitap-qa` is a scratch mirror — edit it freely, it is not the release
copy at `~/apitap-lib`. The `qa_probe` module for P0-2 is appended to
`crates/apitap-core/src/wire/pgcopy.rs` there.

---

## Addendum — what v0.53.0 closed, and what it did not

Written after the fact, against the same policy: every line below is either a
gate leg that ran or is marked as unmeasured.

**Closed.**

- `cargo clippy` deny-level error — `co_control`'s outer `loop` never looped
  (every branch returned). Removed; the crate now lints clean.
- Being stopped on purpose. SIGTERM was fatal; it now lands the CDC window in
  flight. Gates S4 (Postgres) and S5 (MySQL binlog), each carrying a control run
  with `APITAP_GRACEFUL_STOP=0` that must die by signal.
- URL errors that named neither the fault nor the password. Gate U1, whose
  leg 5 percent-encodes a password holding `@ / : ? # [ ]` per the message's own
  advice and requires the result to actually connect.
- BigQuery bootstrap loads dying on a throttle. `begin_session` had no backoff
  and `put_chunk` only retried 5xx, while `rateLimitExceeded` arrives as 403.
  *Unit tests only — a real throttle cannot be forced on demand.*

**Found while closing them, worth recording as method.**

An adversarial panel over the SIGTERM draft raised 26 claims and refuted 24.
Both survivors were the same race — `Drop` released the nesting depth before
restoring the disposition, so a concurrent `install()` could read apitap's own
handler out of the live disposition and record it as "the previous one". One of
the *refuted* claims was also real: the handler distinguished a first SIGTERM
from a second by `REQUESTED.swap(true)`, and `request_stop()` sets that same
flag. The panel dismissed it as harmless because applies are idempotent — which
is true of nearly every defect in this engine, and is exactly why that reasoning
cannot be allowed to decide.

**Still open.**

- `cargo audit` — 1 vulnerability, 2 unsound, 1 yanked. Untouched by this
  release.
- Peak RSS is bounded by the widest row, not by the budget. Printed as a KNOWN
  GAP by `e2e_review_gate.py` leg 3 on every run.
- No tests for `logbased/run.rs` (~1500 lines), `sink/mysql.rs`, the PyO3
  boundary, or `_predicate_sql`.
- MySQL binary JSON is refused per-table rather than rendered.
