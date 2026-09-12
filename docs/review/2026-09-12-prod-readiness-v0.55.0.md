# Production-readiness audit of v0.55.0 — handoff for implementation

**Status:** analysis complete, nothing implemented. This document is the brief
for whoever codes the fixes next. Read all of it before touching a file.

**Method:** 10-lens read-only audit (8 lenses completed, 2 did not — see §6),
230 raw findings, blocker/high findings put to an adversarial refuter that had
to QUOTE the code to refute. Every "confirmed" item below carries a refuter
verdict with file:line citations at HEAD `e32bb37`. Every "unverified" item is
labeled so. The verdicts and raw findings are in the session journal; the
extracted JSON is in the scratchpad (`findings_blocker.json`,
`findings_high.json`, `findings_all.json`).

**Standing rules that apply to every fix here** (from the project's memory —
not optional):

- `cargo build/check/test`, `maturin`, and every e2e leg run **only on the bench
  VPS**. Never on the MacBook. rsync with an ABSOLUTE source path, `touch` the
  synced `.rs` files before cargo, launch long jobs with `setsid nohup … &` and
  confirm by artifact (`docker ps`, a growing log), never by `pgrep` on a name.
- Fix by **root cause, not per finding** ("jangan tambal sulam").
- Before claiming a rule is "now one function, every sink calls it", **grep every
  call site**. The 0.55.0 interim commit claimed exactly that and was wrong for
  six of seven sinks; the gate caught it.
- The release gate is `benchmarks/gate.py` on the VPS against the release wheel
  (verify the installed `.so` md5 changed). 36/36 is required before a tag.
- Every bench run ends with a dest-table drop; keep the seeds.
- Commit attribution: **no** Co-Authored-By trailer — the project memory's
  standing rule wins over the harness default (decided in §0). Only a message
  from the user in chat changes that.

---

## 0. Decisions already made — implement, do not reopen

The implementer's job is to code. Every judgment call below has been made;
if one turns out to be impossible in the code, say so in the commit and pick
the nearest thing, but do not redesign.

| decision | choice | why |
|---|---|---|
| MySQL TLS default (E1) | **TLS required for any non-loopback host.** `?ssl-mode=disabled` is the explicit opt-out; the error on a plaintext-only server names it verbatim. Same for the MySQL **source**. | A tool that calls itself production-ready cannot ship credentials in clear by default. mysql_async has no opportunistic mode, so it is binary; the loud error makes a broken internal deploy a ten-second fix. Release-note it. |
| CDC guard + start-instant TOCTOU | **One mechanism for both: an atomic-create lock artifact** (`Artifact::Lock`, §2 A2 + §4), used by bulk `prepare` and by CDC drain start. 0.56.0. | Both problems are "two runs must agree who owns the table, atomically". Every engine has an atomic create; none has a portable timestamp. Token tie-breaks do not work when scans are not simultaneous (proof in §4). |
| Legacy un-tokenized name (A3) | **Refuse, never delete.** New `Found::Legacy`. | The design rule already in `naming.rs`: nothing is collected on a guess. |
| Stale watermark (C1) | **0.55.1: reader-side safety net. 0.56.0: pending-swap column.** | The net is engine-agnostic and cheap; the protocol is per-sink and must not hold the point release. |
| Commit attribution | **Follow project memory: no Co-Authored-By trailer.** | The user set that preference for this repo; the harness default does not override an explicit standing instruction. If the user says otherwise in chat, obey the chat. |
| Version | **0.55.1** for §3 steps 1–8; **0.56.0** for step 9. | Point release = the guard's own edges + the gate hole; minor = new artifact kind + state-contract extension. |

---

## 1. The honest verdict

v0.55.0 closed the headline concurrency defect for the case it was designed
for — two bulk runs of one table with byte-identical source URLs — and is
**wrong at every edge of that case**. Four confirmed findings are the same root
cause: the guard keys on the wrong identity, the CDC lane never enters it, error
paths do not clean up behind it, and a pre-0.55 leftover is collected while it
may be live. A fifth confirmed blocker (swap-before-state on three sinks) is
older than the guard but shares its shape: the publish and the bookkeeping are
not one atomic step off Postgres.

None of these is a polish item. Three produce **wrong data under a green run**,
which is the project's own definition of blocker. Ship a **0.55.1** that closes
the guard's edges and the gate hole, then a **0.56.0** for the two items whose
fix touches the artifact set and the state contract (CDC run identity via
`Artifact::Lock`, swap/state atomicity via the pending-swap column) — both are
fully specified in §2; nothing in 0.56.0 needs a design session.

The 0.55.0 release note's claim that "the concurrency matrix" is enforced is
false for the `log_based` rows. apitap.dev says the same. That must be corrected
in the same change that fixes it or, if the fix slips, corrected first.

---

## 2. Confirmed findings, grouped by root cause

### Root cause A — the concurrency guard keys on the wrong identity and skips a lane

**A1. `RunId::mint` hashes the raw source URL string** — *blocker, confirmed.*
`crates/apitap-core/src/pipeline/dispatch.rs:337` and `:364`:
`RunId::mint(land_kind(opts.mode), src_url)` with `src_url` forwarded verbatim
from `lib.rs:389`. `naming.rs:260-264` md5s `source_id.as_bytes()`.
Meanwhile `_apitap_state` keys the watermark on `pipeline/mod.rs:38-58
source_identity()`, which folds `postgresql`→`postgres`, fills the default
port, and drops userinfo and query. The existing unit test at `mod.rs:709-712`
asserts `postgres://user:s3cret@db.example:5432/prod ==
postgresql://other:pw@db.example/prod`. So two overlapping `append` runs whose
URLs differ only in alias/port/query/credentials get different `source_hash`
values, `naming.rs:365 (Incremental, Incremental) => mine.source_hash ==
peer.source_hash` says fan-in, neither is refused, both read the same watermark,
and `postgres.rs:816-826 INSERT INTO … SELECT * FROM …` lands the delta twice.
No test covers it: `naming.rs` tests hand-build `"aaa"/"bbb"` hashes;
`e2e_concurrent_runs.py` uses one byte-identical `SRC` for every leg.

*Fix shape:* mint from the normalized identity — `source_identity(src_url,
table)` with the `::table` suffix stripped (i.e. `norm(scheme)://host:port/db`).
One call-site change in `dispatch.rs` (both arms). Add the unit test the
refuter wrote:

```rust
#[test]
fn aliased_urls_share_one_source_hash() {
    let a = peer(LandKind::Incremental, "postgres://bench:pw@127.0.0.1:5432/bench");
    let b = peer(LandKind::Incremental, "postgresql://bench:pw@127.0.0.1/bench?application_name=x");
    assert_eq!(a.source_hash, b.source_hash);
    assert!(peer_blocks(&a, &b));
}
```

and a new e2e leg in `e2e_concurrent_runs.py`: two appends, aliased URLs,
gated the leg-1 way → `LockedError`; control with byte-identical URLs behaves
the same. (This also finally gives the file its missing fan-in leg: two appends
from **genuinely different** sources must still both land.)

**A2. The CDC lane never runs the guard** — *blocker (docs-vs-code + defect),
confirmed.* `crates/apitap-core/src/lib.rs:360-373`: `if opts.mode ==
Mode::LogBased { … return reported(…, logbased::run::run_task(…)) }` returns
**before** `pipeline::dispatch::single` at `:388`, and `:413` does the same for
multi-table. `RunId::mint` is called only inside dispatch. The drain
(`logbased/run.rs`) calls no sink's `prepare`/`reap_and_check_peers`. So
`LandKind::Cdc` is never minted, two drains of one table are never refused, a
drain beside a bulk `replace` is never refused in either direction, and
`docs/failure-modes.md:238` `| log_based | anything | refused |` is false. Only
the Postgres-source path has a slot-active check; MySQL-source drains have
nothing.

*Fix shape (two steps, do both):*
1. **Now:** correct `docs/failure-modes.md` (the matrix), `docs/stability.md`
   (the committed-surface row), `README.md`, `py-apitap/README.md`, and the
   apitap.dev copies: the guard covers **bulk** modes; CDC drains are not yet
   guarded. Stop claiming otherwise.
2. **The real fix (0.56.0) — `Artifact::Lock`, exact spec.** One artifact
   kind, atomic create, used by BOTH lanes. This also closes the start-instant
   window (§4), so do not implement A2 and §4 separately.

   *Name:* `artifact_ident(table, Artifact::Lock, limit)` — the **un-tokenized**
   form, because it is the mutex, not a workspace: `<table>__apitap_lock`. Add
   `Lock` to `Artifact::ALL` so namespace reservation and discovery exclusion
   follow (that is what `ALL` is for). `naming::is_artifact` must return true
   for it.

   *Contents:* the owner's `RunId` token + `LandKind` + `started_unix`, so
   `locked_error` can still say who holds it and what they are doing. Table
   engines: a one-column table with one row (or the table COMMENT); object
   stores: the object body; Iceberg: keep the existing claim marker and rename
   it to this kind.

   *Acquire* — one atomic create per engine, failure = "someone holds it":
   - Postgres: `CREATE TABLE <lock> (…)` inside `prepare`'s transaction — a
     duplicate is `42P07`. (Keep the existing `reap_and_check_peers` scan too;
     it is what names a *staging* leftover.)
   - MySQL: `CREATE TABLE <lock> …` — error 1050.
   - ClickHouse: `CREATE TABLE <lock> … ENGINE=TinyLog` (ON CLUSTER when the
     run is) — code 57 `TABLE_ALREADY_EXISTS`.
   - BigQuery: `tables.insert` returns 409 on exists.
   - S3: `PutObject` with `If-None-Match: *` → 412 on exists (`aws.rs`).
   - GCS: JSON upload with `ifGenerationMatch=0` → 412 (`gcp.rs`).
   - Iceberg: the catalog commit of the claim is already atomic.
   The loser reads the lock's contents and raises `naming::locked_error` with
   the owner's token — the message already exists.

   *Release:* in `finalize` (success) and in `discard` (B1's error arm), by
   name. A SIGKILLed run leaves it; the next run refuses and the error names
   `<table>__apitap_lock` to drop — the same documented cost as staging.

   *Where it is called:* bulk — first statement of every sink's `prepare`,
   before the staging scan. CDC — at the top of `logbased::run::run_task`
   (before the bootstrap decision) via the destination's lane
   (`dest_pg/my/ch/bq/ice`), released when the drain returns or errors. The
   bootstrap's internal `Replace` then finds its own lock (same token →
   `Found::Mine`) and proceeds.

   *Matrix:* unchanged — `peer_blocks(mine, owner)` decides; fan-in still
   passes because two `append`s from different sources do not both hold the
   lock at once… **no**: with a lock, fan-in would be refused. So the lock is
   taken by `replace`, `log_based`, and same-source `append/merge` only; a
   different-source incremental run skips the lock and relies on the staging
   scan (which already permits it). Encode that in one function
   `naming::needs_lock(kind, mine_source, owner_source) -> bool` with a unit
   test per matrix row.

   *Tests:* a new `e2e_concurrent_runs.py` leg 7 (two `log_based` drains, one
   table → second raises `LockedError`; a drain vs a bulk `replace` → refused
   both directions) and leg 6 (start-instant burst) upgraded from "invariants
   only" to "exactly one wins, the other raises `LockedError`, no orphan".

**A3. A pre-0.55 (un-tokenized) leftover is collected while it may be live** —
*high, UNVERIFIED by a refuter; mechanism read and judged plausible by the
author of `classify`.* `naming.rs:421-427`: the exact legacy name returns
`Found::Dead`; every sink deletes `Dead` unconditionally. A 0.55.0 run that
overlaps a still-running ≤0.54.0 run (rolling upgrade; a laptop backfill on the
old wheel beside the upgraded cron) deletes the old run's staging mid-load. On
Postgres/MySQL/ClickHouse the old run then dies loudly. On BigQuery
(`bigquery.rs:1971 "createDisposition": "CREATE_IF_NEEDED"`) and the object
stores (per-part uploads) it silently re-creates the staging and publishes the
remainder as a full load.

*Fix shape:* stop deleting it. Add `Found::Legacy` to the enum; sinks refuse it
with a `locked_error` variant whose recovery sentence says "an apitap older
than 0.55.0 may be using this; if none is, drop `<name>`". Consistent with the
design rule already written in `naming.rs`: nothing is collected on a guess.
The one cost — a genuine pre-0.55 orphan needs a manual drop once — is the
same cost the design already accepted for tokenized leftovers. Update the unit
test `the_pre_token_name_is_collectable` to assert the opposite, with the
reason.

### Root cause B — error paths do not clean up, so the guard refuses stale leftovers

**B1. An ordinary error between `prepare` and `finalize` leaves staging (or the
Iceberg claim) behind** — *high, confirmed by two independent refuters.*
`crates/apitap-core/src/pipeline/mod.rs:422-443` is a straight `?` chain:
`sink.prepare(…)?; … src.span_stmts(…)?; … sink.loader()?; …
src.run_workers(…)?; sink.rows_staged(…)?; sink.finalize(…)?` — no error arm.
The `Sink` trait (`sink/mod.rs:131-185`) has no discard/rollback hook for the
pipeline to call. `postgres.rs:1018-1023` `abort` only drops the COPY stream;
the staging **table** persists. `iceberg.rs:1078-1104` releases the claim only
inside `finalize`. Since 0.55.0 a foreign-token leftover is always
`Found::Live`, so **every transient failure** — source connection drop,
statement timeout, destination DDL error — becomes a `locked:` refusal on the
next scheduled run, claiming a run "started Ns ago" that is not running.
`docs/failure-modes.md:45` says "Every *ordinary* error path still drops its
own staging; only a kill skips it." That sentence is false. The e2e rig masks
it: `e2e_failure_modes.py` case 3 ends with `drop_ch(ch, T)` before the next
case, which is how it passes.

*Fix shape:* add `async fn discard(&self)` to the `Sink` trait, implemented
per sink as "drop **my own** tokenized artifacts" (each sink already has the
drop function; Iceberg's is `release_claim`). In `pipeline::run`, wrap
everything after `prepare` so that on `Err(e)` the pipeline calls
`sink.discard().await` **best-effort** (log, never mask `e`) and then returns
`e`. Same in the multi-table driver, per table. Then delete the `drop_ch(ch,
T)` between cases in `e2e_failure_modes.py` and assert instead that after case
3 (cut connection) **no staging remains** — that is the leg that proves the
sentence in the docs.

### Root cause C — publish and bookkeeping are not one step off Postgres

**C1. Replace on ClickHouse, MySQL and BigQuery swaps durably BEFORE touching
`_apitap_state`** — *blocker, confirmed.* Only Postgres puts DROP/RENAME, the
state-row DELETE and the new state row in one transaction. ClickHouse
`clickhouse.rs:1697-1718`: `EXCHANGE TABLES` → `DROP TABLE {staging}` →
`ensure_state_table` → `DELETE FROM _apitap_state …` → `write_state` — five
fallible round-trips after the data is durable; the on_cluster branch at
`:1683-1689` even names the failure shape in a comment. MySQL and BigQuery have
the same order. A partition, restart or permission error after the swap returns
an error while the destination already holds the new data and `_apitap_state`
still holds the pre-replace watermark; the next `append` reads that watermark
and lands rows the replace already contains (duplicates) or, if the replace
loaded less than the old watermark, skips rows. Either way: wrong data, green
run.

*Fix shape (two steps):*
1. ~~**0.55.1 — a safety net at the reader**~~ — **ALREADY IN THE CODE, and the
   finding is half wrong.** Implementing it turned up `WmArbitration::Greatest`
   in `plan.rs`, which ClickHouse, MySQL and BigQuery all pass to
   `resolve_watermark`: when a state row and a data max disagree, the FRESHER
   wins. The audit lens that filed C1 read `finalize` and never read
   `dest_state`, so it reported the ordering without the guard that compensates
   for it.

   What that guard actually covers, pinned in
   `plan.rs::greatest_covers_a_stale_state_row_forward_but_not_backward`: a
   replace that moved the cursor FORWARD and then failed before clearing state
   is fully handled — the data max is higher, it wins, no duplicates. A replace
   that moved the cursor BACKWARD is not: the stale row is the higher value,
   wins, and rows landing in between are skipped later. `Greatest` cannot tell
   that from an ordinary foreign delete, where trusting the state row is right.

   So C1's severity drops from blocker to **medium**, its remaining scope is
   "replace that shrinks the cursor, then fails mid-bookkeeping", and the fix
   for it is step 2 — there is nothing sensible to add in 0.55.1 that step 2
   does not do better. The `WmArbitration` doc comment claimed "never a skip",
   which was wrong in exactly this direction; that is corrected.
2. **0.56.0 — pending-swap protocol, exact spec.** Extend `_apitap_state`
   with one nullable column `pending TEXT` (the contract lives in
   `docs/usage.md` and `e2e_state_contract.py`; older readers ignore unknown
   columns, so this is additive). On CH/MySQL/BQ `finalize` for `replace`:
   1. `UPSERT` the state row with `pending = '<run token>'` and the **new**
      watermark/row count (one statement, durable).
   2. swap (`EXCHANGE`/`RENAME`/copy job).
   3. `UPDATE … SET pending = NULL` (one statement).
   A failure after (1) and before (3) leaves `pending = <token>`. **Every**
   run's `dest_state` read treats a non-NULL `pending` as: "check whether the
   swap in step 2 happened" — compare the destination's row count to the
   `last_rows` in that row; equal → the swap landed, clear `pending` and
   proceed; different → the swap did not land, restore the previous watermark
   (keep it in a second column `prev_watermark` written in step 1) and clear
   `pending`. Postgres keeps its single transaction and never writes
   `pending`. Leg: `e2e_replace_hazards.py` gains a case per engine that kills
   the process between steps 1 and 2, and between 2 and 3, and asserts the
   next append lands exactly the right rows.

### Root cause D — the gate could not fail on four legs

**D1. `e2e_changelog_{my,bq,group,percolumn}.py` compute `ok`, print
ALL GREEN/FAILED, and exit 0 regardless** — *blocker, confirmed by grep; three
lenses converged on it.* `gate.py` keys PASS on `returncode == 0`. Only
`e2e_changelog_ch.py` has `sys.exit(0 if ok else 1)`. The v0.55.0 gate log
shows all four printed ALL GREEN, so the 36/36 verdict stands on the printed
text — but the **mechanism** had a hole that would have hidden a failure.

*Fix shape:* `raise SystemExit(0 if ok else 1)` at the end of each of the four
files. **And** in `gate.py`, treat a leg as FAIL when its returncode is 0 but
its stdout tail matches `FAILED` (belt and braces for the next leg someone
writes without an exit). Add a self-test: a dummy leg that prints FAILED and
exits 0 must be recorded as FAIL.

### Root cause E — an insecure default that says nothing

**E1. MySQL destination defaults to no TLS, silently** — *high, confirmed.*
`sink/mysql.rs:157-158` returns `ssl=None` when the URL carries no
`ssl-mode`/`sslmode`; `:215-217` sets `ssl_opts` only when a mode was parsed;
the doc comment at `:138-140` concedes mysql_async has no opportunistic mode.
So a plain `mysql://user:pw@prod-host/db` sends credentials and data in clear
unless the operator knew to add a parameter the manual does not lead with.

*Fix — decided (see §0):* in `sink/mysql.rs` (and the source's twin in
`source/mysql.rs`), when the URL carries no `ssl-mode`/`sslmode` **and** the
host is not `localhost`/`127.0.0.1`/`::1`, set `ssl_opts` to required-with-
verification. If the server cannot do TLS the connect fails; make that error
say: `mysql://…: this server does not offer TLS and the host is not loopback.
apitap requires TLS off-box by default since 0.55.1; add ?ssl-mode=disabled to
the URL to send credentials in clear on purpose.` Add a unit test on the URL
→ options mapping for the four cases (loopback/none, remote/none,
remote/disabled, remote/required) and an e2e case in `e2e_tls_mysql.py`
that a plain remote URL against the TLS-only rig now **succeeds** without a
parameter. ClickHouse is unchanged (`clickhouse://` vs `clickhouse+https://`
is the scheme's job) but `docs/usage.md` must say so in the TLS section.

---

## 3. Suggested order

Each step is one commit, one gate run on the VPS, and it de-risks the next.

1. **D1** — gate exits + verdict-line check. Ten minutes; everything after this
   is measured by a gate that can actually fail.
2. **A1** — normalized source identity + unit test + the aliased-URL e2e leg
   (which also delivers the long-missing fan-in leg).
3. **B1** — `Sink::discard` + pipeline error arm + un-mask
   `e2e_failure_modes.py`. After this, "every ordinary error path drops its
   own staging" is true and the guard stops refusing ghosts.
4. **A3** — `Found::Legacy` → refuse, never delete.
5. **A2 step 1** — docs: CDC is not guarded yet. Same commit: re-sync
   `web/content/docs/*` and redeploy apitap.dev (recipe in memory
   `apitap-web-ui`).
6. **C1 step 1** — the stale-watermark safety net + leg.
7. **E1** — after the user picks (a) or (b).
8. Gate 36/36 (now 38+ with the new legs) → **tag v0.55.1**. Release note
   must say plainly: 0.55.0's guard was correct only for identical-URL bulk
   runs; these are its edges; CDC guarding is 0.56.0.
9. **0.56.0:** A2 step 2 (CDC claim markers), C1 step 2 (pending-swap
   protocol), and §6 below.

---

## 4. Explicitly deferred past 0.55.1, with reasons

- **The start-instant TOCTOU window** — designed, not deferred: it is
  closed by `Artifact::Lock` (§2 A2 step 2) in 0.56.0. For the record, why a
  token tie-break was rejected: "lower token yields" only works when both runs
  scan **after** both announced. If A scans before B announces, A proceeds;
  B then announces, scans, sees A — and if B's token is higher, B proceeds
  too. Two winners. The only rule that never loses data without a lock is
  "any peer seen → yield", which makes both yield when they see each other —
  safe but a double failure. An atomic create has exactly one winner by
  construction, on every engine, with no clock and no ordering assumption.
- **aarch64 / macOS wheels + sdist** (stability.md road-to-1.0 item 2). Not a
  correctness issue; a CI matrix change. Schedule after 0.55.1.
- **The gate in CI** (item 1). Needs a runner that can reach the rig; design
  separately.
- **Disk-full and object-store destinations under failure** (item 3), the
  **multi-day soak** (item 4).

---

## 5. Unverified backlog — DO NOT treat as facts

**39 `high` findings** from the 8 completed lenses received no refuter verdict
(session limits). They are in `findings_high.json`. Three overlap with
confirmed items above. The rest span: TLS verification on other engines,
identifier quoting on specific dialects, resource leaks on error paths, several
docs-vs-code mismatches in the type-mapping tables, and operability gaps (error
messages that name a cause but not the object). Verify before fixing — see
§7 for how to do that within one session window.

**101 medium, 82 low** — unjudged. Backlog.

---

## 6. Lenses that did not run

`cdc-correctness` (watermark commit atomicity per destination, per-key collapse
under PK-change+delete+reinsert, TOAST, slot lifecycle, binlog event coverage)
and `resource-bounds` (the "memory = pipes × chunk" claim, fd/connection/task
leaks on error paths, cgroup detection) both exhausted the session window
twice — they are the lenses that read the most code, and they are the two most
likely to hold **another** silent-data-loss finding. They must run. Split each
into two narrower prompts (one file group each) and run them **alone**, first
in a fresh window, before any verifier.

---

## 7. How to verify cheaply next time

Three session windows produced six verdicts. The verifiers were told to "read
the code at ${REPO}" and each roamed ~100K tokens. Do this instead:

- Inline the cited file's ±80 lines into the prompt; **forbid** further reads.
  ~15K tokens per verdict, ~40 verdicts per window.
- Run finders and verifiers in **separate** windows; never both in one.
- One refuter first; reproducer + triager only for what survives.
- Keep the rule that "harmless because idempotent" is not a refutation.

---

## 8. What was wrong in my own earlier claims (so it is on the record)

- `3fb85d2` said "one rule, every sink calls it" — six sinks did not, until
  `e32bb37`.
- The v0.55.0 release note and apitap.dev say the concurrency matrix's
  `log_based` rows are enforced. They are not (A2).
- `docs/failure-modes.md` says ordinary error paths drop their staging. They
  do not (B1).
- The 36/36 gate had four legs that could not fail (D1); the verdicts were
  nonetheless genuinely green.
