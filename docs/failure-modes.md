# Failure modes — what is left behind, and what to do

Benchmarks answer "how fast, and is it correct?". This page answers the
question production actually asks: **the run did not finish — where am I?**

Every row below was produced by creating the failure on purpose against live
servers, not by reading the code. The harness is
[`benchmarks/e2e_failure_modes.py`](../benchmarks/e2e_failure_modes.py) and
[`benchmarks/e2e_cdc_retention.py`](../benchmarks/e2e_cdc_retention.py); both
run as gate legs before a release is tagged, so these answers cannot rot
silently.

The two properties everything else rests on:

- **A bulk load publishes only at the end.** Rows stream into a staging table;
  the destination is replaced by an atomic swap (`EXCHANGE TABLES`, `RENAME`).
  Until that swap, readers see the previous table, whole. For a **table**
  destination there is no window in which a query returns half a load — the
  swap is what buys that. **Object stores do not get the same promise**: S3 and
  GCS have no swap, so finalize publishes the part objects one at a time, and a
  reader that LISTs the prefix while a run is finishing can see a subset — or,
  for a Parquet directory, a mix of old and new parts. If something reads those
  paths while runs may be finalizing, gate it on a manifest or
  `_SUCCESS`-style marker of your own, or read only after the run reports done.
- **A CDC watermark is written last, and replay is idempotent by
  construction** — the apply clears the keys it is about to write before it
  writes them. What that buys you depends on what the destination can promise,
  and the promise is not the same everywhere:

  | destination | window + watermark | what a crash mid-window leaves |
  |---|---|---|
  | Postgres, MySQL, Iceberg | **one transaction** | either the whole window and its watermark, or neither |
  | ClickHouse | several statements, watermark last | some of the window may be applied; the watermark is not, so the next run re-applies it — the same keys, cleared and rewritten |
  | BigQuery | one transaction per chunk, watermark last | as ClickHouse: partial apply, unmoved watermark, re-applied on the next run |

  In every case a crash costs a repeat, never a gap: no window is ever
  recorded as applied unless it was. ClickHouse and BigQuery reach that
  through repetition instead of rollback, because neither offers a
  transaction that spans the statements this apply needs.

## The table

| what happened | state left behind | recovery | verified |
|---|---|---|---|
| **Process SIGKILLed mid bulk transfer** | Previous destination table **intact and readable throughout** (proven: 1,000 rows and their marker unchanged while 10M rows were streaming into staging). A staging table `<dest>__apitap_staging` may be orphaned. | Just re-run. The next run starts with `DROP TABLE IF EXISTS` on staging, so the orphan costs disk until then, nothing more. Proven: re-run landed 10,000,000 rows = source count. | `e2e_failure_modes.py` case 1 |
| **Process SIGKILLed mid CDC window** | Watermark **unmoved** — the destination is exactly where the last completed window left it. (SIGKILL only: SIGTERM is now handled and lands the window instead — see the row below.) | Re-run. Every change is applied exactly once (proven by digest, not by row count alone: 4,000 rows and `sum(id)` identical to the source after the kill + replay). | case 2 |
| **SIGTERM mid CDC window** (pod evicted, Airflow run cleared, `systemctl stop`) | The window in flight is **applied**, not discarded, and the watermark advances with it. The run exits 0 with a report of the rows it landed. | Nothing. The next run picks up from the new watermark. A second SIGTERM is not absorbed — the default disposition comes back and the process ends at once, which is the SIGKILL case above and equally safe. | `e2e_sigterm.py` (Postgres, incl. a control run with the mechanism disabled), `e2e_sigterm_my.py` (MySQL binlog) |
| **Two runs of the same destination table at once** (0.55.0+) | **Refused, at `prepare`, before a row moves.** The second run exits non-zero with a `locked:` error naming how long ago the other started, what it is doing, and why the two cannot share the table. Nothing is written; the first run finishes normally. On 0.54.0 and earlier the two interleaved destructively — see [Two runs, one table](#two-runs-one-table). | Run them one at a time — a scheduler's own concurrency setting is the usual answer (Airflow `max_active_runs=1`, a cron `flock`). If the other run is dead rather than slow, drop the staging object the error names and re-run; nothing collects it for you, and the section below says why. | `e2e_concurrent_runs.py` (Postgres: the refusal, the survivor, a control, and fan-in) |
| **Source connection cut mid-COPY** (server restart, `pg_terminate_backend`, idle/statement timeout, network drop) | Nothing published. The destination table is **not even created** — it only comes into existence at the swap. | Re-run. The error says so explicitly rather than making you guess. | case 3 |
| **Structural DDL on the source during a bulk run** | Cannot interleave on Postgres: `COPY` holds `ACCESS SHARE`, and `ALTER TABLE … DROP/ADD COLUMN` needs `ACCESS EXCLUSIVE`, so the DDL **waits for the read to finish**. Column mapping cannot drift mid-stream. | Nothing to do. If the DDL wins a race we do not yet know about, the run fails loudly rather than writing values into the wrong column — that is the assertion the test makes. | case 4 |
| **CDC schedule stopped for a long time — Postgres** | The replication slot keeps holding WAL on the **source**, which is the guarantee CDC rests on and also how a stopped schedule fills the source's disk. | Run the drain: a backlog is not a reason to refuse, it is a reason to run. apitap prints the retained WAL every run and warns past `APITAP_SLOT_WAL_WARN` (default 4 GiB). Set `max_slot_wal_keep_size` on the server so an abandoned slot is *invalidated* instead of filling the disk — apitap reports that as slot-is-GONE and recovers with a fresh bootstrap. | `e2e_cdc_retention.py` |
| **CDC schedule stopped for a long time — MySQL/MariaDB** | The opposite risk: the server **purges** binlogs on its own retention, so the stored position can simply be gone. Resuming would skip every change in between. | apitap refuses before asking the server, names the missing file and position, and tells you the only correct recovery: clear that table's state on the destination and re-bootstrap. Prevention: keep binlog retention longer than the longest gap between runs. | `e2e_cdc_retention.py` |
| **`durable=False` and the machine crashes** | Postgres destinations only, and the label is on the tin: the swapped-in table stays `UNLOGGED`, and PostgreSQL **truncates unlogged tables during crash recovery**. | `ALTER TABLE … SET LOGGED` after the load, or do not use the flag for anything you cannot rebuild. | documented behaviour, not a bug |
| **Destination table has dependent views (Postgres)** | The swap `DROP`s the old table, which Postgres refuses while views depend on it. Fails **safely**: old table and staging both intact. | Drop and recreate the views around the load. Fixing this to fail at probe time instead of at the end is open work. | `docs/usage.md` caveat |

## What is NOT covered yet, and I would rather say so

- **Destination disk full.** Not tested. The expected shape is a loud write
  error with the staging table left behind, i.e. the same recovery as a killed
  run — but expected is not measured, and this page is only worth something if
  it distinguishes the two.
- **Destination-side crash mid-apply** (ClickHouse or BigQuery restarting under
  us). The apply is idempotent by design and the watermark is written last, so
  the expectation is a clean replay — again, expectation, not receipt.
- **Wide values.** `chunk_bytes` bounds a chunk, not one row, and Postgres
  ships one protocol message per row, so nothing caps a single value. A memory
  model that holds at 170 MB for 100 GB does not hold for a table with a PDF in
  it. Measured on v0.53.0, pg -> ClickHouse, `mode="replace"`, each point in its
  own process:

  | widest value | rows | fat payload | peak RSS |
  |---|---|---|---|
  | 1 MB | 3 | 3 MB | 28 MB |
  | 4 MB | 3 | 12 MB | 66 MB |
  | 8 MB | 3 | 24 MB | 120 MB |
  | 16 MB | 3 | 48 MB | 246 MB |
  | 32 MB | 3 | 96 MB | 525 MB |
  | 64 MB | 3 | 192 MB | 1037 MB |
  | 8 MB | 16 | 128 MB | 480 MB |
  | 8 MB | 64 | 512 MB | 1031 MB |

  Two things this corrects. **It is not "the widest row"** — an earlier version
  of this line said peak tracked row width, but 8 MB x 16 costs 480 MB where
  8 MB x 3 costs 120 MB, so the fat bytes a chunk holds is what matters, and
  both axes feed it. **`parallel=` does not help**: the same shapes at
  `parallel=1` measured 583 / 461 / 944 MB against 480 / 525 / 1037, which is
  noise. There is no tuning around this today.

  The working rule: budget about **5x the fat bytes in a chunk**, up to roughly
  a gigabyte where a chunk cap starts binding. In practice a 256 MB container
  is comfortable to ~8 MB values and at the wall by 16 MB.

  **It costs memory, not throughput.** Same 256 MB of payload, five value
  widths, n=3, median MB/s (pg -> ClickHouse, `replace`):

  | value width | rows | median MB/s | median peak RSS |
  |---|---|---|---|
  | 1 KiB | 262,144 | 176 | 226 MB |
  | 16 KiB | 16,384 | 185 | 233 MB |
  | 256 KiB | 1,024 | 202 | 264 MB |
  | 4 MiB | 64 | 171 | 494 MB |
  | 32 MiB | 8 | 144 | 1209 MB |

  Across a 32,000x range in value width the clock moves 1.4x and memory moves
  5.3x. The 32 MiB row is the noisiest (134-167 MB/s across the three runs)
  because the whole transfer is eight rows. So the failure to plan for is the
  container, not the schedule: throughput does not degrade as values grow, it
  stops when the process is killed.

  It corrupts nothing and it is not silent — the container OOMs, loudly.
  `benchmarks/e2e_review_gate.py` leg 3 measures it on every run and reports it
  as a KNOWN GAP rather than passing; the number in that output is the current
  state of this line.

  One caveat on that leg's number: it reads `RUSAGE_CHILDREN.ru_maxrss`, which
  is a HIGH-WATER MARK over every child the script has reaped, so it is only
  the wide-row child's peak as long as no earlier leg spawned a hungrier one.
  The table above was measured one point per process for that reason.

## Being stopped on purpose

A scheduler stopping a job is not an accident, and it is the most common way a
CDC run ends in production: Kubernetes evicts the pod, Airflow clears the run,
an operator restarts the service. All of them send SIGTERM first and SIGKILL a
few seconds later.

apitap used to die on the first signal. That was safe — the watermark is
written after the rows it covers and a replay is idempotent — but it was
expensive: everything the in-flight window had already drained went back to
the WAL and was read again next time. On a busy table that is minutes of work
lost per redeploy, every redeploy.

Now the first SIGTERM sets a flag. Both drains read it where they already read
their wall-clock deadline — between events, never inside a half-decoded frame
— so the window ends at the last COMPLETE commit, applies, and advances the
watermark exactly as a budget-limited window does. The process exits 0 with a
report of what it landed.

Details worth knowing before you rely on it:

- **A second SIGTERM is not absorbed.** The handler restores the default
  disposition and re-raises, so an operator who wants the process gone now
  gets it gone now. Once to ask, twice to insist.
- **An existing handler is kept.** Whatever SIGTERM pointed at before the run
  is called from inside apitap's, so `signal.signal(signal.SIGTERM, ...)` still
  works. This matters because CPython's handler only *schedules* your Python
  function — it runs when the interpreter next reaches the eval loop, which
  during a transfer is after the run returns. Chaining is what makes both
  happen: the drain stops now, your handler runs when control comes back.
- **Two dispositions are left alone**: `SIG_IGN`, and a handler installed with
  `SA_SIGINFO` (three arguments, which cannot be called safely through the
  one-argument prototype). Those hosts can call `apitap.request_stop()`
  instead — which is also how you stop a transfer running on another thread.
- **Nothing is interrupted mid-read.** If the source has gone quiet the flag
  is not seen until the next event or keepalive: seconds on a live connection,
  the second SIGTERM on a wedged one.
- **`APITAP_GRACEFUL_STOP=0`** turns the whole thing off and restores the
  kernel default. `e2e_sigterm.py` leg 0 runs with it set, on the same
  scenario, and requires the process to die by signal — the file's own proof
  that the legs below it are measuring something.

- **Only CDC.** A bulk `replace` / `append` / `merge` run installs no handler
  and still dies on the first signal. That is deliberate: a bulk load publishes
  at the swap, so there is no partial result worth landing, and the recovery is
  the killed-run row in the table above.

SIGKILL is not catchable and is not handled. If the apply outlives the
scheduler's grace period, SIGKILL still arrives, and that is still safe for
the reason it always was.

## The 24-hour soak, and what it did and did not settle

File-descriptor growth, replication-slot growth and watermark drift are
DURATION bugs: none of them can appear in the two-minute runs every other
measurement on this page is made from. So the shape apitap is actually deployed
in — a fresh process per drain, on a schedule, against a source being written to
the whole time — was run for a day.

`benchmarks/soak_cdc.py`, 2026-08-22, Postgres → ClickHouse, one drain every
30 s for 24 hours while a writer pushed inserts, updates and deletes
continuously:

| | first | max | last |
|---|---|---|---|
| peak RSS per drain | 20 MB | 25 MB | 25 MB |
| open file descriptors | 4 | 4 | 4 |
| replication slots | 1 | 2 | 1 |
| WAL retained by the slot | 128 KB | 289 MB | 2.5 MB |

**2,880 drains, 0 failures, 96 exact verifications.** Every 30th drain paused
the writer, let the drain catch up, and compared both sides on count, `sum(id)`
and `sum(touched)` — all 96 matched exactly.

The WAL figure is the one worth reading carefully, because "it went up" is not
the same as "it leaks": retention rises while a drain is behind and falls when
the watermark advances past it. A number that rose and never fell would be the
duration bug this was looking for. It rose to 289 MB and came back to 2.5 MB.

Each drain reports its OWN peak RSS from inside the child process. That is not
a detail: `RUSAGE_CHILDREN.ru_maxrss` in the parent is a high-water mark over
every child it has ever reaped, so across thousands of runs it can only rise —
it would have manufactured exactly the leak the soak existed to look for.

What this does NOT settle: a soak is one shape at one rate on one rig. It says
nothing about a month, about a source with a different write pattern, or about
the object-store destinations, which were not exercised.

## Two runs, one table

Before v0.55.0 every run of a destination table used the same staging object,
and `prepare` began by dropping it. Two overlapping runs therefore interleaved
destructively: A streams its rows in, B's prepare drops that object and creates
a fresh empty one, and A's finalize publishes B's empty table over the
destination. Measured against v0.54.0, the loser died with

    rename staging: relation "orders__apitap_staging" does not exist

— a catalog error naming an object the user never created, for a table they were
simply loading twice.

**The run's identity is now part of the staging name.** A run can only publish
an object whose name it minted, and that is enforced by the object system
itself. It needs no lock, which matters because advisory locks exist on
Postgres and MySQL and nowhere else: ClickHouse, BigQuery and the object stores
have nothing equivalent, and a marker written *beside* the staging object could
never be checked atomically with the publish on those engines.

`prepare` no longer drops anything blindly. It lists what is present, collects
only what it can PROVE is dead, and refuses to start beside anything else. The
one thing it can prove is the un-tokenized name an older apitap wrote: no
current run mints that name, so nothing living can own it.

**Not every peer is refused, and the distinction is the point:**

| this run | a live peer | outcome |
|---|---|---|
| `replace` | anything | refused — a swap replaces the whole table, so whichever finishes second throws the other's work away |
| `log_based` | anything | refused — a CDC drain owns the watermark and the replication slot |
| `append`/`merge` | `replace` or `log_based` | refused |
| `append`/`merge` | same source | refused — both would read the same watermark and land the same rows twice |
| `append`/`merge` | **different** source | **allowed** — this is fan-in, and `_apitap_state` keys watermarks per source precisely so it works |

That last row is why the guard is a matrix rather than a mutex. Refusing every
concurrent pair would have removed a capability the manual advertises.

**A crashed run's leftover is NOT collected for you, and that is deliberate.**
An earlier draft of this feature aged staging objects out after an hour, and
the review of all seven sinks killed it. The timestamp in the name records when
the RUN started, not when the object was created — table 40 of a long
multi-table run mints its staging under a token that is already hours old. So
`now - token` is an UPPER bound on the object's age and can never prove the
object is old, while the load it belongs to is still writing into it. On
Postgres the victim of a wrong guess dies loudly at its next statement; on
BigQuery and the object stores the deleted staging is silently re-created and
the run reports a full row count over a truncated table — which is the exact
defect this whole mechanism exists to remove, one horizon later.

Refusing is the safe action and collecting is the dangerous one, so they get
different standards of proof: anything that could be live is refused, and only
the provably-dead un-tokenized name is removed. Automatic collection needs a
liveness signal from the engine itself — an object mtime that advances as the
run writes, a catalog lock — which every engine has and every engine spells
differently. That is follow-up work, not something to guess at.

**What that costs you:** after a hard kill (SIGKILL, OOM, a lost node — every
ordinary error path still drops its own staging), the leftover stays, and later
runs of that table refuse until someone removes it. The error message names the
exact object to drop. It is the right trade against silently truncating a live
run's data, and it is the one operational cost this design knowingly accepts.

**The refusal has its own type.** It raises `apitap.LockedError`, a
`RuntimeError` subclass, so a scheduler branches on the class rather than
matching the message — the engine has always kept `Locked` as its own error
variant, and from 0.55.0 that distinction survives the trip into Python:

```python
try:
    apitap.transfer(src, dst, table="public.orders")
except apitap.LockedError:
    return          # someone else has it; nothing was written
```

**What the guard does NOT cover: two runs starting in the same instant.**
`prepare` lists the destination's catalog and then creates its staging object.
Two runs that start inside that gap both see an empty catalog and both proceed —
it is check-then-act, and this is the act it cannot see. Measured on Postgres,
the outcome is two-valued: usually both land (the swap serialises, last writer
wins), and sometimes the loser fails at `RENAME` with a duplicate-key error
instead of a clean `LockedError`.

Two things hold either way, and they are the ones that matter: **the destination
is whole afterwards, and no staging object is orphaned.** The defect this whole
mechanism exists to remove — a run reporting a full row count over a truncated
table — does not come back through this window; a same-instant collision is loud
or harmless, never silently wrong. `e2e_concurrent_runs.py` leg 6 asserts those
two invariants and deliberately does not assert the outcome, because the outcome
is genuinely non-deterministic.

Closing it needs announce-then-check ordering plus a deterministic tie-break —
without one, both runs discover each other and both refuse, trading a rare wrong
error for a rare double failure. That is a design change across all seven
destinations, so it is follow-up work rather than something to improvise. Until
then the scheduler setting below is what actually prevents it.

**Why it fails instead of waiting.** The common cause of two runs on one table
is a scheduler overrun, and waiting turns an overrun into a queue, a queue into
a pile-up, and a pile-up into resource exhaustion at 3am with every run still
reporting healthy. Failing on the first occurrence makes the overrun visible
while it is still cheap. Retry and backoff belong to the scheduler, which
already has them; a clean non-zero exit composes with all of them, and a hang
composes with none.

## Rules of thumb

1. **Re-running is the recovery.** Every failure mode above is repaired by
   running the same call again. If you find one that is not, that is a bug worth
   reporting.
2. **A killed run costs disk, never correctness.** Orphaned staging tables are
   the one mess left behind; they are named `<dest>__apitap_staging` and the
   next run removes them.
3. **The dangerous direction is a paused schedule, not a failed run.** A failure
   is loud and idempotent. A schedule that quietly stops is what fills a
   Postgres disk or outruns a MySQL binlog — the two cases apitap now reports
   and refuses on, respectively.

## The hole that was open, and how it closed

A binlog coordinate is `(file name, position)`, and after a server's log is
reset the names start again at `000001`. Two shapes were already refused — the
file is gone, or the stored position is ahead of the server's — but a third was
not: **a reset log that has since grown past the old position**. The name
matches, the position exists, and the bytes there belong to different history.
A drain resumed into it and reported success.

That is closed. apitap now records the source server's identity next to the
watermark (`@@server_uuid`, or `@@server_id` where MariaDB offers no uuid) and
refuses to resume against a different one — which covers the reset log, a
promoted replica, a restored backup, and a DNS record moved during a failover,
all of which look identical from a connection string. A table with no recorded
identity adopts what it is reading now; from the run after that, a switch is
refused. Proven in `benchmarks/e2e_review_gate.py` leg 4, which bootstraps
against one server and then points the same table at another.

When that refusal fires — after restoring a source from a backup, rebuilding a
replica, `RESET MASTER`, or repointing a URL at a different server — the
recovery is the same as the purged-binlog row above: clear that table's state
on the destination and let the next run bootstrap against the server it is
actually reading. The refusal is the feature; what it replaced was a drain
that resumed into foreign history and reported success.
