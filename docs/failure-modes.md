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
  Until that swap, readers see the previous table, whole. There is no window in
  which a query returns half a load.
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
- **Long-duration soak.** File-descriptor growth, slot growth and watermark
  drift are duration bugs; a 24–48 hour CDC soak has not been run. The longest
  measured run is a 200-million-change campaign over ~2 minutes of apply work
  plus its bootstrap.
- **Destination-side crash mid-apply** (ClickHouse or BigQuery restarting under
  us). The apply is idempotent by design and the watermark is written last, so
  the expectation is a clean replay — again, expectation, not receipt.
- **A single very wide value.** `chunk_bytes` bounds a chunk, not one row, and
  Postgres ships one protocol message per row — so a table with 32 MB values
  drives peak RSS by row width instead of by the budget. Measured: three 32 MB
  rows peak at ~440 MB. It does not corrupt anything and it is not silent (the
  container OOMs, loudly), but a memory model that holds at 170 MB for 100 GB
  does not hold for a table with a PDF in it. `benchmarks/e2e_review_gate.py`
  leg 3 measures it on every run and reports it as a KNOWN GAP rather than
  passing — the number in that output is the current state of this line.

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

Closing this needs the watermark to carry the server's identity (`server_uuid`
on MySQL, the GTID domain on MariaDB) so a changed identity refuses on sight.
That is a change to the destination's state schema and is not in this release —
recorded here rather than left for someone to discover.

Until then, treat these as bootstrap-again events: restoring a source from a
backup, rebuilding a replica, `RESET MASTER`, or repointing a URL at a
different server.
