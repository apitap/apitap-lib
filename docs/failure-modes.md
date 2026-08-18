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
- **A CDC watermark advances only with its data, in the same transaction.** So a
  window either landed and was recorded, or neither. Replay is idempotent by
  construction: the apply clears the keys it is about to write before writing
  them.

## The table

| what happened | state left behind | recovery | verified |
|---|---|---|---|
| **Process SIGKILLed mid bulk transfer** | Previous destination table **intact and readable throughout** (proven: 1,000 rows and their marker unchanged while 10M rows were streaming into staging). A staging table `<dest>__apitap_staging` may be orphaned. | Just re-run. The next run starts with `DROP TABLE IF EXISTS` on staging, so the orphan costs disk until then, nothing more. Proven: re-run landed 10,000,000 rows = source count. | `e2e_failure_modes.py` case 1 |
| **Process SIGKILLed mid CDC window** | Watermark **unmoved** — the destination is exactly where the last completed window left it. | Re-run. Every change is applied exactly once (proven by digest, not by row count alone: 4,000 rows and `sum(id)` identical to the source after the kill + replay). | case 2 |
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

## The hole that is still open

A binlog coordinate is `(file name, position)`, and after a server's log is
reset the names start again at `000001`. apitap now refuses the two shapes it
can see — the file is gone, or the stored position is ahead of the server's —
but there is a third it cannot: **a reset log that has since grown past the old
position**. The name matches, the position exists, and the bytes there belong
to different history. A drain would resume into it and report success.

Closing this needs the watermark to carry the server's identity (`server_uuid`
on MySQL, the GTID domain on MariaDB) so a changed identity refuses on sight.
That is a change to the destination's state schema and is not in this release —
recorded here rather than left for someone to discover.

Until then, treat these as bootstrap-again events: restoring a source from a
backup, rebuilding a replica, `RESET MASTER`, or repointing a URL at a
different server.
