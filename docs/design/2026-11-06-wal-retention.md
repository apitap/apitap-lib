# WAL / binlog retention under batch CDC: findings and work queue

Status: **design only, nothing implemented.** Written 2026-11-06 from a code review
of `crates/apitap-core/src/logbased/` after the market research
(`reports/Daya saing apitap di pasar.md`). Line numbers are as of commit `f4c82ad`
plus the uncommitted 0.66 tree; re-grep before editing.

## 0. Positioning (decided by the user, do not re-litigate)

apitap CDC is **batch by design**, not "real-time that isn't finished yet". The
pitch is: CDC with no extra infrastructure. It is a Python lib, one Airflow task
in a DAG you already have. Debezium needs Kafka + Connect, PeerDB needs its own
server + Temporal, and Estuary/Fivetran are SaaS.

The price of batch is the retention gap between runs. That is the first question
every DBA asks, so the goal of this work is to make it **bounded, self-healing and
visible from outside the run**. Retention cannot be eliminated, because that is
how logical replication works. Do not propose sub-second streaming as the answer.

## 1. The risk is asymmetric

| | Postgres | MySQL / MariaDB |
|---|---|---|
| What the server keeps for apitap | every WAL segment the slot has not confirmed | nothing; binlogs purge on the server's own clock (`binlog_expire_logs_seconds`, `expire_logs_days`, `PURGE BINARY LOGS`) |
| Run stopped too long (paused DAG, dead scheduler, long weekend) | WAL piles up, the **source disk** fills, and the primary goes down | resume position purged: a **data hole**, and a full re-bootstrap is needed |
| Side note | a slot retains the WAL of the **whole cluster**, not only the published tables, so busy unrelated tables inflate it | apitap adds zero disk cost to the source; the binlog exists anyway for replication and backups |

## 2. What already exists (verified in code)

### Postgres (`logbased/run.rs`)

- `slot_wal_report` (`run.rs:3285`), called every drain at `run.rs:2032`:
  - Emits gauge `slot.wal` with `retained_bytes`, `warn_at_bytes` and `over_threshold`.
  - Warns past `APITAP_SLOT_WAL_WARN` (default 4 GiB). The warning mentions it when
    `max_slot_wal_keep_size` is unlimited (`-1`).
  - Best-effort: permission errors are swallowed.
- `other_slots_report` (`run.rs:3246`):
  - Reports inactive `apitap_%` slots that publish this run's tables, for example a
    slot orphaned by a change of destination, group or `slots=`.
  - Emits gauge `slot.other` and prints the `pg_drop_replication_slot` + `DROP PUBLICATION`
    statement. It never drops anything itself, because another destination may own the slot.
- Lost slot: `wal_status = 'lost'` (PG 13+, `slot_status_lost` at `run.rs:3166`) returns an
  error with a **manual** remedy (`run.rs:2052`): drop the slot, clear `_apitap_state`, re-run.
- Missing slot with an existing watermark: error at `run.rs:2073`, same manual remedy.
- Active slot means another drain is running: refused.
- Standby awareness: `WAL_END` (`run.rs:2885`) reads `pg_last_wal_replay_lsn()` when
  `pg_is_in_recovery()`. The intent is PG 16 logical decoding on a standby.
  - **No gate leg proves this end-to-end.** `grep standby benchmarks/` only hits unrelated files.
- README already documents the gauge and the knob (`README.md:465`, `:489`), and the manual
  re-bootstrap (`README.md` "What it does not do").

### MySQL / MariaDB (`logbased/mysource.rs`, `logbased/myrun.rs`)

- `binlog_retention_report` (`mysource.rs:136`), called at `myrun.rs:368`:
  - Emits gauge `binlog.retention` with `resume_file`, `files_retained`,
    `files_before_ours`, `retained_bytes` and `at_purge_edge`.
  - Warns when the resume file is the oldest the server still has.
  - **Measured in files, not time.**
- Purged resume file: checked before `COM_BINLOG_DUMP` (`binlog_file_present`,
  `mysource.rs:184`). The error at `myrun.rs:337` gives a **manual** remedy instead of the
  raw error 1236.
- Stored position AHEAD of the server's position (RESET MASTER, restored backup, other
  server): refused at `myrun.rs:308`.
- Server fingerprint (`@@server_uuid`, or `@@server_id` on MariaDB): a different server
  behind the same URL is refused.
- GTID events are **read and skipped** (`wire/mybinlog.rs:1240`). The watermark is
  `(file, pos)` only.

### Tests

- `benchmarks/e2e_cdc_retention.py` proves both halves against live servers:
  - It purges a MariaDB binlog on purpose and checks the refusal.
  - It reads a slot's retained WAL from Postgres.
- Extend this file, or add siblings, for the work below.

## 3. Gaps

1. **Every check runs only when a run runs.** The dangerous case is exactly when runs stop,
   and then nothing reports.
2. **Recovery is manual.** A lost or missing slot, or a purged binlog, needs a human to drop
   the slot, clear `_apitap_state` and re-run.
3. **MySQL retention is reported in files, not hours.** Operators ask "how long until my
   position is gone".
4. **Postgres does not report `safe_wal_size`** (PG 13+): the bytes left before the cap
   invalidates the slot.
5. **No preflight for the common traps.**
   - Postgres with `max_slot_wal_keep_size = -1`. Today this is only a warning, and only past 4 GiB.
   - RDS/Aurora MySQL `binlog retention hours = NULL`. Binlogs are purged as soon as
     possible, so hourly batch CDC breaks unless this is changed.
     **Unverified on real RDS. Verify before shipping a check.**
   - Cloud SQL / Azure equivalents: not researched.
6. **Standby decoding (PG 16+) is untested.** This is the strongest DBA answer: WAL is held
   on the replica and the primary disk is untouched.
7. **No GTID resume on MySQL.** Every failover to a promoted replica forces a full
   re-bootstrap, and failover is routine in production MySQL.

## 4. Work queue (order agreed with the user)

### W1: `on_lost="rebootstrap"` (Postgres + MySQL)

New `transfer()` argument.

- Default `"error"`, which keeps today's behaviour. The other value is `"rebootstrap"`.
- Triggers:
  - PG: `wal_status = 'lost'`, or the slot is gone while a watermark exists.
  - MySQL: the resume binlog is purged.
- Action:
  - PG: drop the lost slot and its publication.
  - Both: clear the group's `_apitap_state` rows, then run the normal bootstrap
    (snapshot + new slot/position) **in the same call**.
  - The report must say it happened, through a gauge plus a `warn`, with the reason and the
    old watermark.
- **Do NOT** auto-rebootstrap on "position AHEAD of server" or "server fingerprint changed".
  Those mean a different server, and must stay refused until GTID exists (W5).
- Group semantics: one slot covers many tables, so the whole group re-bootstraps together.
  `slots=N` shards are independent; re-bootstrap only the lost shard if the state layout
  allows it, otherwise the whole group, and document which.
- Keep the user API one line (memory: `one-line-api-mandate`).
- Tests (memory: `control-the-test-first`): the leg must go RED with `on_lost="error"`
  and GREEN with `"rebootstrap"`.
  - PG lost: create the slot with a tiny `max_slot_wal_keep_size`, generate WAL past it,
    and assert `wal_status='lost'` from the SERVER before running.
  - MySQL: purge the binlog, as `e2e_cdc_retention.py` already does.
  - After rebootstrap: the destination equals the source by checksum, not by exit code
    (memory: `cdc-test-traps`).

### W2: `apitap.slots(src)`: a health check outside the run

One-line call that returns per-pipeline health without draining anything. It is meant to run
as a separate Airflow sensor every few minutes, independent of the CDC DAG.

- PG fields per `apitap_%` slot:
  - `slot`, `active`, `wal_status`, `retained_bytes` (`pg_wal_lsn_diff(WAL_END, restart_lsn)`)
  - `safe_wal_size` (PG 13+, NULL when uncapped), the server's `max_slot_wal_keep_size`
  - tables published (via `<slot>_pub`)
  - age of the last confirm, if derivable. Otherwise store `last_drain_at` in
    `_apitap_state` and read it from the destination; decide which.
- MySQL fields:
  - resume file and position, `files_before_ours`, `binlog_expire_logs_seconds`
    (or `expire_logs_days` on old MariaDB)
  - `retention_left_seconds`. Derive it from the timestamp of the last applied event
    (binlog event header) or from a stored `last_drain_at`.
- Returns structured data, and optionally `raise_if=` thresholds so an Airflow task fails
  loudly.
- Must not need write privileges. Degrade per field on missing privilege, never fail the call.
- Engine-side in Rust (memory: `engine-work-in-rust`); the Python shim only exposes it.

### W3: Preflight checks on the first `log_based` run

- PG `max_slot_wal_keep_size = -1`: warn at bootstrap, not only past 4 GiB. Suggest a size.
  Do not refuse; that is the user's server.
- MySQL on RDS/Aurora (detect via `@@basedir` / `@@version_comment` / `rds_*` variables;
  verify the signal first) with binlog retention NULL or shorter than a sane floor: refuse
  with `CALL mysql.rds_set_configuration('binlog retention hours', N)`. Verify the exact
  behaviour on a real RDS instance or AWS docs before writing the check.
- `binlog_expire_logs_seconds` shorter than, say, 24 h: warn.

### W4: Logical decoding on a PG 16+ standby

- Add a gate leg: primary + streaming standby with `hot_standby_feedback=on`, slot created on
  the standby, bootstrap + drain from the standby.
- Assert the primary's `pg_replication_slots` is empty, and that the primary's WAL is not
  held.
- Cover the known standby edges:
  - slot invalidation on catalog-row conflict (`wal_status`/`conflicting` column, PG 16);
  - exported snapshot from a standby (check bootstrap consistency);
  - promotion of the standby. PG 17 failover slots are a separate question.
- Document it as the recommended setup for DBAs who will not put a slot on the primary.

### W5: MySQL GTID resume

- Store the GTID set (MySQL `gtid_executed` style, MariaDB `domain-server-seq`) alongside
  `(file, pos)` in `_apitap_state`. On fingerprint mismatch or a missing file, try to resume
  by GTID (`COM_BINLOG_DUMP_GTID` for MySQL, `SET @slave_connect_state` for MariaDB).
- Effect: failover to a promoted replica resumes instead of re-bootstrapping.
- Largest item. Needs its own design pass and gate legs: MySQL 8.0/8.4 and MariaDB 10.6+,
  failover simulation, GTID gaps, `gtid_mode=OFF` fallback to today's behaviour.

### W6: `apitap.wal_plan(src, every="1h")` (sizing helper, docs)

- PG: samples `pg_current_wal_lsn()` over N seconds and recommends `max_slot_wal_keep_size`
  = rate × interval × safety factor. States that the rate is cluster-wide.
- MySQL: recommends a retention in hours relative to `every`.

### W7 (later, only on demand): split capture from apply

A small frequent "capture" drain writes changes to spill on local disk or S3 (build on
`logbased/spill.rs`) and confirms the slot. The batch apply reads the spill. This moves
retention from the source disk to cheap storage. Large change; do not start without the
user's go.

## 5. Rules that apply (from memory)

- cargo build/check/test **only on the VPS** (`no-local-builds`). Touch synced sources before
  cargo (`rsync-mtime-stale-build`). Use rsync with absolute paths only (`rsync-absolute-paths`).
- Run `gate.py --matrix` after registering any leg (`gate-matrix-after-register`). Never
  timeout-kill a gate leg (`no-timeout-kill-gate-legs`), because it leaves slots behind and the
  next leg fails.
- Every new test must be seen RED without the fix (`control-the-test-first`). Ask the server
  for the fact, not the exit code (`cdc-test-traps`).
- Check `df` before gates (`vps-disk-full-lab-ch`). Tiny-`max_slot_wal_keep_size` legs must
  set the value back afterwards.
- Commit and push only when the user asks; no Co-Authored-By Claude trailer (`no-claude-attribution`).

## 6. Pitch once W1–W3 ship

"CDC without extra infrastructure: one Airflow task. The source disk is safe: the slot is
capped, a lost slot heals itself with one reload, and a separate sensor warns you hours before
anything breaks." Add after W4: "or put the slot on a replica and the primary is never touched."
