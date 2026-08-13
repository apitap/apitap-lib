# Design: `mode="log_based"` — batch CDC from Postgres

Status: **design for review** — no code yet.
Sources studied (full-code reads, 2026-08-02): [ape-dts](https://github.com/apecloud/ape-dts)
(`dt-connector/src/extractor/pg/*`, pipeline/parallelizer/sinker, resumer, docs) and
[PipelineWise](https://github.com/transferwise/pipelinewise)
(`tap-postgres/sync_strategies/logical_replication.py`, fastsync commons, cli).

## One sentence

Each run drains a Postgres **logical replication slot** since the last
destination-committed LSN, collapses the window per primary key, applies it
**set-based at wire speed** (staging COPY + one DELETE + one upsert, or an
Iceberg row-delta commit), commits the new LSN **in the same destination
transaction as the data**, and only then tells Postgres the WAL may be
discarded — PipelineWise's batch cadence, ape-dts's capture fidelity,
apitap's apply speed.

```python
apitap.transfer(src_pg, dst, table="public.orders", mode="log_based")
# first run: slot + gap-free full load; every later run: drain the delta
```

## Why each parent, and what we fix

| | keep | fix |
|---|---|---|
| **PipelineWise LOG_BASED** | run-bounded drain (`end_lsn` stop-line captured at run start), state advanced only after destination flush, fastsync-then-cdc derived from state | per-stream `min()` LSN disease (one quiet table pins the slot forever → unbounded WAL); bookmarks at `msg.data_start` (mid-transaction resume); state file as racy IPC; TRUNCATE silently dropped; overlap-window handoff (duplicates by design); wal2json dependency (not in stock PG) |
| **ape-dts CDC** | pgoutput protocol handling (proto v1 text tuples), slot bootstrap + confirmed_flush clamp, commit-boundary-only positions (rows stamped with the *previous* tx end LSN), ack-after-destination-commit, Relation-message column ordering, `RdbMerger` collapse (delete-set + last-write-wins upsert-set + ordered residue), heartbeat table, replica-identity precheck script | realtime daemon (we want runs); apply speed (~10K rows/s: batch-200 row SQL — their own docs' bench table); manual snapshot→CDC handoff (operator copies an LSN by hand); silent failure modes (REPLICA IDENTITY NOTHING → keyless deletes, empty-string→NULL conversion bug, `.unwrap()` on unknown type OIDs) |

## The run lifecycle

Every `log_based` run is the same function; "first run" is just the branch
where no state exists.

```
1  connect (regular SQL) to source; connect (replication=database) walsender
2  ensure publication (FOR TABLE <list>, not FOR ALL TABLES)
3  read dest state: lsn watermark from _apitap_state / iceberg props
4  slot exists?
     no  → CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput EXPORT_SNAPSHOT
           → (consistent_point, snapshot_name)
           → FULL LOAD: existing snapshot path, but inside
             REPEATABLE READ + SET TRANSACTION SNAPSHOT <snapshot_name>
             (range pipes each SET the same snapshot — exact, no overlap)
           → dest commit writes rows + watermark = consistent_point
           → done (this run was the bootstrap)
     yes → reconcile: wm < confirmed_flush_lsn → ERROR (impossible unless
           state was tampered; ape-dts silently clamps — we don't);
           wm ≥ confirmed_flush → start from wm (crash-after-commit-
           before-ack replays idempotently)
5  stop-line = pg_current_wal_lsn()   (PipelineWise's break_at_end_lsn)
6  heartbeat: INSERT into apitap._heartbeat (published, never synced)
   — guarantees confirmable WAL even on an idle source
7  START_REPLICATION SLOT <slot> LOGICAL <wm>
     ("proto_version" '1', "publication_names" '<pub>')
   session first: SET extra_float_digits=3; SET TIME ZONE 'UTC'
8  drain: decode pgoutput events, collapse per table (below);
   STOP at the first Commit whose end_lsn ≥ stop-line, or when a
   keepalive says caught-up; never mid-transaction; max_run_seconds guard
9  apply per destination (below), dest tx also writes watermark = last
   collapsed Commit end_lsn
10 standby_status_update(wm, wm, wm, now, reply=1)  ← only now may PG
   discard WAL; then disconnect
```

Crash anywhere before 9's commit: nothing moved, next run replays from the
old watermark. Crash between 9 and 10: watermark > confirmed_flush; next
run's replay is absorbed by the idempotent apply. **At-least-once transport,
exactly-once effect.**

## Capture fidelity (the ape-dts bar — all operations)

pgoutput proto v1, text tuples (binary option deliberately omitted, like
ape-dts — text sidesteps per-type binary decoders and feeds our existing
text→typed encoders; `extra_float_digits=3` + UTC make it lossless).

- **INSERT** → upsert row.
- **UPDATE** → key from before-image chain: `old_tuple` (RI FULL) →
  `key_tuple` (RI DEFAULT/USING INDEX) → project PK from after-image
  (pgoutput omits the old tuple when the key didn't change). Collapse as
  delete(old key) + insert(new row) — PK-changing updates come out correct
  for free.
- **DELETE** → key from `old_tuple`/`key_tuple` → delete-set entry.
- **TRUNCATE** → captured (PipelineWise drops it!): flushes the collapse
  buffer for that table, applies as a truncate/DELETE-all event in
  sequence, then continues. Iceberg: an overwrite-to-empty snapshot… v1
  applies it as delete-all within the run's commit.
- **Transaction boundaries** → rows carry the *previous* tx end LSN;
  only `Commit.end_lsn` values are candidate watermarks (ape-dts's
  load-bearing discipline, copied exactly).
- **Relation messages** → per-run oid→meta map, columns reordered to WAL
  order (never trust catalog order); fresh connection per run means fresh
  Relation messages before each table's first row — the periodic-run shape
  deletes ape-dts's cache-invalidation problem entirely.
- **TOAST**: `TupleData` has three arms — Null, Text, **UnchangedToast**.
  An update whose row holds an unchanged TOASTed column omits that value.
  Policy (the biggest correctness trap in all of batch CDC):
  those rows are routed out of the fat upsert path into a **column-masked
  UPDATE residue** (per distinct present-column mask, still batched);
  a naive full-row upsert would null real data. `REPLICA IDENTITY FULL`
  on the table makes the fast path universal — documented, not required.
- **Loud-error guards** (ape-dts/PipelineWise fail silently; we refuse):
  table with `REPLICA IDENTITY NOTHING` or no PK/unique identity → error
  at prepare with the exact `ALTER TABLE … REPLICA IDENTITY` to run;
  unknown type OID → error naming the column (their code panics/unwraps);
  empty-string ≠ NULL (their converter conflates them — we must not,
  Null is a separate TupleData arm).

## The collapse (ape-dts RdbMerger, adapted)

Per table over the drained window, keyed by replica-identity columns:
- last event wins; update = delete(before-key) + insert(after-row);
- insert-then-delete nets to **delete** (kept — dropping it leaves phantoms);
- residue (ordered, applied after the set phase): key-column NULL,
  unchanged-TOAST masks, hash-collision double-check failures.
- Output per table: `deletes: Vec<Key>`, `upserts: Vec<Row>`,
  `residue: Vec<Event>` — sized by the window, not the table.

## Apply per destination (where apitap earns its keep)

One destination transaction per run (pg/mysql), or one snapshot commit
(iceberg):

- **postgres**: COPY-binary the upsert rows into an UNLOGGED staging table
  (existing encoder — the text tuples render through the same
  `Delivered`-typed paths the MySQL source already uses), then
  `DELETE FROM t USING keys`, `INSERT … ON CONFLICT (id) DO UPDATE`,
  residue as masked UPDATEs, watermark row — all one transaction.
- **mysql**: LOAD DATA staging + `DELETE t JOIN keys` + upsert + watermark
  (state table write is post-swap like today's merge — same at-least-once
  contract, absorbed by idempotence).
- **iceberg**: the delta IS a row-delta commit — equality-delete file =
  delete-set ∪ upsert-keys, data files = upsert rows, watermark = table
  property, **one atomic snapshot**. The machinery shipped in v0.16.0;
  log_based rides it unchanged.
- **bigquery**: each window lands in an all-STRING staging table
  (WRITE_TRUNCATE → replay-idempotent) and applies with ONE `MERGE` inside a
  multi-statement transaction that also advances the watermark. Column types
  come from the destination's own DDL, so a MySQL binlog source (every OID 0)
  applies through the same path and needs no source pool. Unchanged-TOAST
  cells ride a per-row mask column resolved inside the MERGE
  (`IF(masked, T.c, S.c)`). Requires a billed project — CDC uses row-level DML.
  BigQuery is LATENCY-bound (each window is a load + MERGE job round-trip, ~0
  local CPU), so it diverges from the CPU-bound paths: it fills a bigger window
  (lever `APITAP_CDC_WINDOW_BYTES`), applies a group's tables CONCURRENTLY,
  bootstraps a group's tables with a bounded fan-out, and clusters a large
  target on its PK at bootstrap so the MERGE prunes (lever `APITAP_BQ_CLUSTER=0`;
  auto-skipped below ~1M rows where the MERGE floor dominates). Measured wins at
  0.5 cpu / 256 MB: CDC 1.9× (single) / 3.5× (10-table group), full-load 3.6×
  (10-table group). See `benchmarks/bq-cdc-optimize.md`.
- **gcs / s3**: deferred loudly in v1 with a clear error (append-only object
  stores need a compaction design worth its own session).

Expected apply rate is the existing merge path's (~450K rows/s into pg on
the bench box) vs ape-dts's ~10K rows/s row-SQL sinker — the 45× wedge is
the point of doing CDC *in apitap* rather than next to it.

## State

`_apitap_state` grows two nullable columns: `kind` (`cursor` | `lsn`) and
the slot name; one row per (dest_table, source). For the slot itself there
is ONE lsn per (source db, slot) — not per table (kills PipelineWise's
`min()` disease); multi-table tasks share a slot + publication and commit
one watermark. Iceberg: `apitap.watermark.*` properties, same shape.

## Driver decision (RESOLVED)

Mainline `tokio-postgres`/sqlx can't open `replication=database`
connections; ape-dts pins an apecloud fork of rust-postgres. Our vendored
crate turns out to be `sqlx-core` only — the Postgres protocol lives in
the unvendored `sqlx-postgres`, so "patch the vendor" would mean vendoring
a large new crate. **Decision: hand-roll a minimal walsender client**
(`wire/walsender.rs`): startup packet with `replication=database`, auth
(SCRAM-SHA-256 / md5 / cleartext — sha2+hmac already in the tree), simple
query for the replication commands, CopyBoth framing (XLogData in,
standby-status-update out). ~600–900 lines including the pgoutput decoder
that every option needs anyway; the SigV4/Iceberg-REST precedent says the
hand-rolled protocol layers end up the most robust code we own. Regular
SQL (state reads, prechecks, pg_current_wal_lsn) stays on sqlx over a
normal connection.

## Scope v1

pg source only; pg/mysql/iceberg dests; single-column-PK-or-identity
tables (composite identity keys work for delete/upsert but merge-key
capture generalizes — same limit as iceberg merge today); no DDL sync
(start-of-run schema check errors loudly on drift, like today); no
two-phase-commit tx (pgoutput v1 ignores prepared tx until commit —
fine); runs assume `wal_level=logical` and warn about
`max_slot_wal_keep_size` in docs (an unconsumed slot retains WAL forever).

## Bench plan (server, per house rules)

pgbench-style mixed workload on the 11.8M-row rig: initial bootstrap vs
apitap replace (should be ≈ equal — same path + snapshot pin), then drains
at 100K/1M/5M-event windows vs ape-dts CDC catch-up on identical windows
(their config: `parallel_type=rdb_merge`, batch 200) and vs PipelineWise
tap-postgres→target-postgres. Validate: row counts + checksums after every
window, including delete-heavy and PK-update-heavy mixes, TOAST columns,
and a kill-mid-apply crash-replay test.
