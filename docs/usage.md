# apitap — usage guide

apitap copies whole tables between databases, fast: one function, no config files, no
pipeline DAGs. This guide covers everything the library does today. For benchmark
methodology see [benchmarks/README.md](../benchmarks/README.md); for the architecture
see the [README](../README.md#architecture-adding-a-database).

## Install

```bash
pip install apitap
```

Wheels currently ship for Linux x86-64 (Python ≥ 3.9, one abi3 wheel for all
versions). No Rust toolchain needed.

## Quickstart

```python
import apitap

report = apitap.transfer(
    "postgres://user:pass@src-host:5432/db",
    "postgres://user:pass@dst-host:5432/db",
    table="public.events",
)
print(f"{report.rows:,} rows in {report.elapsed_ms} ms over {report.parallel} pipes")
```

One call = one full-table copy: the source table is read in parallel key ranges,
streamed into a staging table on the destination, and atomically swapped in. The
route is picked from the URL schemes:

```python
# Postgres → Postgres: raw binary COPY passthrough (no row decode at all)
apitap.transfer("postgres://…/srcdb", "postgres://…/dstdb", table="public.events")

# Postgres → ClickHouse: binary COPY transcoded in-flight to RowBinary
apitap.transfer("postgres://…/srcdb", "clickhouse://default:pass@ch-host:8123/db",
                table="public.events")

# MySQL → ClickHouse: binary wire protocol decoded straight into RowBinary
apitap.transfer("mysql://root:pass@my-host:3306/srcdb", "clickhouse://…/db",
                table="events")

# MySQL → Postgres: wire decode → binary COPY (lossless, exact decimals)
apitap.transfer("mysql://…/srcdb", "postgres://…/dstdb", table="events")
```

## Connection URLs

| database | scheme | notes |
|---|---|---|
| Postgres | `postgres://` or `postgresql://` | standard DSN: `postgres://user:pass@host:5432/db` |
| MySQL | `mysql://` | `mysql://user:pass@host:3306/db` |
| ClickHouse | `clickhouse://` | HTTP interface: `clickhouse://user:pass@host:8123/db`. Port defaults to 8123; `clickhouse+https://` (or port 8443) switches to TLS. |

Table names may be schema-qualified (`public.events`, `mydb.events`); unqualified
Postgres names resolve through the connection's `search_path`. Materialized views
work as Postgres sources.

## API

### `apitap.transfer(src, dst, table, *, dest_table=None, parallel=None, cursor=None, chunk_bytes=None, durable=True) -> TransferReport`

| parameter | default | meaning |
|---|---|---|
| `src`, `dst` | — | connection URLs (see above); the pair picks the route |
| `table` | — | source table, optionally schema-qualified |
| `dest_table` | same as `table` | destination table name |
| `parallel` | auto | concurrent range pipes. Auto derives from the container's CPU count **and** cgroup memory limit (details below). An explicit value is never overridden. `0` is rejected. |
| `cursor` | auto | numeric column used to split the table into ranges. Auto-detects a single-column integer primary key. |
| `chunk_bytes` | 4 MiB | bytes coalesced per network send (floor 64 KiB) |
| `durable` | `True` | Postgres destinations only — see [Durability](#durability) |

### `TransferReport`

| field | meaning |
|---|---|
| `rows` | rows landed in the destination |
| `elapsed_ms` | wall-clock duration, including connection time |
| `parallel` | pipes actually used (`0` = empty source, `1` = single stream) |

Errors raise `ValueError` for invalid input (unknown table, bad URL, unsupported
type, `parallel=0`) and `RuntimeError` for transfer failures. A failed transfer
never touches the existing destination table.

## How a transfer runs

1. **Probe** — the source catalog is read once: columns, native types, nullability,
   primary key.
2. **Wire-format negotiation** — the fastest format both sides speak is picked
   automatically. Postgres→ClickHouse uses binary→`RowBinary` transcoding when every
   column has a binary mapping, and falls back to text↔`TabSeparated` (with
   server-side casts) when one doesn't — the copy still runs, just through the text
   lane.
3. **Staging** — rows land in `<dest_table>__apitap_staging`, never in the live table.
4. **Parallel spans** — the table is split into many contiguous key ranges feeding a
   work-stealing queue; each pipe holds one connection on each side.
5. **Atomic swap** — Postgres: `DROP` + `RENAME` in one transaction; ClickHouse:
   `EXCHANGE TABLES`. Readers never observe a partial table.

### Parallelism and memory

Auto `parallel` is route-specific (measured, not guessed): Postgres→Postgres uses up
to 8 pipes (destination COPY is writer-bound), ClickHouse destinations up to 32,
MySQL→Postgres up to 16. The auto value is then **capped by the cgroup memory
limit** — each pipe budgets ~8 × `chunk_bytes` — so the same code that uses 32 pipes
on a big host uses 5 in a 256 MB container instead of getting OOM-killed. Memory use
is `parallel × chunk_bytes`-scale regardless of table size; bytes stream with TCP
backpressure.

### Cursor and PK-less tables

Range splitting needs a numeric column. By default apitap uses the table's
single-column integer primary key. Without one:

- **Postgres sources** fall back to physical TID ranges (PostgreSQL 14+) — full
  parallelism with no index at all.
- **MySQL sources** fall back to a single stream.

Pass `cursor="some_int_column"` to split on any other numeric column (works for
non-PK columns; rows where the cursor is NULL are not covered by range predicates —
prefer NOT NULL columns).

## Incremental sync (append & merge)

```python
# only rows past the destination's current max(cursor) — stateless watermark
apitap.transfer(src, dst, table="public.events", mode="append")

# upsert changed + new rows by the destination's PRIMARY KEY
apitap.transfer(src, dst, table="public.events", mode="merge", cursor="updated_at")
```

- **The watermark lives in `_apitap_state`, a small table in the destination
  database** — one row per (destination table, source), holding the exact
  `max(cursor)` value of the last successful load, the mode, row count, and sync
  time. On Postgres it is written **in the same transaction** as the data, so state
  and data can never drift apart; because it lives in the destination DB it is also
  backed up and restored *together with* the data (restore from a backup and the
  watermark rewinds with it). The source key is credential-free
  (`postgres://host:5432/db::table`). Per-source rows make fan-in safe: two sources
  appending into one table keep independent watermarks. If the state row is missing
  (tables built by older versions, or by plain replace), the run falls back once to
  deriving the watermark from the data and writes the row.
- Because the watermark comes from the state row — not from `max()` over the data —
  **other processes may write to the destination table without corrupting the
  sync**.
- **Resets behave sanely**: `mode="replace"` clears every state row for the
  destination (all sources — the swap destroyed their rows too), and a `TRUNCATE`d
  destination is treated as watermark-less (full resync on the next incremental
  run). Adding a *second* source to a destination that already has state rows
  requires an explicit choice — the run fails loudly instead of guessing a
  watermark; seed a state row manually or rebuild with replace.
- Avoid running two syncs of the same (source, destination) pair concurrently —
  they would each read the same watermark and land the same delta twice. (The
  ClickHouse state table is a `ReplacingMergeTree`; it self-compacts old state
  versions in the background.)
- **`append`** loads rows with `cursor >` watermark and lands them atomically
  (Postgres: one transaction; ClickHouse: a metadata-only partition attach — the
  append-mode sibling of `EXCHANGE TABLES`). Cost is proportional to the delta, not
  the table: 1M new rows appended onto a 10M-row table in ~10 s.
- **`merge`** (Postgres destinations) loads rows with `cursor >=` watermark and
  upserts them by the destination's PRIMARY KEY (`INSERT … ON CONFLICT DO UPDATE`,
  deduplicated per key on the highest cursor value). Use a `last_updated`-style
  cursor so updated rows re-enter the delta.
- **Bootstrap**: if the destination table doesn't exist, the run is a full replace —
  and a merge bootstrap also recreates the source's PRIMARY KEY on the destination
  so the next run can upsert.
- **Cursors** may be integer or date/time columns. Integer cursors parallelize the
  delta over ranges; timestamp cursors parallelize via Postgres TID ranges (other
  sources read the delta in one stream — deltas are small).
- **Schema drift** (source columns ≠ destination columns) fails with a clear error;
  run once with `mode="replace"` to realign.

### Incremental semantics you must know

- **Append assumes the cursor is monotonic with COMMIT order.** Cursor values are
  assigned before commit — a transaction that commits late with a lower id/timestamp
  than an already-loaded row is *permanently skipped* by `cursor > watermark`. This
  is inherent to stateless cursor incremental (every such tool shares it). For
  update-prone or concurrently-written tables, prefer `merge` with a fine-grained
  `updated_at` cursor, or schedule runs with a safety lag behind the writers.
- **Same-cursor ties**: `append`'s strict `>` skips rows that share the exact
  watermark value but arrive later — don't use coarse cursors (`date`, second-
  precision timestamps under heavy write rates) with append; `merge`'s `>=` re-reads
  the boundary and dedupes instead.
- **Rows with a NULL cursor are never synced** by incremental modes.
- **ClickHouse**: incremental requires ClickHouse ≥ 23.6 (the session timezone is
  pinned to UTC so naive-datetime watermarks are frame-exact on any server timezone).
  `merge` on ClickHouse is not supported yet. The state row can't be written
  atomically with the partition attach, so the effective watermark is the GREATEST
  of the state row and the data — a crash between the two merely re-reads a bounded
  delta, never skips ahead.
- Parallel delta spans don't share a snapshot: a row updated *while the run reads*
  can appear in two spans. `merge` dedupes this; under `append`, treat the source
  table as insert-only (that's what append is for).
- On replace, index/constraint/grant restore runs *after* the atomic swap — a crash
  in that narrow window loses the captured DDL (the data is intact). Column DEFAULTs,
  identity ownership, triggers, RLS policies, and grants invisible to the connecting
  role are not preserved.

## Durability

`durable=False` (Postgres destinations only) loads through an **UNLOGGED** staging
table — skipping WAL, the measured write bottleneck — and roughly halves destination
write cost (~-30% wall time at 10M rows). The tradeoff is explicit: the swapped-in
table *stays* unlogged, and PostgreSQL truncates unlogged tables during crash
recovery. Use it for rebuildable data, then optionally:

```sql
ALTER TABLE public.events SET LOGGED;   -- restore crash-durability after the load
```

## Type mappings

### Postgres → ClickHouse (binary lane)

| Postgres | ClickHouse |
|---|---|
| `smallint` / `integer` / `bigint` | `Int16` / `Int32` / `Int64` |
| `real` / `double precision` | `Float32` / `Float64` |
| `numeric(p≤38, s)` | `Decimal(p, s)` — exact |
| `numeric` (no precision) | `Float64` (documented lossy) |
| `boolean` | `UInt8` |
| `date` | `Date32` |
| `timestamp` / `timestamptz` | `DateTime64(6)` / `DateTime64(6, 'UTC')` |
| `uuid` | `UUID` |
| `json` / `jsonb` / text types | `String` |
| anything else, or `numeric(p>38)` | text fallback lane → `String` / `Decimal(p≤76)` |

Nullable columns become `Nullable(T)`; the cursor column backs `ORDER BY`.

### MySQL → ClickHouse

| MySQL | ClickHouse |
|---|---|
| integer types (signed/unsigned) | `Int8..Int64` / `UInt8..UInt64` — lossless |
| `float` / `double` | `Float32` / `Float64` |
| `decimal(p≤38, s)` | `Decimal(p, s)` — exact |
| `decimal(p>38, s)` (up to 65) | `String` — exact text, no precision loss |
| `date` / `datetime` / `timestamp` | `Date32` / `DateTime64(6)` / `DateTime64(6,'UTC')` |
| `year` | `UInt16` |
| char/text/enum/set/json/time | `String` |
| binary/blob/bit | `String` (raw bytes) |

`TIMESTAMP` values are read in a UTC session, so they land as absolute instants.

### MySQL → Postgres (lossless by construction)

| MySQL | Postgres |
|---|---|
| `tinyint` / `smallint` / `int` / `bigint` | `smallint` / `smallint` / `integer` / `bigint` |
| unsigned variants | widened: `smallint`→`integer`→`bigint`; `bigint unsigned` → `numeric(20,0)` |
| `decimal(p, s)` up to `DECIMAL(65,30)` | `numeric(p, s)` — exact binary encoding |
| `datetime` / `timestamp` | `timestamp(6)` / `timestamptz(6)` (UTC) |
| `json` | `jsonb` |
| binary/blob | `bytea` |
| char/text/enum/set/time | `text` |

### Postgres → Postgres

The staging table mirrors the source's exact type spellings (`varchar(20)`,
`numeric(18,4)`, …) and the stream is the raw binary COPY bytes — byte-faithful for
every type Postgres can COPY, including extension types.

Unsupported source types fail **at probe time** with the type named — never mid-copy.

## Schema changes

There is nothing to configure: **every transfer re-derives the schema from the
source**. The staging table is built from a fresh catalog probe, so added columns,
dropped columns, renames, and type changes all propagate on the next run — the
destination is always an exact mirror of the source's current schema. (Tools that
append into existing tables need "schema evolution" machinery; replace semantics
make the problem disappear.)

Three honest caveats:

- **Dependent views (Postgres destinations)**: the atomic swap `DROP`s the old
  table, which Postgres refuses if views depend on it. The transfer fails *safely*
  (old table and staging both intact) — drop/recreate dependent views around the
  load for now.
- **Indexes, constraints, and grants on the destination are not carried over** —
  staging is created bare, so the swapped-in table has none of the old table's
  indexes or permissions. Recreate them after the load (loading bare + building
  indexes afterwards is also faster than loading into an indexed table).
- Incremental sync (on the roadmap) will need real drift handling; today's
  replace-based behavior is documented here precisely so that design can be honest
  about the difference.

## Guarantees

- **Atomic** — readers of the destination never see a partial load; a mid-run
  failure leaves the previous table exactly as it was.
- **0-row guard** — an empty source never wipes an existing destination table.
- **Validated** — every route in the test suite is checksum-verified across engines
  at 10M rows before its numbers are published (see benchmarks/README.md).
- **Bounded memory** — a 256 MB container moves tables of any size.

## Current limitations

- Full-table replace today — incremental sync (cursor-based append & merge) is on
  the roadmap.
- Wheels: Linux x86-64 today (aarch64 and macOS planned).
- One table per call — loop for many (or run several calls in parallel: each call
  holds `parallel`+1 connections per side).
- The GIL is released for the whole transfer, so other Python threads keep running.

## Troubleshooting

| symptom | cause / fix |
|---|---|
| `invalid input: source table … not found` | table name/schema wrong, or the URL points at the wrong database |
| `invalid input: mysql type '…' is not supported yet` | column type outside the map above — open an issue |
| `invalid input: parallel must be at least 1` | `parallel=0` was passed |
| transfer slower than the benchmarks | benchmarks run on localhost; across a network the wire is the wall. Also check the DB's own CPU — at high pipe counts the *database* is usually the bottleneck, not apitap |
| destination table lost after a crash | it was loaded with `durable=False`; re-run the load or `SET LOGGED` after loading |
| `NULL in non-nullable column` (ClickHouse route) | the range-split cursor column must be NOT NULL (it backs `ORDER BY`) |
