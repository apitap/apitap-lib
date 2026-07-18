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
| Google Sheets (source) | `gsheets://` | `gsheets://<spreadsheet_id>?credentials=/path/key.json` — the id from the sheet's URL. See [Google Sheets source](#google-sheets-source). |
| GitHub (source) | `github://` | `github://<owner>/<repo>[/dir]?ref=main` — CSV files as tables. See [GitHub source](#github-source-csv-files-as-tables). |
| GitHub API (source) | `github+api://` | `github+api://<owner>/<repo>` — issues, PRs, commits, stars … as typed tables. See [GitHub API source](#github-api-source-the-project-as-tables). |
| Google Cloud Storage (destination) | `gcs://` | `gcs://<bucket>[/prefix]?format=csv\|parquet&credentials=/path/key.json`. See [GCS destination](#gcs-destination-csv--parquet-files). |

Table names may be schema-qualified (`public.events`, `mydb.events`); unqualified
Postgres names resolve through the connection's `search_path`. Materialized views
work as Postgres sources.

## API

### `apitap.transfer(src, dst, table, *, dest_table=None, parallel=None, cursor=None, chunk_bytes=None, durable=True, mode="replace", engine=None, order_by=None, on_cluster=None) -> TransferReport`

| parameter | default | meaning |
|---|---|---|
| `src`, `dst` | — | connection URLs (see above); the pair picks the route |
| `table` | — | source table, optionally schema-qualified |
| `dest_table` | same as `table` | destination table name |
| `parallel` | auto | concurrent range pipes. Auto derives from the container's CPU count **and** cgroup memory limit (details below). An explicit value is never overridden. `0` is rejected. |
| `cursor` | auto | numeric column used to split the table into ranges. Auto-detects a single-column integer primary key. |
| `chunk_bytes` | 4 MiB | bytes coalesced per network send (floor 64 KiB) |
| `durable` | `True` | Postgres destinations only — see [Durability](#durability) |
| `mode` | `"replace"` | `"append"` / `"merge"` — see [Incremental sync](#incremental-sync-append--merge) |
| `engine` | `MergeTree` | ClickHouse destinations only — see [ClickHouse table engines](#clickhouse-table-engines) |
| `order_by` | cursor | ClickHouse destinations only — `ORDER BY` of the created table |
| `on_cluster` | — | ClickHouse destinations only — run the table DDL `ON CLUSTER` |

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

## Multi-table transfers

One call moves a list of tables — or a whole schema — through **one resource
budget**:

```python
apitap.transfer(src, dst, tables=["public.orders", "public.users"])
apitap.transfer(src, dst, schema="public")      # every base table in the schema
apitap.transfer(src, dst, schema="mydb")        # MySQL: the database
```

Exactly one of `table`, `tables`, `schema` picks the scope; destination tables
keep their source names (`dest_table` is single-table only).

**The budget.** A multi-table run gets exactly the pipe budget a single-table run
would (route CPU heuristic, capped by the cgroup memory model — or your explicit
`parallel`). Tables draw pipes from that one pool: they start **largest first**,
each atomically acquires its ask (sized from the planner's row estimates), and
the grant is **re-fitted the moment the real span count is known** — a table
that can't split (say, no integer PK on a MySQL source) hands its pipes straight
back to the siblings, one whose stats under-estimated tops back up from whatever
is free. Peak memory stays at the single-table ceiling — `budget × ~8×chunk +
reserve` — whether you move 1 table or 500. Small tables take one pipe each and
overlap the big ones, so N small tables cost far less than N separate calls
(shared connection pools, shared auth, one catalog probe).

**Per-table isolation.** Every table keeps the single-table guarantees — own
staging, atomic swap, 0-row guard. If some tables fail, the rest keep going;
at the end a `MultiTransferError` is raised whose `.report.tables` lists every
outcome, and the tables that succeeded ARE committed. The report's `parallel`
is the shared budget; each `TableResult.parallel` is the pipe count that table
actually ran with.

**Schema scope details.**

- Postgres: base tables, partitioned parents and materialized views travel;
  partition/`INHERITS` children whose parent sits in the same schema are skipped
  (the parent's scan covers their rows — listing both would double them). A
  child whose parent lives in *another* schema is copied standalone.
- MySQL: base tables of the named database (views are derived data and stay).
- apitap's own artifacts (`*__apitap_staging`, `*__apitap_old`, `_apitap_state`)
  never travel.
- Colliding destination names fail **up front**, before any table moves —
  including `a.events` + `b.events` into ClickHouse/MySQL/BigQuery (both would
  land on bare `events`) and `events` + `public.events` on Postgres (one
  relation under two spellings).

**Mixing full and incremental tables.** `mode=` applies to the whole call, so
group your tables by how they should land — one call per group, each group
moving through its own budget:

```python
# dimensions / small tables → full refresh
apitap.transfer(src, dst,
    tables=["public.dim_products", "public.dim_regions"], mode="replace")

# append-only facts (events, logs) → incremental append,
# per-table watermark auto-detected from the integer PK
apitap.transfer(src, dst,
    tables=["public.events", "public.api_logs"], mode="append")

# update-prone tables → merge (upsert by PK), updated_at cursor so
# UPDATEs travel; start these with mode="merge" from their FIRST run
apitap.transfer(src, dst,
    tables=["public.orders", "public.customers"],
    mode="merge", cursor="updated_at")
```

Rule of thumb: `replace` for dimensions and PK-less tables, `append` for
append-only facts, `merge` + an `updated_at` cursor for anything that gets
UPDATEd. Group merge tables that share the cursor column (one `cursor=` serves
the whole call).

**Incremental across many tables.** `mode="append"`/`"merge"` apply per table —
each table keeps its own watermark, so a delta run moves exactly each table's
delta (verified: +50k on one table and +1k on another moved 51,000 rows, not a
reload). An explicit `cursor=` applies to *every* table in the run, so leave it
auto unless all tables share the column (e.g. `cursor="updated_at"` across a
uniformly-designed schema). Two semantics worth repeating from the incremental
section:

- **Merge needs the destination PRIMARY KEY.** Start the tables you intend to
  merge with `mode="merge"` from their FIRST run — the merge bootstrap creates
  the PK on the destination. A destination first created by `replace`/`append`
  has no PK and a later merge refuses it loudly.
- **A PK cursor doesn't see UPDATEs** to rows below the watermark. For
  update-prone tables use an `updated_at`-style cursor — verified in the
  multi-table path: 100 updates + 10 inserts moved exactly 110 rows, upserted
  with zero duplicates.

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

## ClickHouse table engines

By default apitap creates ClickHouse destinations as a plain `MergeTree`. When your
pipeline's semantics live in the engine — `FINAL` dedup on a `ReplacingMergeTree`,
replication on a cluster — pick it at the call site:

```python
apitap.transfer(
    PG, CH,
    table="etl.account_v",
    dest_table="raw_account",
    mode="append", cursor="id",
    engine="ReplicatedReplacingMergeTree(ins_dt)",   # any MergeTree-family engine
    order_by="id",                                   # the Replacing dedup key
    on_cluster="my_cluster",                         # required for path-less Replicated*
)
```

How it works: data still streams into a local `MergeTree` staging table at full
speed; apitap then creates the final table with **your** engine and moves the parts
in with a metadata-only `ATTACH` before the atomic name swap. A `Replicated*` engine
fans those parts out to the other replicas from there.

Rules and behavior:

- Any **MergeTree-family** engine spelling is accepted (`MergeTree`,
  `ReplacingMergeTree(v)`, `Summing`/`Aggregating`/`Collapsing`/
  `VersionedCollapsing`/`Graphite`, each also as `Replicated*`). Non-MergeTree
  engines are rejected — the staging→ATTACH choreography needs part-based storage.
- `ReplicatedReplacingMergeTree(ins_dt)` **without ZooKeeper-path arguments**
  requires `on_cluster` (ClickHouse mints the unique `{uuid}` path only for
  ON CLUSTER DDL on Atomic databases) — recommended. Explicit paths work too, but
  `mode="replace"` into an **existing** explicit-path table is rejected: the
  shadow copy would collide on the path — use append, or drop the table first.
- Columns named in the engine arguments or `order_by` are declared **non-nullable**
  (ClickHouse requires it for version/sorting columns). An actual `NULL` arriving in
  one of them fails loudly rather than corrupting the stream.
- `on_cluster` requires a `Replicated*` engine — with a local engine the other
  replicas would get the table but never the data.
- With `mode="append"`, if the destination table **already exists**, it is the
  structural authority: apitap mirrors its structure and keys into staging and
  appends into it as-is, checking that the requested engine family/arguments and
  `order_by` agree with the table (a drifted dedup key errors instead of silently
  deduping wrong). Pre-creating the table yourself therefore remains fully
  supported — TTL, codecs, projections and any other DDL you own stay untouched.
  With `mode="replace"` (the default) the table is **rebuilt** with the requested
  engine and pre-existing DDL extras are replaced.
- `on_cluster` accepts single-shard clusters only — apitap loads one replication
  group and keeps its `_apitap_state` on the connected host. Appending via a
  different host later is safe (the watermark falls back to the data and a
  Replacing `FINAL` absorbs the bounded re-read) but keep a stable endpoint for
  exactly-once deltas.
- Engines whose merges rewrite rows (`Summing`/`Aggregating`/`Collapsing`/…)
  accept `mode="append"` only when the cursor is part of the sorting key —
  otherwise merges could move `max(cursor)` and poison the watermark.
- `mode="append"` + `ReplacingMergeTree(version)` + reading with `FINAL` gives you
  upsert-like semantics: re-delivered keys resolve to the row with the highest
  version. Pair it with an `updated_at` cursor to capture row updates.

## BigQuery destination

```python
apitap.transfer(
    "postgres://user:pass@host:5432/db",
    "bigquery://my-project/my_dataset?credentials=/path/service-account.json",
    table="public.events",
    mode="append", cursor="id",
)
```

- **Auth**: a service-account key file via `?credentials=` or the
  `GOOGLE_APPLICATION_CREDENTIALS` env var. The key is exchanged for a ~1h OAuth2
  token via the JWT-bearer grant; the private key never leaves the process.
  Needed roles: BigQuery Data Editor + Job User on the project/dataset.
- **Ingest path** (built for wall-clock, chosen per box): with 4+ pipes each
  pipe decodes Postgres **binary COPY** straight into **Parquet (ZSTD)**
  column chunks — no text round-trip, and BigQuery's fastest parse; on small
  boxes (<4 pipes) a leaner CSV+gzip transcode wins instead (typed builders
  cost more CPU than the half-core has). Both lanes stream into rotating
  resumable-upload **load jobs** (free; each worker loads its OWN staging
  table — BigQuery allows ~5 metadata updates/10s per table — sealed at
  ≥12 MiB and ≥6s so quotas can't trip). A single multi-source **copy job**
  (atomic, metadata-only, free) lands everything in the final table:
  `WRITE_TRUNCATE` for replace, `WRITE_APPEND` for append. The streaming
  `insertAll` API is never used — it bills per byte and its buffer is
  invisible to copies.
- **Parquet lane type notes**: exact `NUMERIC` as 16-byte decimals;
  `json`/`jsonb` land as **STRING** (BigQuery rejects Parquet loads into
  JSON-typed columns — the text is valid JSON; `PARSE_JSON` works on read);
  exotic types (arrays, ranges, `inet`, …) are rejected loudly — cast them
  in a source view. The CSV lane keeps the JSON column type.
- **Types** (Postgres route): int2/4/8 → INT64; `boolean` → INT64 0/1;
  float4/8 → FLOAT64; `numeric(p,s)` → NUMERIC or BIGNUMERIC by precision
  (values are shipped as strings, so they stay EXACT — no double round-trip);
  unconstrained `numeric` → BIGNUMERIC (a value beyond its 38.38 digits fails
  the load loudly); `date` → DATE; `timestamptz` → TIMESTAMP; `timestamp` →
  DATETIME; `json`/`jsonb` → JSON; `uuid`, `text`, and everything else
  (including `bytea`, as its hex form) → STRING. Explicit schema — nothing is
  inferred. `?location=EU` pins the location when apitap creates the dataset.
- **Incremental**: `mode="append"` with the `_apitap_state` watermark table in
  the dataset, exactly like the other destinations. The state MERGE runs right
  after the copy job commits (BigQuery has no cross-statement transaction);
  the effective watermark is `greatest(state, data max)`, so a crash between
  the two costs a bounded re-read, never a skip. `mode="merge"` is not
  supported yet.
- **Cost**: load and copy jobs are free. The billed queries are the
  watermark reads (`MAX(cursor)` scans one column of the destination and of
  the staged rows per incremental run) and the tiny state-row statements.
- **Append semantics**: BigQuery has no cross-statement transaction, so the
  state row lands right after the copy job. A crash between the two leaves
  the data ahead of the state — the next run's `greatest(state, data max)`
  watermark absorbs it (no skip, no duplicate). BigQuery tables don't dedup,
  so never point two pipelines at one destination table without the fan-in
  guard tripping first.

## Google Sheets source

```python
apitap.transfer(
    "gsheets://1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms?credentials=/path/service-account.json",
    "postgresql://user:pass@host:5432/db",
    table="Class Data",          # a TAB name — quotes/spaces are fine
)
# or every tab at once (the multi-table machinery, same as databases):
apitap.transfer("gsheets://<id>?credentials=…", dst, schema="*")
apitap.transfer("gsheets://<id>?credentials=…", dst, tables=["Sheet1", "Q3 data"])
```

- **Model**: the spreadsheet is the database, its **tabs are the tables**. Row 1
  is the header row and becomes the column names (blank header cells become
  `col_N`; duplicate headers fail loudly — rename them in the sheet).
- **Auth**: the same service-account key flow as BigQuery (`?credentials=` or
  `GOOGLE_APPLICATION_CREDENTIALS`), read-only Sheets scope. **Share the
  spreadsheet with the service account's email** (viewer is enough) — that's
  how Google grants a robot access; sheets that are link-visible work without
  sharing.
- **Types**: every column arrives as **nullable TEXT**, rendered exactly as the
  sheet displays it (`FORMATTED_VALUE`). Sheets are untyped — a typed cast
  belongs in the destination, where it can fail loudly per value. Blank cells
  land as `NULL`.
- **Destinations**: Postgres and ClickHouse (all-text delivery over the binary
  lanes) and MySQL (`LOAD DATA` text lane, `TEXT` columns). BigQuery is not
  wired for this source yet.
- **Modes**: `mode="replace"` only. Sheets carry no usable incremental cursor,
  so append/merge are refused loudly.
- **Paging**: rows stream in 10k-row pages (override with
  `APITAP_GSHEETS_PAGE_ROWS` if you hit per-request API limits). The Sheets API
  caps a spreadsheet at 10M cells, so a tab is always small by database
  standards — one pipe moves it.

## GitHub source (CSV files as tables)

```python
apitap.transfer(
    "github://apitap/apitap-lib/tests/data",   # repo directory
    "postgresql://user:pass@host:5432/db",
    table="people",                            # tests/data/people.csv
)
# every .csv under the directory at once, or an explicit list:
apitap.transfer("github://owner/repo/exports?ref=v2.1", dst, schema="*")
apitap.transfer("github://owner/repo", dst, tables=["users", "2026/orders"])
```

- **Model**: the repo (or one directory) is the database; every `.csv` under it
  is a table, named by its file stem (`exports/2026/users.csv` → `users`). Two
  files sharing a stem each get their directory-relative path as the table name
  instead (`archived/ecdc/full_data`, `archived/who/full_data`) — nothing
  collides, nothing blocks. Address a table by stem or by that relative path,
  always **without** `.csv` (the extension is refused — a `.` in a table name
  reads as a schema qualifier downstream, which is also why a file stem
  containing `.` fails loudly: rename the file). Row 1 is the header row
  (blank headers become `col_N`, duplicates fail loudly).
- **`schema=`** filters to one sub-directory (on a whole path segment:
  `schema="2026"` picks `2026/…`, never `2026-backup/…`); `"*"` means every
  table.
- **Ref**: `?ref=` pins a branch, tag, or commit SHA; default is the repo's
  default branch. Pinning a SHA gives perfectly reproducible loads.
- **Auth**: set `GITHUB_TOKEN` (or `GH_TOKEN`) for private repos; it also lifts
  the API rate limit (60/h anonymous → 5,000/h). The token rides only in the
  Authorization header — never put it in the URL.
- **Types**: every column arrives as **nullable TEXT** exactly as written in
  the file — a typed cast belongs in the destination. An empty field is `NULL`;
  a short row NULL-pads its missing trailing fields; a row with MORE fields
  than the header is refused loudly (silently dropping fields is data loss).
  Parsing is RFC-4180: quoted fields, embedded commas/newlines, `""` escapes.
- **Destinations**: Postgres, ClickHouse, and MySQL. Files stream — size never
  bounds memory.
- **Modes**: `mode="replace"` only (files carry no usable cursor).

## GitHub API source (the project as tables)

```python
apitap.transfer("github+api://apitap/apitap-lib", pg_url, table="issues")
apitap.transfer("github+api://owner/repo", ch_url, schema="*")   # every entity
# incremental — only issues updated since the watermark travel:
apitap.transfer("github+api://owner/repo", pg_url, table="issues",
                mode="merge", cursor="updated_at")
```

- **Tables**: `issues` · `pull_requests` · `commits` · `stargazers` ·
  `releases` · `issue_comments` · `workflow_runs` · `branches` · `tags` ·
  `labels`. Each ships a curated TYPED column set (ids/counts as int8, flags
  as bool, times as timestamptz, labels as jsonb) **plus a `raw` jsonb column
  with the whole API object** — nothing the API returned is lost, and the
  schema is declared, not inferred. The issues endpoint's interleaved PRs are
  filtered out (they live in `pull_requests`).
- **Incremental**: `issues` and `issue_comments` (`cursor="updated_at"`) and
  `commits` (`cursor="committed_at"`) sync incrementally — their APIs filter
  server-side with `since=`. `mode="merge"` (Postgres dests) also carries
  edits; entities without a server-side filter refuse incremental loudly.
- **Auth**: `GITHUB_TOKEN`/`GH_TOKEN` — required for private repos **and for
  `stargazers`** (its `starred_at` media type is auth-only), and lifts
  the API rate limit from 60 requests/hour to 5,000 (a page is 100 rows, so
  that's ~500k rows/hour of headroom).
- **Destinations**: Postgres and ClickHouse.

## GCS destination (CSV & Parquet files)

```python
apitap.transfer(
    "postgres://user:pass@host:5432/db",
    "gcs://my-bucket/exports?format=parquet&credentials=/path/service-account.json",
    table="public.events",
)
```

- **Layout**: `format=csv` (default) writes ONE gzipped object per table —
  `<prefix>/<table>.csv.gz`, header row included. Workers stream their own
  staging parts and finalize COMPOSES them server-side, so the final object
  appears atomically (readers never see a partial file). `format=parquet`
  writes the columnar convention instead: `<prefix>/<table>/part-NNNNN.parquet`
  (ZSTD), one part per pipe — parts can't be concatenated, so the directory
  swap is per-object (each file lands atomically; the set is not a
  transaction).
- **Auth**: the same service-account flow as BigQuery (`?credentials=` or
  `GOOGLE_APPLICATION_CREDENTIALS`); the key needs `storage.objectAdmin` on
  the bucket.
- **CSV semantics**: unquoted-empty = NULL, quoted `""` = empty string;
  values holding a delimiter, quote, or newline are quoted with `""` doubling
  — the same dialect the BigQuery lane ships, readable by every CSV parser.
- **Types** (Parquet): the BigQuery Parquet lane's mappings — exact decimals,
  microsecond timestamps, real int/float/bool/date types; exotic columns
  (arrays, ranges, `inet`, …) are refused loudly — cast in a source view or
  use `format=csv`.
- **Modes**: `mode="replace"` only; the 0-row guard leaves the destination
  untouched when the source is empty. Works with `tables=`/`schema=` multi.
  Switching `format=` re-points the table: the other format's previous output
  is swept on the next successful run.
- **Limits**: `format=csv` takes at most 31 pipes (GCS composes header + one
  part per pipe, 32-object cap — auto-parallel never exceeds 8); a
  single-NULLABLE-column table is refused on the CSV lane (a NULL row would be
  a blank line, which CSV readers silently drop — use Parquet). A Parquet
  finalize interrupted mid-rename can leave old and new parts mixed until the
  next successful run.

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

- Wheels: Linux x86-64 today (aarch64 and macOS planned).
- BigQuery: `mode="merge"` not supported yet; not wired as a destination for the
  Google Sheets source.
- Google Sheets: source only, `mode="replace"` only (no usable cursor in a sheet).
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
