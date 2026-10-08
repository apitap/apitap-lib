# apitap

**Move whole tables between databases at wire speed, in bounded memory.**

apitap is a transfer engine, not an API client. It speaks the databases' own
wire formats — binary `COPY`, RowBinary, `LOAD DATA`, logical replication, the
MySQL binlog — through its own Rust wire clients, moves tables over parallel
range pipes, publishes them with atomic swaps, and keeps memory flat no matter
how big the table is. The Python package is a thin binding in the spirit of
Polars: one call, no daemon, no local state files. It is the open-source engine
behind apitap cloud (apitap.dev).

```bash
pip install apitap
```

```python
import apitap

report = apitap.transfer(
    "postgres://user:pass@src-host/db",
    "clickhouse://user:pass@warehouse:8123/db",
    table="public.events",
)
print(f"{report.rows:,} rows in {report.elapsed_ms} ms over {report.parallel} pipes")
```

The same call does incremental loads and batch CDC, and mixes modes per table:

```python
apitap.transfer(src, dst, table="public.orders", mode="append", cursor="id")
apitap.transfer(src, dst, table="public.orders", mode="log_based")      # full WAL capture
apitap.transfer(src, dst, tables={                                        # one call, one replication slot
    "orders":    "log_based",
    "customers": "log_based",
    "dim_date":  "replace",
})
df = apitap.read(src, table="public.orders").to_polars()                 # or .lazy() for tables bigger than RAM
```

---

**Contents** — [At a glance](#at-a-glance) · [Why apitap](#why-apitap) ·
[Quick start](#quick-start) · [Connection URLs](#connection-urls) · [The API](#the-api) ·
[How a transfer runs](#how-a-transfer-runs) · [Incremental sync](#incremental-sync-append-and-merge) ·
[Batch CDC](#batch-cdc-modelog_based) · [Guarantees](#guarantees) · [Operating it](#operating-it) ·
[Performance, measured](#performance-measured) · [Type mapping](#type-mapping) ·
[Roadmap](#roadmap) · [Development](#development)

---

## At a glance

| | |
|---|---|
| **Sources** | `postgres://` · `mysql://` (MySQL 8.x, MariaDB) · `clickhouse://` · `gsheets://` · `github://` (repo CSVs as tables) · `github+api://` (issues, PRs, commits… as typed tables) |
| **Destinations** | `postgres://` · `mysql://` · `clickhouse://` · `bigquery://` · `gcs://` · `s3://` (AWS, MinIO, R2, any S3-compatible store) · `iceberg://` (any REST catalog) |
| **Modes** | `replace` (default, atomic swap) · `append` · `merge` (Postgres, Iceberg) · `log_based` batch CDC from Postgres and MySQL/MariaDB into Postgres, ClickHouse, MySQL, BigQuery, Iceberg |
| **DataFrames** | `apitap.read()` → polars, pyarrow, duckdb, pandas through the Arrow C stream protocol, zero-copy, no pyarrow dependency |
| **Runtime** | CPython ≥ 3.9, one `abi3` wheel (`manylinux_x86_64`); the engine is Rust, nothing in the data path is Python |
| **Memory** | bounded by *pipes × buffers*, planned from the container's cgroup limits — never by table size; a 256 MB container moves 100 GB |
| **Version** | 0.59.0 — pin exactly while pre-1.0 (`apitap==0.59.0`) |
| **License** | MIT |

## Why apitap

Moving a lot of data should not require a lot of machine. Most ingestion
pipelines pay twice — once in wall-clock hours, once in the oversized workers
those hours run on. An engine that is careful about memory and wire formats
finishes the same job faster on the smallest container you can rent, and that
difference is real money every hour, on every pipeline.

Where that stands today, every number measured on the published wheel and
checksum-validated against the source (environments are spelled out under
*Performance, measured*):

- **100 GB (232M rows, 15 columns) through a 0.5 vCPU / 256 MB container in 8m57s**,
  peak RSS 170.8 MB; the same table also completes inside a **44 MB** cap. On
  three dedicated machines the same zero-config call takes **30.3 s** (~3.3 GB/s).
  The tools we compared against were OOM-killed in ~21 s on the small box and had
  landed zero rows when cut on the big one.
- **Ten 1M-row tables in one call, the tool alone capped at 0.5 vCPU / 256 MB:**
  Postgres → ClickHouse in **18.4 s** at a 119 MB peak; MySQL → ClickHouse in
  **36.1 s** at 99 MB. walshadow and ingestr were OOM-killed in every leg.
- **CDC:** one 650K-event replication window, every tool at 0.5 vCPU / 256 MB —
  apitap **12 s**, ape-dts 22 s, ingestr 78 s, pipelinewise 227 s, all row-matched.
  A **30-table group** in one slot sustains **1.90M changes/min** pure catch-up on
  half a core, checksum-exact, under 200 MB. At fleet scale (180M changes from
  four sharded Postgres into one ClickHouse, both movers capped at 6 CPU / 2 GB)
  apitap verifies 400/400 tables in 125 s of apply work at ~3 µs of CPU per
  change; PeerDB had not finished when the 30-minute cap fell.
- **DataFrames:** filter + group_by over **50M rows in 9.9 s on 0.5 vCPU / 256 MB**,
  tying raw SQL run inside Postgres itself; polars/connectorx is OOM-killed on
  that box and ADBC takes 45 s.

The next tiny-box goal is stated as plainly as the first one was: **3 million
changes per minute of CDC on 0.5 vCPU / 256 MB**, with throughput that rises by
itself when the box grows. Today's honest number is 1.9M/min catch-up and
~1.5M/min keep-up; the design is written and is being executed one measured
lever at a time. If a number here looks wrong or a workload goes badly, open an
issue — this project has been corrected by its own failed runs more than once.

**Try it in a browser:** apitap.dev/lab runs the real PyPI wheel next to ingestr
and dlt against a seeded Postgres and ClickHouse; pick the box (1 GB / 2 vCPU or
256 MB / 0.5 vCPU), press run, and every result is row-count-verified before a
number appears.

## Quick start

```python
import apitap

PG = "postgres://user:pass@pg-host:5432/shop"
CH = "clickhouse://default:pass@ch-host:8123/analytics"      # clickhouse+https:// for TLS
MY = "mysql://user:pass@my-host:3306/shop"                   # TLS required off-loopback; ?ssl-mode=disabled to opt out

# 1. Full refresh — staging table, atomic swap, never a partial table
apitap.transfer(PG, CH, table="public.events")

# 2. Incremental — only rows past the destination's watermark (integer or timestamp cursor)
apitap.transfer(PG, CH, table="public.events", mode="append", cursor="id")
apitap.transfer(PG, "postgres://…/warehouse", table="public.customers", mode="merge", cursor="updated_at")

# 3. Batch CDC — first run bootstraps from a pinned snapshot, every later run drains the log
apitap.transfer(PG, CH, table="public.orders", mode="log_based")
apitap.transfer(MY, CH, table="shop.orders", mode="log_based")                # MySQL / MariaDB binlog
apitap.transfer(PG, CH, tables=["orders", "order_items", "payments"],        # one slot, one window, one watermark
                mode="log_based")
apitap.transfer(PG, CH, tables=["t1", "t2", …, "t100"], mode="log_based", slots=4)   # sharded over 4 slots
apitap.transfer(PG, CH, tables=["orders"], mode="log_based", changelog=True)         # append every change, keep history

# 4. Many tables, one memory budget — or a whole schema
apitap.transfer(PG, CH, tables=["public.a", "public.b"])
apitap.transfer(PG, CH, schema="public")
apitap.transfer(PG, CH, tables={"orders": "log_based", "customers": "log_based", "dim_date": "replace"})

# 5. ClickHouse DDL: engine, ORDER BY, ON CLUSTER
apitap.transfer(PG, CH, table="public.events",
                engine="ReplicatedReplacingMergeTree(ins_dt)", order_by="client_id, id", on_cluster="prod")

# 6. Warehouses, lakes, files
apitap.transfer(PG, "bigquery://my-project/raw?credentials=/path/key.json", table="public.events")
apitap.transfer(PG, "iceberg://catalog:8181/lake?warehouse=s3://lake/wh&endpoint=http://minio:9000",
                table="public.events", mode="merge")
apitap.transfer(PG, "s3://bucket/exports?format=parquet&endpoint=http://minio:9000", table="public.events")
apitap.transfer(PG, "gcs://bucket/exports?format=csv&credentials=/path/key.json", table="public.events")

# 7. Spreadsheets and repositories as sources (all-text columns, replace only)
apitap.transfer("gsheets://1AbC…?credentials=/path/key.json", PG, table="Sheet1")
apitap.transfer("github://apitap/apitap-lib/tests/data?ref=main", PG, table="users")
apitap.transfer("github+api://apitap/apitap-lib", CH, table="issues", mode="append", cursor="updated_at")

# 8. DataFrames — same parallel pipes, Arrow batches built in Rust
df  = apitap.read(PG, table="public.events").to_polars()
top = (apitap.read(PG, table="public.events").lazy()
       .filter(pl.col("amount") > 100).group_by("status").agg(pl.len())
       .collect(engine="streaming"))                                        # tables bigger than RAM
apitap.read(MY, table="shop.orders").to_parquet("orders.parquet")           # constant memory
```

Schedule any of these from cron, Airflow or Kubernetes; a CDC run is just the
same call again. There is nothing to install at the databases and nothing to
keep running between runs.

## Connection URLs

| scheme | form | notes |
|---|---|---|
| Postgres | `postgres://user:pass@host:5432/db?sslmode=verify-full` | `sslmode` = `disable` · `prefer` · `require` · `verify-full` (`verify-ca` is refused by name — it cannot be expressed safely). A cleartext or MD5 password request on a channel that is not `verify-full` is refused (`APITAP_ALLOW_INSECURE_AUTH=1` overrides). Unqualified table names mean `public.` |
| MySQL / MariaDB | `mysql://user:pass@host:3306/db?ssl-mode=verify_identity` | TLS is **required for any non-loopback host** since 0.55.1; `ssl-mode` = `disabled` (send in clear, on purpose) · `required` (encrypt, no verify) · `verify_ca` · `verify_identity` (default off-loopback). Tables are `db.table`; the URL's database is the default |
| ClickHouse | `clickhouse://user:pass@host:8123/db` · `clickhouse+https://…:8443/db` | HTTP interface; `wait_end_of_query=1` so a failed INSERT is a failed run; `session_timezone=UTC` |
| BigQuery | `bigquery://project/dataset?credentials=/path/key.json[&location=EU]` | service-account key; `GOOGLE_APPLICATION_CREDENTIALS` is the fallback. Bulk modes use the free load/copy-job path and work on sandbox projects; CDC needs billing (row-level DML) |
| GCS | `gcs://bucket[/prefix]?format=csv\|parquet[&credentials=…]` | `csv` = one composed `.csv.gz` per table (atomic visibility); `parquet` = a directory of ZSTD parts |
| S3-compatible | `s3://bucket[/prefix]?format=parquet[&endpoint=…][&region=…][&access_key_id=…&secret_access_key=…][&session_token=…]` | `AWS_*` env vars are the fallback; an explicit `endpoint` means path-style (MinIO, R2, OVH/Scaleway/Hetzner); SigV4 signed by hand, no SDK |
| Iceberg | `iceberg://catalog-host[:port]/namespace?warehouse=…[&base=…][&token=…][&tls=0\|1][&endpoint=…][&region=…][&access_key_id=…&secret_access_key=…]` | any REST catalog (Lakekeeper, Polaris, Nessie, Glue REST, R2 Data Catalog, S3 Tables); format-version 2, single-level namespace, data on `s3://` |
| Google Sheets | `gsheets://<spreadsheet_id>?credentials=/path/key.json` | tabs are tables, row 1 the header, every column nullable text as displayed |
| GitHub files | `github://owner/repo[/dir]?ref=main` | `.csv` files are tables (RFC 4180, streamed); `GITHUB_TOKEN`/`GH_TOKEN` for private repos and the 5,000/h rate limit |
| GitHub API | `github+api://owner/repo` | `issues`, `pull_requests`, `commits`, `stargazers`, `releases`, `issue_comments`, `workflow_runs`, `branches`, `tags`, `labels` as typed tables plus a `raw` jsonb column; incremental where the API filters server-side |

A password containing `@ / ? # : [ ]` must be percent-encoded, or the URL means
something else (`p@ss` turns `ss` into the host): `quote(password, safe='')`
from `urllib.parse`. apitap never prints a password — a URL that fails to parse
is shown with the secret struck out and the offending character named.

## The API

```python
apitap.transfer(src, dst, table=None, *, tables=None, schema=None, dest_table=None,
                mode="replace", cursor=None, parallel=None, chunk_bytes=None, durable=True,
                engine=None, order_by=None, on_cluster=None, partition_by=None,
                changelog=False, slots=None) -> TransferReport
```

| option | applies to | meaning |
|---|---|---|
| `table` / `tables` / `schema` | all | exactly one: a table (`schema.table` or bare), a list of tables, a `{table: mode}` dict (per-table modes; all `log_based` members share one slot), or every base table of a schema |
| `dest_table` | single table | destination name; defaults to `table`. Multi-table runs keep source names |
| `mode` | all | `replace` · `append` · `merge` · `log_based` |
| `cursor` | append/merge/read | integer or date/timestamp column; default = the integer primary key |
| `parallel` | bulk/read | concurrent range pipes; default auto from the cgroup's CPU **and** memory; an explicit value is never overridden |
| `chunk_bytes` | bulk | bytes coalesced per send, default 4 MiB (floor 64 KiB); the planner thins it to 2 MiB on memory-capped boxes |
| `durable` | Postgres dest | `False` loads through an UNLOGGED staging table (~30% less wall); the swapped-in table stays unlogged until `ALTER TABLE … SET LOGGED` |
| `engine`, `order_by`, `on_cluster` | ClickHouse dest | engine of the created table (any MergeTree family, Replicated included), its ORDER BY (default the cursor; the dedup key for Replacing engines), and `ON CLUSTER` DDL (requires a `Replicated*` engine). `append` into an existing table treats it as the authority and only checks engine family, arguments and ORDER BY agree |
| `partition_by` | ClickHouse, BigQuery changelog | a column name = monthly partitions on it (both engines), or a verbatim ClickHouse expression; a `{table: clause}` dict for multi-table runs; default monthly on `_apitap_at` |
| `changelog` | `log_based` into ClickHouse/BigQuery | `True` appends every change with `_apitap_op` (`I`/`U`/`D`/`T`, `B` for the bootstrap baseline), `_apitap_lsn`, `_apitap_seq`, `_apitap_at`, and derives a `<table>__current` view — nothing is ever updated or deleted |
| `slots` | `log_based`, many tables, Postgres | drain the group over N replication slots in parallel (cut deterministically from the sorted table list; changing N renames the slots and is refused until the old state is cleared) |

**Results and errors.** `TransferReport(rows, elapsed_ms, parallel, tables)`;
for a multi-table run `tables` is a tuple of `TableResult(table, rows,
elapsed_ms, parallel, error)` and `rows` sums the successful ones. Bad input —
unknown table, unsupported type, a bad URL — raises `ValueError` at probe time,
never mid-copy; transfer failures raise `RuntimeError`; `apitap.LockedError`
(a `RuntimeError` subclass) means another run already holds the destination
table, so a scheduler can back off on a type; `apitap.MultiTransferError`
carries `.report` when some tables of a multi-table run failed — the ones that
succeeded are committed. The GIL is released for the whole transfer.
`apitap.request_stop()` asks a running CDC drain to land its window and return.

```python
apitap.read(src, table=None, *, cursor=None, parallel=None, query=None, columns=None) -> Reader
```

| `Reader` method | what you get |
|---|---|
| `.to_polars()` · `.to_arrow()` · `.to_pandas()` | the whole table, built as one batch per pipe in Rust (fewest FFI crossings) |
| `.lazy()` | a polars LazyFrame over the stream; `.collect(engine="streaming")` runs in constant memory; the query's column projection **and** a conservative subset of its filter (arithmetic, comparisons, AND/OR, string `=`) are pushed into the SQL |
| `.batches()` | small polars DataFrames, one per Arrow batch |
| `.to_parquet(path, compression="zstd", row_group_bytes=None)` | table → Parquet file at constant memory; returns the row count |
| `__arrow_c_stream__` | `pl.DataFrame(reader)`, `pa.table(reader)`, DuckDB, pandas — zero-copy |

`parallel=1` preserves source order; `columns=` reads a projection; `query=`
is refused today (whole tables and cursor ranges only). Typed end to end:
int16/32/64, float32/64, bool, decimal128, date32, timestamp µs (UTC or naive),
utf8, binary; uuid, json/jsonb and exotic types arrive as text, so every table
reads.

## How a transfer runs

```
probe ─► cursor & spans ─► negotiate wire format ─► stage ─► N parallel pipes ─► count ─► atomic swap
```

1. **Probe** the source catalog: columns, types, nullability, primary key,
   row estimate. Everything that can fail on types fails here.
2. **Plan spans.** An integer cursor splits into contiguous key ranges
   (`parallel × 6`, so stragglers balance); a PK-less Postgres table splits
   by `ctid` page ranges (PG 14+ TID range scans); anything else streams as one
   span. A `delta` predicate (append/merge) and the lazy plugin's pushed-down
   filter ride inside every span statement.
3. **Negotiate the wire format** — the first format the destination accepts
   that the source can produce for every column:

   | route | how bytes move |
   |---|---|
   | Postgres → Postgres | raw binary `COPY` passthrough: no row decode at all, like `psql \| psql` without the shell |
   | Postgres → ClickHouse | binary `COPY` transcoded in flight to `RowBinary` (byte swaps, epoch rebasing, exact `numeric → Decimal`); TSV only for types RowBinary cannot carry |
   | Postgres → MySQL | binary `COPY` rendered in flight as `LOAD DATA` text (exact `numeric` to `DECIMAL(65,30)`, `bytea` as HEX) |
   | Postgres → BigQuery / GCS / S3 / Iceberg | binary `COPY` → typed Parquet column chunks (ZSTD-1), no `arrow` dependency; small cores take the CSV+gzip lane into BigQuery |
   | MySQL → ClickHouse / Postgres / MySQL | binary-protocol rows read by apitap's own MySQL wire client and encoded straight into RowBinary / binary `COPY` / `LOAD DATA` text — one dispatch and at most one copy per cell |
   | ClickHouse → ClickHouse | `RowBinary` relayed untouched, every column cast server-side to the exact destination type so the bytes are correct by construction |
   | Sheets / GitHub → any | text rows framed into the destination's format |

4. **Stage.** A staging table (or object prefix) named with this run's
   identity is created; it mirrors the source DDL on same-engine routes and
   maps types losslessly otherwise.
5. **Pipes.** Each pipe is one source connection streaming its spans into one
   destination loader (one `COPY`, one HTTP `INSERT`, one `LOAD DATA`, one
   resumable upload…), coalescing to `chunk_bytes` and recycling its buffers.
   A failed pipe cancels its siblings before anything is swept.
6. **Count and swap.** The staged count must equal what the loaders report;
   then `DROP` + `RENAME` (Postgres, one transaction), `EXCHANGE TABLES`
   (ClickHouse), `RENAME TABLE` (MySQL), a copy job (BigQuery), a snapshot
   commit (Iceberg), or a server-side copy of parts (GCS/S3). A 0-row source
   never touches an existing table.

**Parallelism and memory are planned, not hoped.** The pipe count starts from a
per-route CPU profile — Postgres→Postgres `cores` (1–8), anything → ClickHouse
`8 × cores` (2–32), MySQL → Postgres `4 × cores` (2–8), MySQL → MySQL `4 × cores`
(2–16), → BigQuery `2 × cores` (2–8) — and is then **fitted to the cgroup memory
limit**: 40 MiB reserved, then per pipe 10 × `chunk_bytes` plus whatever the
destination lane holds (a Parquet pipe adds an 8 MiB part buffer, a 1 MiB frame
buffer, a 2 MiB page and three row groups of 24/8/4 MiB, chosen to fit). When
memory, not CPU, is the limit the chunk thins to 2 MiB before a pipe is dropped,
so a 128 MB container is a differently shaped run, not a slower copy of the
256 MB one. `APITAP_MEM_BUDGET` caps the budget below the cgroup when the
container is shared. Multi-table runs share **one** pipe budget, scheduled
largest-first with per-table grants re-fitted to real span counts, so peak
memory stays at the single-table ceiling no matter how many tables you pass.
`read()` plans the same way: `(mem − 56 MiB) / (4 × (workers + queue + 1))`
per batch, clamped 1–32 MiB.

## Incremental sync (append and merge)

`mode="append"` loads rows whose `cursor` is past the destination's watermark;
`mode="merge"` does the same and upserts by the destination's primary key
(Postgres: one `INSERT … SELECT DISTINCT ON (pk) … ON CONFLICT DO UPDATE`;
Iceberg: an equality-delete file plus the delta's data files in **one**
snapshot). Merge is not available on ClickHouse, MySQL or BigQuery — use
`append`, or `log_based` for a true replica.

- The watermark lives in **`_apitap_state`**, a plain table in the
  destination (`dest_table, source_id, cursor_col, watermark, mode, last_rows,
  synced_at`), written in the same transaction as the data on Postgres; on
  Iceberg it is a table property committed in the same snapshot. On
  ClickHouse, MySQL and BigQuery (no cross-statement transaction) the next run
  takes the *greatest* of the state row and `max(cursor)` in the data — a crash
  between the two costs a bounded re-read, never a skip. BigQuery's state table
  is append-only (no DML on sandbox projects); readers take the newest row.
- The first run of a missing table is a `replace`. An emptied table carries no
  watermark, so `TRUNCATE` is a legitimate resync. `replace` clears every
  source's state for the table.
- `append` assumes the cursor is monotonic with commit order; for update-prone
  or concurrently written tables use `merge` with an `updated_at` cursor, or
  CDC. A state row written by a different cursor, or by a CDC drain (whose
  watermark is an LSN), is **refused**, never silently reinterpreted.
- Fan-in — two `append` runs from *different* sources into one table — is
  supported: each (table, source) has its own watermark.

## Batch CDC: `mode="log_based"`

Every change the log saw — inserts, updates (primary-key changes included),
deletes, `TRUNCATE`, unchanged-TOAST columns — captured on a schedule, with no
daemon and no extra columns in your rows.

**Postgres source.** The first run creates a logical replication slot
(`pgoutput`) and a publication for the member tables, exports the slot's
snapshot, and bootstraps with a full load pinned to it — no gap, no duplicate.
Every later run drains the WAL delta. Requirements: `wal_level=logical`, a role
that can create the publication (table ownership or superuser) and replicate, a
primary key on every member (`REPLICA IDENTITY FULL` works for the rest;
`NOTHING` is refused), and `max_replication_slots` ≥ the slots you use.
`logical_decoding_work_mem` follows the server's value (`APITAP_DECODE_WORKMEM`
opts into a per-session value, capped at 256 MiB).

**MySQL / MariaDB source.** The binlog coordinate is captured *before* an
idempotent full load, then every later run drains the binlog from it — the same
guarantee without a slot. Requirements: `log_bin=ON`, `binlog_format=ROW`,
`binlog_row_image=FULL`, no binlog compression or partial-JSON row values,
UTF-8 (utf8mb4) string columns, a primary key per table, and `REPLICATION
SLAVE`/`REPLICATION CLIENT`. MyISAM/Aria transactions ending on a `QUERY
COMMIT` are handled; a purged binlog, a position ahead of the server, or a
changed server identity is refused with its remedy. `server_id` is minted
uniquely per connection.

**How a window lands.** The drain reads the log into a *window* bounded by
bytes (default `(mem − 24 MiB) / 16`, capped at 24 MiB — 14.5 MiB in a 256 MB
container; `APITAP_CDC_WINDOW_BYTES` overrides), by `APITAP_WINDOW_MAX_SECS`,
or by the stop line captured at run start; a window never spans a source
table's column change, and one source transaction is never split. Each table's
changes are collapsed to one final image per key, then applied **set-based**:
delete the touched keys, insert the final images, in one unit per window whose
watermark commits with the data — one transaction on Postgres and MySQL, one
fenced multi-statement transaction on BigQuery, one snapshot on Iceberg, and
on ClickHouse a key table + lightweight `DELETE` + one `INSERT` per table with
the watermark written in one statement. The next window drains while the
previous one applies. Replaying a window is idempotent, so a crash anywhere
converges on the next run.

| destination | shape | notes |
|---|---|---|
| Postgres | temp tables + `COPY` + `DELETE USING` + `INSERT SELECT`, one tx per table-window | a destination FK pointing into the group is refused at admission; a PK is added after bootstrap |
| ClickHouse | MergeTree family; lightweight `DELETE` (patch parts on 25.7+) | `Replicated*` and `ON CLUSTER` tables are refused for CDC unless `APITAP_CH_CDC_ALLOW_REPLICATED=1` **and** the URL names one node; `changelog=True` makes a window one `INSERT` and never mutates a part |
| MySQL | `TEMPORARY` tables + `LOAD DATA LOCAL` + `DELETE … JOIN` + `INSERT SELECT`, one tx | GTID-enforced destinations work |
| BigQuery | all-STRING staging table + one `MERGE` per table inside a fenced transaction | needs a **billed** project (DML); `changelog=True` replaces the ~7 s MERGE floor with a load job plus one `INSERT … SELECT` |
| Iceberg | data file + equality-delete file + watermark properties in **one** snapshot | single-column integer/text/uuid key, unpartitioned tables, format v2; Iceberg drains are the one destination the concurrency guard does not cover |

**Groups and slots.** `tables=[…]` shares **one** slot: every member lands at
the same LSN each window, one retention risk, one watermark; a group fails as a
unit and nothing is confirmed past a window every member applied. `slots=N`
splits a large group across N slots drained concurrently — Postgres decodes
each slot in one `walsender` process that saturates a core long before apitap
does, so on 100 tables / 100M changes `slots=4` went from 121,789 to 278,947
changes/s (2.29×; every slot decodes the whole WAL and keeps its own tables, so
gains flatten past 4–16). The source pays one busy core per slot.

**Memory.** Windows are sized from the cgroup; the bootstrap uses the bulk
planner (divided by `slots`); one source transaction is capped at 256 MiB of
buffered changes (`APITAP_TX_BUF_BYTES`) and **refused** with its table, size
and knob rather than OOM-killing the container. Measured: a 2.1M-event backlog
replays inside a 0.5 vCPU / **44 MB** container (33 MB peak); the 30-table
group runs under 200 MB.

**What it does not do.** DDL is not replicated: when a source column set
changes the window closes at that point, and a destination that lacks the
column is refused with the remedy (`mode="replace"` once to realign). A CDC
schedule paused past the source's retention (`wal_status = lost`, a purged
binlog) is refused before replication starts — clear the table's
`_apitap_state` rows to re-bootstrap. `changelog=True` is at-least-once on
ClickHouse (the append and the watermark are two statements, so a crash
between them can leave duplicate history under a new `_apitap_lsn`;
`<table>__current` stays correct); row stores and Iceberg refuse it.

## Guarantees

- **Atomic.** Rows land in a staging object and are published in one metadata
  operation; readers never see a partial table and a mid-run failure leaves
  the previous table untouched. **0-row guard:** an empty source never wipes a
  destination.
- **Exact types or a refusal.** Every mapping is lossless by construction
  (exact `NUMERIC` up to `DECIMAL(65)`, `BIGINT UNSIGNED` → `numeric(20,0)`,
  UTC-normalized timestamps, `jsonb` round trips); a value a destination
  cannot hold — `NaN` into MySQL, `infinity` dates, a year outside MySQL's
  range — fails the run instead of being coerced. The 15-column benchmark
  schema is asserted byte-faithful, values and column types, in the test suite.
- **One run per destination table.** The run's identity (start time, mode,
  source, nonce) is part of every staging name, so a run can only publish what
  it minted; a second run is refused at `prepare` with `LockedError` naming
  the run that holds the table. A run announces itself *before* it looks, so
  two runs starting in the same instant cannot both proceed (when each sees
  the other, both fail loudly with nothing written). Drains are guarded the
  same way against bulk runs and against each other, across versions since
  0.55.1. A drain renews a **lease** while it runs and every write it makes is
  conditional on that lease, so a drain killed outright stops blocking its
  table once the lease lapses (`APITAP_LEASE_TTL_SECS`, 300 s) and the next
  run resumes from the watermark; a killed *bulk* run's staging holds data and
  must be dropped by hand — the error names it, and nothing collects it on a
  guess.
- **Refuse rather than corrupt or hang.** A source transaction past the
  buffer cap, a half-open replication socket (TCP keepalive plus a 120 s
  silence budget, `APITAP_REPLICATION_SILENCE_SECS`), a lost slot, a foreign
  key into a CDC group, a non-UTF-8 MySQL column, a `REPLICA IDENTITY NOTHING`
  table, a cursor or lane mismatch in `_apitap_state`, an ambiguous Iceberg
  commit (settled by the client-chosen snapshot id, never assumed failed) —
  each is a named error before or instead of a wrong write. One source feeding
  two destinations gets two slots, because slot names carry the destination's
  identity.
- **Safe by default.** TLS required for MySQL off-loopback; cleartext/MD5
  Postgres auth refused off a verified channel; credentials only in the
  `Authorization` header, never in a URL or a log line; a `Link` header that
  leaves the GitHub API host is not followed; every wire decoder is run
  against mutated and truncated input in the test suite, and lengths a peer
  chooses are capped before they become allocations.
- **Stoppable.** A `log_based` run takes the first SIGTERM — what Kubernetes,
  Airflow and systemd send — as a request to land the window in flight,
  advance the watermark and exit 0, and chains to any handler your process
  installed; a second SIGTERM is not absorbed. Bulk runs and a CDC table's
  first (bootstrap) run stay killable, since a half-done bulk load has nothing
  worth keeping.

Every one of these sentences is a row in the release gate's claim matrix,
proved by a leg that asks the *server* what happened, not the exit code —
including what a killed process leaves behind, produced on purpose against live
databases. 0.59.0 shipped on **87 legs passed, 0 failed, 0 skipped**.

## Operating it

**Progress, with no flag.** On a terminal, one rewritten line every 2 s:

```
apitap ▸ bank_transfer · 6,261,134 rows · 2.37 GB · ≈63% (est) · 844K/s · 0:08 · 32 pipes
```

Everywhere else — Airflow, Kubernetes, docker logs, cron — a plain `key=value`
line every 30 s, flushed per line, no ANSI:

```
2026-08-17T06:48:41Z apitap progress table=bank_transfer rows=206656 bytes=8495323 rows_per_s=497554 bytes_per_s=20453727 elapsed_s=0.4 pipes=32
2026-08-17T06:48:42Z apitap done     table=bank_transfer rows=1000000 bytes=413190127 rows_per_s=608954 bytes_per_s=251614149 elapsed_s=1.8 pipes=32
```

A CDC run counts changes and windows (`cdc window 3 · 1,240,000 changes`) and
emits a `slot.wal` gauge with the WAL the slot is retaining on the source,
warning past `APITAP_SLOT_WAL_WARN` (4 GB) — the difference between a paused
schedule and a full source disk. `APITAP_PROGRESS=json` makes every line one
JSON object for Loki/Fluentd. Security-relevant notes print even when progress
is off.

**Environment knobs** (all optional; the defaults are the measured ones):

| variable | default | effect |
|---|---|---|
| `APITAP_PROGRESS` / `APITAP_PROGRESS_INTERVAL` | auto · 2 s / 30 s | `0` silences, `json` structures; the cadence in seconds |
| `APITAP_MEM_BUDGET` | the cgroup limit | plan pipes, batches and windows against a smaller number in a shared container (`200M`, `1G`) |
| `APITAP_GRACEFUL_STOP` | `1` | `0` = die on the first SIGTERM instead of landing the window |
| `APITAP_LEASE_TTL_SECS` | `300` (30–3600) | how long a drain's claim outlives its last renewal |
| `APITAP_CDC_WINDOW_BYTES` | auto (≤ 24 MiB) | bytes buffered per CDC window; 32 MiB is the measured best at 256 MB |
| `APITAP_WINDOW_MAX_SECS` | `3600` | wall-clock cap on one window |
| `APITAP_TX_BUF_BYTES` | 256 MiB | the one-transaction buffer cap behind the refusal |
| `APITAP_REPLICATION_SILENCE_SECS` | `120` | silence budget on the replication socket (lower only) |
| `APITAP_READ_LOWAT` | `64K` | receive watermark on the plain-TCP replication socket so one `recvfrom` carries a backlog chunk instead of ~5 KB (8–16× fewer syscalls, measured on 0.59.0); the reader waits at most 2 ms for it, so keepalives still flow; `0`/`off` disables (the A/B control) |
| `APITAP_DECODE_WORKMEM` | server value | per-session `logical_decoding_work_mem`, ≤ 256 MiB |
| `APITAP_CDC_APPLY_LANES` | auto (ClickHouse `min((mem−96 MiB)/20 MiB, 16·cores)` clamped 1–16; BigQuery ≤ 8; Postgres/MySQL 1) | concurrent table applies per window |
| `APITAP_CH_MAX_BODY` | one request per pipe | cap each ClickHouse request body (`512K`, `64M`) for a proxy with a body limit |
| `APITAP_CH_CDC_ALLOW_REPLICATED` | unset | allow CDC into a `Replicated*` table when the URL names a single node |
| `APITAP_HTTP_CONNECT_TIMEOUT` / `APITAP_HTTP_READ_TIMEOUT` | `15` s / off | connect deadline; the read timeout is a *total* request deadline and stays off because a long load sends nothing until it is done — TCP keepalive detects dead peers instead |
| `APITAP_ALLOW_INSECURE_AUTH` | `0` | accept a cleartext/MD5 password request on an unverified Postgres channel |
| `APITAP_SLOT_WAL_WARN` | `4G` | retained-WAL threshold for the warning |
| `APITAP_PG_BINARY` | `0` | ask the walsender for binary `pgoutput` (measured neutral on the wall, −9% walsender CPU; opt-in) |
| `APITAP_DEBUG` | off | per-phase timings on stderr |

A/B escape hatches also exist (`APITAP_RAW_COPY=0`, `APITAP_MY_RAW=0`,
`APITAP_MY_ARROW=0`, `APITAP_BATCH_BYTES`, `APITAP_BQ_APPLY_LANES`,
`APITAP_BQ_CLUSTER=0`, `APITAP_GSHEETS_PAGE_ROWS`); they select slower
reference lanes and are not tuning knobs.

**Troubleshooting**

| you see | why | do |
|---|---|---|
| `invalid port number`, or a connect error naming a host you never typed | a password with `@ / ? # : [ ]` is being read as URL syntax | `quote(password, safe='')` before building the URL |
| `mysql source connect TLS: server does not support TLS` | TLS is required off-loopback and the server has none | add `?ssl-mode=disabled` to send in clear on purpose, or enable TLS |
| `sslmode=verify-ca is not implemented` | chain-only verification cannot be expressed safely | use `verify-full` or `require` |
| ClickHouse `413 Payload Too Large` | a proxy (nginx) caps request bodies | `APITAP_CH_MAX_BODY=512K` (bulk: also `chunk_bytes=256*1024`) |
| `log_based: … Replicated… refused` | CDC into a replicated/cluster table would apply on one replica | point at one node and set `APITAP_CH_CDC_ALLOW_REPLICATED=1`, or use a non-replicated table |
| `locked: another apitap run is already loading this table` | a live peer, or a dead one's leftovers | wait; a dead drain clears itself after the lease TTL; a dead bulk run's object is named in the message — drop it once |
| `wal_status = lost` / `binlog … purged` | the schedule paused past the source's retention | clear the table's `_apitap_state` rows (both `t` and `schema.t` spellings) and re-run: it re-bootstraps |
| `one transaction … exceeds the cap` | a source transaction bigger than the buffer | split the writer's transaction, or raise `APITAP_TX_BUF_BYTES` within the box |
| `state row tracks cursor 'x'` / `is CDC-managed` | the table is owned by another lane or cursor | keep that lane, or clear the state rows to hand it over |
| `column 'c' is in the WAL but not in the … target` | the source gained a column | `mode="replace"` once to realign, then resume |
| `string column … charset latin1` | non-UTF-8 text cannot round-trip through the CDC lane | convert the column to `utf8mb4` |
| BigQuery CDC fails on DML | sandbox (no-billing) project | CDC into BigQuery needs billing; bulk modes work on sandbox |

## Performance, measured

Every table here is a full refresh of the same 15-column table (ingestr's own
benchmark schema and value generators), each tool in its own container with
identical caps, stock Docker databases, warm runs, and the destination
checksummed against the source (19 per-column aggregates) before a time counts.
Rivals run their own documented best configuration. Timing is inside the
container around the transfer only.

**10M rows, every tool at 16 vCPU / 4 GB** (apitap from PyPI):

| route | apitap | ingestr 1.0.75 | dlt default | dlt + pyarrow |
|---|---|---|---|---|
| Postgres → Postgres | **20.2 s** | 500 s | 2,604 s | 708 s |
| Postgres → ClickHouse | **9.9 s** | 111 s | 1,893 s | 360 s |
| MySQL → ClickHouse | **10.4 s** | 97 s | 2,231 s | failed (type inference) |
| MySQL → Postgres | **22.5 s** | 481 s | 2,899 s | failed (type inference) |
| Postgres → MySQL | **64.3 s** | 366 s | 329.7 s at 1M (52×) | 181.5 s at 1M (28×) |
| Postgres → BigQuery | **28.4 s** | 860 s | 2,160 s | — |

dlt + connectorx was OOM-killed on every route at the same 4 GB. At 10M the
Postgres → ClickHouse source itself is the wall: 16 parallel binary `COPY`s to
`/dev/null` take ~11 s on that host with no tool involved. `durable=False` cuts
pg → pg from ~24 s to **15.5 s** and mysql → pg from ~27 s to **19.5 s**.

**The tool alone capped at 0.5 vCPU / 256 MB, databases uncapped** (0.57.0 from
PyPI, three interleaved rounds):

| job | apitap | the other tool |
|---|---|---|
| Postgres → ClickHouse, 10 × 1M rows, one call | **18.4 s**, 119 MB peak, 30/30 MATCH | walshadow 0.1.2: OOM-killed 6/6 legs (needs ~550 MB; lands all ten at 1 GB in ~125 s) |
| MySQL → ClickHouse, 10 × 1M rows, one call | **36.1 s**, 99 MB peak, 30/30 MATCH | ingestr 1.1.61: OOM-killed 6/6 legs, 0 rows |
| Postgres → ClickHouse **CDC**, 30 tables, 30M-row bootstrap | **63.8 s**, 105 MB peak, 60/60 MATCH; then 2.97M changes at a flat 65.6 MB | walshadow: OOM-killed 8/8 legs, ≤ 70,656 rows |
| MySQL → ClickHouse **CDC**, 30 tables, 30M-row bootstrap | **106.2 s**, 119 MB peak, 60/60 MATCH; then 2.97M changes at a flat 55.5 MB | ingestr: refuses ClickHouse as a CDC destination |
| Postgres → ClickHouse, 5M rows, apitap.dev/lab rig | **25.6 s** (29.1 s at 1 GB / 2 vCPU — fewer pipes, less insert contention) | ingestr 1.1.1 201 s; dlt 1.29 + pyarrow OOM-killed |
| TPC-H, 10 tables × 1M / × 5M, Postgres → Postgres, one `schema=` call | **33 s / 168 s**, **52–53 MB** peak, 10/10 | dlt OOM-killed in 7–14 s; ingestr (10 sequential invocations) 327 s / 1,238 s |

**Scale.** 232M rows / 101 GB, Postgres → ClickHouse, 0.5 vCPU / 256 MB: **8m57s**,
170.8 MB peak (latest ingestr and dlt+pyarrow OOM-killed in ~21 s on the same
box); the same table completes inside a **44 MB** cap. 100M rows / 46 GB at
16 vCPU / 4 GB: **139.2 s** over 32 pipes (1.2 TB/hour, past the source's page
cache). Three dedicated GCE machines over an internal VPC: the 232M-row table in
**30.3 s** (~3.3 GB/s), the alternatives at 0 rows when cut.

**CDC** (CDC side at 0.5 vCPU / 256 MB, writer unconstrained, every leg
count- and digest-verified):

| shape | result |
|---|---|
| 650K-event window, Postgres → Postgres, every tool at 0.5 vCPU / 256 MB | **apitap 12 s** · ape-dts 22 s · ingestr 78 s · pipelinewise 227 s; apitap's 12 s is the same on a 16-core box |
| 650K-event backlog, MySQL → ClickHouse | **15.6 s**; ape-dts had not converged at 900 s |
| 10 tables, 40M changes per source, 80M total | MySQL binlog **84K changes/s** (5 cols, updates) to **135K/s** (inserts); Postgres WAL **51K/s** (5 cols) to **34K/s** (15 wide cols) — the wide-row figure is 92% of what `pg_recvlogical` manages in the same cage just receiving the stream |
| `changelog=True` vs replica | free on the Postgres lane; **+34%** on MySQL (**113.8K/s**) |
| 30 tables in one group, 15 columns, 0.5 CPU | **31,679 changes/s = 1.90M/min** pure catch-up (15.35 µs of client CPU per change, 163 MB, 30/30); ~**1.50M/min** paced by a 35K/s writer. Flat above 1 CPU: a single-threaded drain and one `walsender` are the ceiling the 3M/min design removes |
| 1 table, 0.59.0 wheel | kept up with a **2.19M/min** writer for 150 s at **0.40 core**, 101 MB peak, digest matched |
| 100 tables, 100M changes, 1 core | `slots=4`: **278,947 changes/s** (2.29× one slot) |
| 180M changes, 4 sharded Postgres → 1 ClickHouse, both movers at 6 CPU / 2 GB | apitap **125 s** of apply work, 400/400 tables verified, ~3 µs CPU/change; PeerDB (its own best configuration) DNF at ~89% when the 30-minute cap fell, ~47 µs/change, 5.9 of 6 cores pegged (n=1 per leg) |
| the stress question: 1M changed rows per table per minute × 30 tables at 0.5 CPU | **no** — the slot was lost before a change committed, and the refusal said so |

**`read()`** (0.5 vCPU / 256 MB unless noted): 10M Postgres rows → polars in
**13.2 s** at ~100 MB (bench box uncapped: 14.9 s vs connectorx 55.9 s, pandas
295 s); 11.8M string-heavy MySQL rows in **29 s** at 126 MB, count-style scans in
3.4 s (driver-based readers: 51 s); `.lazy()` filter + group_by over 50M rows in
**9.9 s** at ~180 MB (polars/connectorx OOM-killed until 1–2 GB; ADBC 45.2 s);
50M rows filtered and sunk to Parquet in **34 s**; a Postgres-50M × MySQL-50M
join as one polars expression in 165 s on 4 cores, 154 s on the half-core box
with both extracts concurrent.

**Files and lakes** (16 vCPU / 4 GB, every output read back with DuckDB and
checksummed): Postgres → GCS 10M in **15.5 s** (csv.gz) / **18.2 s** (Parquet),
ingestr 124 s, dlt ~7–29 min extrapolated; → S3/MinIO Parquet **14.5 s** (dlt
379 s; ingestr OOM-killed with 0 objects); → Iceberg full 10.3M / append +1M /
merge 1M upsert in **15.2 s · 2.2 s · 3.8 s** (dlt+pyiceberg OOM-killed;
ingestr has no Iceberg destination). ClickHouse → ClickHouse 10M: **8.4 s**
uncapped, **20.6 s** inside 256 MB.

**What these numbers do not show.** Source and destination ran on one host
(loopback) in most rows; a two-instance control landed within noise and a netem
study found the gap *narrows* as RTT rises, so WAN production is slower than
this and the margin is smaller. Databases were uncapped and ordinary-sized — a
1.5 GB ClickHouse cannot receive a 5M-row insert from any tool. First-run
install time is excluded for everyone. The CDC rows are a 15-column shape; row
width moves every number. Two legs in the original runs failed because the
benchmark filled its own disk and were re-run after cleanup; a comparison that
retracted one of our own numbers (5.2× → 2.6× on `read()`) is recorded the same
way.

## Type mapping

**Postgres source** → each destination (RowBinary / Parquet lanes; anything not
listed rides as text):

| Postgres | ClickHouse | MySQL | BigQuery | Iceberg / Parquet |
|---|---|---|---|---|
| `int2` · `int4` · `int8` | `Int16` · `Int32` · `Int64` | `SMALLINT` · `INT` · `BIGINT` | `INT64` | `long` |
| `float4` · `float8` | `Float32` · `Float64` | `FLOAT` · `DOUBLE` | `FLOAT64` | `float` · `double` |
| `numeric(p,s)`, p ≤ 38 | `Decimal(p,s)` | `DECIMAL(p,s)` (≤ 65,30) | `NUMERIC` / `BIGNUMERIC` | `decimal(p,s)` |
| `numeric` unconstrained | `Float64` | `DOUBLE` (range-checked) | `BIGNUMERIC` | text lane |
| `bool` | `UInt8` | `TINYINT(1)` | `BOOL` | `boolean` |
| `date` | `Date32` | `DATE` | `DATE` | `date` |
| `timestamp` · `timestamptz` | `DateTime64(6)` · `DateTime64(6,'UTC')` | `DATETIME(6)` (UTC session) | `DATETIME` · `TIMESTAMP` | `timestamp` · `timestamptz` |
| `uuid` | `UUID` | `CHAR(36)` | `STRING` | `string` |
| `text` · `varchar(n)` · `bpchar` · `json` | `String` | `LONGTEXT` · `VARCHAR(n)` | `STRING` | `string` |
| `jsonb` | `String` | `JSON` | `STRING` | `string` |
| `bytea` | `String` | `LONGBLOB` (via HEX) | `BYTES` | `binary` |

**MySQL source:** `tinyint`…`bigint` → ClickHouse `Int8`…`Int64` /
`UInt8`…`UInt64` for `unsigned`; → Postgres `smallint`/`int`/`bigint` widened for
`unsigned`, `bigint unsigned` → `numeric(20,0)`. `decimal(p,s)` exact (p ≤ 38 →
`Decimal`, larger → `String` on ClickHouse; `numeric` on Postgres up to
`DECIMAL(65)`). `datetime` → `DateTime64(6)` / `timestamp`; `timestamp` →
`DateTime64(6,'UTC')` / `timestamptz` (sessions run UTC). `json` → `String` /
`jsonb`; `enum`/`set`/`time` → text; `binary`/`blob`/`bit` → `String` / `bytea`.
Same-engine routes (pg → pg, my → my) mirror the source DDL verbatim, charset
and collation included.

## Roadmap

- [x] Postgres, MySQL/MariaDB, ClickHouse, Google Sheets, GitHub files and the
      GitHub API as sources; Postgres, MySQL, ClickHouse, BigQuery, GCS,
      S3-compatible stores and Apache Iceberg as destinations — the full mesh
      enforced by a test that fails the build on an unlisted pair
- [x] Incremental `append` / `merge` with a transactional state table
- [x] Batch CDC from Postgres and MySQL/MariaDB into five destinations; groups
      on one slot, `slots=N`, `changelog=True`, per-table modes in one call
- [x] `apitap.read()` → polars / pyarrow / duckdb / pandas with projection and
      filter pushdown; Parquet sinks at constant memory; cross-engine joins
- [x] Multi-table and whole-schema transfers under one memory budget
- [x] Concurrency guard with run identity in every artifact name, leases that
      let a killed drain heal itself, SIGTERM that lands the window, progress
      in terminal / plain / JSON shapes
- [ ] **3M changes/min of CDC on 0.5 vCPU / 256 MB**, scaling automatically
      with the box (measured today: 1.90M/min catch-up)
- [ ] `query=` for `read()` (arbitrary SQL, not just tables)
- [ ] Snowflake destination
- [ ] aarch64 and macOS wheels, plus an sdist

## Development

```bash
cargo test -p apitap-core           # engine tests (MSRV 1.94; a patched sqlx-core is vendored)
uv pip install -e py-apitap         # build the wheel with maturin (needs Rust)
python benchmarks/gate.py --matrix  # release pre-flight: every claim × engine must have a proving leg
```

Everything lives in one crate, `crates/apitap-core`; `py-apitap` is a thin PyO3
shim that releases the GIL and exports an Arrow C stream for `read()`.
`pipeline/` runs every bulk route's lifecycle and holds the route table and the
memory planner; `source/` and `sink/` implement `Source` and `Sink` per engine;
`wire/` holds the encoders and apitap's own wire clients (Postgres walsender and
`COPY`, `pgoutput`, MySQL protocol and binlog, RowBinary, `LOAD DATA` text,
Parquet, Arrow); `logbased/` is CDC (the drain/collapse/window pipeline, groups
and slots, one apply per destination); `naming.rs`, `guard.rs` and `lease.rs`
are the run identity, the announce-then-check guard and the fence every CDC
write is conditional on. Encoders are deliberately per-(source, format) and
fully monomorphized — there is no neutral in-memory row representation, because
the fast lanes *are* the product. A new destination is one `sink/<name>.rs`
(plus a `logbased/dest_<name>.rs` for CDC) and its route-table arms; it then
works with every source whose wire format it accepts.

The release gate runs 87 end-to-end legs against live servers (Postgres with
logical replication, MySQL 8.4, MariaDB, ClickHouse, MinIO and a REST Iceberg
catalog, BigQuery, two older wheels for upgrade and rollback legs) and prints
one verdict; a skipped leg is a reported leg. Longer write-ups — the benchmark
ledgers with raw logs, the failure-mode catalogue, the CDC design and the
3M/min plan, the per-release reviews — live under `benchmarks/`, `docs/`,
`docs/design/` and `docs/review/` in this repository.

## License

MIT. The managed cloud — scheduling, always-on per-tenant workers, monitoring,
a UI — is apitap.dev.
