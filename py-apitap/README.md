# apitap

**Move whole tables between databases at wire speed, in bounded memory.**

apitap is an open-source transfer engine — a Rust core with Python bindings, in the
spirit of Polars. It moves data the way the databases themselves would: raw
wire-format streams, parallel range pipes, atomic swaps, and memory that stays flat
no matter how big the table is.

```bash
pip install apitap
```

```python
import apitap

report = apitap.transfer(
    "postgres://user:pass@src-host/db",
    "clickhouse://user:pass@warehouse/db",
    table="public.events",
)
print(f"{report.rows:,} rows in {report.elapsed_ms} ms over {report.parallel} pipes")
```

## Try it before you install it

**[apitap.dev/lab](https://apitap.dev/lab)** runs this exact wheel — alongside
ingestr and dlt, each pip-installed next to it — against a seeded Postgres and
ClickHouse, in your browser. Pick a tool, pick the container it runs in
(1 GB / 2 vCPU or **256 MB / 0.5 vCPU**), press run, and watch the engine's own
output. Every result is row-count-verified before a number appears.

That box picker is the point — it limits the **tool**, not the databases:

| PG → ClickHouse, 5M rows | tool in 256 MB / 0.5 vCPU | tool in 1 GB / 2 vCPU |
|---|---|---|
| **apitap** | **25.6 s** | **29.1 s** |
| ingestr 1.1.1 | 201 s | 62.1 s |
| dlt 1.29 + pyarrow | **OOM-killed** | 208 s |

dlt materializes the result set, so it dies before the data arrives; ingestr
streams and survives but crawls; apitap barely notices the box — it is marginally
*faster* on the small one, because fewer vCPUs means fewer pipes and less insert
contention at the destination.

And it holds at scale: **100 GB — 232M rows — through that same 256 MB /
0.5 vCPU container in 8m57s**, peak RSS 170.8 MB, every row checksum-verified;
on that table ingestr v1.1.14 and dlt 1.29.1 (pyarrow) are OOM-killed in
~21 s. Peak memory is `pipes × chunk_bytes`, never table size — the same
100 GB also lands inside a **44 MB** container. Give it real hardware and the
same zero-config call does the same 100 GB in **30.3 seconds** (~3.3 GB/s,
three dedicated GCE machines — where the alternatives had landed zero rows
when cut). v0.15.0 additionally auto-thins chunks on memory-capped boxes
(128 MB tier: 2.5× faster than v0.14.0) and fixes a silent hang against
MySQL 8.4 servers. Ladder, methodology and raw logs:
[benchmarks/profiling.md](https://github.com/apitap/apitap-lib/blob/main/benchmarks/profiling.md)
· [gcp-benchmark.md](https://github.com/apitap/apitap-lib/blob/main/benchmarks/gcp-benchmark.md).

## Routes

Five sources × seven destinations — **all 35 wired**, enforced by a test that fails
the build if any pair is neither implemented nor explicitly deferred with a reason.

**Sources:** `postgres://` · `mysql://` · `gsheets://` (tabs as tables) ·
`github://` (repo CSVs as tables) · `github+api://` (issues, PRs, commits, stars …
as typed tables)

**Destinations:** `postgres://` · `mysql://` · `clickhouse://` · `bigquery://` ·
`gcs://` (CSV.gz or Parquet) · `s3://` (S3-compatible — AWS, MinIO, R2,
OVH/Scaleway/Hetzner object storage; Parquet, SigV4-signed, no SDK) ·
`iceberg://` (Apache Iceberg via any REST catalog — Lakekeeper, Polaris, Nessie,
Glue, R2 Data Catalog, S3 Tables; **replace, append and merge are all real
snapshot commits**, incremental state rides in the table itself)

Each pair negotiates the fastest wire format both sides speak — for example:

| route | how it moves |
|---|---|
| `postgres://` → `postgres://` | raw binary `COPY` passthrough — no row decode at all |
| `postgres://` → `clickhouse://` | binary COPY transcoded in-flight to `RowBinary` |
| `postgres://` → `mysql://` | binary COPY rendered in-flight as `LOAD DATA` text |
| `mysql://` → `postgres://` | wire decode → binary COPY (exact decimals to `DECIMAL(65,30)`) |
| any → `bigquery://` | Parquet or CSV load jobs — free path, sandbox-safe |

Every transfer stages and swaps in atomically — readers never see a partial table,
an empty source never wipes a good one, and a mid-run failure leaves the previous
table untouched.

## How fast?

**10M rows, every tool capped at 16 vCPU / 4 GB, auto settings, stock Docker
databases** — measured from the published wheel, every number checksum-validated
across engines:

| route | apitap | [ingestr](https://github.com/bruin-data/ingestr) | dlt (default) | dlt + pyarrow |
|---|---|---|---|---|
| Postgres → Postgres | **20.2 s** | 500 s | 2 604 s | 708 s |
| Postgres → ClickHouse | **9.9 s** | 111 s | 1 893 s | 360 s |
| MySQL → ClickHouse | **10.4 s** | 97 s | 2 231 s | failed¹ |
| MySQL → Postgres | **22.5 s** | 481 s | 2 899 s | failed¹ |
| Postgres → MySQL | **64.3 s** | 366 s | — ² | — ² |
| Postgres → BigQuery | **28.4 s** | 860 s | 2 160 s | — |

¹ dlt's pyarrow backend refuses MySQL `DOUBLE` without hand-written schema hints;
its connectorx backend was OOM-killed on all four routes at the same 4 GB cap.
² dlt has no native MySQL destination; via its documented `sqlalchemy` path it is
28–52× slower (measured at 1M).

Full methodology, validation queries, and honest caveats — including what these
runs do *not* show:
[benchmarks/README.md](https://github.com/apitap/apitap-lib/blob/main/benchmarks/README.md).

## API

```python
apitap.transfer(
    src, dst, table=None, *,
    tables=None,         # a list of tables, or…
    schema=None,         # …a whole schema — one shared resource budget
    dest_table=None,     # defaults to `table`
    mode="replace",      # "append"/"merge" incremental · "log_based" batch CDC
    cursor=None,         # auto: integer PK; PK-less Postgres uses TID ranges
    parallel=None,       # auto: CPU- and memory-aware; an explicit value wins
    chunk_bytes=None,    # per-send coalescing, default 4 MiB
    durable=True,        # False = UNLOGGED staging on Postgres dests (~-30% wall)
    engine=None, order_by=None, on_cluster=None,   # ClickHouse DDL
) -> TransferReport      # .rows, .elapsed_ms, .parallel, .tables
```

`mode="append"` loads only rows past the last synced watermark; `mode="merge"`
upserts the delta by primary key. `mode="log_based"` is **batch CDC** for
Postgres sources: the first run creates a logical replication slot and
bootstraps with a full load pinned to the slot's exported snapshot (no gap,
no duplicates); every later run drains the WAL delta — inserts, updates
(PK changes included), deletes, TRUNCATEs, TOAST handled — and applies it
set-based in one destination transaction that also advances the LSN
watermark. Schedule the same call from cron/Airflow; no daemon. The watermark lives in **`_apitap_state`** — a
plain, queryable table in the destination database, one row per (table, source),
written **in the same transaction as the data** on Postgres. On Iceberg it lives
in the table's own properties, committed **in the same snapshot as the data**.
No local state files, no opaque blobs, no extra columns in your rows. A 1M-row delta lands on a 10M-row
table in ~10 s — cost is proportional to the delta, not the table.

Multi-table runs share one pipe budget, so peak memory is a single table's ceiling
no matter how many tables you pass. Each table lands atomically and independently:
one failure never poisons its siblings.

The GIL is released for the whole transfer. Errors are `ValueError` for bad input
(unknown table, unsupported type — always at probe time, never mid-copy) and
`RuntimeError` for transfer failures.

### `apitap.read()` → Arrow / polars

```python
df = apitap.read(src, table="events").to_polars()    # polars DataFrame

# tables BIGGER than RAM: one line, ordinary polars, streaming underneath —
top = (apitap.read(src, table="events").lazy()
       .filter(pl.col("amount") > 100)
       .group_by("status").agg(pl.len())
       .collect(engine="streaming"))

# MySQL reads the same way — and joins ACROSS engines are just polars:
orders = apitap.read("postgres://…", table="orders").lazy()
events = apitap.read("mysql://…", table="events").lazy()
daily = orders.join(events, on="id").group_by("day").agg(pl.len()).collect(engine="streaming")

# land a FILTERED projection straight to Parquet, streaming end to end:
(apitap.read(src, table="events").lazy()
 .filter(pl.col("amount") > 100).select("id", "amount")
 .sink_parquet("events.parquet", compression="zstd"))

apitap.read(src, table="events").to_parquet("events.parquet")  # full-table dump
tbl = apitap.read(src, table="events").to_arrow()              # pyarrow Table
```

The same parallel pipes every transfer route uses feed Rust-side
Arrow column builders; batches cross into Python zero-copy through the Arrow
C stream protocol (`__arrow_c_stream__`), so polars, pyarrow, duckdb and
pandas consume the reader natively — the wheel depends on none of them.
Postgres rides raw binary COPY; MySQL rides its own hand-rolled wire
client — binary-protocol rows decode straight off the socket into the
column builders (no driver, no per-row allocations).
`.lazy()` registers the stream as a polars scan and pushes the query's
COLUMN PROJECTION all the way into the SQL: a query touching 2 of 15
columns makes the server serialize and this side decode only those 2 — the
compute itself (filter/group/join) stays in polars, and no loop ever
appears in your code. Typed end to end (int16/32/64, float32/64, bool,
decimal128, date32, timestamp µs, utf8, binary); uuid/jsonb/exotics
arrive as text, so every table reads.
Measured on the bench box: 10M rows → polars in **14.9 s** (connectorx
55.9 s, pandas 295 s, same box). In a **0.5 vCPU / 256 MB** container:
ten million Postgres rows stream through in **13.2 s** flat at ~100 MB;
11.8 million MySQL rows (15 columns, string-heavy) stream through in
**29 s at 126 MB** on the same half core — count-style thin scans in
**3.4 s** — where driver-based readers take 51 s for the same drain;
a real `.lazy()` filter + group_by lands in **2.4 s** — and the
same query over FIFTY million rows in **9.9 s** at a flat ~180 MB, tying
raw SQL run inside Postgres itself. The lazy plan also SINKS: 50M rows
filtered and landed as Parquet in **34 s inside that same container**,
row-count-verified against the database. Cross-engine works at scale —
a Postgres-50M × MySQL-50M join (one polars expression, 100M rows,
digit-verified) runs in **165 s on 4 cores**, and two engines extract
CONCURRENTLY: 50M from Postgres + 50M from MySQL, aggregated per day and
joined across engines, **154 s total on the half-core / 256 MB box** —
the Postgres leg hides entirely inside the MySQL scan.
Alternatives, same cage, same query, same digits: plain polars
(`read_database_uri`/connectorx) is OOM-killed at 256 MB — and needs
1–2 GB before it survives at all; ADBC (`iter_batches`) does stream, but
single-connection: **45.2 s where apitap takes 9.9 s** (4–5× across every
shape we measured).
`parallel=1` preserves source order; `cursor=` picks the split column;
`columns=` reads a projection directly.

Full usage guide — connection URLs, per-route type mappings, incremental semantics,
troubleshooting:
[docs/usage.md](https://github.com/apitap/apitap-lib/blob/main/docs/usage.md).

## Roadmap

- [x] The route mesh — Postgres, MySQL, Google Sheets, GitHub files and the
      GitHub API into Postgres, MySQL, ClickHouse, BigQuery, GCS, S3-compatible
      object stores (MinIO, R2, …) and Apache Iceberg
- [x] Incremental sync — `mode="append"` / `mode="merge"` (transactional state table)
- [x] Batch CDC — `mode="log_based"`: logical-replication drains on a schedule,
      every WAL operation captured, snapshot-pinned bootstrap, crash-safe
      LSN watermark committed with the data (Postgres→Postgres first)
- [x] Apache Iceberg destination — overwrite/append/row-delta snapshots on any
      REST catalog; watermarks committed as table properties **in the same
      snapshot as the data**; bootstrap from parquet footer stats (picks up
      incremental on tables written by Spark/Trino/pyiceberg too)
- [x] Multi-table and whole-schema transfers under one memory budget
- [x] ClickHouse table engines — `engine=`, `order_by=`, `on_cluster=`
- [x] `apitap.read()` → Arrow / polars / pyarrow / duckdb — zero-copy Arrow C
      stream, 10M → polars 14.9 s (connectorx 55.9 s); `.lazy()` runs ordinary
      polars queries with projection pushdown — 50M rows, filter+group_by,
      0.5 vCPU / 256 MB: 9.1 s; `.to_parquet()` streams a table to a file at
      constant memory; `columns=` for direct projections
- [x] MySQL source for `read()` — a hand-rolled wire client decodes
      binary-protocol rows straight into Arrow, 1.8× a driver-based full
      drain (5.5× on thin scans) at 0.5 vCPU / 256 MB; cross-engine joins
      (MySQL × Postgres) are one ordinary polars expression
- [ ] `query=` for `read()` (arbitrary SQL, not just tables)
- [ ] Snowflake destination
- [ ] aarch64 + macOS wheels

## License

MIT. Source: [github.com/apitap/apitap-lib](https://github.com/apitap/apitap-lib).
