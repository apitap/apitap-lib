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

## Routes

Five sources × five destinations — **all 25 wired**, enforced by a test that fails
the build if any pair is neither implemented nor explicitly deferred with a reason.

**Sources:** `postgres://` · `mysql://` · `gsheets://` (tabs as tables) ·
`github://` (repo CSVs as tables) · `github+api://` (issues, PRs, commits, stars …
as typed tables)

**Destinations:** `postgres://` · `mysql://` · `clickhouse://` · `bigquery://` ·
`gcs://` (CSV.gz or Parquet)

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
    mode="replace",      # "append" / "merge" for incremental
    cursor=None,         # auto: integer PK; PK-less Postgres uses TID ranges
    parallel=None,       # auto: CPU- and memory-aware; an explicit value wins
    chunk_bytes=None,    # per-send coalescing, default 4 MiB
    durable=True,        # False = UNLOGGED staging on Postgres dests (~-30% wall)
    engine=None, order_by=None, on_cluster=None,   # ClickHouse DDL
) -> TransferReport      # .rows, .elapsed_ms, .parallel, .tables
```

`mode="append"` loads only rows past the last synced watermark; `mode="merge"`
upserts the delta by primary key. The watermark lives in **`_apitap_state`** — a
plain, queryable table in the destination database, one row per (table, source),
written **in the same transaction as the data** on Postgres. No local state files,
no opaque blobs, no extra columns in your rows. A 1M-row delta lands on a 10M-row
table in ~10 s — cost is proportional to the delta, not the table.

Multi-table runs share one pipe budget, so peak memory is a single table's ceiling
no matter how many tables you pass. Each table lands atomically and independently:
one failure never poisons its siblings.

The GIL is released for the whole transfer. Errors are `ValueError` for bad input
(unknown table, unsupported type — always at probe time, never mid-copy) and
`RuntimeError` for transfer failures.

Full usage guide — connection URLs, per-route type mappings, incremental semantics,
troubleshooting:
[docs/usage.md](https://github.com/apitap/apitap-lib/blob/main/docs/usage.md).

## Roadmap

- [x] The 5 × 5 route mesh — Postgres, MySQL, Google Sheets, GitHub files and the
      GitHub API into Postgres, MySQL, ClickHouse, BigQuery and GCS
- [x] Incremental sync — `mode="append"` / `mode="merge"` (transactional state table)
- [x] Multi-table and whole-schema transfers under one memory budget
- [x] ClickHouse table engines — `engine=`, `order_by=`, `on_cluster=`
- [ ] `read_postgres()` → Arrow / Polars
- [ ] Snowflake destination
- [ ] aarch64 + macOS wheels

## License

MIT. Source: [github.com/apitap/apitap-lib](https://github.com/apitap/apitap-lib).
