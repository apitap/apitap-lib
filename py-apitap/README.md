# apitap

**Move whole tables between databases at wire speed, in bounded memory.**

apitap is the open-source transfer engine behind [apitap cloud](https://apitap.dev) —
a Rust core with Python bindings, in the spirit of Polars. It moves data the way the
databases themselves would: raw wire-format streams, parallel range pipes, atomic
swaps, and memory that stays flat no matter how big the table is.

```bash
pip install apitap
```

```python
import apitap

report = apitap.transfer(
    "postgres://user:pass@src-host/db",
    "postgres://user:pass@dst-host/db",
    table="public.events",
)
print(f"{report.rows:,} rows in {report.elapsed_ms} ms over {report.parallel} pipes")
```

## Routes

The URL schemes pick the route; each pair negotiates the fastest wire format both
sides speak:

| route | how it moves |
|---|---|
| `postgres://` → `postgres://` | raw binary `COPY` passthrough — no row decode at all |
| `postgres://` → `clickhouse://` | binary COPY transcoded in-flight to `RowBinary` |
| `mysql://` → `clickhouse://` | binary wire protocol decoded straight into `RowBinary` |
| `mysql://` → `postgres://` | wire decode → binary COPY (exact decimals up to `DECIMAL(65,30)`) |

Every transfer stages into `<table>__apitap_staging` and swaps in atomically —
readers never see a partial table, an empty source never wipes a good one, and a
mid-run failure leaves the previous table untouched.

## How fast?

**10M rows, every tool capped at 16 vCPU / 4 GB, auto settings, stock Docker
databases** — apitap measured from this exact published wheel (`pip install apitap`),
every number checksum-validated across engines:

| route | apitap | [ingestr](https://github.com/bruin-data/ingestr) 1.0.75 | dlt 1.x (default) | dlt + pyarrow |
|---|---|---|---|---|
| Postgres → Postgres | **20.2 s** | 500 s | 2 604 s | 708 s |
| Postgres → ClickHouse | **9.9 s** | 111 s | 1 893 s | 360 s |
| MySQL → ClickHouse | **10.4 s** | 97 s | 2 231 s | failed¹ |
| MySQL → Postgres | **22.5 s** | 481 s | 2 899 s | failed¹ |
| Postgres → BigQuery² | **28.4 s** | 860 s | 2 160 s | — |

² Measured at 0.5.0, 8 pipes uncapped (40.3 s at 2 vCPU); the wall is upload +
BigQuery-side parsing, not local CPU. 100% free-path ingestion — load and copy
jobs only, no `insertAll`, works on sandbox (no-billing) projects. At
0.5 vCPU / 256 MB apitap moves 1M rows in 17.9 s while ingestr and dlt+pyarrow
are both OOM-killed before finishing.

¹ dlt's pyarrow backend refuses MySQL `DOUBLE` without hand-written schema hints;
its connectorx backend was OOM-killed on all four routes at the same 4 GB cap apitap
runs in. Full methodology, validation queries, and honest caveats:
[benchmarks/README.md](https://github.com/apitap/apitap-lib/blob/main/benchmarks/README.md).

**In a 0.5 vCPU / 256 MB container** apitap still completes every route (28–68 s) —
the pipe count auto-derives from the cgroup's CPU *and* memory, so it never
OOM-kills itself.

## API

```python
apitap.transfer(
    src, dst, table, *,
    dest_table=None,     # defaults to `table`
    parallel=None,       # auto: CPU- and memory-aware; explicit value never overridden
    cursor=None,         # auto: integer PK; PK-less Postgres tables use TID ranges
    chunk_bytes=None,    # per-send coalescing, default 4 MiB
    durable=True,        # False = UNLOGGED staging on Postgres dests (~-30% wall,
                         # table stays unlogged until ALTER TABLE … SET LOGGED)
) -> TransferReport      # .rows, .elapsed_ms, .parallel
```

`mode="append"` loads only rows past the last synced watermark; `mode="merge"`
(Postgres destinations) upserts the delta by the destination's primary key. The
watermark lives in **`_apitap_state`** — a plain, queryable table in the destination
database, one row per (table, source), written **in the same transaction as the
data** on Postgres. No local state files, no opaque blobs, no extra columns injected
into your rows: state that is backed up, restored, and debugged together with the
data it describes. A 1M-row delta lands on a 10M-row table in ~10 s — cost is
proportional to the delta, not the table.

The GIL is released for the whole transfer. Errors are `ValueError` for bad input
(unknown table, unsupported type — always at probe time, never mid-copy) and
`RuntimeError` for transfer failures.

Full usage guide — connection URLs, per-route type mappings, durability semantics,
troubleshooting:
[docs/usage.md](https://github.com/apitap/apitap-lib/blob/main/docs/usage.md).

## Roadmap

- [x] Postgres → Postgres · Postgres → ClickHouse · MySQL → ClickHouse · MySQL → Postgres
- [x] Postgres → BigQuery — dual Parquet/CSV lanes, free-path load+copy jobs,
      DML-free incremental state (sandbox-safe)
- [x] Incremental sync — `mode="append"` / `mode="merge"` (transactional state table)
- [x] ClickHouse table engines — `engine=`, `order_by=`, `on_cluster=`
- [ ] MySQL → BigQuery
- [ ] `read_postgres()` → Arrow / Polars
- [ ] Snowflake destination
- [ ] aarch64 + macOS wheels

## License

MIT. Source: [github.com/apitap/apitap-lib](https://github.com/apitap/apitap-lib).
The managed cloud (scheduling, always-on per-tenant workers, monitoring, a UI) is
[apitap.dev](https://apitap.dev).
