# apitap

**Move whole tables between databases at wire speed, in bounded memory.**

apitap is the open-source transfer engine behind [apitap cloud](https://apitap.dev) — a
Rust core with Python bindings, in the spirit of Polars. It moves data the way the
databases themselves would: raw wire-format streams, parallel range pipes, atomic swaps,
and memory that stays flat no matter how big the table is.

```python
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

## Why apitap exists

apitap is built around one simple belief: **moving a lot of data should not
require a lot of machine.** Most ingestion pipelines burn money twice — once
in wall-clock hours, once in the oversized workers those hours run on. If a
transfer engine is careful enough about memory and wire formats, the same job
that needs gigabytes of RAM elsewhere can finish, faster, on the smallest
container you can rent. That difference is real cost savings, every hour, on
every pipeline.

Where that belief stands today, honestly measured: about **1M rows in ~18
seconds on 0.5 vCPU / 256 MB** (~3.3M rows/minute) — a box where, in our
benchmarks, the tools we compared against could not finish at all. The goal
we are quietly working toward is **10M rows per minute on that same tiny
box**. We are roughly 3× away, and we might never fully get there — but every
step is taken the same way: one lever at a time, measured, checksum-validated,
and written down in [benchmarks/README.md](benchmarks/README.md) including
the caveats and our own mistakes.

If you see a number that looks wrong, an assumption that doesn't hold, or a
workload where apitap does badly — please open an issue. This project has
been corrected by its own failed runs more than once, and it is better for
each of them.

## Documentation

**📰 Launch post:** [*I moved 10 million rows in 9.9 seconds with pip install apitap — and learned why your ELT benchmark is probably lying to you*](https://medium.com/p/i-moved-10-million-rows-in-9-9-seconds-with-pip-install-apitap-e3c6a826b253) — the origin story, the full showdown vs ingestr and dlt, and three lessons about "rows per hour".

The full usage guide — connection URLs, every option, per-route type mappings,
durability semantics, troubleshooting — lives in [docs/usage.md](docs/usage.md).

## Why it's fast

- **No per-row decode.** Postgres→Postgres pipes raw `COPY (FORMAT binary)` bytes
  straight through — byte-for-byte, like `psql | psql` without the shell.
- **Parallel range pipes.** The table is split into N contiguous ranges of its integer
  primary key (auto-detected) and each range streams concurrently.
- **Bounded memory.** Bytes stream with TCP backpressure; memory is
  `parallel × chunk_bytes`, not the table size. A 256 MB container moves 10M+ rows.
- **Warehouse-native ingestion.** BigQuery gets rotating parallel load jobs
  (Parquet or CSV, picked per box) and an atomic copy — the free path end to
  end, with incremental state that never needs DML (sandbox projects work).

**Measured against [ingestr](https://github.com/bruin-data/ingestr)** — running their
own benchmark (their exact schema, value generators, and CLI invocation, at their
latest release), on the same box, stock databases, checksum-validated. Reproduce it
yourself:

```bash
./benchmarks/run-server.sh            # or benchmarks/run.py on a laptop
```

**10M rows, both tools at 16 vCPU / 4 GB** (auto settings, no tuning for either tool;
apitap = the published wheel, installed with `pip install apitap` — its sha256 matches
the PGO release build byte for byte; mean of two runs; every number checksum-validated
in the same session on the same box):

| route | apitap 0.1.0 | ingestr 1.0.75 | speedup |
|---|---|---|---|
| Postgres → Postgres | **20.2 s** | 500 s | **25×** |
| Postgres → ClickHouse | **9.9 s** | 111 s | 11× |
| MySQL → ClickHouse | **10.4 s** | 97 s | 9× |
| MySQL → Postgres | **22.5 s** | 481 s | 21× |
| Postgres → BigQuery¹ | **28.4 s** | 860 s | **30×** |

¹ BigQuery route measured at 0.5.0 (8 pipes uncapped; 40.3 s at 2 vCPU / 2 GB —
the caps the other rows use don't apply cleanly because the wall is upload +
BigQuery-side parsing, not local CPU). dlt-default: 2,160 s. 100% free-path
ingestion: load + copy jobs only, works on sandbox (no-billing) projects.

**On a tiny 0.5 vCPU / 256 MB container** — the box you'd actually pay for — apitap
completes every route (memory-bounded by design; the pipe count auto-sizes to the
cgroup's CPU *and* memory):

| route | apitap | ingestr 1.0.75 | speedup |
|---|---|---|---|
| Postgres → Postgres | **68 s** | 868 s | 12.8× |
| Postgres → ClickHouse | **28 s** | 428 s | 15.3× |
| MySQL → ClickHouse | **53 s** | 399 s | 7.5× |
| MySQL → Postgres | **58 s** | 849 s | 14.6× |
| Postgres → BigQuery (1M) | **17.9 s** | OOM-killed | ∞ |

(On the BigQuery row both ingestr and dlt+pyarrow died at exit 137 — the kernel
OOM-killer — before finishing 1M rows in 256 MB; apitap's memory is bounded by
pipe buffers, not table size.)

apitap scales with the cores you give it and with the databases'; ingestr barely
moves between 0.5 and 16 vCPUs — a mostly serial pipeline. Full per-tier numbers and
methodology live in [benchmarks/README.md](benchmarks/README.md).

The ClickHouse route moves 10M rows in ~10s because the tool never touches text:
Postgres streams `COPY (FORMAT binary)`, apitap transcodes it in-flight to ClickHouse
`RowBinary` (byte-swaps, epoch rebasing, exact `NUMERIC→Decimal` scaling), and at that
point the SOURCE database is the measured bottleneck — 16 parallel binary COPYs to
`/dev/null` with no apitap involved take ~11 s on the same box. Give the databases more
cores and the transfer keeps scaling; the engine no longer costs anything.

Part of the gap is structural: dlt's full refresh loads into a temp table and then
rewrites all rows into the final one (every row written twice), while apitap COPYs
once into staging and swaps it in with a metadata-only `RENAME`.

The same 15-column schema also backs the end-to-end test suite
(`py-apitap/tests/test_ingestr_schema.py`) — every distinct Postgres type is asserted
byte-faithful, values and column types both.

Postgres destinations also take `durable=False`: the load runs through an UNLOGGED
table (no WAL — the measured write wall), cutting `pg→pg` from ~24 s to **~15.5 s** and
`mysql→pg` from ~27 s to **~19.5 s** at 10M rows. The tradeoff is explicit: the
resulting table is truncated by crash recovery until you `ALTER TABLE … SET LOGGED` —
use it for rebuildable destinations.

## Guarantees

- **Atomic** — rows land in a staging table, swapped in with `DROP` + `RENAME` in one
  transaction. Readers never see a partial load; a mid-run failure leaves the previous
  table untouched.
- **0-row guard** — an empty source never wipes an existing destination table.

## Roadmap

- [x] Postgres → Postgres (raw binary passthrough, parallel)
- [x] Postgres → ClickHouse (binary→RowBinary transcode; TSV fallback for exotic types;
      parallelizes even without a primary key via TID range scans)
- [x] MySQL → ClickHouse (wire decode → RowBinary; lossless unsigned ints, exact
      decimals, UTC-normalized timestamps)
- [x] MySQL → Postgres (wire decode → binary COPY; exact NUMERIC encoding up to
      DECIMAL(65), `BIGINT UNSIGNED`→`numeric(20,0)`, JSON→`jsonb`)
- [x] ClickHouse table engines — `engine="ReplicatedReplacingMergeTree(v)"`,
      `order_by=`, `on_cluster=`: apitap creates the destination with your engine
      (full MergeTree family) and appends into pre-created tables as-is
- [x] Incremental sync — `mode="append"` and `mode="merge"` (upsert by primary
      key); the watermark lives in `_apitap_state`, a queryable table in the
      destination, written in the same transaction as the data; cost proportional
      to the delta, not the table
- [ ] Postgres → Parquet / Arrow (`read_postgres()` → pyarrow / Polars, zero-copy FFI)
- [x] Postgres → BigQuery (dual lanes picked per box: binary COPY → Parquet
      ZSTD, or CSV+gzip on small cores; parallel resumable load jobs → atomic
      multi-source copy; DML-free incremental state, sandbox-safe —
      **10M in 28.4 s (40.3 s at 2 vCPU) vs ingestr 860 s / dlt 2,160 s**,
      checksum-validated; see [benchmarks](benchmarks/README.md))
- [x] Multi-table transfers — `tables=[…]` or a whole `schema=`, through ONE pipe
      budget: largest-first scheduling, per-table grants re-fitted to real span
      counts, shared pools/auth, per-table failure isolation; peak memory stays at
      the single-table ceiling no matter the table count
- [x] MySQL → MySQL (wire decode → `LOAD DATA LOCAL INFILE`, the only bulk path
      MySQL exposes; charset/collation preserved into the destination DDL, exact
      types, binary columns via `UNHEX`, UTC-normalized timestamps)
- [x] Google Sheets → Postgres / ClickHouse / MySQL (`gsheets://<id>?credentials=…` —
      tabs are the tables, row 1 the headers, everything nullable TEXT as the
      sheet displays it; service-account auth shared with BigQuery; works with
      `tables=`/`schema=` multi-table too)
- [ ] Postgres → Snowflake

## Development

```bash
cargo test -p apitap-core          # engine tests
uv pip install -e py-apitap        # build the Python package (needs Rust)
```

### Architecture (adding a database)

One generic driver (`crates/apitap-core/src/driver.rs`) runs every route's lifecycle —
probe → wire-format negotiation → staging → parallel span workers → count → atomic
swap. Databases live in `crates/apitap-core/src/connectors/<name>.rs` and implement
`Source` (probe the schema, plan read spans, run decode/encode workers) and/or `Sink`
(staging DDL from the neutral column model, one streaming loader per worker, the
swap). A new destination is one connector file plus a dispatch arm in `transfer()`;
it immediately works with every source that produces a wire format it accepts.
Encoders are deliberately per-(source, format) and fully monomorphized — there is no
neutral in-memory IR, because the fast lanes (raw `COPY` passthrough, binary→RowBinary
transcode) *are* the product.

## License

MIT. The managed cloud (scheduling, always-on per-tenant workers, monitoring, a UI)
is [apitap.dev](https://apitap.dev).
