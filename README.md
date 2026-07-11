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

## Why it's fast

- **No per-row decode.** Postgres→Postgres pipes raw `COPY (FORMAT binary)` bytes
  straight through — byte-for-byte, like `psql | psql` without the shell.
- **Parallel range pipes.** The table is split into N contiguous ranges of its integer
  primary key (auto-detected) and each range streams concurrently.
- **Bounded memory.** Bytes stream with TCP backpressure; memory is
  `parallel × chunk_bytes`, not the table size. A 256 MB container moves 10M+ rows.

**Measured against [ingestr](https://github.com/bruin-data/ingestr)** — running their
own benchmark (their exact schema, value generators, and CLI invocation, at their
latest release), on the same box, stock databases, checksum-validated. Reproduce it
yourself:

```bash
./benchmarks/run-server.sh            # or benchmarks/run.py on a laptop
```

**10M rows, both tools at 16 vCPU / 4 GB** (auto settings, no tuning for either tool;
apitap = the PGO release wheel, mean of two runs; every number checksum-validated the
same day on the same box):

| route | apitap 0.1.0 | ingestr 1.0.75 | speedup |
|---|---|---|---|
| Postgres → Postgres | **21.2 s** | 508 s | **24×** |
| Postgres → ClickHouse | **10.5 s** | 110 s | 10× |
| MySQL → ClickHouse | **11.1 s** | 94 s | 8× |
| MySQL → Postgres | **23.3 s** | 480 s | 21× |

**On a tiny 0.5 vCPU / 256 MB container** — the box you'd actually pay for — apitap
completes every route (memory-bounded by design; the pipe count auto-sizes to the
cgroup's CPU *and* memory):

| route | apitap | ingestr 1.0.75 | speedup |
|---|---|---|---|
| Postgres → Postgres | **68 s** | 868 s | 12.8× |
| Postgres → ClickHouse | **28 s** | 428 s | 15.3× |
| MySQL → ClickHouse | **53 s** | 399 s | 7.5× |
| MySQL → Postgres | **58 s** | 849 s | 14.6× |

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
- [ ] Postgres → Parquet / Arrow (`read_postgres()` → pyarrow / Polars, zero-copy FFI)
- [ ] Postgres → Snowflake / BigQuery
- [ ] MySQL → MySQL

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

MIT. The managed cloud (scheduling, per-tenant workers, incremental sync, a UI) is
[apitap.dev](https://apitap.dev).
