# apitap

**Move whole tables between databases at wire speed, in bounded memory.**

Despite the name, this is not an API-client library: apitap is a Rust
transfer engine that speaks the databases' own wire formats — binary
`COPY`, RowBinary, `LOAD DATA`, logical replication. How it compares to
Airbyte-class tools, CDC daemons and Python pipelines, with measured
numbers: [docs/vs.md](docs/vs.md).

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

The same call does batch CDC — logical replication on a schedule, no daemon —
and mixes modes per table:

```python
apitap.transfer(src, dst, table="public.orders", mode="log_based")   # full WAL capture

apitap.transfer(src, dst, tables={          # one call, one slot for the CDC tables
    "orders":    "log_based",
    "customers": "log_based",
    "dim_date":  "replace",
})
```

## Why apitap exists

apitap is built around one simple belief: **moving a lot of data should not
require a lot of machine.** Most ingestion pipelines burn money twice — once
in wall-clock hours, once in the oversized workers those hours run on. If a
transfer engine is careful enough about memory and wire formats, the same job
that needs gigabytes of RAM elsewhere can finish, faster, on the smallest
container you can rent. That difference is real cost savings, every hour, on
every pipeline.

Where that belief stands today, honestly measured: the tiny-box goal we set at
launch — **10M rows per minute on 0.5 vCPU / 256 MB** — has been reached and
passed: the transcode route now moves **232M rows (101 GB) in 8m57s** in that
container (~26M rows/minute, peak RSS 170.8 MB), the same table still completes
inside a **44 MB** cap, and on three dedicated machines the identical transfer
takes **30.3 seconds** (~3.3 GB/s, checksum-matched). The tools we compare
against were OOM-killed in ~21 s on the small box and had landed **zero rows**
when cut on the big one. The same belief now covers CDC: on one 650K-event
replication window with every tool capped at 0.5 vCPU / 256 MB, apitap
catches up in **12 s** where ape-dts takes 22 s and pipelinewise 227 s —
all three row-matched ([benchmarks/logbased-cdc.md](benchmarks/logbased-cdc.md)).
In the same cage, **thirty tables in one CDC group** catch up 30M rows in
**63.8 s at a 105 MB peak** while walshadow is OOM-killed before landing
71K of them ([bench-capped-pg-ch-cdc-0.57.md](benchmarks/bench-capped-pg-ch-cdc-0.57.md)),
and the steady-state record on that group is **31,679 changes/s = 1.90M/min**
pure catch-up at 0.5 CPU, checksum-exact
([cdc-steady-30t-0.58.md](benchmarks/cdc-steady-30t-0.58.md)). At fleet scale —
180M changes out of four sharded Postgres into one ClickHouse, both movers
capped at 6 CPU / 2 GB — apitap finishes in 125 s of apply work at ~3 µs of
CPU per change where PeerDB did not finish ([docs/vs.md](docs/vs.md)).

The next tiny-box goal is written down as plainly as the first one was:
**3 million changes per minute of CDC on 0.5 vCPU / 256 MB**, and throughput
that rises by itself when the box grows. Today's honest number is 1.9M/min
catch-up and ~1.5M/min keep-up; the review that found the gap and the two
design documents that close it are in
[docs/review/2026-10-07-system-review-3jt-per-menit.md](docs/review/2026-10-07-system-review-3jt-per-menit.md)
and [docs/design/](docs/design/). Every step got here the same way: one lever at a
time, measured, checksum-validated, and written down in
[benchmarks/README.md](benchmarks/README.md) including the caveats and our own
mistakes — the profiling story behind the last 2-2.5× is
[benchmarks/profiling.md](benchmarks/profiling.md), and the fast-rig run is
[benchmarks/gcp-benchmark.md](benchmarks/gcp-benchmark.md) (Round C).

If you see a number that looks wrong, an assumption that doesn't hold, or a
workload where apitap does badly — please open an issue. This project has
been corrected by its own failed runs more than once, and it is better for
each of them.

## Try it without installing anything

**[apitap.dev/lab](https://apitap.dev/lab)** runs the real PyPI wheel — plus
ingestr and dlt, each pip-installed alongside it — against a seeded Postgres and
ClickHouse on the site's own hardware. Pick a tool, pick the box it runs in
(1 GB / 2 vCPU or **256 MB / 0.5 vCPU**), press run, and watch the engine's own
output stream back. Every result is row-count-verified before a number appears,
and the destination table is dropped afterwards.

The point of the box picker: at 256 MB, apitap moves 5M rows in ~26 s, ingestr
crawls through in ~200 s, and dlt is OOM-killed. Those exact numbers, with the
methodology, are in
[benchmarks/README.md](benchmarks/README.md#constrain-the-tool-not-the-databases--pg--clickhouse-at-256-mb).

## Documentation

**📰 Launch post:** [*I moved 10 million rows in 9.9 seconds with pip install apitap — and learned why your ELT benchmark is probably lying to you*](https://medium.com/p/i-moved-10-million-rows-in-9-9-seconds-with-pip-install-apitap-e3c6a826b253) — the origin story, the full showdown vs ingestr and dlt, and three lessons about "rows per hour".

**📰 Part 2:** [*I moved 100 GB between databases in 9 minutes — on half a CPU core and less RAM than a browser tab*](https://medium.com/@abdul.haris.djafar/i-moved-100-gb-between-databases-in-9-minutes-on-half-a-cpu-core-and-less-ram-than-a-browser-tab-84f15850535d) — the profiling day behind v0.14.0: the allocator swap that measured worse, the buffer recycling that beat it, the 44 MB floor, the 100 GB ladder, and the same-box control runs. Numbers and raw logs: [benchmarks/profiling.md](benchmarks/profiling.md).

The full usage guide — connection URLs, every option, per-route type mappings,
durability semantics, troubleshooting — lives in [docs/usage.md](docs/usage.md).

## Why it's fast

- **No per-row decode.** Postgres→Postgres pipes raw `COPY (FORMAT binary)` bytes
  straight through — byte-for-byte, like `psql | psql` without the shell.
- **Parallel range pipes.** The table is split into N contiguous ranges of its integer
  primary key (auto-detected) and each range streams concurrently.
- **Bounded memory.** Bytes stream with TCP backpressure; memory is
  `parallel ×` (chunk buffers + what the destination holds per pipe), never the
  table size. A 256 MB container moves 10M+ rows.
- **Warehouse-native ingestion.** BigQuery gets rotating parallel load jobs
  (Parquet or CSV, picked per box) and an atomic copy — the free path end to
  end, with incremental state that never needs DML (sandbox projects work).
- **Set-based CDC apply.** A drained replication window is collapsed per key
  and applied as bulk statements — clear the touched keys, insert the final
  images — so the destination server does the work and the client barely
  needs a CPU. Row-at-a-time appliers pay per event; this pays per window.
- **Own wire clients, no driver in the hot path.** Postgres `COPY` and the
  replication stream are read by apitap's own walsender client straight off
  the socket; MySQL rows and binlog events by its own hand-rolled MySQL
  client (TLS included); `pgoutput` cells are byte ranges into the frame
  they arrived in, not copies. The row data is copied once, from the kernel
  into a buffer, and once more into the destination's wire body.
- **A memory model that is priced, not hoped.** Every pipe has a byte price
  (chunk buffers plus what the destination holds per pipe); the planner reads
  the cgroup's CPU **and** memory limits and fits as many pipes as the budget
  allows, thinning the chunk size before it drops a pipe. The same reading
  sizes CDC windows and apply lanes. Peak RSS is a planning input, and a
  release leg fails when a plan does not fit the cage it was planned into.

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

**Ten tables at once, the tool alone capped at 0.5 vCPU / 256 MB** (apitap 0.57.0
from PyPI, every landed table checksum-matched, three interleaved rounds; the
databases are ordinary uncapped servers):

| job | apitap | the other tool |
|---|---|---|
| Postgres → ClickHouse, 10 × 1M rows, one call | **18.4 s**, 119 MB peak, 30/30 MATCH | walshadow 0.1.2: OOM-killed in 6/6 legs, ≤93K rows landed |
| MySQL → ClickHouse, 10 × 1M rows, one call | **36.1 s**, 99 MB peak, 30/30 MATCH | ingestr 1.1.61: OOM-killed in 6/6 legs, 0 rows |
| Postgres → ClickHouse **CDC**, 30 tables in one group, 30M-row bootstrap | **63.8 s**, 105 MB peak, 60/60 MATCH | walshadow 0.1.2: OOM-killed in 8/8 legs |
| MySQL → ClickHouse **CDC**, 30 tables in one group, 30M-row bootstrap | **106.2 s**, 119 MB peak, 60/60 MATCH | ingestr 1.1.61: refuses ClickHouse as a CDC destination (exit 1, 0 rows) |

Ledgers: [pg→ch](benchmarks/bench-capped-pg-ch-0.57.md) ·
[my→ch](benchmarks/bench-capped-my-ch-0.57.md) ·
[pg→ch CDC](benchmarks/bench-capped-pg-ch-cdc-0.57.md) ·
[my→ch CDC](benchmarks/bench-capped-my-ch-cdc-0.57.md) · and the one that says
**no**: a 0.5 CPU drain cannot absorb 1M changed rows per table per minute across
30 tables ([the stress ledger](benchmarks/bench-capped-pg-ch-cdc-stress-0.57.md)).

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
- **One run per destination table** (0.55.0+) — the run's identity is part of the
  staging object's name, so a run can only publish an object it minted. A second run
  of the same table is refused at `prepare`, before a row moves, with an error naming
  the run that holds it. Fan-in is still allowed: two `append` runs from *different*
  sources into one table have independent watermarks, so they are not a collision.
  No advisory lock is involved — which is why it works identically on ClickHouse,
  BigQuery and the object stores, where no such lock exists.
  Since 0.56.0 a run **announces itself before it looks**, so two runs starting in
  the same *instant* can no longer both proceed — at most one does. The trade is
  the other branch: when each sees the other, both fail, loudly, with nothing
  written.
  Since 0.56.0 `mode="log_based"` drains enter the same guard: a drain and a
  bulk run refuse each other in both directions, and so do two drains — and
  since 0.57.0 that holds across an upgrade as well: a 0.55.1 bulk run is refused
  beside a 0.57.0 drain, because the drain also announces itself in the older
  vocabulary. A 0.55.1 drain is the one thing nothing can see; upgrade drains
  first. Iceberg destinations are the exception — their drains are still
  unguarded, and one drain per Iceberg table stays the scheduler's job.
  A drain also **renews a lease** while it runs, and every write it makes — rows
  and watermark — is conditional on that lease still being its own, so one
  killed outright stops blocking the table by itself once that lease lapses
  (`APITAP_LEASE_TTL_SECS`, 300s by default) and the next scheduled run resumes
  from its watermark with nothing for an operator to do. A killed *bulk* run's
  staging table is not collected that way and still needs a manual drop — it
  holds data, and nothing can prove a live loader is not still writing into it.
  ([the matrix, the window, and what a killed run leaves](docs/failure-modes.md))

- **Refuse rather than corrupt or hang** (0.59.0, "safety first"). A CDC drain
  no longer has a way to buffer without end or to park forever: one source
  transaction is capped at 256 MiB of buffered changes (`APITAP_TX_BUF_BYTES`)
  and refused with its table and size instead of OOM-killing the container;
  the replication socket carries TCP keepalives and a 120 s silence budget
  (`APITAP_REPLICATION_SILENCE_SECS`), so a half-open connection fails the
  run and the next run recovers every row; a slot whose `wal_status` is
  `lost` is refused with its remedy before replication starts; a destination
  foreign key into a CDC group, a MySQL string column in a non-UTF-8 charset,
  and `infinity` dates are refused before a row moves. Replication slot names
  carry the destination's identity, so one source feeding two destinations
  gives each its own slot. A cleartext or MD5 password request on an
  unverified channel is refused unless the connection is `verify-full`
  (`APITAP_ALLOW_INSECURE_AUTH=1` to override); MySQL connections to any
  non-loopback host require TLS since 0.55.1 unless the URL says
  `ssl-mode=disabled`. Error messages never echo a password: a URL that fails
  to parse is shown with the secret struck out and the offending character
  named.
- **Stoppable and observable.** A `log_based` run takes the first SIGTERM as
  a request to land the window in flight and exit cleanly (`apitap.request_stop()`
  does the same from another thread; a second SIGTERM is not absorbed), and
  every run reports progress — a live line on a terminal, `key=value` or JSON
  lines in a container — including CDC windows, applied changes, and the WAL
  the slot is retaining on the source
  ([stopping](docs/usage.md#stopping-a-run-on-purpose) ·
  [progress](docs/usage.md#progress-while-it-runs)).

The failure modes these guarantees do *not* cover — a killed process, a cut
connection, a DDL change mid-run, a CDC schedule paused past the source's
retention — are written down, each one produced on purpose against live servers,
in [docs/failure-modes.md](docs/failure-modes.md). What is stable enough to
depend on, and what may still move, is [docs/stability.md](docs/stability.md).
Every sentence those pages promise about the guard, the lease, the apply
contracts and memory is a row in the release gate's claim matrix
(`benchmarks/gate.py --matrix`), proved by a leg that asks the server, not the
exit code; 0.59.0 shipped on **87 passed, 0 failed, 0 skipped**.

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
- [x] Batch CDC — `mode="log_based"`: **Postgres logical replication, MySQL and
      MariaDB binlog**, on a schedule, no daemon. Everything the log saw (inserts,
      updates incl. PK changes, deletes, TRUNCATEs, TOAST handled) collapsed per key
      and applied set-based to **Postgres, ClickHouse, MySQL, BigQuery or
      Iceberg** (BigQuery via a staging table + one `MERGE`, billed project
      required), with the watermark committed atomically with the data. Memory-bounded
      drain windows fit the smallest tier: a 2.1M-event backlog replays in
      a 0.5 vCPU / 44 MB container (33 MB peak). Same 650K-event window,
      everyone capped at 0.5 vCPU / 256 MB: **apitap 12 s** vs ape-dts 22 s
      vs ingestr 78 s vs pipelinewise 227 s, all row-matched — and apitap's
      number is the same 12 s on a full 16-core box: the drain rides
      pgoutput v2 streaming, a spill-free decode the server is told to use
      per-session, an overlapped apply, and a socket pump, so the client
      needs ONE core (0.48 measured) and the work happens set-based at the
      destination ([the ledger](benchmarks/logbased-cdc.md)). Many tables
      share ONE replication slot (`tables=[…]` — same-LSN windows across
      members, one retention risk, 22% faster than N slots), and a
      ``{table: mode}`` dict mixes CDC and bulk modes in one call. `slots=N`
      opens N parallel replication slots for a sharded source, and
      `changelog=True` turns an analytical destination into an append-only
      audit trail (`_apitap_op` per row, a `<table>__current` view) instead of a
      replica — free on the Postgres lane, 34% faster on the MySQL one
      ([the ledger](benchmarks/changelog-cdc.md)). Steady state on a
      30-table group at 0.5 CPU: **1.90M changes/min** pure catch-up and
      ~1.50M/min paced, checksum-exact, under 200 MB
      ([the 0.58 campaign](benchmarks/cdc-steady-30t-0.58.md)); a single
      table kept up with a 2.19M/min writer at 0.40 core and 101 MB
      ([0.59.0 release notes](docs/review/2026-10-07-release-0.59.0.md)).
      The 3M/min target is open and designed, not claimed (see
      [docs/design/](docs/design/))
- [x] Postgres & MySQL → Polars / Arrow — `apitap.read(src, table=…)`:
      parallel range pipes decoded into Arrow batches in Rust, handed
      to Python zero-copy (hand-rolled Arrow C stream, no pyarrow
      dependency). Postgres rides raw binary COPY; MySQL rides its own
      hand-rolled wire client (TLS included) — **1.8× a driver-based full
      drain, 5× on thin scans**. `.lazy()` pushes each query's column
      projection into the SQL: filter+group_by over **50M rows in 9.9 s
      on 0.5 vCPU / 256 MB**, tying raw SQL-in-Postgres; the same lazy
      plan sinks a filtered projection to Parquet (50M rows, 34 s, same
      cage) and joins ACROSS engines in one polars expression (Postgres
      50M × MySQL 50M, digit-verified). Same cage, same query: plain
      polars/connectorx is OOM-killed until 1–2 GB; ADBC streams but runs
      4–5× behind ([the ledger](benchmarks/read-showdown.md))
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
- [x] GitHub → Postgres / ClickHouse / MySQL (`github://owner/repo[/dir]?ref=…` —
      CSV files are the tables, streamed RFC-4180 with strict ragged-row rules;
      `GITHUB_TOKEN` for private repos; `?ref=` pins a branch/tag/SHA for
      reproducible loads)
- [x] Postgres → GCS (`gcs://bucket/prefix?format=csv|parquet` — one composed
      `.csv.gz` per table (atomic visibility) or a directory of ZSTD Parquet
      parts; streams through resumable uploads, so file size never bounds
      memory; reuses the BigQuery lane's proven transcoders)
- [x] GitHub API → Postgres / ClickHouse (`github+api://owner/repo` — issues,
      PRs, commits, stars, releases … as TYPED tables + a raw jsonb column;
      incremental on the entities whose API filters server-side)
- [x] Any source → S3-compatible object storage (`s3://bucket/prefix` — AWS,
      MinIO, Cloudflare R2, OVH/Scaleway/Hetzner; ZSTD Parquet parts, SigV4
      signed by hand, no AWS SDK in the dependency tree; a killed run's
      multipart uploads are aborted rather than billed)
- [x] Any source → **Apache Iceberg** (`iceberg://` on any REST catalog —
      Lakekeeper, Polaris, Nessie, Glue, R2 Data Catalog, S3 Tables). Replace,
      append and merge are all real snapshot commits, and the incremental
      watermark rides in the table's own properties, committed **in the same
      snapshot as the data** — bootstrapped from parquet footer stats, so it
      picks up tables Spark/Trino/pyiceberg wrote ([the ledger](benchmarks/iceberg-showdown.md))
- [x] ClickHouse → ClickHouse (`RowBinary` relayed untouched, no transcode at
      all — 10M rows server-to-server in 8.4 s, or 20.6 s inside the 256 MB cage)
- [ ] Postgres → Snowflake
- [ ] `query=` for `read()` — arbitrary SQL, not just whole tables

## Development

```bash
cargo test -p apitap-core          # engine tests (MSRV 1.94; vendored sqlx-core in vendor/)
uv pip install -e py-apitap        # build the Python package with maturin (needs Rust)
python benchmarks/gate.py --matrix # release pre-flight: every claim × engine, exit 0 or do not tag
```

The wheel is `abi3` for CPython ≥ 3.9, built once and published for
`manylinux_x86_64`; aarch64 and macOS wheels are on the road to 1.0
([docs/stability.md](docs/stability.md)). The release gate and what it needs
(a Postgres with logical replication, MySQL 8.4 and MariaDB, ClickHouse, MinIO +
a REST Iceberg catalog, a BigQuery project, two older wheels for the upgrade
legs) are described in [benchmarks/README.md](benchmarks/README.md#the-release-gate).

### Architecture (adding a database)

Everything lives in one crate, `crates/apitap-core`; `py-apitap` is a thin PyO3
shim that releases the GIL and hands back a report (and an Arrow C stream for
`read()`), with no logic of its own.

- `pipeline/mod.rs` runs every bulk route's lifecycle — probe → cursor →
  destination state → wire-format negotiation → staging → parallel span
  workers (the `Pipes` crew in `pipe.rs`) → count → atomic swap — and holds the
  memory planner (`fit`) that reads the cgroup limits. `pipeline/dispatch.rs` is
  the route table: a `routes!` macro pairs every source with every destination
  it serves, with a per-route CPU profile, and a test fails the build if a pair
  is neither implemented nor explicitly deferred with a reason.
- `source/*.rs` implement `Source` (probe the schema, plan read spans, run the
  decode/encode workers); `sink/*.rs` implement `Sink` (staging DDL from the
  neutral column model, one streaming `Loader` per worker, the swap, the
  incremental state row). `wire/` holds the encoders and wire clients:
  `pgcopy`, `rowbinary`, `pgmytsv`, `arrowcol`, `bqparquet`, the Postgres
  walsender/COPY client (`walsender.rs`), the `pgoutput` decoder, and the MySQL
  wire and binlog decoders (`mywire.rs`, `mybinlog.rs`).
- `logbased/` is batch CDC: `run.rs` (slots, groups, bootstrap, the drain/apply
  pipeline and its lanes), `drain.rs` + `collapse.rs` + `window.rs` (a window
  collapsed per key), `myrun.rs`/`mysource.rs` for the binlog lane, and one
  `dest_*.rs` apply per destination. `naming.rs`, `guard.rs` and `lease.rs` are
  the run identity, the announce-then-check guard and the lease every CDC
  write is fenced on; `shutdown.rs` is the SIGTERM handler; `progress.rs` the
  reporter.
- `read_impl.rs` is `apitap.read()`: the same span workers feeding Arrow column
  builders (`wire/arrowcol.rs`), exported through the hand-rolled C Data
  Interface in `py-apitap/src/capsule.rs`.

A new destination is one `sink/<name>.rs` plus its `routes!` arms (and a
`logbased/dest_<name>.rs` if it should take CDC); it then works with every source
that produces a wire format it accepts. Encoders are deliberately
per-(source, format) and fully monomorphized — there is no neutral in-memory IR,
because the fast lanes (raw `COPY` passthrough, binary→RowBinary transcode) *are*
the product. Design notes: [docs/design/log_based.md](docs/design/log_based.md)
(the CDC contracts), the two 2026-10-08 documents in [docs/design/](docs/design/)
(the road to 3M changes/min and automatic scaling), and the reviews in
[docs/review/](docs/review/).

## License

MIT. The managed cloud (scheduling, always-on per-tenant workers, monitoring, a UI)
is [apitap.dev](https://apitap.dev).
