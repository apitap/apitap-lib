# Benchmark methodology

Every number apitap publishes comes from the harness in this directory. This page
records exactly what runs, where the numbers came from, and what they do — and do
not — show. If anything here looks unfair, open an issue; the harness is small enough
to audit in one sitting.

## What is compared

PG→PG full-table transfer: **apitap** (this library) vs **[ingestr](https://github.com/bruin-data/ingestr)**,
the closest comparable OSS tool (single-command DB→DB copies, Python ecosystem).

To keep it apples-to-apples we run **ingestr's own benchmark**, not one we invented:

- **Schema + data**: [`seed.sql`](seed.sql) is ingestr's benchmark table verbatim —
  the 15-column schema from their `benchmarks/sql/postgres_seed.sql` and the value
  generators ported 1:1 from their `benchmarks/sql/duckdb_seed.sql` (same string
  shapes, same distributions, same JSONB payloads).
- **Invocation**: ingestr is called exactly as their own benchmark runner calls it
  (`ingestr ingest --source-uri … --source-table … --dest-uri … --dest-table …
  --yes --full-refresh`), at their **latest release** (1.0.75 at the time of the
  recorded runs — their own runner pins the older 0.14.141, which is slower; we
  benchmark against the newest because that is the fairer fight).
- **Work**: both tools perform a full refresh of the same table into the same
  destination database. apitap runs with **zero configuration** — no knobs set; it
  auto-sizes to whatever CPU/memory it finds.

## Validation — a time only counts if the data is right

After each tool finishes, the destination is checksummed against the source with 19
per-column aggregates (row count, integer sums, `md5(string_agg(… ORDER BY id))` for
strings, float/decimal sums, boolean counts, min/max for date/timestamp/timestamptz,
a JSONB field digest). **Any mismatch fails the run.** The test suite
(`py-apitap/tests/test_ingestr_schema.py`) additionally asserts the destination's
*column types* match the source exactly.

## Recorded results

### Ubuntu VPS, resource-capped (2026-07-10)

Environment:

- Host: Ubuntu VPS (OVH), 16 vCPU / 61 GB, NVMe; otherwise-idle.
- **Each tool ran inside its own docker container capped at `--cpus=2 --memory=2g`**
  (`python:3.12-slim` base). No other limits or tuning for either tool.
- Timing measured **inside** the container around the transfer only — container
  startup, image pulls, and pip/uv installs are excluded (all images warm).
- Postgres: `postgres:16-alpine` in docker, `--shm-size=1g`, source and destination
  databases on **one instance** (loopback; see "what this does not show").
- apitap 0.1.0 (this repo, `pip`-built wheel), ingestr 1.0.75 on Python 3.12.

| rows | apitap | ingestr 1.0.75 | speedup | validated |
|---|---|---|---|---|
| 1,000,000 | **4.9 s** (2 pipes) | 55.5 s | 11.4× | both ✅ |
| 10,000,000 | **40.7 s** (2 pipes) | 564.8 s | **13.9×** | both ✅ |

Notes observed during the runs:

- apitap auto-clamped to 2 parallel pipes — `num_cpus` reads the cgroup limit, so the
  auto-sizing needed no flags.
- The gap is partly structural, visible live in `pg_stat_user_tables`: dlt (ingestr's
  engine) full-refresh loads all rows into a temp table
  (`…_ti_<timestamp>`) and then **rewrites every row** into the final table — two full
  writes. apitap COPYs once into a staging table and swaps it in with a metadata-only
  `DROP` + `RENAME` in one transaction.
- `docker stats` during the runs told the deeper story: **apitap's container idled at
  ~24% of one core** (it pipes bytes; the actual work happens inside Postgres — the
  destination's write path is the bottleneck, i.e. the transfer runs as fast as the
  database itself can go), while **ingestr pegged its CPU cap** (it decodes every row
  into Python objects and re-encodes them — its own CPU is its bottleneck). Giving
  apitap more cores wouldn't make it faster; giving ingestr more cores would, at cost.

### Postgres → ClickHouse, same environment (2026-07-11)

Same harness (`DEST=clickhouse ./benchmarks/run-server.sh`), same caps (2 vCPU / 2 GB
per tool), destination `clickhouse-server:24.8` in docker, ingestr via dlt's native
clickhouse destination. Validation adds cross-engine normalization (decimal
`trim_scale`, formatted timestamps, `Int128` big-int sums — ClickHouse's plain `sum()`
wraps Int64 silently at 10M of this data) and still requires every aggregate to match.

| rows | apitap | ingestr 1.0.75 | speedup | validated |
|---|---|---|---|---|
| 1M | **4.7 s** | 14.1 s | 3.0× | both ✅ |
| 10M | **10.7 s** | 134.7 s | **12.6×** | both ✅ |

The apitap 10M number is the result of a measured optimization ladder (each step
checksum-gated): 37.5 s CSV baseline → parallel range pipes (17.1) → TSV (14.9) →
work-stealing span queue + one insert per worker (13.9) → **Postgres binary COPY
transcoded in-flight to RowBinary (10.4–10.7)**. At that point the source database
itself is the wall: 16 parallel `COPY … (FORMAT binary)` to `/dev/null` — no apitap,
no ClickHouse — measure ~11 s on this host. The tool's own container is idle enough
that capping it at 2 vCPU vs uncapping it changes nothing; throughput scales with the
databases' cores.

### MySQL → ClickHouse, same environment (2026-07-11)

Same harness pattern and caps; source `mysql:8` in docker, seeded by
[`seed_mysql.sql`](seed_mysql.sql) (the same 15 generators ported to MySQL dialect).
MySQL has no COPY protocol, so apitap decodes the wire values and encodes RowBinary
directly — a fundamentally more expensive source than Postgres, visible in the numbers.
String checksums use order-independent `SUM(CRC32(col))` on both engines
(a 10M `GROUP_CONCAT` is not practical on MySQL); ints, exact decimals, dates,
timestamps (UTC session) and a JSON field are validated as before.

| rows | apitap | ingestr 1.0.75 | speedup | validated |
|---|---|---|---|---|
| 10M | **17.9 s** (16 pipes) | 107.1 s | **6.0×** | both ✅ |

Unlike the Postgres routes (byte pipes), the MySQL decode is real CPU inside apitap —
so this route scales with the TOOL's cores too. Same run with more CPU for apitap's
container (MySQL at a 6 GB buffer pool): 4 vCPU → 10.8 s, 8 vCPU / 24 pipes →
**10.0 s**, and it plateaus there — at that point MySQL itself serving 10M rows over
its per-row wire protocol is the measured wall (12 vCPU and uncapped runs land the
same). A protocol without bulk export sets a higher floor than Postgres' COPY.

### MySQL → Postgres, same environment (2026-07-11)

Same caps and pattern; apitap decodes the MySQL wire values and encodes Postgres
**binary COPY** directly (exact `numeric` up to DECIMAL(65) via digit-string encoding,
`BIGINT UNSIGNED` → `numeric(20,0)`, JSON → `jsonb`, epochs rebased) into a staging
table swapped in atomically. Validation: full-table aggregates (counts, integer sums,
exact decimal, byte lengths, date/timestamp bounds under a UTC session) plus an
ordered-md5 content hash over a 1-in-997 sample of the string and JSON columns
(a 10M `GROUP_CONCAT` is not practical on MySQL).

| rows | apitap | ingestr 1.0.75 | speedup | validated |
|---|---|---|---|---|
| 10M | **23.8 s** (8 pipes) | 504.7 s | **21.2×** | both ✅ |

### Grand run — all four routes at 8 vCPU / 4 GB each (2026-07-11)

One sequential run of every route × both tools, identical caps of **8 vCPU / 4 GB per
tool**, same seeds, every result checksum-validated (the PG-source routes with the
17-aggregate canonical checksum; the MySQL-source routes with full aggregates plus a
1-in-997 ordered-md5 content sample — remember MySQL's `group_concat_max_len`
defaults to 1024 BYTES, which silently truncates and once produced a false MISMATCH
for both tools until bumped):

| route | apitap | ingestr 1.0.75 | speedup |
|---|---|---|---|
| Postgres → Postgres | **21.2 s** (8 pipes) | 509.2 s | 24.0× |
| Postgres → ClickHouse | **10.6 s** (16 pipes) | 110.1 s | 10.4× |
| MySQL → ClickHouse | **10.7 s** (16 pipes) | 95.0 s | 8.9× |
| MySQL → Postgres | **19.5 s** (12 pipes) | 505.6 s | **25.9×** |

Two observations: apitap's PG→PG dropped from 40.7 s (2 pipes under the 2-vCPU cap) to
21.2 s at 8 auto pipes — the routes scale with cores; ingestr's times barely moved
from the 2-vCPU runs (564.8→509.2, 134.7→110.1, 107.1→95.0, 504.7→505.6) — extra
cores don't help a mostly serial pipeline.

### Big box — 16 vCPU / 4 GB each, stock databases (2026-07-11)

Fresh stock containers with log caps, fresh seeds, both tools capped identically at
the host's full 16 cores (so tool and databases share the machine — effectively
uncapped). apitap counts validated on every route:

| route | apitap | ingestr 1.0.75 | speedup |
|---|---|---|---|
| Postgres → Postgres | **27 s** (8 pipes) | 545 s | 20.2× |
| Postgres → ClickHouse | **13 s** (16 pipes) | 119 s | 9.2× |
| MySQL → ClickHouse | **14 s** (16 pipes) | 100 s | 7.1× |
| MySQL → Postgres | **27 s** (12 pipes) | 520 s | 19.3× |

(An ops footnote in the spirit of full disclosure: the first attempt at the ingestr
legs died because a NEIGHBORING container on the host — unrelated to either tool —
was in a logging loop that filled the disk; the legs were re-run after cleanup.)

### Tiny box — 0.5 vCPU / 256 MB each (2026-07-11)

The smallest container anyone would rent, both tools capped identically, 10M rows,
stock databases. apitap completed every route with validated counts (its pipe count
auto-derives from the cgroup's CPU **and memory** — an earlier build OOM'd here at the
CPU-derived 8 pipes, which is exactly why the memory cap exists). ingestr also
completed every route (dlt spills to disk) — 7.5–15× slower:

| route | apitap | ingestr 1.0.75 |
|---|---|---|
| Postgres → Postgres | **68 s** (1 pipe) | 868 s |
| Postgres → ClickHouse | **28 s** (5 pipes) | 428 s |
| MySQL → ClickHouse | **53 s** (8 pipes) | 399 s |
| MySQL → Postgres | **58 s** (4 pipes) | 849 s |

(Two ingestr legs initially failed on this host because the benchmark itself had
filled the disk; they were re-run after cleanup and passed — recorded here because a
benchmark that hides its own operational mistakes isn't worth trusting.)

### vs dlt directly — default, pyarrow, and connectorx backends (2026-07-12)

ingestr is a CLI over [dlt](https://dlthub.com); to separate the wrapper from the
engine we also ran dlt's own `sql_database` source directly, in its three documented
backends, against the same seeds, same box, same caps (16 vCPU / 4 GB each), auto
settings on both sides. apitap here is the PGO release wheel; every completed result
below checksum-validated:

| route | apitap (PyPI wheel) | dlt (default) | dlt + pyarrow | dlt + connectorx |
|---|---|---|---|---|
| Postgres → Postgres | **20.2 s** | 2 604 s (43 min) | 708 s | OOM-killed |
| Postgres → ClickHouse | **9.9 s** | 1 893 s | 360 s | OOM-killed |
| MySQL → ClickHouse | **10.4 s** | 2 231 s | failed (type inference) | OOM-killed |
| MySQL → Postgres | **22.5 s** | 2 899 s (48 min) | failed (type inference) | OOM-killed |

(Re-run in full after apitap 0.1.0 shipped, with apitap installed from PyPI like any
user would — `pip install apitap`, wheel sha256 verified identical to the release
build. dlt's numbers reproduced within 0.1–2% of the previous day's run.)

Notes, all reproducible:

- **dlt default (sqlalchemy backend)** moves row objects through Python one at a
  time — 125–210× slower than apitap. ingestr's 508 s on the same pg→pg route shows
  how much of ingestr's speed is ingestr's own tuning on top of dlt, not dlt itself.
- **dlt + pyarrow** (the Arrow path) is a real ~4× improvement over dlt's default —
  and still 33–35× slower than apitap where it runs. On the MySQL source it failed
  outright with auto settings: dlt infers MySQL `DOUBLE` as `decimal(38,9)` and the
  Arrow conversion refuses (`PyToArrowConversionException … Consider setting
  precision and scale hints`) — moving this table requires hand-written schema hints.
- **dlt + connectorx** (the Rust/Arrow extractor, dlt's fastest documented backend)
  was OOM-killed (exit 137) on ALL four routes: it materializes the full result set
  in memory, and 10M rows does not fit in the same 4 GB cap apitap runs in — apitap
  moves the same table in 256 MB. Bounded memory is a design property, not a knob.
- **Tuning dlt per dltHub's own benchmark blog changes nothing here.** We re-ran
  the pyarrow legs with the exact recipe from
  [dlthub.com/blog/benchmark-dlthub](https://dlthub.com/blog/benchmark-dlthub)
  (workers 8/8/8, `chunk_size=150000`, typed loader formats — parquet for
  ClickHouse, csv for Postgres): pg→ch 370 s (vs 360 s untuned), pg→pg 701 s (vs
  694 s). dlt's workers parallelize across *tables*; their TPC-H benchmark had
  eight. On the single-big-table job — the case this benchmark measures — dlt's
  extract is one sequential reader no matter the settings, while apitap
  parallelizes *within* the table over key ranges. Their headline (~360M rows/hour,
  tuned, narrow TPC-H rows, BigQuery file loads) and our measurement (~100M
  rows/hour on this wider table) are consistent once row width and table count are
  accounted for; apitap moves ~3.5B rows/hour on the same table, untuned.
- dlt's ClickHouse destination ignores `http_port`/`secure` URL query parameters;
  they must be passed as `DESTINATION__CLICKHOUSE__CREDENTIALS__*` env vars (the
  first attempt at every CH leg failed on TLS port 8443 until we did).
- The two dlt-default `→ Postgres` legs outlived our first 40-minute timeout; they
  were left to finish and their full wall times are reported.

**"But dltHub's own benchmark says 360M rows/hour?"** It does — and both numbers are
right. Rows are not a unit: their TPC-H rows average **185 bytes** (43.3M rows = 8 GB);
this benchmark's rows average **462 bytes** (10M rows = 4.41 GB, measured with
`pg_table_size`). Normalize to bytes and their tuned headline is **68 GB/hour** while
dlt+pyarrow measured here does **44 GB/hour** on one wide table — the same class once
you account for the remaining two differences: TPC-H is *eight* tables (dlt's workers
parallelize across tables, so their extract ran 8 streams where a single-table job
gets one), and their destination is BigQuery, which ingests uploaded parquet files
server-side, outside the measured pipeline. apitap on the same single table moves
**~1.6 TB/hour** (4.41 GB in 9.9 s) — ~23× their headline per byte, untuned, from the
published wheel.

### Scale check — 100M rows / 46 GB, one transfer (2026-07-12)

To make the throughput claim honest beyond a 10-second burst, the pg→ch route was
re-run at 10× the size: 100M rows (46.4 GB by `pg_table_size`, same 464-byte rows),
apitap installed from PyPI, same 16 vCPU / 4 GB cap. **139.2 s, 32 pipes,
checksum-MATCH — a measured 1.2 TB/hour**, sustained past the source's page cache
(the 10M table fits in RAM; this one doesn't, so real disk reads are included).
Row-rate: ~2.6 billion rows/hour.

### M2 Pro laptop, uncapped (2026-07-10)

MacBook M2 Pro (10 cores / 16 GB), docker PG, no resource caps, 100k rows, warm
caches: apitap 0.4 s vs ingestr 1.0.75 2.9 s. Small-row runs flatter apitap (fixed
per-run overhead dominates ingestr at this scale) — treat the capped 10M run above as
the representative number.

### Postgres → BigQuery — apitap vs ingestr vs dlt (2026-07-15)

Same 10M-row / 15-column table, same VPS (OVH Canada), BigQuery dataset in
`US`, each tool in its own container capped at **2 vCPU / 2 GB**, timed inside
the container (image build/install not counted), 9 aggregate checksums
(count, sums, decimal sum, string lengths, bool count, max timestamp/date)
required to MATCH Postgres before a time counts. The BigQuery project is a
**sandbox (no billing)** — every tool used the free load-job path.

| tool | wall time | vs apitap | validated |
|---|---|---|---|
| **apitap** (this repo) | **85.1 s** | — | 9/9 MATCH |
| ingestr 1.0.75 | 860 s | 10.1× slower | 9/9 MATCH |
| dlt 1.x default (`sql_database`, no backend hint) | 2,160 s | 25.4× slower | 9/9 MATCH |

Uncapped, apitap does the same transfer in **71.2 s** over 8 pipes.

Honest notes:

- All three tools ingest through BigQuery **load jobs** (free); the gap is in
  the extract/encode leg, not in BigQuery. dlt's default backend extracts
  row-by-row in Python (one core pegged for ~31 of its 36 minutes); ingestr
  (dlt underneath, tuned) streams via its own arrow path. apitap reads
  Postgres text COPY and transcodes to gzipped NDJSON in Rust, N pipes in
  parallel, each feeding its own resumable load job.
- Network context matters: the runner is in Canada, the dataset in `US`
  multi-region. gzip (~5× fewer bytes on the wire) is part of apitap's
  margin; a runner inside GCP would shrink the upload leg for everyone.
- Two earlier dlt runs "failed silently" — that was a bug in OUR harness
  (`docker run` without `-i`, so the pipeline script never reached Python),
  not in dlt. Fixed, rerun, and the number above is dlt's real one.
- One ingestr run was discarded because our validator, not ingestr, was
  wrong (`bool` lands as BOOL vs apitap's INT64; the comparison query needed
  a cast). Its second run differed by 1.7% (875 s vs 860 s).
- Types differ slightly across tools (BOOL vs INT64 for `boolean`; ingestr
  and dlt add `_dlt_id`/`_dlt_load_id` columns). Checksums compare the 15
  source columns only.

## What these runs do NOT show

Recorded honestly, because a benchmark is only useful if its edges are visible:

- **No WAN**: source and destination Postgres were on the same host (loopback). The
  recorded runs measure tool overhead + throughput under identical conditions, not
  network transfer. A follow-up 1M run with source and destination as **two separate
  Postgres instances** (each its own container/WAL/buffers) landed within noise of the
  single-instance numbers — apitap 4.5 s vs ingestr 56.3 s (12.6×), both validated —
  so the topology isn't what the gap is made of. WAN-separated runs are future work.
- **The Postgres server itself was uncapped** (both tools hit the same server, so this
  is symmetric, but absolute numbers depend on it).
- **First-run install time is excluded by design** — the first ingestr invocation
  downloads its dependency tree; we report warm runs only. apitap's wheel install is
  likewise excluded.
- **One workload**: full-refresh of one wide table. Incremental syncs, many-small-runs,
  and non-Postgres routes are not covered by these numbers.
- ingestr runs on Python **3.12** rather than 3.13 because its `pendulum` dependency
  ships no CPython 3.13 arm64 wheel and its sdist fails to build (pyo3 link error);
  the ingestr version is identical.

## Reproduce it

Laptop (docker + uv required):

```bash
uv venv .venv && uv pip install -e py-apitap --python .venv/bin/python
.venv/bin/python benchmarks/run.py --rows 1000000 --with-ingestr            # uncapped
.venv/bin/python benchmarks/run.py --rows 1000000 --with-ingestr --capped   # 2 cpu / 2g each
```

Linux server (docker required; no Python/psql needed on the host):

```bash
./benchmarks/run-server.sh                       # 1M rows, capped 2 cpu / 2g each
ROWS=10000000 ./benchmarks/run-server.sh         # the 10M run
CAP_CPUS=4 CAP_MEM=4g ./benchmarks/run-server.sh # different caps
```

By default the server script starts its own throwaway Postgres containers bound to
`127.0.0.1` only. Benchmark an existing server instead with
`benchmarks/.env` (see [`.env.example`](.env.example)). Tear down afterwards:

```bash
docker rm -f apitap-bench-pg-src apitap-bench-pg-dst
```

Run everything twice and report the second (warm) number. If your numbers disagree
with the table above, please open an issue with the full output.
