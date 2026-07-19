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
| **apitap** (this repo) | **41.7 s** | — | 9/9 MATCH |
| ingestr 1.0.75 | 860 s | 20.6× slower | 9/9 MATCH |
| dlt 1.x default (`sql_database`, no backend hint) | 2,160 s | 51.8× slower | 9/9 MATCH |

Uncapped, apitap does the same transfer in **37.2 s** over 8 pipes.

The apitap number was 85.1 s when this table was first recorded; the
optimization pass below (same day, same environment, re-validated) halved
it. The ingestr/dlt numbers predate that pass but their tooling didn't
change; the ranking wasn't close either way.

Scripts: [`run-bigquery.sh`](run-bigquery.sh) (all three tools, env-driven
caps/rows/backend) + [`validate-bigquery.sh`](validate-bigquery.sh) (the 9
aggregates). The tiny-box run is the same script with
`ROWS=1000000 CAP_CPUS=0.5 CAP_MEM=256m DLT_BACKEND=pyarrow`.

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

### Postgres → BigQuery on a tiny box — 0.5 vCPU / 256 MB (2026-07-15)

Same environment as above, 1M rows (kept small deliberately — the project now
has billing enabled and validation queries are billed), each tool capped at
**0.5 vCPU / 256 MB**:

| tool | outcome | validated |
|---|---|---|
| **apitap** | **16–18 s**, 2 pipes (27.4 s before the optimization pass) | 9/9 MATCH |
| ingestr 1.0.75 | **OOM-killed** (exit 137) ~100k rows in; its own progress log read 310 MB just before the kernel killed it | no table landed |
| dlt 1.x + pyarrow backend | **OOM-killed** (exit 137) during extract | no table landed |

This is the point of the streaming design rather than a gotcha: apitap's
memory is bounded by pipe count × chunk buffers (~tens of MB), independent of
table size, so the same 256 MB container that moves 1M rows also moves 100M.
ingestr at 2 vCPU / 2 GB reported a 556 MB peak on the 10M run — it needs the
headroom; at 256 MB it cannot finish 1M. Both failures were rerun once to
capture the real exit codes; neither is a harness artifact this time.

### The BigQuery optimization pass (2026-07-15) — instrument first, then cut

Per-phase instrumentation (`APITAP_DEBUG=1`) on the tiny box showed the time
was NOT where intuition said (gzip was 1.4 s): the wall was `job_wait` —
gzip files aren't splittable, so BigQuery parsed each worker's single big
file single-threaded while apitap sat idle. Levers, one at a time, 3 runs
each, checksums on every configuration:

1. **File rotation + background job polls** — a new load job every ~12 MiB
   compressed, polled while the next file streams (job_wait 14–18 s → 5–9 s).
2. **CSV instead of NDJSON** — probed live that BigQuery CSV keeps the
   NULL vs empty-string distinction (unquoted empty = NULL, quoted `""` =
   empty), so the reason for NDJSON was void; ~40% fewer bytes into the
   compressor, much faster server-side parse (job_wait → 2.7–4 s).
3. **zlib-rs gzip backend + bulk-run escape loops** (transcode −30%).
4. **Client-side watermark** — loaders track `MAX(cursor)` during transcode;
   one less billed query per incremental run.
5. **Per-worker staging tables + time-gated rotation** — BigQuery allows ~5
   metadata updates / 10 s / table; fast workers sealing 12 MiB files every
   second tripped `rateLimitExceeded` at 10M. Each worker now loads into its
   own staging (one multi-source copy job finalizes) and seals at
   ≥12 MiB **and** ≥6 s (hard cap 96 MiB).

6. **Parquet lane** (binary COPY → typed column chunks → Parquet ZSTD-1, no
   text round-trip, no `arrow` dependency) — BigQuery's fastest parse
   (job_wait ~2-3 s at every scale). Its typed builders cost more CPU per
   row than CSV string-slinging, so it wins where cores exist and loses on
   half-core boxes: the lane is picked per connection (4+ pipes → Parquet,
   fewer → CSV). Measured: 10M uncapped 37.2 → **26.9-27.9 s**; 10M at
   2 vCPU statistically tied (41.7 vs 41.7-45.6); tiny box stays on CSV.
   `json`/`jsonb` land as STRING on this lane (BigQuery rejects Parquet
   loads into JSON columns).

Net: 10M capped 85.1 s → **41.7 s**; 10M uncapped 71.2 s → **26.9 s**;
tiny-box 1M 27.4 s → **16–18 s**. All re-validated 9/9.

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

## Multi-table on the tiny box — TPC-H, 10 × 1M rows, 256 MB / 0.5 CPU

The multi-table release (`tables=[…]` / `schema=`) was benchmarked on REAL TPC-H
data: 10 tables × 1,000,000 rows each (lineitem ×3 — 16 columns, composite key,
TID path; orders ×3 — integer PK, range path; customer ×2; supplier ×2), sliced
from a scale-factor-15 TPC-H Postgres. Route PG→PG. The TOOL runs in a docker
container capped at `--cpus=0.5 --memory=256m`; both databases are stock,
uncapped. Latest released versions; per-tool cap 40 min; per-table checksums
(count + key sum + amount sum) gate every result.

| Tool | Multi-table mode | Wall | Peak RSS | Checksums |
|---|---|---:|---:|---|
| **apitap 0.6.0** | one call, `schema="public"` | **33 s** | **52 MB** | **10/10** |
| dlt 1.29.0 (pyarrow) | native `table_names=[…]` | OOM-killed at 7 s (rc 137) | >256 MB | 0/10 — nothing landed |
| dlt 1.29.0 (default) | native `table_names=[…]` | OOM-killed at 13 s (rc 137) | >256 MB | 0/10 — nothing landed |
| ingestr 1.1.0 | none for SQL copies — looped 10 invocations | 327 s | 194 MB | 10/10 |

Can the others run tables in parallel? **dlt: yes, natively** —
`sql_database(table_names=[…])` extracts tables through worker pools; on this
box that same fan-out is what pushes it past 256 MB before a single row lands
(both backends, within seconds). **ingestr: no** — `--source-table` takes one
table per invocation (multi-table exists only in its CDC mode), so its
multi-table story is a sequential loop, which does finish, 10× slower than
apitap's single call.

Honest note on apitap's own number: at 0.5 CPU the budget heuristic resolves to
ONE pipe, so the tables effectively stream one after another — the win here is
the engine's per-byte cost and bounded memory (52 MB used of 256 MB), not
cross-table concurrency. The concurrency shows on bigger boxes: on 16 cores the
same one-call multi-table run is ~2.1× faster than looping `transfer()`
per table, at identical peak RSS. Raw log: [`tinybox-tpch-raw.log`](tinybox-tpch-raw.log).

### Scaled up: 10 × 5M rows (50M, ~8 GB) — same 256 MB / 0.5 CPU box

Same setup, five times the data (lineitem and orders are pure 5M slices of the
SF-15 TPC-H; customer/supplier cycled). This run also answers "can the others
parallelize?" empirically, giving each tool its best available mode:

| Tool / mode | Wall | Peak RSS | Tables intact |
|---|---:|---:|---|
| **apitap** — one call, `schema=` | **168 s (2.8 min)** | **53 MB** | **10/10** |
| ingestr — sequential loop (its default) | 1,238 s (20.6 min) | 193 MB | 10/10 |
| ingestr — `--extract-partition-by` (intra-table parallel) | **refused**: `incremental-strategy "replace" cannot be combined with extract partitioning` — its parallelism is for bounded/incremental reads only, not full copies | — | 0/10 |
| ingestr — 10 invocations in parallel (shell) | 433 s | 188 MB | **3/10** — 7 processes OOM-killed inside the shared 256 MB |
| dlt (pyarrow, native `table_names` multi) | OOM-killed at 7 s | >256 MB | 0/10 |
| dlt (default) | OOM-killed at 14 s | >256 MB | 0/10 |

So on a full-table copy at this box size, neither competitor has a *usable*
parallel mode: dlt's native multi-table fan-out is what OOMs it, ingestr's
intra-table partitioning rejects `replace` loads outright, and OS-level
parallel ingestr silently loses 7 tables of 10. apitap's shared-budget
concurrency is the same code path that runs 10/10 checksum-clean in 53 MB —
and note the memory: 1M/table → 52-53 MB, 5M/table → 53 MB. The ceiling does
not move with data size, by construction.

An explicit `parallel=4` on this box (mixed-size workload) is ~1.4× faster
than the auto budget (the CPU heuristic resolves to 1 pipe on a 0.5-CPU
cgroup; memory would allow more) — the knob is there when you want it. Raw
log: [`tinybox-tpch5m-raw.log`](tinybox-tpch5m-raw.log).

## Postgres → GCS (files): apitap vs ingestr vs dlt

Same methodology as every other table here: same box, one tool at a time in a
`--cpus=16 --memory=4g` container, stock Postgres, the ingestr benchmark
schema, and **every output downloaded back from the bucket and
checksum-validated** (count | sum(id) | md5(small_str ordered)) before a time
counts. All five 1M outputs matched the source exactly — including ingestr's
and dlt's. Reproduce with [`run-gcs.sh`](run-gcs.sh); raw log in
[`gcs-showdown-raw.log`](gcs-showdown-raw.log).

**1M rows, PG → GCS:**

| tool | output | time | vs apitap |
|---|---|---|---|
| **apitap** `format=csv` | one composed `.csv.gz` | **4.7 s** | — |
| **apitap** `format=parquet` | 8 ZSTD part files | **5.3 s** | — |
| ingestr 1.0.75 (`gs://` dest) | 1 parquet file | 14.2 s | 3.0× |
| dlt `filesystem` + pyarrow backend | 4 parquet files | 42.6 s | 9.1× |
| dlt `filesystem` default backend | 4 parquet files | 173.8 s | **37×** |

**10M rows** (tools that finished 1M fast enough to attempt; dlt extrapolates
to ~7 min (pyarrow) / ~29 min (default) and was not run):

| tool | time | vs apitap |
|---|---|---|
| **apitap** `format=csv` | **15.5 s** | — |
| **apitap** `format=parquet` | **18.2 s** | — |
| ingestr 1.0.75 | 124.3 s | 8.0× |

Notes for fairness: ingestr's `gs://` destination also writes parquet, so the
parquet rows are apples-to-apples; dlt numbers use its documented `filesystem`
destination with `loader_file_format="parquet"`, fresh `pipelines_dir` per run,
and its faster pyarrow backend shown separately (its best case). apitap was
the gcs-feature build later released as-is (the dist wheel's sha256 is the
release). The bucket lived in `us-east1`; all tools shared the same network
path.

## Postgres → MySQL: apitap vs ingestr vs dlt

The 25th route (apitap 0.13.0's pgcopy-binary → `LOAD DATA` transcoder),
head-to-head on the same box. Each tool ran in an identical uncapped
`python:3.12-slim` container on the host network, against the same stock
Postgres source and stock MySQL 8 destination (both loopback Docker
containers), moving the ingestr benchmark table. **Every destination table
was checksum-validated** (count | sum(id) | md5(small_str ordered)) against
the source before a time counted — all four 1M outputs and both 10M outputs
matched exactly. Reproduce with [`run-mysql-dest.sh`](run-mysql-dest.sh);
raw log in [`pgmy-showdown-raw.log`](pgmy-showdown-raw.log).

dlt (1.29.0) has no native MySQL destination; its documented path is the
generic `sqlalchemy` destination (pymysql), shown with both its default and
its faster pyarrow source backend. ingestr was v1.1.1, warm run reported
(cold and warm were within 8%).

**1M rows, PG → MySQL:**

| tool | time | vs apitap |
|---|---|---|
| **apitap** | **6.4 s** | — |
| ingestr 1.1.1 | 32.8 s | 5.1× |
| dlt 1.29.0 `sqlalchemy` + pyarrow backend | 181.5 s | 28× |
| dlt 1.29.0 `sqlalchemy` default backend | 329.7 s | **52×** |

**10M rows** (dlt extrapolates to ~30 min (pyarrow) / ~55 min (default) and
was not run):

| tool | time | vs apitap |
|---|---|---|
| **apitap** | **64.3 s** | — |
| ingestr 1.1.1 | 366.4 s | 5.7× |

The shape mirrors every other route here: apitap streams Postgres binary
COPY and renders MySQL's `LOAD DATA` text dialect in-flight (one bulk-load
statement per pipe), while the Python tools decode rows into Python/Arrow
objects and re-insert them through SQLAlchemy. MySQL's own `LOAD DATA`
ingest speed is the floor on this route for every tool — apitap sits close
to it.
