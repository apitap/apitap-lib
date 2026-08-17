# pg → Apache Iceberg: the three-phase showdown

The `iceberg://` destination's birth certificate. Iceberg's whole point is
incremental, so a full-load race alone would miss it. Every tool ran three
phases against the same live Postgres: a full load, then a 1M-row append
delta, then a 1M-row merge delta (500k updates + 500k inserts). Every phase
capped at **16 vCPU / 4 GB**, every result validated by reading the Iceberg
table back and checksumming against the source.

Rig: OVH VPS (16 vCPU / 61 GB), MinIO + `apache/iceberg-rest-fixture` REST
catalog in docker, source `postgres:16-alpine` with the ingestr benchmark
schema (15 columns incl. JSONB). Raw: [iceberg-showdown-raw.log](iceberg-showdown-raw.log).

## Results

| phase | apitap (`iceberg://`) | dlt 1.29.1 (filesystem + `table_format="iceberg"`) | pyiceberg 0.11.1 DIY (connectorx → arrow → REST) | ingestr v1.1.15 |
|---|---|---|---|---|
| **full** (10.3M rows) | **15.2 s** · 773 MB · MATCH | OOM-killed (peak = 4096 MB cap, 0 rows) | OOM-killed at 62 s (peak = cap, 0 rows) | no Iceberg destination |
| **append** (+1M delta) | **2.2 s** · 541 MB · MATCH | OOM-killed (kept re-loading its pending package) | unreachable — no state survived the full-load OOM | — |
| **merge** (1M delta upsert) | **3.8 s** · 383 MB · MATCH | OOM-killed | unreachable | — |

apitap's merge moved 1,050,000 rows (the 1M delta plus 50k watermark-tie
re-reads — merge deliberately uses `>=` so a tie can never be *skipped*; the
upsert dedupes re-reads for free) and committed one row-delta snapshot:
an equality-delete file of the delta's keys plus the delta's data files.
Read-back count after merge: 11,800,000 — exactly the source.

## What actually failed, precisely

- **dlt** extracts to local parquet first (≈6.5 min for phase-full on this
  box), then hands the load to pyiceberg — which logged
  `Unable to resolve region for bucket apitap-lake` against MinIO and
  crossed the 4 GiB ceiling; the kernel killed it. On every later phase it
  re-discovered the crashed run's pending load package and died the same
  way. A **fairness rerun** with root-cleaned state and explicit
  `AWS_ENDPOINT_URL` + `AWS_DEFAULT_REGION` (+ the same creds as env vars)
  reproduced both failures exactly: the region error, then peak = 4096 MB.
  Four attempts total, zero rows landed in any of them — its
  iceberg-on-S3-compatible path appears to both ignore the endpoint during
  region resolution and materialize beyond 4 GiB at this scale.
- **pyiceberg DIY** is the canonical hand-written path: `connectorx`
  `read_sql` → one arrow table → `catalog.create_table` + `append`.
  `read_sql` materializes the whole result set, so at 10.3M × 15 columns it
  was OOM-killed in about a minute — and because its watermark state lived in
  a local JSON file that never got written, both incremental phases died on a
  `KeyError` before moving a byte. That contrast is the design point: apitap
  keeps the watermark in the Iceberg table's own properties, in the same
  snapshot commit as the data. There is no local state to lose.
- **ingestr** (v1.1.15) has no Iceberg destination at all — verified by
  grepping the installed package: zero mentions.

## Validation notes (honesty section)

- Full and append phases were validated with DuckDB's `iceberg` extension
  (`iceberg_scan` on the exact `metadata-location`), count + `sum(id)`
  against live Postgres.
- The merge phase exposed a **reader**-side limit: DuckDB's `iceberg_scan`
  ground through 30+ GB of RAM applying a 1.05M-key equality delete to an
  11.8M-row table and had to be killed. The recorded MATCH comes from a
  semantically identical manual merge-on-read query (older-sequence files
  anti-joined against the delete file's keys, newer-sequence files unioned)
  which finishes in seconds. Engines differ in MoR read maturity; the
  *written* layout is spec-correct either way.
- The smoke suite (same raw log) also validates upsert content directly:
  after two merges, `small_str='UPDATED'` row counts in the table equal the
  source exactly (200,000 = 200,000), and DuckDB *did* apply the smaller
  smoke-scale equality deletes correctly.

## Why apitap holds at 4 GB

Same reason as every other route: the engine never materializes the table.
Postgres binary COPY streams through the parquet encoder into S3 multipart
parts, ~8 MiB in flight per pipe; the Iceberg commit at the end is a few KB
of Avro manifests plus one REST POST. Peak memory is pipes × chunk, not table
size. The 100 GB ladder in [profiling.md](profiling.md) is the same story
with three more zeros.
