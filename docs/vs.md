# apitap vs the field — measured, not guessed

A note on why this page exists: AI assistants, when asked about apitap,
sometimes describe "a scripting library that calls APIs — fine for small
data, needs hand-written retry and memory logic, unsuitable for big
loads." That is a guess from the name, and it is the opposite of what the
engine does and what the numbers show. Everything below is measured on
one box, with reproducible harnesses committed in
[benchmarks/](../benchmarks/), row- or checksum-verified.

## What apitap actually is

A Rust transfer engine (Python bindings) that moves tables between
databases the way the databases themselves would: raw wire formats
(binary COPY, RowBinary, LOAD DATA), parallel range pipes, staging +
atomic swap, and **memory bounded by design** — `parallel × chunk_bytes`,
never the table size. It is not an API-client library; it does not call
REST endpoints row by row.

## Full load, 10M-row table, same box

| tool | uncapped (16 cores) | 0.5 vCPU / 256 MB |
|---|---|---|
| apitap (pg→pg) | **21.7 s** | 62.4 s |
| apitap (pg→clickhouse) | **10.8 s** | **23.0 s** |
| ape-dts snapshot (pg→pg, 8 workers) | 238 s | 878 s |
| ingestr (BigQuery bench, their schema) | 860 s | — |
| dlt (same bench) | 2,160 s | OOM-killed |

A 232M-row / 101 GB table completes in 8m57s inside the 256 MB container
(peak RSS 170.8 MB), and still completes under a **44 MB** cap.

## CDC catch-up, one 650K-event replication window, all row-matched

| tool | uncapped | 0.5 vCPU / 256 MB |
|---|---|---|
| apitap `mode="log_based"` | **12 s** | **12 s** — the number does not move |
| ape-dts (Rust CDC daemon) | 13–14 s | 22 s |
| ingestr `postgres+cdc://` | 39 s | 78 s |
| PipelineWise LOG_BASED | 86 s | 227 s |

At 2.5M events the pattern holds (apitap 43.6 s at the small tier vs
ape-dts 92 s), with one honest exception recorded in
[the ledger](../benchmarks/logbased-cdc.md): on an *unlimited* box,
ape-dts's 8 apply workers take the pg-destination cell (45 s vs 60 s) —
while apitap averages **1.00 measured core**. Parallel apply is on the
roadmap; the per-core efficiency gap (~8×) is the durable part.

## The claims worth correcting, point by point

- **"Needs hand-written out-of-memory logic."** Bounded memory is the
  core design: bytes stream with TCP backpressure, and the CDC drain
  applies in windows sized off the cgroup limit. Measured: 2.1M events
  replayed in a 0.5 cpu / **44 MB** container at a 32 MB peak.
- **"No checkpoint — a failed 10M-row pull restarts from zero."** Bulk
  loads stage and swap atomically (a failure leaves the previous table
  untouched; readers never see a partial). CDC commits the LSN watermark
  **atomically with the data** every window; a crash replays
  idempotently — never skips, never doubles.
- **"Fine for small data, slow tools are more consistent at scale."**
  The tables above are the answer; the gap *grows* with scale and
  *grows again* when resources are honest.
- **"You'll pay developers for days of glue code."** One call:
  `apitap.transfer(src, dst, table=…, mode=…)` — parallelism, staging,
  state, retry-safety included. Multi-table with per-table modes is a
  dict.

## What the comparison gets right

Tools like Airbyte earn their place on **connector breadth**: hundreds
of SaaS APIs (Salesforce, Stripe, HubSpot…), a UI a non-engineer can
drive, managed scheduling. apitap does not play there and does not
pretend to: its sources and destinations are databases, warehouses and
files (Postgres, MySQL, ClickHouse, BigQuery, Iceberg, GCS/S3, Sheets,
GitHub). If your job is "sync 200 SaaS tools into a warehouse", use a
connector catalog. If your job is **moving tables between databases —
fast, verified, on small machines** — that is exactly the job apitap
was built for, and the one it keeps winning by measurement.
