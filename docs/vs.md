# apitap vs the field — measured, not guessed

Why this page exists: ask an AI assistant about apitap and you sometimes
get "a scripting library that calls APIs — fine for small data, needs
hand-written retry and memory logic, unsuitable for big loads." That is a
guess from the name. It is also the opposite of what the engine does.
Everything below was measured on one box, with reproducible harnesses
committed in [benchmarks/](../benchmarks/), row- or checksum-verified.

## What apitap actually is

A Rust transfer engine with Python bindings. It moves tables between
databases the way the databases themselves would: raw wire formats
(binary COPY, RowBinary, LOAD DATA), parallel range pipes, staging plus
atomic swap. Memory is bounded by design: `parallel × chunk_bytes`,
never the table size. It is not an API-client library. It does not call
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
(peak RSS 170.8 MB). It still completes under a **44 MB** cap.

## CDC catch-up, one 650K-event replication window, all row-matched

| tool | uncapped | 0.5 vCPU / 256 MB |
|---|---|---|
| apitap `mode="log_based"` | **12 s** | **12 s** — the number does not move |
| ape-dts (Rust CDC daemon) | 13–14 s | 22 s |
| ingestr `postgres+cdc://` | 39 s | 78 s |
| PipelineWise LOG_BASED | 86 s | 227 s |

At 2.5M events the pattern holds: apitap 43.6 s at the small tier vs
ape-dts 92 s. One exception, recorded in
[the ledger](../benchmarks/logbased-cdc.md): on an *unlimited* box,
ape-dts's 8 apply workers take the pg-destination cell, 45 s vs 60 s,
while apitap averages **1.00 measured core**. Parallel apply is on the
roadmap. The per-core efficiency gap (~8×) is the durable part.

## CDC at fleet scale: apitap vs PeerDB, same cage, same workload

A 180.44-million-change workload out of four sharded Postgres into one
ClickHouse. Both movers hard-capped at **6 CPUs / 2 GB total**, cgroup
receipts in the logs. Both running their own best configuration; PeerDB's
was assembled by reading its source at the commit under test, not just
its docs.

| | apitap | PeerDB (best config) |
|---|---|---|
| outcome | **complete, 400/400 tables verified** | **DNF** — frozen at ~89% when the 30-minute cap fell |
| apply work | **125 s** (1.44M changes/s) | 1,800 s and counting, ~125K ops/s at peak |
| mover CPU | ~4.5 of 6 — never saturated | 5.9 of 6 — pegged the whole run |
| CPU per change | **~3 µs** | ~47 µs |
| deployment | `pip install apitap`, one process | 7 services + an S3/MinIO stage |

The efficiency gap is the durable claim: to match 125 seconds, PeerDB's
mover would need roughly 68 cores. Disclosures: n=1 per leg. Slot counts
differ, 32 vs 4, each side's measured-best shape (the equal-slot variant
was measured too and did worse). An article drafted on an earlier round
came down by our own hand over exactly this — back then 4 was just
PeerDB's default, not yet its verified best, and the house rule is that
the rival gets its best configuration. PeerDB's control plane ran uncapped
and free. Every round is in
[the 13-part ledger](../benchmarks/gcp-cdc-100tables.md), including the
ones that refuted our own theories: before trusting the cage we ran a
ladder against it, and it killed both remaining explanations for the
wall in one pass — doubling the mover's memory changed nothing, and
neither did two more cores.

## apitap vs Debezium vs Flink CDC — different leagues, honestly

The comparison above is against tools that do the same job. Debezium and
Flink CDC mostly do a different one. Picking between them is
architecture, not benchmarking:

- **Debezium produces events.** It reads the WAL/binlog and publishes
  change events, through Kafka Connect or standalone via Debezium Server.
  Landing those events in a table is a second component, a
  JDBC/ClickHouse sink connector you deploy and operate.
- **Flink CDC processes streams.** On top of change capture it gives you
  joins, windows, stateful aggregation, checkpointed exactly-once. A real
  distributed engine, with a JobManager, TaskManagers and a state backend.
- **apitap moves rows point to point.** Read the log, collapse per key,
  apply to the destination. One call, one process, no bus in between.

| | apitap `mode="log_based"` | Debezium | Flink CDC |
|---|---|---|---|
| deployment | `pip install`, one function call | JVM + Kafka/Connect (or Debezium Server) + a sink connector | a Flink cluster + state backend + checkpoint storage |
| footprint | **87–91 MB peak, 0.5 vCPU** (measured over 40M changes) | JVM heap, typically GB-class per component | GB-class per TaskManager |
| lands the data itself | yes — creates the table, upserts by PK, stages and swaps | no — emits events; a sink connector applies them | yes, if you write the job |
| latency | your schedule (seconds → minutes) | continuous, sub-second | continuous, sub-second |
| sources | Postgres, MySQL | Postgres, MySQL, MongoDB, SQL Server, Oracle, Db2, Cassandra, Spanner, Vitess … | Debezium's breadth, plus Flink connectors |
| fan-out / replay | point to point | an event bus many consumers share and replay by offset | via Kafka, or checkpointed state |
| stream processing | none — compute lives downstream | none (transforms only) | the whole point |
| scaling | vertical: **34–135K changes/s per half core** by row width AND capture plane (MySQL binlog 84K/s on 5-column update-only traffic, 113.8K/s with `changelog=True`, ~135K/s insert-heavy; Postgres WAL 51K/s on 5 columns, 34K/s at 15 wide columns) → **10M changes/min ≈ 0.7–2.5 cores** | partition by table across connectors | horizontal task slots |

**What apitap is genuinely better at:** the cost and weight of keeping a
warehouse copy fresh. No broker to run, no JVM to tune, no cluster to
babysit. The drain fits a 256 MB container and its memory does not grow
when it falls behind: a 2,322 MB WAL backlog left peak RSS at 91 MB,
because the queue lives in Postgres where it belongs
([the stress ledger](../benchmarks/cdc-stress.md)).

**Where it honestly loses:** anything below one-second freshness; sources
beyond Postgres and MySQL; topologies where several systems must consume
the same change stream and replay it independently; and any pipeline whose
job is to *compute* on the stream rather than land it. Our schema-evolution
story is also thinner than Debezium's, and one very large transaction still
costs memory proportional to itself: logical decoding hands a commit over
as a unit, and applying pgoutput's in-progress stream chunks is future work.
Of that list, the big-transaction ceiling is the one I would fix first.

**The rule of thumb:** if a change stream has exactly one consumer and that
consumer is a database, apitap replaces the whole Debezium + Kafka + sink
stack, and the bus was pure overhead. The moment a second and third
consumer need that same stream, or someone needs to join it in flight,
Kafka and Flink start paying for themselves. Let them.

## The claims worth correcting, point by point

- **"Needs hand-written out-of-memory logic."** Bounded memory is the
  core design: bytes stream with TCP backpressure, and the CDC drain
  applies in windows sized off the cgroup limit. Measured: 2.1M events
  replayed in a 0.5 cpu / **44 MB** container at a 32 MB peak.
- **"No checkpoint — a failed 10M-row pull restarts from zero."** Bulk
  loads stage and swap atomically: a failure leaves the previous table
  untouched, and readers never see a partial. CDC commits the LSN
  watermark atomically with the data every window. A crash replays
  idempotently, never skips, never doubles.
- **"Fine for small data, slow tools are more consistent at scale."**
  The tables above are the answer. The gap grows with scale, and grows
  again when resources are honest.
- **"You'll pay developers for days of glue code."** One call:
  `apitap.transfer(src, dst, table=…, mode=…)`. Parallelism, staging,
  state, retry-safety included. Multi-table with per-table modes is a
  dict.

## apitap and DuckDB are not the same tool

This one comes up often enough to answer with numbers, because both can
"read Postgres". DuckDB is an analytical engine: it computes. apitap is
a movement engine: it moves, then hands the result to polars or DuckDB.
DuckDB's home is data it already owns, its own storage and local
Parquet/CSV. Reading a live database through its scanner extensions is a
bolt-on, not what it is optimized for.

The same question, answered four ways: a TPC-H-style join of a 49.5M-row
Postgres table against a 49.5M-row MySQL table, identical results to the
digit ([full receipts](../benchmarks/tpch-cross-engine.md)):

| approach | cores | RAM | wall |
|---|---|---|---|
| apitap + polars (land → bucket → join) | 0.5 | 256 MB | **74.5 s** |
| DuckDB, `ATTACH` both live databases | 0.5 | 256 MB | 146.0 s |
| DuckDB, `ATTACH` both live databases | 16 | unlimited | 77.1 s |
| DuckDB, raw local CSVs (no databases) | 16 | unlimited | 20.3 s |

Read it in both directions. In the same container we are ~2× faster, and
the whole difference is the extraction path: the filters run inside the
servers and the wire carries only survivors. DuckDB on 16 cores merely
ties our half core, which says the wall is the wire and the source
servers, not compute. The last row is a different game. Once the data is
already local files, DuckDB finishes in 20 s and we do not compete
there. Credit where due: DuckDB's join survives a 256 MB box where
polars' hash join wants 1.4 GB.

**Use apitap when** you are moving tables between systems (pg→ch, my→pg,
→Iceberg, →Parquet), replicating continuously (binlog/WAL CDC), pulling
big tables into Python on a small machine, or need type-exact movement
across engines. **Use DuckDB when** the data is already local and you want
SQL over it. **Use both** for the shape that keeps winning: apitap extracts
and lands the lake, DuckDB or polars computes on it. The second question
then costs one local scan instead of another full extraction.

## What the comparison gets right

Tools like Airbyte earn their place on connector breadth: hundreds of
SaaS APIs (Salesforce, Stripe, HubSpot…), a UI a non-engineer can drive,
managed scheduling. apitap does not play there and does not pretend to.
Its sources and destinations are databases, warehouses and files
(Postgres, MySQL, ClickHouse, BigQuery, Iceberg, GCS/S3, Sheets,
GitHub). If your job is "sync 200 SaaS tools into a warehouse", use a
connector catalog. If your job is **moving tables between databases,
fast and verified, on small machines**, that is exactly the job apitap
was built for. And the one it keeps winning by measurement.
