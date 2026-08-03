# CH ingest-format receipts (2026-08-03)

Prompted by a r/Clickhouse comment on the 232M/30s post ("duckdb eats a
tsv faster; how much of the 30s was just postgres?"). Both measured on
the bench VPS (16 cores), 10M rows / 15 cols (bench_data_10m_cap),
scripts: ~/ch_format_ab.sh, ~/pipe_race.sh (VPS), logs in apitap-live.log.

## 1. File on disk -> clickhouse-client (format cost only, export prepaid)

| format | round 1 | round 2 | file size |
|---|---|---|---|
| TSV | 18.2s | **17.8s** | 4.69 GB |
| CSV | 19.8s | 19.2s | 4.91 GB |
| RowBinary | 20.8s | 22.2s | 4.13 GB |

TSV WINS the file case: ClickHouse parallel-parses row-delimited text
across cores; RowBinary parses single-threaded. The post's "RowBinary
beat TSV/CSV ingestion by a wide margin" was too strong for this shape —
corrected in the thread with these numbers.

## 2. Streamed end-to-end, no intermediate file (the shape a db->db
## transfer actually is)

| pipeline | wall |
|---|---|
| apitap: COPY binary -> in-flight transcode -> HTTP RowBinary | **13.5s** |
| psql COPY text STDOUT \| clickhouse-client INSERT TSV | 56.7s / 55.1s |

Streaming exposes the cost the file case prepays: Postgres formatting
every value to text + the single-stream pipe + the parse. 4x. Binary's
win is the PRODUCER side (and parallel pipes), not CH's parser.
