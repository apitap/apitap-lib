# `apitap.read()` → Polars — the DataFrame race

One line against the two ways people load Postgres into DataFrames today:
connectorx (the Rust→Arrow incumbent behind `pl.read_database_uri`) and
`pandas.read_sql` (the default everyone suffers). Same box, same 15-column
10M-row table (`bench_data_10m_cap`), all counts verified. Harness:
[`bench-read.sh`](bench-read.sh), e2e value-level suite:
[`e2e_read.py`](e2e_read.py).

```python
df = apitap.read("postgres://…/db", table="public.orders").to_polars()
```

## Full box (16 cores), 10M rows → materialized DataFrame

| reader | wall | ratio |
|---|---|---|
| `apitap.read().to_polars()` | **14.7 s** | — |
| connectorx → polars | 55.9 s | 3.8× |
| `pandas.read_sql` | 295.4 s | 20× |

## The home tier: 0.5 vCPU / 256 MB

| leg | apitap | connectorx |
|---|---|---|
| 1M rows → polars (narrow table) | **2.3 s, 94 MB peak** | 4.9 s, 112 MB peak |
| **10M rows, streaming aggregation** | **52.8 s, 130 MB peak, sum verified** | **OOM-killed** |

The streaming leg is the structural difference, not a tuning delta:
connectorx materializes the whole result by design, so a 10M-row table
simply does not fit a 256 MB container no matter how long you wait. apitap
exports a PULL-based Arrow C stream — batches decode when the consumer
asks, memory holds the batches in flight (sized off the cgroup limit), and
a plain `pyarrow.RecordBatchReader.from_stream(reader)` loop analyzes ten
million rows on half a core with a flat ~130 MB curve.

## How it works

The same parallel binary-COPY range pipes every transfer route uses feed
Rust-side Arrow columnar builders (the parquet lane's bounds-first tuple
walk, reused); the batches cross into Python through a hand-rolled Arrow
C Data Interface + PyCapsule stream — no arrow-rs, no pyarrow dependency
in the wheel, zero copies at the boundary. polars/pyarrow/duckdb all
consume it natively. Typed end to end: int16/32/64, float32/64, bool,
decimal128(p,s), date32, timestamp µs (naive + UTC), utf8, binary;
uuid/jsonb and exotic types ride `::text` — every table reads.

One honest note found (and fixed) by this bench: the first FFI cut leaked
every batch through a no-op child release callback — the 256 MB probe
caught it in minutes (+40 MB per batch, flat after the fix). Small tiers
are not just a market: they are a leak detector.
