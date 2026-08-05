# TPC-H across two live databases, on half a core (2026-08-05)

A real pipeline, not a micro-benchmark: **TPC-H SF 33.34** generated with
DuckDB's `dbgen`, `lineitem` loaded into Postgres and `orders` into MySQL,
then landed as a Parquet lake and joined — every stage inside a
**0.5 CPU / 256 MB** container.

| where | table | rows | columns |
|---|---|---|---|
| Postgres | `lineitem` | 49,487,825 | 16 |
| MySQL | `orders` | 49,500,000 | 9 |

The question is TPC-H Q3's, minus the customer join: **revenue by order
priority, for orders placed before 1995-03-15 whose line items shipped after
it.** Both filters are dates — which is why the lazy plane learned to
translate date literals into SQL for this (0.26.x).

## The pipeline

```python
# stage 1 — land: the filter and the projection run INSIDE each database,
# and the lake is written already bucketed on the join key
(apitap.read(PG, table="lineitem", parallel=4).lazy()
   .filter(pl.col("l_shipdate") > CUT)                    # -> WHERE in Postgres
   .select("l_orderkey",
           (pl.col("l_extendedprice").cast(pl.Float64)
            * (1 - pl.col("l_discount").cast(pl.Float64))).alias("revenue"),
           (pl.col("l_orderkey").hash(seed=42) % 16).alias("bucket"))
   .sink_parquet(pl.PartitionBy("/land/lineitem_buckets", key="bucket",
                                include_key=False), row_group_size=20_000))

# ... the same shape for MySQL orders (o_orderdate < CUT) ...

# stage 2 — compute: each bucket pair is a complete join, partials add up
parts = [pl.scan_parquet(f"/land/lineitem_buckets/bucket={k}/*.parquet")
           .join(pl.scan_parquet(f"/land/orders_buckets/bucket={k}/*.parquet"),
                 left_on="l_orderkey", right_on="o_orderkey")
           .group_by("o_orderpriority")
           .agg(pl.sum("revenue"), pl.len().alias("lineitems"))
           .collect(engine="streaming")
         for k in range(16)]
```

## Receipts

```
--- stage 1a · land Postgres lineitem   16 buckets, 178 MB   30.1s  peak 258MB
--- stage 1b · land MySQL orders        16 buckets,  46 MB   24.9s  peak 171MB
--- stage 2  · join 16 bucket pairs + aggregate              19.5s  peak 236MB

┌─────────────────┬──────────┬───────────┐
│ o_orderpriority ┆ revenue  ┆ lineitems │
╞═════════════════╪══════════╪═══════════╡
│ 1-URGENT        ┆ 8.9111e9 ┆ 245169    │
│ 2-HIGH          ┆ 9.0672e9 ┆ 249446    │
│ 3-MEDIUM        ┆ 9.0232e9 ┆ 248237    │
│ 4-NOT SPECIFIED ┆ 8.9770e9 ┆ 247199    │
│ 5-LOW           ┆ 8.9863e9 ┆ 247527    │
└─────────────────┴──────────┴───────────┘
```

**Independent truth** — DuckDB answering the same question from the raw
`dbgen` CSVs, 16 cores, no memory cap: **20.3 s**, and every digit matches
(`8.911096e+09 / 245169`, …). The point is not that half a core beats 16;
DuckDB starts from local files and finishes the whole thing in 20 s. The
point is that the same answer, to the digit, came out of **two live
databases through a 256 MB box**.

Filter selectivity, for scale: 49.5M lineitem rows → 26,678,288 survive the
ship-date filter; 49.5M orders → 24,049,698 survive the order-date filter;
only 1,237,578 line items belong to an order on both sides of the cutoff.
The server does that arithmetic — the wire never carries the rest.

## Four walls, and what they teach

1. **Date literals had to learn SQL.** TPC-H filters on dates; without a
   `Date` arm in the predicate translator the whole query fell back to a
   client-side filter and the pushdown never fired.
2. **A 24M-key hash join costs polars ~1.4 GB** (~60 B per build row): it is
   OOM-killed in a 1 GB container, never mind 256 MB. The fix is not more
   RAM, it is **bucketing the lake on the join key** — `CLUSTERED BY (key)
   INTO 16 BUCKETS`, the oldest trick in the warehouse. Each bucket pair is
   a complete join, so the partial aggregates simply add up.
3. **TPC-H order keys are sparse** (`(i/8)*32 + i%8`), so an arithmetic
   `% 16` collapses into 4-8 residues and the buckets come out lopsided.
   Hash the key instead — both sides hash identically, so matching rows
   still meet.
4. **Auto-sizing off the cgroup is wrong on a shared box.** apitap sizes its
   batches from the container limit, which is correct when it owns the
   container and suicidal when a polars sink lives there too: same
   configuration, no env → OOM-killed at 256 MB *and* at 384 MB (the batches
   grow with the box); with `APITAP_MEM_BUDGET=96M` → lands cleanly. That
   knob exists now.

Row-group size is the other contract: polars' default row group is sized for
a machine, and the parquet reader prefetches ~121 row groups at a time. At
`row_group_size=20_000` that prefetch is 39 MB; at the default it is the
whole file. Write for the smallest consumer you expect.
