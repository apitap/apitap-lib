"""Stage 2 — join + aggregate the bucketed lake, one bucket at a time.

Buckets are disjoint on the join key, so each pass is a complete join of its
slice and the partial aggregates simply add up.
"""
import gc
import sys
import time

import polars as pl

K = int(sys.argv[1])
trace = len(sys.argv) > 2
peak = lambda: int(open("/sys/fs/cgroup/memory.peak").read()) // 1048576

t0 = time.time()
parts = []
for k in range(K):
    li = pl.scan_parquet(f"/land/lineitem_buckets/bucket={k}/*.parquet")
    od = pl.scan_parquet(f"/land/orders_buckets/bucket={k}/*.parquet")
    parts.append(
        li.join(od, left_on="l_orderkey", right_on="o_orderkey")
        .group_by("o_orderpriority")
        .agg(pl.sum("revenue").alias("revenue"), pl.len().alias("lineitems"))
        .collect(engine="streaming")
    )
    gc.collect()
    if trace:
        print(f"    bucket {k:2d}: {time.time()-t0:5.1f}s peak={peak()}MB", flush=True)

result = (
    pl.concat(parts)
    .group_by("o_orderpriority")
    .agg(pl.sum("revenue").round(2).alias("revenue"), pl.sum("lineitems").alias("lineitems"))
    .sort("o_orderpriority")
)
print(result)
print(f"  stage 2 · join {K} buckets + aggregate: {time.time()-t0:.1f}s peak={peak()}MB", flush=True)
