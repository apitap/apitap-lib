"""Land ONE source straight into hash buckets — no giant intermediate file."""
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

os.environ.setdefault("APITAP_MEM_BUDGET", "64M")

import apitap
import polars as pl

which, par, K = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench?ssl-mode=disabled"
CUT = date(1995, 3, 15)

out = Path(f"/land/{'lineitem' if which == 'pg' else 'orders'}_buckets")
if out.exists():
    shutil.rmtree(out)
out.mkdir(parents=True)

t0 = time.time()
if which == "pg":
    lf = (
        apitap.read(PG, table="lineitem", parallel=par)
        .lazy()
        .filter(pl.col("l_shipdate") > CUT)
        .select(
            "l_orderkey",
            (
                pl.col("l_extendedprice").cast(pl.Float64)
                * (1 - pl.col("l_discount").cast(pl.Float64))
            ).alias("revenue"),
            (pl.col("l_orderkey").hash(seed=42) % K).alias("bucket"),
        )
    )
else:
    lf = (
        apitap.read(MY, table="orders", parallel=par)
        .lazy()
        .filter(pl.col("o_orderdate") < CUT)
        .select(
            "o_orderkey",
            "o_orderpriority",
            (pl.col("o_orderkey").hash(seed=42) % K).alias("bucket"),
        )
    )
lf.sink_parquet(
    pl.PartitionBy(str(out), key="bucket", include_key=False), row_group_size=20_000
)
size = sum(f.stat().st_size for f in out.rglob("*.parquet")) / 1e6
peak = int(open("/sys/fs/cgroup/memory.peak").read()) // 1048576
print(
    f"  land {which} -> {len(list(out.iterdir()))} buckets, {size:.0f}MB total: "
    f"{time.time()-t0:.1f}s peak={peak}MB",
    flush=True,
)
