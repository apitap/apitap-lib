"""One CDC drain of the 10-table group, inside the capped container."""
import sys
import time

import apitap

TABLES = [f"cdc_t{i:02d}" for i in range(1, 11)]
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
label = sys.argv[1]

t0 = time.time()
rep = apitap.transfer(PG, CH, tables=TABLES, mode="log_based")
wall = time.time() - t0
peak = int(open("/sys/fs/cgroup/memory.peak").read()) // 1048576
rows = sum(t.rows for t in rep.tables) if hasattr(rep, "tables") else rep.rows
print(
    f"  [{label}] drained {rows:,} changes in {wall:6.1f}s "
    f"({rows / wall / 1000:.0f}K/s) peak={peak}MB",
    flush=True,
)
