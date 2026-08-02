"""Runs INSIDE the capped container (e2e_logbased_capped.sh): one log_based
drain over a multi-window backlog, then prints the cgroup peak."""
import time

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"

t0 = time.time()
r = apitap.transfer(SRC, DST, table="cdc_cap", mode="log_based")
print(f"drain: events={r.rows:,} in {time.time()-t0:.1f}s")

for f in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
    try:
        peak = int(open(f).read().strip())
        print(f"cgroup peak: {peak/1048576:.1f} MB")
        break
    except OSError:
        pass
