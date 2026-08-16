"""One timed CDC drain for the pgoutput-binary A/B (runs in the apitap venv).

Prints wall, the apitap process's own CPU (getrusage covers every thread),
and a RESULT line the orchestrator parses. APITAP_PG_BINARY is inherited
from the environment — the lever under test.
"""
import os
import resource
import sys
import time

import apitap

TABLES = [f"bin_t{i:02d}" for i in range(1, 11)]
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"

label = sys.argv[1]
mode = os.environ.get("APITAP_PG_BINARY", "unset")
t0 = time.time()
rep = apitap.transfer(PG, CH, tables=TABLES, mode="log_based")
wall = time.time() - t0
ru = resource.getrusage(resource.RUSAGE_SELF)
cpu = ru.ru_utime + ru.ru_stime
rows = sum(t.rows for t in rep.tables) if hasattr(rep, "tables") else rep.rows
print(
    f"  [{label}] APITAP_PG_BINARY={mode} rows={rows:,} wall={wall:.1f}s "
    f"apitap_cpu={cpu:.1f}s ({rows / max(wall, 1e-3) / 1000:.0f}K/s)",
    flush=True,
)
print(f"RESULT rows={rows} wall={wall:.3f} apitap_cpu={cpu:.3f}", flush=True)
