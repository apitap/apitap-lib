"""DuckDB joining the two LIVE databases directly, via its postgres/mysql
extensions — the same question, no landing zone."""
import sys
import time

import duckdb

mem = sys.argv[1] if len(sys.argv) > 1 else None
threads = sys.argv[2] if len(sys.argv) > 2 else None

con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres; INSTALL mysql; LOAD mysql;")
if mem:
    con.execute(f"SET memory_limit='{mem}'")
if threads:
    con.execute(f"SET threads={threads}")
con.execute(
    "ATTACH 'host=127.0.0.1 port=5544 dbname=apitap_bench_src user=postgres "
    "password=bench' AS pg (TYPE postgres, READ_ONLY)"
)
con.execute(
    "ATTACH 'host=127.0.0.1 port=3307 database=bench user=root password=bench' "
    "AS my (TYPE mysql, READ_ONLY)"
)

t0 = time.time()
rows = con.execute(
    """
    SELECT o_orderpriority,
           round(sum(CAST(l_extendedprice AS DOUBLE)
                     * (1 - CAST(l_discount AS DOUBLE))), 2) AS revenue,
           count(*) AS lineitems
    FROM pg.lineitem l
    JOIN my.orders o ON l.l_orderkey = o.o_orderkey
    WHERE l.l_shipdate > DATE '1995-03-15'
      AND o.o_orderdate < DATE '1995-03-15'
    GROUP BY 1 ORDER BY 1
    """
).fetchall()
wall = time.time() - t0
for row in rows:
    print("  ", row)
try:
    peak = int(open("/sys/fs/cgroup/memory.peak").read()) // 1048576
    peak = f" peak={peak}MB"
except Exception:
    peak = ""
print(f"duckdb direct (mem={mem or 'auto'}, threads={threads or 'auto'}): {wall:.1f}s{peak}")
