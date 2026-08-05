"""Generate real TPC-H data with DuckDB's dbgen, sliced to bound disk.

SF 33.34 over 8 children:
  orders   — all 8 slices  -> 50,010,000 rows  -> MySQL
  lineitem — slices 0,1    -> ~50,040,000 rows -> Postgres
Every lineitem orderkey exists in orders (dbgen's key assignment is a pure
function of the row index, so slice k of lineitem references slice k of
orders), so the cross-database join is referentially complete.
"""
import os
import sys
import time

import duckdb

SF = 33.34
CHILDREN = 8
OUT = "/home/ubuntu/tpch"
os.makedirs(OUT, exist_ok=True)

for step in range(CHILDREN):
    db = f"{OUT}/gen_{step}.duckdb"
    for stale in (db, db + ".wal"):
        if os.path.exists(stale):
            os.remove(stale)
    t0 = time.time()
    con = duckdb.connect(db)
    con.execute("SET memory_limit='8GB'; SET threads=8;")
    con.execute("INSTALL tpch; LOAD tpch;")
    con.execute(f"CALL dbgen(sf={SF}, children={CHILDREN}, step={step})")
    n_o = con.execute("select count(*) from orders").fetchone()[0]
    con.execute(
        f"COPY orders TO '{OUT}/orders_{step}.csv' (FORMAT csv, HEADER false)"
    )
    n_l = 0
    if step < 2:
        n_l = con.execute("select count(*) from lineitem").fetchone()[0]
        con.execute(
            f"COPY lineitem TO '{OUT}/lineitem_{step}.csv' (FORMAT csv, HEADER false)"
        )
    con.close()
    os.remove(db)
    if os.path.exists(db + ".wal"):
        os.remove(db + ".wal")
    print(
        f"slice {step}: orders={n_o:,} lineitem={n_l:,} in {time.time()-t0:.0f}s",
        flush=True,
    )

print("TPCH GEN DONE", flush=True)
