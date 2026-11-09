"""PGO training for apitap.sql(): every TPC-H query and an incremental view.

Run inside the instrumented wheel's container (benchmarks/pgo-build.sh): the
star joins, semi-joins, staged plans (mem:// stages, decorrelation, LEFT JOIN,
COUNT DISTINCT), collation compares and the view's bootstrap + window paths are
hot branches of their own, and a branch missing from training can regress.
Needs only the bench servers: the data is loaded and cleaned up through
DuckDB's own Postgres/MySQL connections. argv: [sf] (default 0.1).
"""
import os
import sys
import tempfile

import apitap
import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import e2e_sql_tpch as h  # noqa: E402

PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
PGD_DUCK = "host=127.0.0.1 port=5545 dbname=apitap_bench_dst user=postgres password=bench"
VIEW = ("SELECT o.o_orderpriority, SUM(l.l_extendedprice * (1 - l.l_discount)) AS rev, COUNT(*) AS n "
        "FROM pg.tpch_lineitem l JOIN my.tpch_orders o ON l.l_orderkey = o.o_orderkey "
        "WHERE l.l_quantity > 1 GROUP BY 1 ORDER BY 1")


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 0.1
    con = duckdb.connect()
    h.load(con, sf)
    queries = con.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY 1").fetchall()
    for _ in range(2):
        for n, q in queries:
            r = apitap.sql(h.pointed(q), sources={"pg": h.PG, "my": h.MY})
            print(f"  Q{n} {r.to_polars().height} rows", flush=True)
    # The incremental view: bootstrap, one window of changes on both sides.
    con.execute(f"ATTACH '{h.PG_DUCK}' AS pgw (TYPE postgres)")
    con.execute(f"ATTACH '{h.MY_DUCK}' AS myw (TYPE mysql)")
    con.execute(f"ATTACH '{PGD_DUCK}' AS pgd (TYPE postgres)")
    state = tempfile.mkdtemp(prefix="apitap-pgo-view-")
    os.environ["APITAP_STATE_DIR"] = state
    kw = dict(sources={"pg": h.PG, "my": h.MY}, dest=PGD, table="pgo_view", mode="log_based")
    # A view moves by each change's old and new image.
    con.execute("CALL postgres_execute('pgw', 'ALTER TABLE tpch_lineitem REPLICA IDENTITY FULL')")
    try:
        apitap.sql(VIEW, **kw)
        con.execute("CALL postgres_execute('pgw', 'UPDATE tpch_lineitem SET l_quantity = l_quantity + 1 "
                    "WHERE l_orderkey % 7 = 0')")
        con.execute("CALL postgres_execute('pgw', 'DELETE FROM tpch_lineitem WHERE l_orderkey % 11 = 0')")
        con.execute("CALL mysql_execute('myw', 'UPDATE tpch_orders SET o_orderpriority = ''1-URGENT'' "
                    "WHERE o_orderkey % 5 = 0')")
        apitap.sql(VIEW, **kw)
        print("  view: bootstrap + one window", flush=True)
    finally:
        slots = con.execute("SELECT * FROM postgres_query('pgw', 'SELECT slot_name FROM pg_replication_slots "
                            "WHERE slot_name LIKE ''apitap_v_%''')").fetchall()
        for (s,) in slots:
            con.execute(f"CALL postgres_execute('pgw', 'SELECT pg_drop_replication_slot(''{s}'')')")
        pubs = con.execute("SELECT * FROM postgres_query('pgw', 'SELECT pubname FROM pg_publication "
                           "WHERE pubname LIKE ''apitap_v_%''')").fetchall()
        for (p,) in pubs:
            con.execute(f"CALL postgres_execute('pgw', 'DROP PUBLICATION \"{p}\"')")
        con.execute("CALL postgres_execute('pgd', 'DROP TABLE IF EXISTS pgo_view')")
        for t in h.ON_PG:
            con.execute(f"CALL postgres_execute('pgw', 'DROP TABLE IF EXISTS {h.PREFIX}{t}')")
        for t in h.ON_MY:
            con.execute(f"CALL mysql_execute('myw', 'DROP TABLE IF EXISTS {h.PREFIX}{t}')")
    print("PGO SQL TRAINING DONE")


if __name__ == "__main__":
    main()
