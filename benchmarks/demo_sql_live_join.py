"""Walk-through of an incremental join view: Postgres lineitem x MySQL orders.

The exact scenario of the explanation: run 1 bootstraps, then three changes
(a Postgres insert, a MySQL group change, a MySQL insert that gives an
orphan line its order), run 2 moves the answer by those changes only. After
each run it prints the destination's table next to a full recompute.
"""
import os
import subprocess
import sys
import tempfile

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
STATE = tempfile.mkdtemp(prefix="apitap-demo-")
Q = ("SELECT o.o_orderpriority, SUM(l.l_extendedprice) AS revenue, COUNT(*) AS n "
     "FROM pg.demo_li l JOIN my.demo_ord o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1")


def sh(args, sql):
    o = subprocess.run(args + [sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg(sql):
    return sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src", "-Atc"], sql)


def pgd(sql):
    return sh(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst", "-Atc"], sql)


def my(sql):
    return sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "bench", "-N", "-e"], sql)


def run(label):
    code = ("import apitap\n"
            f"apitap.sql({Q!r}, sources={{'pg': {PG!r}, 'my': {MY!r}}}, dest={PGD!r}, table='demo_out', mode='log_based')\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       env=dict(os.environ, APITAP_DEBUG="1", APITAP_STATE_DIR=STATE))
    print(f"\n== {label}")
    for l in r.stderr.splitlines():
        if "[sql] view" in l:
            print("   ", l.split("] ", 1)[1])
    if r.returncode:
        print(r.stderr[-800:])
        raise SystemExit(1)
    got = pgd("SELECT o_orderpriority, revenue, n FROM demo_out ORDER BY 1").replace("|", " ").splitlines()
    orders = dict(l.split("\t") for l in my("SELECT o_orderkey, o_orderpriority FROM demo_ord").splitlines())
    want = {}
    for l in pg("SELECT l_orderkey, l_extendedprice FROM demo_li").splitlines():
        k, p = l.split("|")
        if k in orders:
            a = want.setdefault(orders[k], [0, 0])
            a[0] += float(p)
            a[1] += 1
    print("    destination:", got)
    print("    recompute  :", [f"{g} {v[0]:.2f} {v[1]}" for g, v in sorted(want.items())])


def main():
    pg("DROP TABLE IF EXISTS demo_li; CREATE TABLE demo_li (id int PRIMARY KEY, l_orderkey bigint, "
       "l_extendedprice numeric(12,2)); ALTER TABLE demo_li REPLICA IDENTITY FULL; "
       "INSERT INTO demo_li VALUES (1,1,10),(2,1,20),(3,2,5),(4,3,7)")
    my("DROP TABLE IF EXISTS demo_ord; CREATE TABLE demo_ord (o_orderkey bigint PRIMARY KEY, "
       "o_orderpriority varchar(10)); INSERT INTO demo_ord VALUES (1,'URGENT'),(2,'LOW')")
    try:
        run("run 1: bootstrap (both tables read once)")
        pg("INSERT INTO demo_li VALUES (5, 1, 100)")
        my("UPDATE demo_ord SET o_orderpriority = 'LOW' WHERE o_orderkey = 1")
        my("INSERT INTO demo_ord VALUES (3, 'URGENT')")
        print("\n-- changes: pg INSERT lineitem(k=1, 100); my UPDATE order 1 URGENT->LOW; my INSERT order (3, URGENT)")
        run("run 2: only the changes are read")
        pg("DELETE FROM demo_li WHERE id = 3")
        print("\n-- change: pg DELETE lineitem id=3 (k=2, 5)")
        run("run 3")
    finally:
        pg("DROP TABLE IF EXISTS demo_li")
        my("DROP TABLE IF EXISTS demo_ord")
        pgd("DROP TABLE IF EXISTS demo_out")
        for s in pg("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_v_%'").split():
            pg(f"SELECT pg_drop_replication_slot('{s}')")
        for p in pg("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_v_%'").split():
            pg(f'DROP PUBLICATION "{p}"')


if __name__ == "__main__":
    main()
