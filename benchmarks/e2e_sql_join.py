"""apitap.sql() joins a Postgres table with a MySQL table and aggregates, exactly, in Rust.

0.65.0 had no way to join two databases inside apitap: rows had to leave for
DuckDB or polars, where a 5 GB × 5 GB join spilled through a 256 MB
container. apitap.sql() runs the join + aggregate itself:

- each table's conjuncts and columns are pushed into its own server;
- the join key's range is intersected and pushed into both;
- the GROUP BY table is reduced to one group id per key;
- the other table streams into exact decimal accumulators.

  1  the TPC-H Q3-shaped join (SUM of decimal arithmetic, COUNT(*), GROUP BY a
     MySQL column, filters on both sides, ORDER BY) equals the answer computed
     here from each server's own text, digit for digit
  2  MIN, MAX, AVG and COUNT(col) over the join, with a NULL in the
     aggregated column
  3  one table with GROUP BY, no join
  4  what v1 does not run is refused before any read, naming it: LEFT JOIN, a
     WHERE naming both tables, SELECT *, a repeated GROUP BY-side key
"""
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}
TL, TO = "sql_li", "sql_od"
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                        "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B",
                        "bench", "-e", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def rows(res):
    df = res.to_polars()
    return [tuple(r) for r in df.iter_rows()]


def main():
    pg(f"DROP TABLE IF EXISTS {TL}; CREATE TABLE {TL} (l_orderkey bigint, l_extendedprice numeric(15,2), "
       f"l_discount numeric(15,2), l_shipdate date); "
       f"INSERT INTO {TL} SELECT (g % 3000) + 1, (g * 1.37)::numeric(15,2), ((g % 11) / 100.0)::numeric(15,2), "
       f"DATE '1995-01-01' + (g % 240) FROM generate_series(1, 60000) g; "
       f"INSERT INTO {TL} VALUES (7, NULL, 0.05, DATE '1995-06-01'), (NULL, 9.99, 0.01, DATE '1995-06-01')")
    my(f"DROP TABLE IF EXISTS {TO}; CREATE TABLE {TO} (o_orderkey bigint PRIMARY KEY, o_orderdate date, "
       f"o_orderpriority varchar(15)); SET SESSION cte_max_recursion_depth = 4000; "
       f"INSERT INTO {TO} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 2500) "
       f"SELECT n, DATE_ADD('1994-12-01', INTERVAL (n % 200) DAY), CONCAT(n % 5, '-PRIO') FROM s")
    try:
        # Truth from each server's own text.
        orders = {}
        for line in my(f"SELECT o_orderkey, o_orderpriority FROM {TO} WHERE o_orderdate < '1995-03-15'").splitlines():
            k, p = line.split("\t")
            orders[int(k)] = p
        lines = []
        for line in pg(f"SELECT l_orderkey, l_extendedprice, l_discount FROM {TL} "
                       f"WHERE l_shipdate > DATE '1995-03-15' AND l_orderkey IS NOT NULL").splitlines():
            k, price, disc = line.split("|")
            lines.append((int(k), None if price == "" else Decimal(price), Decimal(disc)))
        agg = defaultdict(lambda: [Decimal(0), 0, None, None, [], 0])
        for k, price, disc in lines:
            if k not in orders:
                continue
            a = agg[orders[k]]
            a[1] += 1
            if price is not None:
                a[0] += price * (1 - disc)
                a[2] = price if a[2] is None else min(a[2], price)
                a[3] = price if a[3] is None else max(a[3], price)
                a[4].append(price)
                a[5] += 1
        q = (f"SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*) AS n "
             f"FROM pg.{TL} l JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey "
             f"WHERE l.l_shipdate > DATE '1995-03-15' AND o.o_orderdate < DATE '1995-03-15' "
             f"GROUP BY 1 ORDER BY 1")
        got = rows(apitap.sql(q, sources=SRC))
        want = [(p, a[0], a[1]) for p, a in sorted(agg.items())]
        case("1: the Q3-shaped join equals the servers' answer", got == want and len(want) == 5,
             f"got {got[:2]}… want {want[:2]}…")

        q2 = (f"SELECT o_orderpriority, MIN(l_extendedprice), MAX(l_extendedprice), AVG(l_extendedprice), "
              f"COUNT(l_extendedprice) FROM pg.{TL} l JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey "
              f"WHERE l.l_shipdate > DATE '1995-03-15' AND o.o_orderdate < DATE '1995-03-15' GROUP BY 1 ORDER BY 1")
        got = rows(apitap.sql(q2, sources=SRC))
        want = [(p, a[2], a[3], float(sum(a[4]) / len(a[4])), a[5]) for p, a in sorted(agg.items())]
        good = len(got) == len(want) and all(
            g[0] == w[0] and g[1] == w[1] and g[2] == w[2] and abs(g[3] - w[3]) < 1e-6 and g[4] == w[4]
            for g, w in zip(got, want))
        case("2: MIN, MAX, AVG and COUNT(col) over the join, a NULL skipped", good, f"got {got[:1]} want {want[:1]}")

        q3 = f"SELECT o_orderpriority, COUNT(*) FROM my.{TO} WHERE o_orderkey > 100 GROUP BY o_orderpriority ORDER BY 1"
        want = [tuple(l.split("\t")) for l in
                my(f"SELECT o_orderpriority, COUNT(*) FROM {TO} WHERE o_orderkey > 100 GROUP BY 1 ORDER BY 1").splitlines()]
        got = [(p, str(n)) for p, n in rows(apitap.sql(q3, sources=SRC))]
        case("3: one table with GROUP BY", got == want, f"got {got} want {want}")

        for label, bad, frag in (
            ("LEFT JOIN", f"SELECT COUNT(*) FROM pg.{TL} l LEFT JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey", "INNER JOIN"),
            ("a WHERE naming both tables", f"SELECT o_orderpriority, COUNT(*) FROM pg.{TL} l JOIN my.{TO} o "
             f"ON l.l_orderkey = o.o_orderkey WHERE l.l_discount > o.o_orderkey GROUP BY 1", "both tables"),
            ("SELECT *", f"SELECT * FROM pg.{TL}", "SELECT *"),
            ("a repeated GROUP BY-side key", f"SELECT l_shipdate, COUNT(*) FROM pg.{TL} l JOIN my.{TO} o "
             f"ON l.l_orderkey = o.o_orderkey GROUP BY 1", "repeats"),
        ):
            try:
                apitap.sql(bad, sources=SRC)
                case(f"4: {label} is refused", False, "it ran")
            except Exception as e:  # noqa: BLE001 — the message is the contract
                case(f"4: {label} is refused, naming it", frag in str(e), str(e)[:150])
    finally:
        pg(f"DROP TABLE IF EXISTS {TL}")
        my(f"DROP TABLE IF EXISTS {TO}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"   XX {type(e).__name__}: {e}")
        ok = False
    print("SQL JOIN E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
