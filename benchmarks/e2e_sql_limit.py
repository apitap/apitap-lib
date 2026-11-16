"""A LIMIT with no ORDER BY stops the read early, and only the statement's own.

`SELECT a, b FROM t LIMIT n` is answered by grouping every output column with a
hidden row count and repeating the rows after; through 0.67.2 that grouped the
whole table and was refused past the group budget (QA, W1 round P, L1). The
read now stops at the n-th row that passes the WHERE. The stop must belong to
the statement that produces the final rows only: a subquery's key set (IN,
EXISTS), a CTE, or a derived table read under it must still read everything,
or the answer comes back short with no error.

Each case is checked against Postgres: the row count is min(n, the full
answer's count) and every row returned is a row of the full answer (a
multiset inclusion). The fixtures make a cut inner read visible: l1_b holds
995 keys that match nothing in l1_a first, then the 5 that do, so a subquery
read cut at 5 rows answers 0 rows instead of 5.

Usage: e2e_sql_limit.py
"""
import os
import subprocess
import sys
import time
from collections import Counter

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
SRC = {"pg": PG}


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [ln for ln in o.stdout.splitlines() if ln]


def norm(v):
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def wipe():
    pg("DROP TABLE IF EXISTS l1_a; DROP TABLE IF EXISTS l1_b")


# (name, apitap query, Postgres full answer without the LIMIT, n)
CASES = [
    ("plain projection", "SELECT k, v FROM pg.l1_a LIMIT 5", "SELECT k, v FROM l1_a", 5),
    ("projection with duplicates", "SELECT v FROM pg.l1_a LIMIT 7", "SELECT v FROM l1_a", 7),
    ("sparse WHERE", "SELECT k, v FROM pg.l1_a WHERE v = 7 AND k > 1500000 LIMIT 5",
     "SELECT k, v FROM l1_a WHERE v = 7 AND k > 1500000", 5),
    ("IN (subquery)", "SELECT k, v FROM pg.l1_a WHERE k IN (SELECT k FROM pg.l1_b) LIMIT 5",
     "SELECT k, v FROM l1_a WHERE k IN (SELECT k FROM l1_b)", 5),
    ("EXISTS", "SELECT a.k, a.v FROM pg.l1_a a WHERE EXISTS (SELECT 1 FROM pg.l1_b b WHERE b.k = a.k) LIMIT 5",
     "SELECT a.k, a.v FROM l1_a a WHERE EXISTS (SELECT 1 FROM l1_b b WHERE b.k = a.k)", 5),
    ("NOT IN (subquery)", "SELECT k FROM pg.l1_b WHERE k NOT IN (SELECT k FROM pg.l1_a) LIMIT 4",
     "SELECT k FROM l1_b WHERE k NOT IN (SELECT k FROM l1_a)", 4),
    ("CTE", "WITH c AS (SELECT k FROM pg.l1_b) SELECT a.k, a.v FROM pg.l1_a a WHERE a.k IN (SELECT k FROM c) LIMIT 5",
     "WITH c AS (SELECT k FROM l1_b) SELECT a.k, a.v FROM l1_a a WHERE a.k IN (SELECT k FROM c)", 5),
    ("derived table", "SELECT x.k FROM (SELECT k, v FROM pg.l1_a WHERE v > 90) x LIMIT 5",
     "SELECT x.k FROM (SELECT k, v FROM l1_a WHERE v > 90) x", 5),
    ("join", "SELECT a.k, a.v FROM pg.l1_a a JOIN pg.l1_b b ON a.k = b.k LIMIT 3",
     "SELECT a.k, a.v FROM l1_a a JOIN l1_b b ON a.k = b.k", 3),
    ("DISTINCT", "SELECT DISTINCT v FROM pg.l1_a LIMIT 5", "SELECT DISTINCT v FROM l1_a", 5),
    ("LIMIT past the rows", "SELECT k FROM pg.l1_b WHERE k < 2000001 LIMIT 50",
     "SELECT k FROM l1_b WHERE k < 2000001", 50),
    ("LIMIT 0", "SELECT k FROM pg.l1_a LIMIT 0", "SELECT k FROM l1_a LIMIT 0", 0),
    ("COUNT of a LIMITed derived table", "SELECT COUNT(*) AS n FROM (SELECT k FROM pg.l1_a LIMIT 1000) t",
     "SELECT COUNT(*) FROM (SELECT k FROM l1_a LIMIT 1000) t", 1),
    ("DISTINCT ORDER BY LIMIT", "SELECT DISTINCT v FROM pg.l1_a ORDER BY v LIMIT 5",
     "SELECT DISTINCT v FROM l1_a ORDER BY v LIMIT 5", 5),
    ("DISTINCT over a join", "SELECT DISTINCT a.v, b.k FROM pg.l1_a a JOIN pg.l1_b b ON a.k = b.k",
     "SELECT DISTINCT a.v, b.k FROM l1_a a JOIN l1_b b ON a.k = b.k", 1000),
    ("aggregate under LIMIT", "SELECT v, COUNT(*) AS n FROM pg.l1_a GROUP BY v LIMIT 4",
     "SELECT v, COUNT(*) FROM l1_a GROUP BY v", 4),
]


def main():
    wipe()
    # l1_a: 2M rows, no index (the read is a scan in parts). l1_b: 995 keys that
    # match nothing in l1_a, then the 5 that do, in that physical order.
    pg("CREATE TABLE l1_a AS SELECT g AS k, g % 97 AS v FROM generate_series(1, 2000000) g;"
       "CREATE TABLE l1_b (k bigint);"
       "INSERT INTO l1_b SELECT 5000000 + g FROM generate_series(1, 995) g;"
       "INSERT INTO l1_b VALUES (1999001), (1999002), (1999003), (1999004), (1999005);"
       "ANALYZE l1_a; ANALYZE l1_b")
    ok = True
    for name, q, truth_q, n in CASES:
        truth = Counter(pg(truth_q))
        want = min(n, sum(truth.values()))
        t = time.time()
        try:
            df = apitap.sql(q, sources=SRC).to_polars()
            rows = ["|".join(norm(x) for x in r) for r in df.rows()]
            got = Counter(rows)
            extra = got - truth
            good = len(rows) == want and not extra
            why = f"{len(rows)} rows (want {want})" + (f", not in the answer: {list(extra)[:3]}" if extra else "")
        except Exception as e:  # noqa: BLE001
            good, why = False, f"error: {str(e)[:200]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why} in {time.time() - t:.1f}s :: {q}", flush=True)
    wipe()
    print("SQL LIMIT E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
