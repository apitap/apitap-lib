"""One join statement on a role with one or two connections free answers exactly.

A join statement opens several connections at once: the statement's snapshot
holder, the build read's pipes, the probe side's key-range probe beside it.
On a role with CONNECTION LIMIT 1 or 2 (or a MySQL account with
MAX_USER_CONNECTIONS 1) it waited on itself and was refused after the wait
(round P p4). Now each statement asks its sources how many connections are
free and, when few are, runs serially: the key range before the build read,
no overlap, no prefetch, at most that many pipes (one connection: not pinned
to one snapshot either) (W1 round P, p5).

Runs in a capped container (2 CPU, 1 GiB, as pool_many does: the prefetch
path is on at >= 1 GiB) and compares with the server's own join.

Usage: PKG=dir-holding-apitap e2e_sql_pool_low.py
"""
import json
import os
import subprocess
import sys

PG_USER, MY_USER = "l5_low", "l5my"
PG_URL = f"postgres://{PG_USER}:bench@127.0.0.1:5544/apitap_bench_src"
MY_URL = f"mysql://{MY_USER}:bench@127.0.0.1:3307/bench"
BOUND_S = 360

RUNNER = r"""
import apitap, json, sys, time
t = time.time()
try:
    r = apitap.sql(sys.argv[2], sources={"s": sys.argv[1]}).to_polars().rows()[0]
    print("RESULT " + json.dumps({"ok": f"{r[0]}|{r[1]}", "wall": time.time() - t}))
except Exception as e:
    print("RESULT " + json.dumps({"err": str(e)[:300], "wall": time.time() - t}))
"""


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-Atc", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def run(pkg, url, q):
    r = subprocess.run(["docker", "run", "--rm", "--name", "w1-pool-low", "--network", "host", "--cpus=2", "--memory=1g",
                        "--memory-swap=1g", "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp",
                        "-e", "PYTHONPATH=/w/pkg:/qa/py312", "-v", f"{os.path.expanduser('~/qa')}:/qa:ro",
                        "-v", f"{pkg}/apitap:/w/pkg/apitap:ro", "python:3.12-slim", "python", "-c", RUNNER, url, q],
                       capture_output=True, text=True, timeout=BOUND_S + 120)
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")), None)
    return json.loads(line[7:]) if line else {"err": f"no result (exit {r.returncode}) {r.stderr[-200:]}", "wall": 0}


def main():
    pkg = os.environ["PKG"]
    pg(f"DROP TABLE IF EXISTS l5_a; DROP TABLE IF EXISTS l5_b; DROP TABLE IF EXISTS l5_f; DROP TABLE IF EXISTS l5_d1; "
       f"DROP TABLE IF EXISTS l5_d2; DROP ROLE IF EXISTS {PG_USER};"
       "CREATE TABLE l5_f AS SELECT g AS k, g % 50 AS d1, g % 20 AS d2, g % 7 AS v FROM generate_series(1, 300000) g;"
       "CREATE TABLE l5_d1 AS SELECT g AS id, g % 5 AS x FROM generate_series(0, 49) g;"
       "CREATE TABLE l5_d2 AS SELECT g AS id, g AS y FROM generate_series(0, 19) g;"
       "CREATE TABLE l5_a AS SELECT g AS k, g % 97 AS v FROM generate_series(1, 400000) g;"
       "CREATE TABLE l5_b AS SELECT g * 2 AS k, g % 13 AS w FROM generate_series(1, 400000) g;"
       f"ANALYZE l5_a; ANALYZE l5_b; CREATE ROLE {PG_USER} LOGIN PASSWORD 'bench'; GRANT SELECT ON l5_a, l5_b, l5_f, l5_d1, l5_d2 TO {PG_USER}")
    my(f"DROP TABLE IF EXISTS l5m_a; DROP TABLE IF EXISTS l5m_b; DROP USER IF EXISTS '{MY_USER}'@'%';"
       "SET SESSION cte_max_recursion_depth = 100000;"
       "CREATE TABLE l5m_a (k BIGINT PRIMARY KEY, v INT); CREATE TABLE l5m_b (k BIGINT PRIMARY KEY, w INT);"
       "INSERT INTO l5m_a WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 50000) SELECT n, n % 97 FROM g;"
       "INSERT INTO l5m_b WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 50000) SELECT n * 2, n % 13 FROM g;"
       f"CREATE USER '{MY_USER}'@'%' IDENTIFIED BY 'bench' WITH MAX_USER_CONNECTIONS 1;"
       f"GRANT SELECT ON bench.* TO '{MY_USER}'@'%'")
    qpg = "SELECT COUNT(*) AS n, SUM(a.v) AS s FROM s.l5_a a JOIN s.l5_b b ON a.k = b.k"
    qmy = "SELECT COUNT(*) AS n, SUM(a.v) AS s FROM s.l5m_a a JOIN s.l5m_b b ON a.k = b.k"
    want_pg = pg("SELECT count(*) || '|' || sum(a.v) FROM l5_a a JOIN l5_b b ON a.k = b.k")
    want_my = my("SELECT CONCAT(COUNT(*), '|', SUM(a.v)) FROM l5m_a a JOIN l5m_b b ON a.k = b.k")
    qone = "SELECT COUNT(*) AS n, SUM(v) AS s FROM s.l5_a WHERE k > 1000"
    want_one = pg("SELECT count(*) || '|' || sum(v) FROM l5_a WHERE k > 1000")
    qstar = ("SELECT COUNT(*) AS n, SUM(f.v) AS s FROM s.l5_f f JOIN s.l5_d1 d1 ON f.d1 = d1.id JOIN s.l5_d2 d2 ON f.d2 = d2.id "
             "WHERE d2.y < 5 AND d1.x = 2")
    want_star = pg("SELECT count(*) || '|' || sum(f.v) FROM l5_f f JOIN l5_d1 d1 ON f.d1 = d1.id JOIN l5_d2 d2 ON f.d2 = d2.id "
                   "WHERE d2.y < 5 AND d1.x = 2")
    ok = True
    cases = []
    # One connection free: a statement that reads the source once answers;
    # one that reads it twice is refused by name (its reads need one
    # snapshot: one connection to hold it, one to read).
    pg(f"ALTER ROLE {PG_USER} CONNECTION LIMIT 1")
    cases.append(("Postgres limit 1, one table", run(pkg, PG_URL, qone), want_one))
    cases.append(("Postgres limit 1, a join (named refusal)", run(pkg, PG_URL, qpg), "named:one connection free"))
    pg(f"ALTER ROLE {PG_USER} CONNECTION LIMIT 2")
    cases.append(("Postgres limit 2, a join", run(pkg, PG_URL, qpg), want_pg))
    cases.append(("Postgres limit 2, a star join (fact x 2 dimensions)", run(pkg, PG_URL, qstar), want_star))
    cases.append(("MySQL account MAX_USER_CONNECTIONS 1, a join", run(pkg, MY_URL, qmy), want_my))
    for name, res, want in cases:
        if want.startswith("named:"):
            good = "err" in res and want[6:] in res["err"] and res["wall"] <= BOUND_S
        else:
            good = res.get("ok") == want and res["wall"] <= BOUND_S
        ok &= good
        why = f"{res['ok']} (truth {want})" if "ok" in res else f"error: {res['err'][:220]}"
        print(f"  {'ok ' if good else 'BAD'} {name}: {why} in {res['wall']:.1f}s", flush=True)
    pg(f"DROP TABLE IF EXISTS l5_a; DROP TABLE IF EXISTS l5_b; DROP TABLE IF EXISTS l5_f; DROP TABLE IF EXISTS l5_d1; "
       f"DROP TABLE IF EXISTS l5_d2; DROP ROLE IF EXISTS {PG_USER}")
    my(f"DROP TABLE IF EXISTS l5m_a; DROP TABLE IF EXISTS l5m_b; DROP USER IF EXISTS '{MY_USER}'@'%'")
    print("SQL POOL LOW E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
