"""NOT IN THE RELEASE GATE (0.68.0). Next round: RED for the snapshot-holder all-or-none fix;
known to stall 1-4 of 10 rounds at role limit 2 (QA, 2026-11-20: p20 3/10, p22 4/10, p23b 1/10 rounds stalled,
both connections idle in transaction 294-340 s: two statements' snapshot holders, each waiting for a read connection).

Ten staged apitap.sql() statements at once on a role with CONNECTION LIMIT 2, then 3.

QA, round P p23b: one process, ten statements at once (threads), mixing two
CTE shapes refused for their type class and three answerable shapes (a GROUP
BY over a CTE, a two-stage pg x my CTE join, a TPC-H Q14-like pg x my join).
p22 answered all within 48 s at limit 2 (one statement refused by name). p23b
kept each statement's dry-pass planning connections for its run, so
statements held connections while they waited for more: 342 s (ACQUIRE_WAIT
300 + one connect wait + the read), three answerable statements refused,
a class refusal back only at 342 s. p23c keeps nothing across plans.

Asserted per role limit (2 and 3), two rounds each:
  - every refusal shape refused by name for its class, never answered;
  - every answerable shape exact, or refused by name for want of a
    connection ("no connection"), never wrong, never sqlx's bare
    "pool timed out";
  - at most one answerable statement refused per round;
  - the slowest statement within 120 s (p22: 48 s at limit 2).

Its own role (e5ps_l*) and tables (e5ps_*); MySQL as root. The statements run
in one process in a capped container (2 CPU, 2 GiB).

Usage: PKG=dir-holding-apitap e2e_sql_pool_staged.py
"""
import json
import os
import subprocess
import sys
from decimal import Decimal

ROLE = "e5ps_l"
BOUND_S = 120
ROUNDS = 2
PLAN = ["R1", "A3", "R2", "A4", "A5", "R1", "A3", "R2", "A4", "A5"]
Q = {
    "R1": "WITH a AS (SELECT e, id FROM pg.e5ps_ty) SELECT e, id FROM a ORDER BY e, id",
    "R2": "WITH a AS (SELECT iv, id FROM pg.e5ps_ty) SELECT count(*) AS k FROM a WHERE iv = '1 day'",
    "A3": "WITH a AS (SELECT e, id FROM pg.e5ps_ty) SELECT e, count(*) AS k FROM a GROUP BY e",
    "A4": "WITH a AS (SELECT id, k FROM pg.e5ps_pr WHERE id < 40) SELECT a.id, b.x FROM a JOIN my.e5ps_my b ON a.id = b.id",
    "A5": "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) "
          "/ SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM pg.e5ps_li, my.e5ps_part "
          "WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'",
}
# The same answers, from one server (the MySQL tables mirrored in Postgres).
TRUTH = {
    "A3": "SELECT e::text, count(*) FROM e5ps_ty GROUP BY e",
    "A4": "SELECT a.id, b.x FROM e5ps_pr a JOIN e5ps_my_mirror b ON a.id = b.id WHERE a.id < 40",
    "A5": "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) "
          "/ SUM(l_extendedprice * (1 - l_discount)) FROM e5ps_li, e5ps_part_mirror "
          "WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'",
}


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-Atc", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout


def norm(rows):
    """Rows as sorted tuples of strings; numbers compared to 9 significant digits."""
    out = []
    for r in rows:
        t = []
        for v in r:
            try:
                d = Decimal(str(v))
                t.append(f"{d:.9g}" if d == d else str(v))
            except Exception:
                t.append(str(v))
        out.append(tuple(t))
    return sorted(out)


RUNNER = r"""
import apitap, json, sys, threading, time
S = {"pg": sys.argv[1], "my": sys.argv[2]}
Q, PLAN = json.loads(sys.argv[3]), json.loads(sys.argv[4])
out = [None] * len(PLAN)
def one(i, k):
    t = time.time()
    try:
        out[i] = {"k": k, "rows": [list(map(str, r)) for r in apitap.sql(Q[k], sources=S).to_polars().rows()], "wall": time.time() - t}
    except Exception as e:
        out[i] = {"k": k, "err": " ".join(str(e).split())[:400], "wall": time.time() - t}
ts = [threading.Thread(target=one, args=(i, k)) for i, k in enumerate(PLAN)]
[t.start() for t in ts]
[t.join() for t in ts]
print("RESULTS " + json.dumps(out))
"""


def _pkg_dir():
    import importlib.util
    return os.environ.get("PKG") or os.path.dirname(os.path.dirname(importlib.util.find_spec("apitap").origin))


def setup():
    pg("DROP TABLE IF EXISTS e5ps_ty, e5ps_pr, e5ps_li, e5ps_my_mirror, e5ps_part_mirror; DROP TYPE IF EXISTS e5ps_enum;"
       "CREATE TYPE e5ps_enum AS ENUM ('zeta', 'alpha', 'mid');"
       "CREATE TABLE e5ps_ty AS SELECT g AS id, (ARRAY['zeta','alpha','mid'])[1 + g % 3]::e5ps_enum AS e, "
       "  (g % 4) * interval '12 hours' AS iv FROM generate_series(1, 60) g;"
       "CREATE TABLE e5ps_pr AS SELECT g AS id, g % 7 AS k FROM generate_series(1, 200) g;"
       "CREATE TABLE e5ps_li AS SELECT g AS l_orderkey, 1 + (g * 7919) % 20000 AS l_partkey, "
       "  (1000 + (g * 31) % 90000)::numeric(15,2) / 100 * 100 AS l_extendedprice, ((g % 11)::numeric(15,2) / 100) AS l_discount, "
       "  DATE '1995-01-01' + (g % 365) AS l_shipdate FROM generate_series(1, 300000) g;"
       "CREATE TABLE e5ps_my_mirror AS SELECT g AS id, g * 3 AS x FROM generate_series(1, 100) g;"
       "CREATE TABLE e5ps_part_mirror AS SELECT g AS p_partkey, CASE WHEN g % 5 = 0 THEN 'PROMO BRUSHED' ELSE 'STANDARD POLISHED' END "
       "  || ' ' || (g % 7)::text AS p_type FROM generate_series(1, 20000) g;"
       "ANALYZE e5ps_ty; ANALYZE e5ps_pr; ANALYZE e5ps_li;")
    my("DROP TABLE IF EXISTS e5ps_my, e5ps_part;"
       "CREATE TABLE e5ps_my (id INT PRIMARY KEY, x INT);"
       "CREATE TABLE e5ps_part (p_partkey INT PRIMARY KEY, p_type VARCHAR(40));"
       "INSERT INTO e5ps_my SELECT n, n * 3 FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 100) SELECT n FROM s) x;"
       "SET SESSION cte_max_recursion_depth = 30000;"
       "INSERT INTO e5ps_part SELECT n, CONCAT(IF(n % 5 = 0, 'PROMO BRUSHED', 'STANDARD POLISHED'), ' ', n % 7) "
       "  FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 20000) SELECT n FROM s) x;")


def teardown():
    for n in (2, 3):
        pg(f"DROP ROLE IF EXISTS {ROLE}{n}")
    pg("DROP TABLE IF EXISTS e5ps_ty, e5ps_pr, e5ps_li, e5ps_my_mirror, e5ps_part_mirror; DROP TYPE IF EXISTS e5ps_enum")
    my("DROP TABLE IF EXISTS e5ps_my, e5ps_part")


def main():
    pkg = _pkg_dir()
    teardown()
    setup()
    truth = {k: norm([l.split("|") for l in pg(q).splitlines()]) for k, q in TRUTH.items()}
    ok = True
    try:
        for n in (2, 3):
            role = f"{ROLE}{n}"
            pg(f"DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD 'bench' CONNECTION LIMIT {n};"
               f"GRANT SELECT ON e5ps_ty, e5ps_pr, e5ps_li TO {role}")
            pg_url = f"postgres://{role}:bench@127.0.0.1:5544/apitap_bench_src"
            my_url = "mysql://root:bench@127.0.0.1:3307/bench"
            for rnd in range(1, ROUNDS + 1):
                name = f"e5ps-{n}-{rnd}"
                subprocess.run(["docker", "rm", "-f", name], capture_output=True)
                r = subprocess.run(["docker", "run", "--rm", "--name", name, "--network", "host", "--cpus=2", "--memory=2g",
                                    "--memory-swap=2g", "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp",
                                    "-e", "PYTHONPATH=/w/pkg:/qa/py312", "-v", f"{os.path.expanduser('~/qa')}:/qa:ro",
                                    "-v", f"{pkg}/apitap:/w/pkg/apitap:ro", "python:3.12-slim", "python", "-c", RUNNER,
                                    pg_url, my_url, json.dumps(Q), json.dumps(PLAN)],
                                   capture_output=True, text=True, timeout=1500)
                line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULTS ")), None)
                if line is None:
                    print(f"   XX limit {n} round {rnd}: no results (exit {r.returncode}) {r.stderr[-400:]}", flush=True)
                    ok = False
                    continue
                res = json.loads(line[8:])
                bad, refused_answerable = [], 0
                for x in res:
                    k, e = x["k"], x.get("err", "")
                    if k.startswith("R"):
                        if "err" not in x:
                            bad.append(f"{k} answered a refused shape")
                        elif " is refused: " not in e:
                            bad.append(f"{k} refused, but not for its class: {e[:160]}")
                    elif "err" in x:
                        if "pool timed out" in e or "no connection" not in e:
                            bad.append(f"{k} failed, not by name: {e[:200]}")
                        else:
                            refused_answerable += 1
                    elif norm(x["rows"]) != truth[k]:
                        bad.append(f"{k} WRONG: {norm(x['rows'])[:3]} vs {truth[k][:3]}")
                slow = max(x["wall"] for x in res)
                good = not bad and refused_answerable <= 1 and slow <= BOUND_S
                ok &= good
                walls = " ".join(f"{x['k']}={x['wall']:.0f}" for x in res)
                print(f"   {'OK' if good else 'XX'} limit {n} round {rnd}: slowest {slow:.1f}s (bound {BOUND_S}s), "
                      f"answerable refused {refused_answerable} (at most 1), problems {bad[:3]} | {walls}", flush=True)
                for x in res:
                    if "err" in x:
                        print(f"        {x['k']} {x['wall']:.1f}s: {x['err'][:200]}", flush=True)
    finally:
        teardown()
    print("SQL POOL STAGED E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
