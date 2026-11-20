"""Ten apitap.sql() statements at once on a role with fewer connections than they ask for.

Each statement's key-range probe asks for up to eight connections per side
(two sides). On a role with CONNECTION LIMIT 4 (then 2) the statements
together ask for far more than the server gives. Through the round-P p2 wheel
a probe held the connections it got while sqlx kept retrying the ones it
could not get, so statements held each other up and failed with "pool timed
out" (or never finished). Now a probe takes all of its share or none, backs
off with a jitter and asks for half (W1 round P, L4). The bound: every
statement answers exactly or fails by name ("no connection") within
ACQUIRE_WAIT (300 s) plus one connect wait; none hangs, none answers wrong,
none fails with sqlx's raw "pool timed out". A statement refused because its
role has one connection free and it reads the source twice is named too
(class d: concurrent statements on a tight role).

The statements run in one process in a capped container (2 CPU, 1 GiB), like
an application's worker pool would.

Usage: PKG=dir-holding-apitap e2e_sql_pool_many.py
"""
import json
import os
import subprocess
import sys

USER = "l4_many"
PG_URL = f"postgres://{USER}:bench@127.0.0.1:5544/apitap_bench_src"
N = 10
BOUND_S = 300 + 60


def pg(sql, user="postgres"):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-h", "127.0.0.1", "-U", user,
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, env={**os.environ, "PGPASSWORD": "bench"})
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


RUNNER = r"""
import apitap, json, sys, threading, time
url, n = sys.argv[1], int(sys.argv[2])
q = "SELECT COUNT(*) AS n, SUM(a.v) AS s FROM pg.l4m_a a JOIN pg.l4m_b b ON a.k = b.k"
out = [None] * n
def one(i):
    t = time.time()
    try:
        r = apitap.sql(q, sources={"pg": url}).to_polars().rows()[0]
        out[i] = {"ok": f"{r[0]}|{r[1]}", "wall": time.time() - t}
    except Exception as e:
        out[i] = {"err": str(e)[:300], "wall": time.time() - t}
ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
[t.start() for t in ts]
[t.join() for t in ts]
print("RESULTS " + json.dumps(out))
"""



def _pkg_dir():
    """The dir holding the apitap package: PKG, else the interpreter's own
    (the release gate runs every leg with the wheel it gates)."""
    import importlib.util
    return os.environ.get("PKG") or os.path.dirname(os.path.dirname(importlib.util.find_spec("apitap").origin))

def main():
    pkg = _pkg_dir()
    pg(f"DROP TABLE IF EXISTS l4m_a; DROP TABLE IF EXISTS l4m_b; DROP ROLE IF EXISTS {USER}")
    pg("CREATE TABLE l4m_a AS SELECT g AS k, g % 97 AS v FROM generate_series(1, 400000) g;"
       "CREATE TABLE l4m_b AS SELECT g * 2 AS k, g % 13 AS w FROM generate_series(1, 400000) g;"
       "ANALYZE l4m_a; ANALYZE l4m_b;"
       f"CREATE ROLE {USER} LOGIN PASSWORD 'bench';"
       f"GRANT SELECT ON l4m_a, l4m_b TO {USER}")
    want = pg("SELECT count(*) || '|' || sum(a.v) FROM l4m_a a JOIN l4m_b b ON a.k = b.k")
    ok = True
    for limit in (4, 2):
        pg(f"ALTER ROLE {USER} CONNECTION LIMIT {limit}")
        name = f"l4many-{limit}"
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        r = subprocess.run(["docker", "run", "--rm", "--name", name, "--network", "host", "--cpus=2", "--memory=1g",
                            "--memory-swap=1g", "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp",
                            "-e", "PYTHONPATH=/w/pkg:/qa/py312", "-v", f"{os.path.expanduser('~/qa')}:/qa:ro",
                            "-v", f"{pkg}/apitap:/w/pkg/apitap:ro", "python:3.12-slim", "python", "-c", RUNNER, PG_URL, str(N)],
                           capture_output=True, text=True, timeout=BOUND_S + 300)
        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULTS ")), None)
        if line is None:
            print(f"  BAD limit {limit}: no results (exit {r.returncode}) {r.stderr[-400:]}")
            ok = False
            continue
        res = json.loads(line[8:])
        answered = sum(1 for x in res if x.get("ok") == want)
        wrong = [x for x in res if "ok" in x and x["ok"] != want]
        # Refused by name: no connection within the wait, or the statement's
        # own named refusal of a role with one connection free that it reads
        # more than once (class d, round P). Never sqlx's bare "pool timed out".
        NAMED = ("no connection", "one connection free")
        is_named = lambda e: any(k in e for k in NAMED) and "pool timed out" not in e  # noqa: E731
        named = sum(1 for x in res if "err" in x and is_named(x["err"]))
        other = [x["err"][:160] for x in res if "err" in x and not is_named(x["err"])]
        raw = sum(1 for x in res if "err" in x and "pool timed out" in x["err"])
        slow = max(x["wall"] for x in res)
        good = not wrong and not other and raw == 0 and answered + named == N and slow <= BOUND_S
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} role limit {limit}: {answered} answered exactly, {named} refused by name, "
              f"{len(wrong)} wrong, other errors {other[:2]}, slowest {slow:.0f}s (bound {BOUND_S}s)", flush=True)
    pg(f"DROP TABLE IF EXISTS l4m_a; DROP TABLE IF EXISTS l4m_b; DROP ROLE IF EXISTS {USER}")
    print("SQL POOL MANY E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
