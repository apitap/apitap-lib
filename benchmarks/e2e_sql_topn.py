"""A plain SELECT … ORDER BY … LIMIT n over a large table keeps n rows, not the table.

The plain SELECT is answered as GROUP BY every output with a hidden count;
through round P p3 every row of the table became a group before the ORDER BY
and the LIMIT, so `SELECT a, b FROM lineitem_10g ORDER BY a LIMIT 2` was
refused past the group budget at ~470K rows in a 256 MB cage (L8). The read
now keeps the first n rows' groups as it streams (cut back to n past 2n),
so memory is n rows. Under a text key whose collation does not order by
bytes the cut cannot be checked; such a statement keeps every row and is
refused by name past the budget (or answered exactly when it fits). A LIMIT
whose rows alone pass the budget is refused by name.

Runs in a 0.5 CPU / 256 MB cage; each answer is compared with Postgres's own
ORDER BY (the keys are unique, so the rows are determined).

Usage: PKG=dir-holding-apitap e2e_sql_topn.py
"""
import json
import os
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CASES = [
    ("3M rows, int key DESC", "SELECT l_orderkey, l_linenumber, l_quantity FROM pg.lineitem_10g WHERE l_orderkey <= 3000000 "
     "ORDER BY l_orderkey DESC, l_linenumber LIMIT 10", "exact"),
    ("whole table, decimal key", "SELECT l_orderkey, l_linenumber, l_extendedprice FROM pg.lineitem_10g "
     "ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 5", "exact"),
    ("whole table, date then ints", "SELECT l_shipdate, l_orderkey, l_linenumber FROM pg.lineitem_10g "
     "ORDER BY l_shipdate, l_orderkey, l_linenumber LIMIT 5", "exact"),
    ("text key under the database's locale", "SELECT l_comment, l_orderkey, l_linenumber FROM pg.lineitem_10g "
     "WHERE l_orderkey <= 3000000 ORDER BY l_comment, l_orderkey, l_linenumber LIMIT 5", "exact_or_named:collation"),
    ("a LIMIT past the budget", "SELECT l_orderkey, l_linenumber, l_comment FROM pg.lineitem_10g "
     "ORDER BY l_orderkey, l_linenumber LIMIT 3000000", "named:ask for fewer rows"),
]

RUNNER = r"""
import apitap, json, sys, time
q = sys.argv[2]
t = time.time()
try:
    rows = apitap.sql(q, sources={"pg": sys.argv[1]}).to_polars().rows()
    print("RESULT " + json.dumps({"rows": [[str(x) for x in r] for r in rows[:20]], "n": len(rows), "wall": time.time() - t}))
except Exception as e:
    print("RESULT " + json.dumps({"err": str(e)[:400], "wall": time.time() - t}))
"""


def psql(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l for l in o.stdout.splitlines() if l]



def _pkg_dir():
    """The dir holding the apitap package: PKG, else the interpreter's own
    (the release gate runs every leg with the wheel it gates)."""
    import importlib.util
    return os.environ.get("PKG") or os.path.dirname(os.path.dirname(importlib.util.find_spec("apitap").origin))

def main():
    pkg = _pkg_dir()
    ok = True
    for name, q, want in CASES:
        r = subprocess.run(["docker", "run", "--rm", "--name", "w1-topn", "--network", "host", "--cpus=0.5", "--memory=256m",
                            "--memory-swap=256m", "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp",
                            "-e", "PYTHONPATH=/w/pkg:/qa/py312", "-v", f"{os.path.expanduser('~/qa')}:/qa:ro",
                            "-v", f"{pkg}/apitap:/w/pkg/apitap:ro", "python:3.12-slim", "python", "-c", RUNNER, PG, q],
                           capture_output=True, text=True)
        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")), None)
        res = json.loads(line[7:]) if line else {"err": f"no result, exit {r.returncode} (OOM-kill?) {r.stderr[-200:]}", "wall": 0}
        if "err" in res:
            named = want.split(":", 1)[1] if ":" in want else None
            good = named is not None and want.startswith(("named", "exact_or_named")) and named in res["err"]
            why = f"refused in {res['wall']:.0f}s: {res['err'][:200]}"
        else:
            if want.startswith("named"):
                good, why = False, f"ANSWERED {res['n']} rows"
            else:
                truth = [l.split("|") for l in psql(q.replace("pg.", ""))[:20]]
                norm = lambda rows: [[x.rstrip("0").rstrip(".") if "." in x else x for x in r] for r in rows]  # noqa: E731
                good = norm(res["rows"]) == norm(truth)
                why = f"{res['n']} rows in {res['wall']:.0f}s, equal to Postgres: {good}"
                if not good:
                    why += f" got {res['rows'][:2]} want {truth[:2]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why}", flush=True)
    print("SQL TOP-N E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
