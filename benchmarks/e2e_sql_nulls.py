"""ORDER BY places NULLs as the column's source does, unless NULLS FIRST/LAST says.

MySQL sorts NULL lowest (first ascending, last descending), Postgres highest
(last ascending, first descending). apitap placed every column's NULLs the
Postgres way, so `ORDER BY v LIMIT n` on a MySQL column returned other rows
than MySQL (QA C11-C13, pre-existing through 0.67.2), and NULLS FIRST/LAST
was ignored. Each case is compared with the server's own ORDER BY.

Usage: e2e_sql_nulls.py
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l for l in o.stdout.splitlines() if l]


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l.replace("\t", "|").replace("NULL", "") for l in o.stdout.splitlines() if l]


def main():
    vals = "(1, 5, 'a'), (2, NULL, 'b'), (3, 2, 'c'), (4, NULL, 'd'), (5, 9, 'e'), (6, 2, 'f')"
    pg("DROP TABLE IF EXISTS nl_t; CREATE TABLE nl_t (id int PRIMARY KEY, v int, w text); "
       f"INSERT INTO nl_t VALUES {vals}")
    my(f"DROP TABLE IF EXISTS nl_t; CREATE TABLE nl_t (id int PRIMARY KEY, v int, w varchar(5)); INSERT INTO nl_t VALUES {vals}")
    cases = [
        ("my", "SELECT v, COUNT(*) AS n FROM my.nl_t GROUP BY v ORDER BY v LIMIT 2", "SELECT v, COUNT(*) FROM nl_t GROUP BY v ORDER BY v LIMIT 2"),
        ("my", "SELECT v, COUNT(*) AS n FROM my.nl_t GROUP BY v ORDER BY v DESC LIMIT 2", "SELECT v, COUNT(*) FROM nl_t GROUP BY v ORDER BY v DESC LIMIT 2"),
        ("my", "SELECT id, v FROM my.nl_t ORDER BY v, id LIMIT 3", "SELECT id, v FROM nl_t ORDER BY v, id LIMIT 3"),
        ("my", "SELECT id, v FROM my.nl_t ORDER BY v NULLS LAST, id LIMIT 3", "SELECT id, v FROM nl_t ORDER BY v IS NULL, v, id LIMIT 3"),
        ("pg", "SELECT v, COUNT(*) AS n FROM pg.nl_t GROUP BY v ORDER BY v LIMIT 2", "SELECT v, COUNT(*) FROM nl_t GROUP BY v ORDER BY v LIMIT 2"),
        ("pg", "SELECT v, COUNT(*) AS n FROM pg.nl_t GROUP BY v ORDER BY v DESC LIMIT 2", "SELECT v, COUNT(*) FROM nl_t GROUP BY v ORDER BY v DESC LIMIT 2"),
        ("pg", "SELECT id, v FROM pg.nl_t ORDER BY v NULLS FIRST, id LIMIT 3", "SELECT id, v FROM nl_t ORDER BY v NULLS FIRST, id LIMIT 3"),
    ]
    ok = True
    for side, q, truth in cases:
        want = (my if side == "my" else pg)(truth)
        try:
            got = ["|".join("" if x is None else str(x) for x in r) for r in apitap.sql(q, sources=SRC).to_polars().rows()]
            good, why = got == want, f"{got} (server {want})"
        except Exception as e:  # noqa: BLE001
            good, why = False, f"error: {str(e)[:200]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {q}: {why}", flush=True)
    pg("DROP TABLE IF EXISTS nl_t")
    my("DROP TABLE IF EXISTS nl_t")
    print("SQL NULLS E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
