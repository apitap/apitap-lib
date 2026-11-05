"""read(where=...) filters on the server, exactly, on parallel spans, and refuses what is not one expression.

0.65.0 had no row filter on read(): every row of the table crossed the wire
and DuckDB or polars dropped most of them (a 5 GB join paid 63.6 s where the
same filter on the servers paid 32.2 s). where= is the caller's predicate in the
source's own SQL, ANDed into every span statement.

  1  Postgres and MySQL, parallel=4, an OR predicate: exactly the server's own
     rows (an OR spliced without its parentheses would widen every span and
     repeat rows)
  2  text that is not one expression ("1=1) OR (1=1", a second statement) is
     refused before any row is read, naming where=
  3  .lazy() with where=: both filters apply, and the answer equals the server's
"""
import subprocess
import sys

import apitap
import polars as pl

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "read_where"
ROWS = 100_000
OR_PRED = "id < 10 OR id > 99990"
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


def main():
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v int); "
       f"INSERT INTO {T} SELECT g, g % 7 FROM generate_series(1, {ROWS}) g")
    my(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v int); "
       f"SET SESSION cte_max_recursion_depth = {ROWS + 1}; INSERT INTO {T} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < {ROWS}) "
       "SELECT n, n % 7 FROM s")
    try:
        for name, url, srv in (("Postgres", PG, pg), ("MySQL", MY, my)):
            want = sorted(int(x) for x in srv(f"SELECT id FROM {T} WHERE {OR_PRED}").split())
            df = apitap.read(url, table=T, parallel=4, where=OR_PRED).to_polars()
            got = sorted(df["id"].to_list())
            case(f"1: {name} parallel=4 where={OR_PRED!r} returns exactly the server's rows",
                 got == want, f"{len(got)} rows, server {len(want)}")
        for bad in ("1=1) OR (1=1", "id = 1; DROP TABLE read_where"):
            try:
                apitap.read(PG, table=T, parallel=4, where=bad).to_polars()
                case(f"2: where={bad!r} is refused", False, "it read rows")
            except Exception as e:  # noqa: BLE001 — the message is the contract
                case(f"2: where={bad!r} is refused before any row, naming where=", "where=" in str(e),
                     str(e)[:140])
        case("2: the table is still there", pg(f"SELECT count(*) FROM {T}") == str(ROWS))
        want = int(pg(f"SELECT count(*) FROM {T} WHERE v = 3 AND id <= 5000"))
        got = (apitap.read(PG, table=T, where="v = 3").lazy()
               .filter(pl.col("id") <= 5000).select(pl.len()).collect().item())
        case("3: .lazy() filter on top of where= equals the server's count", got == want,
             f"got {got}, server {want}")
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")
        my(f"DROP TABLE IF EXISTS {T}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"   XX {e}")
        ok = False
    print("READ WHERE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
