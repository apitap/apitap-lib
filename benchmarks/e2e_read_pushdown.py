"""read().lazy() filter pushdown never changes which rows come back.

The lazy reader pushes a conservative subset of a polars predicate into the
server's WHERE; polars filters again afterwards, so a pushed predicate may
only ever return MORE rows, never fewer. Each case here was a way it
returned fewer (MASTER review M-46): arithmetic that wraps on an Int16 in
polars but widens on the server, a Float32 literal, a Boolean cast to a
number, 3.7 cast to an int (3 in polars, 3.7 on the server), predicates on
columns read as text (MySQL json, Postgres jsonb/time), and a predicate the
server rejects, which failed the read instead of falling back.

Proof, on Postgres and MySQL: a 1000-row table; for every predicate, the
lazy frame equals the eager read filtered in polars, row for row.
Usage: e2e_read_pushdown.py pg|my
"""
import subprocess
import sys

import apitap
import polars as pl

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "read_pushdown"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def cases():
    c = pl.col
    return {
        "i16 + 1 < 0 (wraps in polars)": (c("s") + 1) < 0,
        "i32 * 2 < 0 (wraps in polars)": (c("i") * 2) < 0,
        "f32 literal": c("d") > pl.lit(0.1, dtype=pl.Float32),
        "bool cast to int": c("n") > pl.lit(True).cast(pl.Int64),
        "3.7 cast to int": c("n") >= pl.lit(3.7).cast(pl.Int64),
        "json read as text": c("j") == '{"k": 1}',
        "plain int (still pushed)": c("n") > 500,
        "plain text equality (still pushed)": c("t") == "row 7",
    }


def main():
    which = sys.argv[1:] == ["pg"] and "pg" or (sys.argv[1:] == ["my"] and "my")
    assert which, "usage: e2e_read_pushdown.py pg|my"
    if which == "pg":
        src = PG
        pg(f"DROP TABLE IF EXISTS {T}")
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, s int2, i int4, n int8, d float8, t text, j jsonb)")
        pg(f"INSERT INTO {T} SELECT g, CASE WHEN g % 10 = 0 THEN 32767 ELSE g END::int2, "
           f"CASE WHEN g % 10 = 1 THEN 2147483647 ELSE g END, g, g / 7.0, 'row ' || g, "
           f"CASE WHEN g % 3 = 0 THEN '{{\"k\": 1}}'::jsonb ELSE '{{\"k\":2}}'::jsonb END "
           f"FROM generate_series(1, 1000) g")
    else:
        src = MY
        my(f"DROP TABLE IF EXISTS {T}")
        my(f"CREATE TABLE {T} (id int PRIMARY KEY, s smallint, i int, n bigint, d double, t varchar(20), j json)")
        my(f"INSERT INTO {T} WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < 1000) "
           f"SELECT x, IF(x % 10 = 0, 32767, x), IF(x % 10 = 1, 2147483647, x), x, x / 7.0, CONCAT('row ', x), "
           f"IF(x % 3 = 0, '{{\"k\": 1}}', '{{\"k\":2}}') FROM g")
    full = apitap.read(src, table=T).to_polars()
    bad = []
    for name, pred in cases().items():
        want = full.filter(pred).sort("id")
        try:
            got = apitap.read(src, table=T).lazy().filter(pred).collect().sort("id")
        except Exception as e:  # noqa: BLE001
            bad.append(f"{name}: lazy read failed: {str(e)[:120]}")
            continue
        if got.height != want.height or not got.equals(want):
            bad.append(f"{name}: lazy {got.height} rows, eager {want.height}")
        else:
            print(f"  {which} {name}: {got.height} rows, equal")
    if which == "pg":
        pg(f"DROP TABLE IF EXISTS {T}")
    else:
        my(f"DROP TABLE IF EXISTS {T}")
    assert not bad, "pushdown changed the answer:\n  " + "\n  ".join(bad)
    print(f"✓ read().lazy() pushdown on {which} returns exactly the rows polars keeps")


if __name__ == "__main__":
    main()
