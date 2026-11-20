"""MIN/MAX of a date or a timestamp answers, as the server's own (round P p11).

Through p10 (and on 0.67.2) `MIN(order_date)` or `MAX(updated_at)` was
refused as "arithmetic on a Date32 column": an ordinary query refused. A date
or a timestamp is a fixed-width integer (days since the epoch, microseconds),
whose order is the value's, so p11 folds it as that integer and answers it in
its own kind.

Each case against the server, values formatted the same on both sides:
Postgres date, timestamp, timestamptz and MySQL DATE, DATETIME(6), TIMESTAMP(6),
grouped and global, NULLs skipped and an all-NULL group NULL, a value before
the epoch, ORDER BY the MAX … LIMIT, a join probe across the two servers, and
a date in arithmetic refused by name.

Usage: e2e_sql_minmax_dates.py
"""
import datetime as dt
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}
ROWS = [
    (1, 1, "2024-01-05", "2024-01-05 10:00:00", "2024-01-05 10:00:00"),
    (2, 1, None, None, None),
    (3, 1, "2023-12-31", "2024-03-01 00:00:00.5", "2023-06-30 23:59:59.25"),
    (4, 2, "1969-12-31", "1969-12-31 23:59:59.5", "1970-01-01 00:00:01"),
    (5, 2, "2100-02-28", "2038-01-19 03:14:08", "2037-12-31 12:00:00"),
    (6, 3, None, None, None),
]


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
    return ["|".join("" if c == "NULL" else c for c in l.split("\t")) for l in o.stdout.splitlines() if l]


def lit(v):
    return "NULL" if v is None else f"'{v}'"


def fmt(x):
    if x is None:
        return ""
    if isinstance(x, dt.datetime):
        if x.tzinfo is not None:
            x = x.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return x.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(x, dt.date):
        return x.strftime("%Y-%m-%d")
    return str(x)


def got(q):
    return ["|".join(fmt(x) for x in r) for r in apitap.sql(q, sources=SRC).to_polars().rows()]


def main():
    vals = ", ".join(f"({i}, {g}, {lit(d)}, {lit(a)}, {lit(b)})" for i, g, d, a, b in ROWS)
    pg("DROP TABLE IF EXISTS mmd_t, mmd_k; SET TIME ZONE 'UTC'; "
       f"CREATE TABLE mmd_t (id int PRIMARY KEY, g int, d date, ts timestamp, tz timestamptz); INSERT INTO mmd_t VALUES {vals}; "
       "CREATE TABLE mmd_k (id int PRIMARY KEY, grp int); INSERT INTO mmd_k VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30), (6, 30)")
    my("DROP TABLE IF EXISTS mmd_t; SET time_zone = '+00:00'; "
       f"CREATE TABLE mmd_t (id int PRIMARY KEY, g int, d date, ts datetime(6), tz timestamp(6) NULL); INSERT INTO mmd_t VALUES {vals}")
    P = {"d": "to_char({}, 'YYYY-MM-DD')", "ts": "to_char({}, 'YYYY-MM-DD HH24:MI:SS.US')",
         "tz": "to_char({} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')"}
    M = {"d": "DATE_FORMAT({}, '%Y-%m-%d')", "ts": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:%s.%f')",
         "tz": "DATE_FORMAT({}, '%Y-%m-%d %H:%i:%s.%f')"}
    cases = []
    for src, srv, F in (("pg", pg, P), ("my", my, M)):
        cols = ["d", "ts", "tz"]
        sel = ", ".join(f"MIN({c}) AS lo_{c}, MAX({c}) AS hi_{c}" for c in cols)
        tsel = ", ".join(f"{F[c].format(f'MIN({c})')}, {F[c].format(f'MAX({c})')}" for c in cols)
        cases.append((f"{src} grouped", f"SELECT g, {sel} FROM {src}.mmd_t GROUP BY g ORDER BY g", srv,
                      f"SELECT g, {tsel} FROM mmd_t GROUP BY g ORDER BY g"))
        cases.append((f"{src} global", f"SELECT {sel} FROM {src}.mmd_t", srv, f"SELECT {tsel} FROM mmd_t"))
        cases.append((f"{src} ORDER BY MAX(d) DESC LIMIT 2", f"SELECT g, MAX(d) AS m FROM {src}.mmd_t GROUP BY g ORDER BY m DESC LIMIT 2",
                      srv, f"SELECT g, {F['d'].format('MAX(d)')} FROM mmd_t GROUP BY g ORDER BY MAX(d) DESC LIMIT 2"))
    # A join probe: the dates on MySQL, the groups on Postgres; truth from
    # MySQL with the same keys inlined.
    keys = "SELECT 1 AS id, 10 AS grp UNION ALL SELECT 2, 10 UNION ALL SELECT 3, 20 UNION ALL SELECT 4, 20 UNION ALL SELECT 5, 30 UNION ALL SELECT 6, 30"
    cases.append(("pg x my join, MIN/MAX of MySQL dates by a Postgres group",
                  "SELECT k.grp, MIN(t.d) AS lo, MAX(t.ts) AS hi FROM pg.mmd_k k JOIN my.mmd_t t ON t.id = k.id GROUP BY k.grp ORDER BY k.grp",
                  my, f"SELECT k.grp, {M['d'].format('MIN(t.d)')}, {M['ts'].format('MAX(t.ts)')} FROM ({keys}) k JOIN mmd_t t ON t.id = k.id "
                      "GROUP BY k.grp ORDER BY k.grp"))
    ok = True
    for name, q, srv, truth in cases:
        try:
            want, have = srv(truth), got(q)
            good, why = have == want, f"{have} (server {want})"
        except Exception as e:  # noqa: BLE001
            good, why = False, f"error: {str(e)[:220]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why}", flush=True)
    try:
        r = got("SELECT g, MAX(d) + 1 AS x FROM pg.mmd_t GROUP BY g")
        good, why = False, f"answered {r} (a date in arithmetic must be refused)"
    except Exception as e:  # noqa: BLE001
        good, why = "is not a number" in str(e), f"refused: {str(e)[:200]}"
    ok &= good
    print(f"  {'ok ' if good else 'BAD'} a date in arithmetic refused: {why}", flush=True)
    pg("DROP TABLE IF EXISTS mmd_t, mmd_k")
    my("DROP TABLE IF EXISTS mmd_t")
    print("SQL MINMAX DATES E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
