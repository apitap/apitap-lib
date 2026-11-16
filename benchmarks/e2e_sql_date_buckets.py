"""GROUP BY / SELECT / ORDER BY on EXTRACT(field) and date_trunc('unit', col): answered as the server does.

Through p8 only EXTRACT(YEAR …) was a key; any other field, and date_trunc,
was refused (L13). Round P p9 computes them as virtual columns of the table
(each column's own source's definitions: on Postgres DOW Sunday 0, ISODOW
Monday 1, ISO WEEK, DOY; on MySQL WEEK = WEEK(d, 0), and DOW/ISODOW/DOY,
which MySQL's EXTRACT lacks, refused by name; QUARTER, HOUR, MINUTE; date_trunc
year … minute, a date's trunc a date, MySQL's week trunc refused).
A timestamp WITH time zone is answered only where the server's default
zone is UTC, else refused by name with the remedy.

Each case is compared with the server's own answer (MySQL with its own
EXTRACT, and DATE_FORMAT for date_trunc; mixed joins against the source of the
bucketed column):
month/quarter/dow/isodow/week/doy/hour buckets, NULLs, a join, ORDER BY the
bucket, LIMIT, date_trunc of a date and a timestamp, and the refusal under a
non-UTC default zone (a role with timezone Europe/Vilnius).

Usage: e2e_sql_date_buckets.py
"""
import datetime as dt
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PG_TZ = "postgres://db_tz:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY, "pgz": PG_TZ}


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


def rows():
    base = dt.datetime(2019, 12, 20, 0, 0, 0)
    out = []
    for i in range(1, 501):
        t = base + dt.timedelta(days=(i * 7) % 430, hours=(i * 5) % 24, minutes=(i * 13) % 60)
        d = None if i % 37 == 0 else t.date()
        out.append((i, d, t, i % 11, i % 4 + 1))
    return out


def lit(v):
    return "NULL" if v is None else f"'{v}'"


def setup():
    vals = ", ".join(f"({i}, {lit(d)}, '{t}', '{t}', {v}, {k})" for i, d, t, v, k in rows())
    pg("DROP TABLE IF EXISTS db_t, db_l; DROP ROLE IF EXISTS db_tz; "
       "CREATE TABLE db_t (id int PRIMARY KEY, d date, ts timestamp, tz timestamptz, v int, k int); "
       f"INSERT INTO db_t VALUES {vals}; "
       "CREATE TABLE db_l (k int PRIMARY KEY, w int); INSERT INTO db_l VALUES (1, 10), (2, 20), (3, 30), (4, 40); "
       "CREATE ROLE db_tz LOGIN PASSWORD 'bench'; ALTER ROLE db_tz SET timezone = 'Europe/Vilnius'; "
       "GRANT SELECT ON db_t, db_l TO db_tz")
    my("DROP TABLE IF EXISTS db_t, db_l; "
       "CREATE TABLE db_t (id int PRIMARY KEY, d date, ts datetime, tz timestamp NULL, v int, k int); "
       f"INSERT INTO db_t VALUES {vals}; "
       "CREATE TABLE db_l (k int PRIMARY KEY, w int); INSERT INTO db_l VALUES (1, 10), (2, 20), (3, 30), (4, 40)")


def norm(rs):
    """Dates and timestamps as the server prints them, cut to the second (PG
    prints date_trunc of a date as a timestamptz)."""
    out = []
    for r in rs:
        cs = []
        for c in r.split("|"):
            if len(c) >= 10 and c[4] == "-" and c[7] == "-":
                c = c[:19].replace("T", " ")
                if c.endswith(" 00:00:00") or len(c) == 10:
                    c = c[:10]
            cs.append(c)
        out.append("|".join(cs))
    return out


def got_rows(q):
    return ["|".join("" if x is None else str(x) for x in r) for r in apitap.sql(q, sources=SRC).to_polars().rows()]


def main():
    setup()
    cases = [
        ("pg month", "SELECT EXTRACT(MONTH FROM d) AS m, COUNT(*) AS n FROM pg.db_t GROUP BY m ORDER BY m",
         pg, "SELECT EXTRACT(MONTH FROM d)::int AS m, COUNT(*) FROM db_t GROUP BY m ORDER BY m NULLS LAST"),
        ("pg quarter x dow", "SELECT EXTRACT(QUARTER FROM d) AS q, EXTRACT(DOW FROM d) AS w, SUM(v) AS s FROM pg.db_t "
         "GROUP BY EXTRACT(QUARTER FROM d), EXTRACT(DOW FROM d) ORDER BY q, w",
         pg, "SELECT EXTRACT(QUARTER FROM d)::int, EXTRACT(DOW FROM d)::int, SUM(v) FROM db_t GROUP BY 1, 2 ORDER BY 1, 2"),
        ("pg isodow, week, doy LIMIT", "SELECT EXTRACT(ISODOW FROM d) AS i, EXTRACT(WEEK FROM d) AS wk, EXTRACT(DOY FROM d) AS y, "
         "COUNT(*) AS n FROM pg.db_t GROUP BY i, wk, y ORDER BY wk DESC, y, i LIMIT 7",
         pg, "SELECT EXTRACT(ISODOW FROM d)::int, EXTRACT(WEEK FROM d)::int, EXTRACT(DOY FROM d)::int, COUNT(*) FROM db_t "
         "GROUP BY 1, 2, 3 ORDER BY 2 DESC, 3, 1 LIMIT 7"),
        ("pg hour of timestamp", "SELECT EXTRACT(HOUR FROM ts) AS h, SUM(v) AS s FROM pg.db_t GROUP BY h ORDER BY h",
         pg, "SELECT EXTRACT(HOUR FROM ts)::int, SUM(v) FROM db_t GROUP BY 1 ORDER BY 1"),
        ("pg hour of timestamptz (UTC default)", "SELECT EXTRACT(HOUR FROM tz) AS h, COUNT(*) AS n FROM pg.db_t GROUP BY h ORDER BY h",
         pg, "SELECT EXTRACT(HOUR FROM tz)::int, COUNT(*) FROM db_t GROUP BY 1 ORDER BY 1"),
        ("pg date_trunc month of a date DESC LIMIT", "SELECT date_trunc('month', d) AS mo, COUNT(*) AS n FROM pg.db_t "
         "GROUP BY mo ORDER BY mo DESC LIMIT 4",
         pg, "SELECT date_trunc('month', d)::date, COUNT(*) FROM db_t GROUP BY 1 ORDER BY 1 DESC NULLS FIRST LIMIT 4"),
        ("pg date_trunc week, hour of a timestamp", "SELECT date_trunc('week', ts) AS w, COUNT(*) AS n FROM pg.db_t "
         "GROUP BY w ORDER BY w LIMIT 5",
         pg, "SELECT date_trunc('week', ts), COUNT(*) FROM db_t GROUP BY 1 ORDER BY 1 LIMIT 5"),
        ("pg join, bucket on the build", "SELECT EXTRACT(MONTH FROM t.d) AS m, SUM(l.w) AS s FROM pg.db_t t JOIN pg.db_l l "
         "ON l.k = t.k GROUP BY m ORDER BY m",
         pg, "SELECT EXTRACT(MONTH FROM t.d)::int, SUM(l.w) FROM db_t t JOIN db_l l ON l.k = t.k GROUP BY 1 ORDER BY 1 NULLS LAST"),
        ("my month", "SELECT EXTRACT(MONTH FROM d) AS m, COUNT(*) AS n FROM my.db_t GROUP BY m ORDER BY m",
         my, "SELECT EXTRACT(MONTH FROM d), COUNT(*) FROM db_t GROUP BY 1 ORDER BY 1"),
        # A MySQL column's fields are MySQL's: WEEK = WEEK(d, 0), compared
        # with MySQL's own EXTRACT.
        ("my week (MySQL's), day, LIMIT", "SELECT EXTRACT(WEEK FROM d) AS wk, EXTRACT(DAY FROM d) AS dd, COUNT(*) AS n "
         "FROM my.db_t GROUP BY wk, dd ORDER BY wk DESC, dd LIMIT 9",
         my, "SELECT EXTRACT(WEEK FROM d), EXTRACT(DAY FROM d), COUNT(*) FROM db_t GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 9"),
        ("my hour of datetime + quarter", "SELECT EXTRACT(QUARTER FROM ts) AS q, EXTRACT(HOUR FROM ts) AS h, SUM(v) AS s "
         "FROM my.db_t GROUP BY q, h ORDER BY q, h",
         my, "SELECT EXTRACT(QUARTER FROM ts), EXTRACT(HOUR FROM ts), SUM(v) FROM db_t GROUP BY 1, 2 ORDER BY 1, 2"),
        # Mixed joins: the key follows its own column's source.
        ("my x pg join, MySQL week on the MySQL column", "SELECT EXTRACT(WEEK FROM t.d) AS wk, SUM(l.w) AS s FROM my.db_t t "
         "JOIN pg.db_l l ON l.k = t.k GROUP BY wk ORDER BY wk",
         my, "SELECT EXTRACT(WEEK FROM t.d), SUM(l.w) FROM db_t t JOIN db_l l ON l.k = t.k GROUP BY 1 ORDER BY 1"),
        ("pg x my join, ISO week on the Postgres column", "SELECT EXTRACT(WEEK FROM t.d) AS wk, SUM(l.w) AS s FROM pg.db_t t "
         "JOIN my.db_l l ON l.k = t.k GROUP BY wk ORDER BY wk",
         pg, "SELECT EXTRACT(WEEK FROM t.d)::int, SUM(l.w) FROM db_t t JOIN db_l l ON l.k = t.k GROUP BY 1 ORDER BY 1 NULLS LAST"),
        ("my date_trunc month, a join", "SELECT date_trunc('month', t.d) AS mo, SUM(l.w) AS s FROM my.db_t t JOIN my.db_l l "
         "ON l.k = t.k GROUP BY mo ORDER BY mo",
         my, "SELECT DATE_FORMAT(t.d, '%Y-%m-01'), SUM(l.w) FROM db_t t JOIN db_l l ON l.k = t.k GROUP BY 1 ORDER BY 1"),
        ("my hour of TIMESTAMP", "SELECT EXTRACT(HOUR FROM tz) AS h, COUNT(*) AS n FROM my.db_t GROUP BY h ORDER BY h",
         my, "SELECT EXTRACT(HOUR FROM tz), COUNT(*) FROM db_t GROUP BY 1 ORDER BY 1"),
    ]
    ok = True
    for name, q, srv, truth in cases:
        try:
            want = norm(srv(truth))
            got = norm(got_rows(q))
            good, why = got == want, f"{got[:6]}{'…' if len(got) > 6 else ''} ({len(got)} rows; server {len(want)}{'' if got == want else ': ' + str(want[:6])})"
        except Exception as e:  # noqa: BLE001
            good, why = False, f"error: {str(e)[:220]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why}", flush=True)
    for name, q, want in [
        ("pg timestamptz, default zone Europe/Vilnius: refused",
         "SELECT EXTRACT(HOUR FROM tz) AS h, COUNT(*) AS n FROM pgz.db_t GROUP BY h", "default time zone is Europe/Vilnius"),
        ("EXTRACT(HOUR) of a date: refused", "SELECT EXTRACT(HOUR FROM d) AS h, COUNT(*) AS n FROM pg.db_t GROUP BY h",
         "has no time of day"),
        ("EXTRACT(DOW) of a MySQL column: refused (no such MySQL field)",
         "SELECT EXTRACT(DOW FROM d) AS w, COUNT(*) AS n FROM my.db_t GROUP BY w", "MySQL's EXTRACT has no Dow"),
        ("date_trunc('week') of a MySQL column: refused", "SELECT date_trunc('week', d) AS w, COUNT(*) AS n FROM my.db_t GROUP BY w",
         "MySQL has no date_trunc"),
    ]:
        try:
            r = got_rows(q)
            good, why = False, f"answered {r[:4]} (must be refused)"
        except Exception as e:  # noqa: BLE001
            good, why = want in str(e), f"refused: {str(e)[:220]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why}", flush=True)
    # L13b/L13c stay refused this round; docs/sql.md names the workaround the
    # probes below show answers (informational, not part of the pass).
    for name, q, truth in [
        ("INFO GROUP BY arithmetic directly", "SELECT id % 3 AS r, SUM(v) AS s FROM pg.db_t GROUP BY id % 3 ORDER BY r",
         "SELECT id % 3, SUM(v) FROM db_t GROUP BY 1 ORDER BY 1"),
        ("INFO arithmetic key via a CTE", "WITH x AS (SELECT id % 3 AS r, v FROM pg.db_t) SELECT r, SUM(v) AS s FROM x GROUP BY r ORDER BY r",
         "SELECT id % 3, SUM(v) FROM db_t GROUP BY 1 ORDER BY 1"),
        ("INFO CASE key via a CTE", "WITH x AS (SELECT CASE WHEN v > 5 THEN 'hi' ELSE 'lo' END AS b, v FROM pg.db_t) "
         "SELECT b, COUNT(*) AS n FROM x GROUP BY b ORDER BY b",
         "SELECT CASE WHEN v > 5 THEN 'hi' ELSE 'lo' END, COUNT(*) FROM db_t GROUP BY 1 ORDER BY 1"),
    ]:
        try:
            got, want = got_rows(q), pg(truth)
            print(f"  {name}: answered, {'equal to' if got == want else 'DIFFERENT from'} the server's ({got[:4]})", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"  {name}: refused: {str(e)[:200]}", flush=True)
    pg("DROP TABLE IF EXISTS db_t, db_l; DROP ROLE IF EXISTS db_tz")
    my("DROP TABLE IF EXISTS db_t, db_l")
    print("SQL DATE BUCKETS E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
