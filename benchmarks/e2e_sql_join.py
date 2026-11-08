"""apitap.sql() joins a Postgres table with a MySQL table and aggregates, exactly, in Rust.

0.65.0 had no way to join two databases inside apitap: rows had to leave for
DuckDB or polars, where a 5 GB × 5 GB join spilled through a 256 MB
container. apitap.sql() runs the join + aggregate itself:

- each table's conjuncts and columns are pushed into its own server;
- the join key's range is intersected and pushed into both;
- the GROUP BY table is reduced to one group id per key;
- the other table streams into exact decimal accumulators.

  1  the TPC-H Q3-shaped join (SUM of decimal arithmetic, COUNT(*), GROUP BY a
     MySQL column, filters on both sides, ORDER BY) equals the answer computed
     here from each server's own text, digit for digit
  2  MIN, MAX, AVG and COUNT(col) over the join, with a NULL in the
     aggregated column
  3  one table with GROUP BY, no join
  4  what v1 does not run is refused before any read, naming it: LEFT JOIN, a
     WHERE naming both tables, SELECT *
  7  a GROUP BY side whose join key repeats (lineitem grouped by l_shipdate,
     counting orders): every match counts, equal to the server's own join (B5)
  5  a build past its budget (APITAP_SQL_BUILD_BYTES=2K, below the 2500-key direct array) is split by key range
     into several passes and still equals the servers' answer (B4)
  6  dest= lands the answer through the ordinary transfer, typed, into Postgres,
     ClickHouse and MySQL, each table equal to the answer (B6)
  9  the probe side's aggregate argument is computed by Postgres as one scaled
     int8 (idea 6); a cube of prices overflows int8 there, and the query runs
     again without the pushdown, digit for digit equal to the server
  10 a selective build (one order in ten) sends its keys to Postgres as a
     bitmap filter (idea 9); the server ships only those keys' lines, and the
     answer equals the servers'
  8  an inner join emits only the groups some row matched: GROUP BY-side groups
     with no matching row do not appear as COUNT 0 rows
  11 an OR across both tables, checked per matched row, compares the MySQL
     utf8mb4_0900_ai_ci column case-insensitively, as MySQL does
"""
import os
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
OUT = "sql_join_out"
TL, TO = "sql_li", "sql_od"
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


def pgd_rows():
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst",
                        "-AtF", "\t", "-c", f"SELECT o_orderpriority, revenue, n FROM {OUT} ORDER BY 1"],
                       capture_output=True, text=True)
    return [tuple(l.split("\t")) for l in o.stdout.splitlines() if l]


def ch_rows():
    o = subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password",
                        "bench", "-q", f"SELECT o_orderpriority, toString(revenue), toString(n) FROM {OUT} ORDER BY 1"],
                       capture_output=True, text=True)
    return [tuple(l.split("\t")) for l in o.stdout.splitlines() if l]


def my_rows():
    return [tuple(l.split("\t")) for l in my(f"SELECT o_orderpriority, revenue, n FROM {OUT} ORDER BY 1").splitlines()]


def drop_out():
    subprocess.run(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst", "-c",
                    f"DROP TABLE IF EXISTS {OUT}"], capture_output=True)
    subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
                    "-q", f"DROP TABLE IF EXISTS {OUT}"], capture_output=True)
    my(f"DROP TABLE IF EXISTS {OUT}")


def rows(res):
    df = res.to_polars()
    return [tuple(r) for r in df.iter_rows()]


def main():
    pg(f"DROP TABLE IF EXISTS {TL}; CREATE TABLE {TL} (l_orderkey bigint, l_extendedprice numeric(15,2), "
       f"l_discount numeric(15,2), l_shipdate date); "
       f"INSERT INTO {TL} SELECT (g % 3000) + 1, (g * 1.37)::numeric(15,2), ((g % 11) / 100.0)::numeric(15,2), "
       f"DATE '1995-01-01' + (g % 240) FROM generate_series(1, 60000) g; "
       f"INSERT INTO {TL} VALUES (7, NULL, 0.05, DATE '1995-06-01'), (NULL, 9.99, 0.01, DATE '1995-06-01')")
    my(f"DROP TABLE IF EXISTS {TO}; CREATE TABLE {TO} (o_orderkey bigint PRIMARY KEY, o_orderdate date, "
       f"o_orderpriority varchar(15)); SET SESSION cte_max_recursion_depth = 4000; "
       f"INSERT INTO {TO} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 2500) "
       f"SELECT n, DATE_ADD('1994-12-01', INTERVAL (n % 200) DAY), CONCAT(n % 5, '-PRIO') FROM s")
    try:
        # Truth from each server's own text.
        orders = {}
        for line in my(f"SELECT o_orderkey, o_orderpriority FROM {TO} WHERE o_orderdate < '1995-03-15'").splitlines():
            k, p = line.split("\t")
            orders[int(k)] = p
        lines = []
        for line in pg(f"SELECT l_orderkey, l_extendedprice, l_discount FROM {TL} "
                       f"WHERE l_shipdate > DATE '1995-03-15' AND l_orderkey IS NOT NULL").splitlines():
            k, price, disc = line.split("|")
            lines.append((int(k), None if price == "" else Decimal(price), Decimal(disc)))
        agg = defaultdict(lambda: [Decimal(0), 0, None, None, [], 0])
        for k, price, disc in lines:
            if k not in orders:
                continue
            a = agg[orders[k]]
            a[1] += 1
            if price is not None:
                a[0] += price * (1 - disc)
                a[2] = price if a[2] is None else min(a[2], price)
                a[3] = price if a[3] is None else max(a[3], price)
                a[4].append(price)
                a[5] += 1
        q = (f"SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)) AS revenue, COUNT(*) AS n "
             f"FROM pg.{TL} l JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey "
             f"WHERE l.l_shipdate > DATE '1995-03-15' AND o.o_orderdate < DATE '1995-03-15' "
             f"GROUP BY 1 ORDER BY 1")
        got = rows(apitap.sql(q, sources=SRC))
        want = [(p, a[0], a[1]) for p, a in sorted(agg.items())]
        case("1: the Q3-shaped join equals the servers' answer", got == want and len(want) == 5,
             f"got {got[:2]}… want {want[:2]}…")

        code = ("import apitap\n"
                f"r = apitap.sql({q!r}, sources={SRC!r}).to_polars()\n"
                "print('ROWS', [tuple(x) for x in r.iter_rows()])\n")
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env=dict(os.environ, APITAP_SQL_BUILD_BYTES="2K", APITAP_DEBUG="1"))
        passes = [l for l in r.stderr.splitlines() if "join done in" in l]
        splits = [l for l in r.stderr.splitlines() if "past" in l and "range" in l]
        out = next((l[5:] for l in r.stdout.splitlines() if l.startswith("ROWS ")), "")
        case("5: a build past its budget is split into passes", r.returncode == 0 and len(splits) >= 1
             and passes and "1 pass" not in passes[-1], f"{len(splits)} split(s), {passes[-1:]} {r.stderr[-200:] if r.returncode else ''}")
        case("5: and the split join still equals the servers' answer", out == repr(want), out[:120])

        want_txt = [(p, str(r), str(n)) for p, r, n in want]
        for name, url, read_back in (
            ("Postgres", PGD, pgd_rows),
            ("ClickHouse", CH, ch_rows),
            ("MySQL", MY, my_rows),
        ):
            try:
                rep = apitap.sql(q, sources=SRC, dest=url, table=OUT)
                got = read_back()
                case(f"6: dest={name} lands the answer, typed", rep.rows == len(want) and got == want_txt,
                     f"rows={rep.rows} got {got[:2]} want {want_txt[:2]}")
            except Exception as e:  # noqa: BLE001
                case(f"6: dest={name} lands the answer, typed", False, f"{type(e).__name__}: {str(e)[:200]}")

        q7 = (f"SELECT l_shipdate, COUNT(*) AS n, SUM(o_orderkey) AS k FROM pg.{TL} l JOIN my.{TO} o "
              f"ON l.l_orderkey = o.o_orderkey WHERE o.o_orderkey <= 2000 GROUP BY 1 ORDER BY 1")
        got = [(str(d), n, k) for d, n, k in rows(apitap.sql(q7, sources=SRC))]
        want7 = [(d, int(n), int(k)) for d, n, k in (l.split("|") for l in pg(
            f"SELECT l_shipdate, count(*), sum(l_orderkey) FROM {TL} WHERE l_orderkey BETWEEN 1 AND 2000 "
            f"GROUP BY 1 ORDER BY 1").splitlines())]
        case("7: a repeated GROUP BY-side key counts every match", got == want7 and len(want7) > 100,
             f"{len(got)} groups, first {got[:2]} want {want7[:2]}")

        # 8: an inner join emits only groups some row matched. Orders span 200
        # dates; lines with l_orderkey <= 40 reach at most 40 of them.
        odate = {int(k): d for k, d in (l.split("\t") for l in my(f"SELECT o_orderkey, o_orderdate FROM {TO}").splitlines())}
        cnt8 = defaultdict(int)
        for line in pg(f"SELECT l_orderkey FROM {TL} WHERE l_orderkey <= 40").splitlines():
            if int(line) in odate:
                cnt8[odate[int(line)]] += 1
        want8 = sorted(cnt8.items())
        q8 = (f"SELECT o_orderdate, COUNT(*) AS n FROM pg.{TL} l JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey "
              f"WHERE l.l_orderkey <= 40 GROUP BY 1 ORDER BY 1")
        got = [(str(d), n) for d, n in rows(apitap.sql(q8, sources=SRC))]
        case("8: an inner join emits no group that nothing matched", got == want8 and 0 < len(want8) < 200,
             f"{len(got)} groups vs {len(want8)}, first {got[:2]} want {want8[:2]}")

        # 11: an OR across both tables is checked here, per matched row; the
        # MySQL column is utf8mb4_0900_ai_ci, so '1-prio' is '1-PRIO' there and
        # must be here too (it was compared bytewise and the branch kept none).
        coll = my(f"SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                  f"AND TABLE_NAME = '{TO}' AND COLUMN_NAME = 'o_orderpriority'")
        prio = {int(k): p for k, p in (l.split("\t") for l in my(f"SELECT o_orderkey, o_orderpriority FROM {TO}").splitlines())}
        want11 = [0, Decimal(0)]
        for line in pg(f"SELECT l_orderkey, l_extendedprice, l_discount FROM {TL} "
                       f"WHERE l_orderkey IS NOT NULL AND l_extendedprice IS NOT NULL").splitlines():
            k, price, disc = line.split("|")
            pr = prio.get(int(k), "").lower()
            if (pr == "1-prio" and Decimal(disc) > Decimal("0.05")) or (pr == "2-prio" and Decimal(disc) < Decimal("0.02")):
                want11[0] += 1
                want11[1] += Decimal(price)
        q11 = (f"SELECT COUNT(*) AS n, SUM(l_extendedprice) AS s FROM pg.{TL} l JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey "
               f"WHERE (o.o_orderpriority = '1-prio' AND l.l_discount > 0.05) "
               f"OR (o.o_orderpriority = '2-PRIO' AND l.l_discount < 0.02)")
        got = rows(apitap.sql(q11, sources=SRC))
        case("11: a case-insensitive MySQL column compares case-insensitively in an OR across tables",
             coll.endswith("_ci") and got == [tuple(want11)] and want11[0] > 100, f"{coll}: got {got} want {want11}")

        # 9: with the opt-in pushdown (APITAP_SQL_PUSH_ARGS=1) the probe's
        # argument is computed by Postgres as one scaled int8; a cube of
        # prices overflows int8 there, and the query runs again without the
        # pushdown, still exact.
        cube = defaultdict(Decimal)
        for line in pg(f"SELECT l_orderkey, l_extendedprice FROM {TL} WHERE l_orderkey IS NOT NULL "
                       f"AND l_extendedprice IS NOT NULL").splitlines():
            k, price = line.split("|")
            if int(k) in odate:
                cube[odate[int(k)]] += Decimal(price) ** 3
        q9 = (f"SELECT o_orderdate, SUM(l_extendedprice * l_extendedprice * l_extendedprice) AS c FROM pg.{TL} l "
              f"JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1")
        code = ("import apitap\n"
                f"r = apitap.sql({q9!r}, sources={SRC!r}).to_polars()\n"
                "print('ROWS', [(str(d), str(c)) for d, c in r.iter_rows()])\n")
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env=dict(os.environ, APITAP_DEBUG="1", APITAP_SQL_PUSH_ARGS="1"))
        out = next((l[5:] for l in r.stdout.splitlines() if l.startswith("ROWS ")), "")
        want9 = repr([(d, str(c)) for d, c in sorted(cube.items())])
        case("9: an argument past int8 runs again without the pushdown, exact",
             r.returncode == 0 and "running again without the pushdown" in r.stderr and out == want9,
             (r.stderr[-300:] if r.returncode else out[:100]))

        # 10: with the opt-in key filter (APITAP_SQL_KEY_FILTER=1) a selective
        # build (one order in ten) sends Postgres its keys as a bitmap; the
        # server ships only those keys' lines, and the answer still equals the
        # servers' own. Without it (the default) the same answer comes back
        # and no bitmap is sent.
        prio = {int(k): p for k, p in (l.split("\t") for l in my(f"SELECT o_orderkey, o_orderpriority FROM {TO} "
                                                                  f"WHERE o_orderkey % 10 = 3").splitlines())}
        a10 = defaultdict(lambda: [Decimal(0), 0])
        for line in pg(f"SELECT l_orderkey, l_extendedprice, l_discount FROM {TL} WHERE l_orderkey IS NOT NULL").splitlines():
            k, price, disc = line.split("|")
            if int(k) in prio and price != "":
                a10[prio[int(k)]][0] += Decimal(price) * (1 - Decimal(disc))
                a10[prio[int(k)]][1] += 1
        q10 = (f"SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)), COUNT(l_extendedprice) FROM pg.{TL} l "
               f"JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey WHERE o.o_orderkey % 10 = 3 GROUP BY 1 ORDER BY 1")
        code = ("import apitap\n"
                f"r = apitap.sql({q10!r}, sources={SRC!r}).to_polars()\n"
                "print('ROWS', [(p, str(v), n) for p, v, n in r.iter_rows()])\n")
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env=dict(os.environ, APITAP_DEBUG="1", APITAP_SQL_KEY_FILTER="1"))
        out = next((l[5:] for l in r.stdout.splitlines() if l.startswith("ROWS ")), "")
        want10 = repr([(p, str(v), n) for p, (v, n) in sorted(a10.items())])
        fl = [l for l in r.stderr.splitlines() if "key filter:" in l]
        case("10: a selective build filters the probe on the server by a key bitmap, exact",
             r.returncode == 0 and fl and out == want10, (r.stderr[-300:] if r.returncode else f"{fl[:1]} {out[:90]}"))
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env={k: v for k, v in dict(os.environ, APITAP_DEBUG="1").items() if k != "APITAP_SQL_KEY_FILTER"})
        out = next((l[5:] for l in r.stdout.splitlines() if l.startswith("ROWS ")), "")
        fl = [l for l in r.stderr.splitlines() if "key filter:" in l]
        case("10b: by default no key bitmap is sent, and the answer is the same",
             r.returncode == 0 and not fl and out == want10, (r.stderr[-300:] if r.returncode else f"{fl[:1]} {out[:90]}"))

        q2 = (f"SELECT o_orderpriority, MIN(l_extendedprice), MAX(l_extendedprice), AVG(l_extendedprice), "
              f"COUNT(l_extendedprice) FROM pg.{TL} l JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey "
              f"WHERE l.l_shipdate > DATE '1995-03-15' AND o.o_orderdate < DATE '1995-03-15' GROUP BY 1 ORDER BY 1")
        got = rows(apitap.sql(q2, sources=SRC))
        want = [(p, a[2], a[3], float(sum(a[4]) / len(a[4])), a[5]) for p, a in sorted(agg.items())]
        good = len(got) == len(want) and all(
            g[0] == w[0] and g[1] == w[1] and g[2] == w[2] and abs(g[3] - w[3]) < 1e-6 and g[4] == w[4]
            for g, w in zip(got, want))
        case("2: MIN, MAX, AVG and COUNT(col) over the join, a NULL skipped", good, f"got {got[:1]} want {want[:1]}")

        q3 = f"SELECT o_orderpriority, COUNT(*) FROM my.{TO} WHERE o_orderkey > 100 GROUP BY o_orderpriority ORDER BY 1"
        want = [tuple(l.split("\t")) for l in
                my(f"SELECT o_orderpriority, COUNT(*) FROM {TO} WHERE o_orderkey > 100 GROUP BY 1 ORDER BY 1").splitlines()]
        got = [(p, str(n)) for p, n in rows(apitap.sql(q3, sources=SRC))]
        case("3: one table with GROUP BY", got == want, f"got {got} want {want}")

        for label, bad, frag in (
            ("LEFT JOIN", f"SELECT COUNT(*) FROM pg.{TL} l LEFT JOIN my.{TO} o ON l.l_orderkey = o.o_orderkey", "INNER JOIN"),
            ("a WHERE naming both tables", f"SELECT o_orderpriority, COUNT(*) FROM pg.{TL} l JOIN my.{TO} o "
             f"ON l.l_orderkey = o.o_orderkey WHERE l.l_discount > o.o_orderkey GROUP BY 1", "more than one table"),
            ("SELECT *", f"SELECT * FROM pg.{TL}", "SELECT *"),
        ):
            try:
                apitap.sql(bad, sources=SRC)
                case(f"4: {label} is refused", False, "it ran")
            except Exception as e:  # noqa: BLE001 — the message is the contract
                case(f"4: {label} is refused, naming it", frag in str(e), str(e)[:150])
    finally:
        pg(f"DROP TABLE IF EXISTS {TL}")
        my(f"DROP TABLE IF EXISTS {TO}")
        drop_out()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"   XX {type(e).__name__}: {e}")
        ok = False
    print("SQL JOIN E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
