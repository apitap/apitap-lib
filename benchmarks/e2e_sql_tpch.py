"""TPC-H through apitap.sql(), every query, across two servers.

DuckDB's dbgen makes a small TPC-H (scale factor in argv, 0.01 by default);
customer, lineitem and partsupp go to Postgres, orders, part, supplier,
nation and region to MySQL, so each join crosses servers. Each of the 22
queries (DuckDB's own text, table names pointed at the two servers) runs
through apitap.sql() and its rows are compared with DuckDB's answer on the
same data, held in DuckDB itself.

A query apitap refuses is reported as REFUSED (and fails the leg unless it
is listed in ALLOWED_REFUSALS); a wrong answer always fails.

Then the same queries again under budgets small enough to force the paths
a large query takes in a small cage (CONFIGS): stages on disk and GROUP BYs
answered in parts (Q15's parts by an output alias), and joins in many key
ranges whose later ranges' probe rows are spilled to disk (grace).

Usage: e2e_sql_tpch.py [sf] [q,q,...]
"""

import decimal
import os
import re
import sys
import time

import apitap
import duckdb

PG = "postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
PG_DUCK = "host=127.0.0.1 port=5544 dbname=apitap_bench_src user=postgres password=bench"
MY_DUCK = "host=127.0.0.1 port=3307 database=bench user=root password=bench"

ON_PG = ["customer", "lineitem", "partsupp"]
ON_MY = ["orders", "part", "supplier", "nation", "region"]
PREFIX = "tpch_"

# Queries not answered yet; the list shrinks as 0.67 lands.
ALLOWED_REFUSALS = set()

# (label, env, only these queries, skip these). Q9's dimensions alone pass a
# 64K build budget (a refusal by design), so the grace run leaves it out; at
# 4K a part's fixed cost passes Q16's group budget, so that run is Q15's.
CONFIGS = [
    ("default", {}, None, set()),
    ("stages on disk, groups in parts", {"APITAP_SQL_STAGE_BYTES": "1K", "APITAP_SQL_GROUP_BYTES": "8K"}, None, set()),
    ("GROUP BY an alias in parts", {"APITAP_SQL_GROUP_BYTES": "4K"}, {15}, set()),
    ("key ranges, grace spill", {"APITAP_SQL_BUILD_BYTES": "64K"}, None, {9}),
]
KNOBS = sorted({k for _, env, _, _ in CONFIGS for k in env})


def load(con, sf):
    con.execute("INSTALL tpch; LOAD tpch; INSTALL postgres; LOAD postgres; INSTALL mysql; LOAD mysql;")
    con.execute(f"CALL dbgen(sf={sf})")
    con.execute(f"ATTACH '{PG_DUCK}' AS pgw (TYPE postgres)")
    con.execute(f"ATTACH '{MY_DUCK}' AS myw (TYPE mysql)")
    for t in ON_PG + ON_MY:
        db = "pgw" if t in ON_PG else "myw"
        con.execute(f"DROP TABLE IF EXISTS {db}.{PREFIX}{t}")
        con.execute(f"CREATE TABLE {db}.{PREFIX}{t} AS SELECT * FROM {t}")
    # Primary keys, as a TPC-H database has them: the planner reads key ranges.
    # Through DuckDB's own connections, so the load needs no docker (the PGO
    # training container runs it too).
    pk = {"customer": "c_custkey", "lineitem": "l_orderkey, l_linenumber", "partsupp": "ps_partkey, ps_suppkey",
          "orders": "o_orderkey", "part": "p_partkey", "supplier": "s_suppkey", "nation": "n_nationkey",
          "region": "r_regionkey"}
    for t in ON_PG:
        con.execute(f"CALL postgres_execute('pgw', 'ALTER TABLE {PREFIX}{t} ADD PRIMARY KEY ({pk[t]}); ANALYZE {PREFIX}{t}')")
    for t in ON_MY:
        con.execute(f"CALL mysql_execute('myw', 'ALTER TABLE {PREFIX}{t} ADD PRIMARY KEY ({pk[t]})')")
        con.execute(f"CALL mysql_execute('myw', 'ANALYZE TABLE {PREFIX}{t}')")
    con.execute("DETACH pgw")
    con.execute("DETACH myw")

def pointed(q):
    """Table names → <source>.tpch_<name>, only where a table is named: right
    after FROM / JOIN, or at the start of a line inside a FROM list (DuckDB's
    text puts one table per line). A column alias such as `AS nation` stays."""
    names = "|".join(ON_PG + ON_MY)

    def to(m):
        t = m.group(2)
        return f"{m.group(1)}{'pg' if t in ON_PG else 'my'}.{PREFIX}{t}"

    out, in_from = [], False
    for line in q.splitlines():
        s = line.strip().upper()
        if re.match(r"((WHERE|GROUP|ORDER|HAVING|LIMIT|SELECT)\b|\))", s):
            in_from = False
        line = re.sub(rf"(\b(?:FROM|JOIN)\s+)({names})\b", to, line, flags=re.I)
        if in_from:
            line = re.sub(rf"^(\s*)({names})\b", to, line)
        if re.search(r"\bFROM\s*$", line, flags=re.I):
            in_from = True
        out.append(line)
    return "\n".join(out).rstrip().rstrip(";")


def norm(v):
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, decimal.Decimal):
        return round(float(v), 4)
    if isinstance(v, str):
        return v.rstrip()
    return v


def same(a, b):
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb):
            x, y = norm(x), norm(y)
            if isinstance(x, float) or isinstance(y, float):
                try:
                    if abs(float(x) - float(y)) > 1e-6 * max(1.0, abs(float(y))):
                        return False
                except (TypeError, ValueError):
                    return False
            elif x != y:
                return False
    return True


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 0.01
    only = {int(x) for x in sys.argv[2].split(",")} if len(sys.argv) > 2 else None
    con = duckdb.connect()
    t0 = time.time()
    load(con, sf)
    print(f"  TPC-H sf={sf} loaded into Postgres + MySQL in {time.time() - t0:.1f}s")
    queries = con.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY 1").fetchall()
    truths = {n: [tuple(r) for r in con.execute(q).fetchall()] for n, q in queries if not only or n in only}
    bad, refused = [], []
    # A query list on the command line runs the default budgets only.
    for label, env, cfg_only, skip in CONFIGS if not only else CONFIGS[:1]:
        for k in KNOBS:
            os.environ.pop(k, None)
        os.environ.update(env)
        print(f"  == {label}" + (f" ({', '.join(f'{k}={v}' for k, v in env.items())})" if env else ""))
        tag = "" if not env else f" [{label}]"
        for n, q in queries:
            if (only and n not in only) or (cfg_only and n not in cfg_only) or n in skip:
                continue
            truth = truths[n]
            t1 = time.time()
            try:
                got = [tuple(r) for r in apitap.sql(pointed(q), sources={"pg": PG, "my": MY}).to_polars().rows()]
            except Exception as e:  # noqa: BLE001 — a refusal is reported, not raised
                msg = str(e).splitlines()[0][:160]
                refused.append(f"{n}{tag}")
                print(f"  Q{n:<2} REFUSED  {msg}")
                continue
            ok = same(sorted(map(lambda r: tuple(map(norm, r)), got), key=repr),
                      sorted(map(lambda r: tuple(map(norm, r)), truth), key=repr))
            print(f"  Q{n:<2} {'OK      ' if ok else 'WRONG   '} {len(got)} rows {time.time() - t1:.2f}s")
            if not ok:
                bad.append(f"{n}{tag}")
                print(f"      apitap {got[:3]}")
                print(f"      duckdb {truth[:3]}")
    for k in KNOBS:
        os.environ.pop(k, None)
    unexpected = [n for n in refused if n.split(" ")[0] not in {str(x) for x in ALLOWED_REFUSALS}]
    print(f"  wrong: {bad or 'none'}; refused: {refused or 'none'}")
    if bad or unexpected:
        print("SQL TPCH E2E: FAILED")
        sys.exit(1)
    print("SQL TPCH E2E: PASSED")


if __name__ == "__main__":
    main()
