"""DuckDB joins two apitap.read() streams directly, with no pyarrow in between.

DuckDB binds a PyCapsule object by its schema before it scans it. A reader
with only `__arrow_c_stream__` was opened twice, once for the schema and once
for the scan, and apitap's one-shot stream refused the scan: 0.65.0 failed
`con.sql("... FROM li JOIN od ...")` with "read stream already consumed",
and the only way round it was wrapping each reader in pyarrow. The reader now
exports `__arrow_c_schema__` from Rust, and DuckDB opens each stream once.

Proof: a Postgres table and a MySQL table, read by apitap and joined plus
aggregated in DuckDB (memory_limit 120MB, one thread). The answer equals the
same join computed here from each server's own text output, with pyarrow
made unimportable: DuckDB imports it on its own when it is installed (to
check an object's type), so blocking it is what proves the data path never
needs it.
"""
import subprocess
import sys


class _NoPyarrow:
    def find_spec(self, name, path=None, target=None):
        if name == "pyarrow" or name.startswith("pyarrow."):
            raise ModuleNotFoundError("No module named 'pyarrow' (blocked by the leg)")
        return None


sys.meta_path.insert(0, _NoPyarrow())
from collections import defaultdict
from decimal import Decimal

import apitap
import duckdb

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
TL, TO = "read_duck_li", "read_duck_od"
Q = """SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)) AS rev, COUNT(*) AS n
  FROM li JOIN od ON l_orderkey = o_orderkey
  WHERE o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
  GROUP BY 1 ORDER BY 1"""


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


def truth():
    """The join, from each server's own text: no apitap, no DuckDB."""
    orders = {}
    for line in my(f"SELECT o_orderkey, o_orderdate, o_orderpriority FROM {TO} "
                   "WHERE o_orderdate < '1995-03-15'").splitlines():
        k, _, p = line.split("\t")
        orders[int(k)] = p
    agg = defaultdict(lambda: [Decimal(0), 0])
    for line in pg(f"SELECT l_orderkey, l_extendedprice, l_discount FROM {TL} "
                   "WHERE l_shipdate > DATE '1995-03-15'").splitlines():
        k, price, disc = line.split("|")
        if int(k) in orders:
            a = agg[orders[int(k)]]
            a[0] += Decimal(price) * (1 - Decimal(disc))
            a[1] += 1
    return [(p, a[0], a[1]) for p, a in sorted(agg.items())]


def main():
    pg(f"DROP TABLE IF EXISTS {TL}; CREATE TABLE {TL} AS SELECT (g % 500) + 1 AS l_orderkey, "
       "(g * 1.5)::numeric(15,2) AS l_extendedprice, ((g % 10) / 100.0)::numeric(15,2) AS l_discount, "
       "DATE '1995-01-01' + (g % 200) AS l_shipdate FROM generate_series(1, 20000) g")
    my(f"DROP TABLE IF EXISTS {TO}; CREATE TABLE {TO} (o_orderkey int PRIMARY KEY, o_orderdate date, "
       "o_orderpriority varchar(15)); "
       f"INSERT INTO {TO} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 500) "
       "SELECT n, DATE_ADD('1994-12-01', INTERVAL (n % 200) DAY), CONCAT(n % 5, '-PRIO') FROM s")
    try:
        con = duckdb.connect(config={"memory_limit": "120MB", "threads": 1})
        li = apitap.read(PG, table=TL, columns=["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"])
        od = apitap.read(MY, table=TO, columns=["o_orderkey", "o_orderdate", "o_orderpriority"])
        got = [(p, Decimal(r), int(n)) for p, r, n in con.sql(Q).fetchall()]
        want = truth()
        assert want, "the fixture joins nothing: the test would prove nothing"
        assert got == want, f"DuckDB over the streams says {got!r}, the servers say {want!r}"
        print(f"  pg x my joined in DuckDB from two apitap streams: {len(got)} groups, "
              f"{sum(n for _, _, n in got)} rows, equal to the servers' own answer")
        assert "pyarrow" not in sys.modules, "pyarrow got past the block"
        print("  with pyarrow unimportable")
    finally:
        pg(f"DROP TABLE IF EXISTS {TL}")
        my(f"DROP TABLE IF EXISTS {TO}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 — the verdict line is the gate's contract
        print(f"  XX {e}")
        print("READ DUCKDB E2E: FAILED")
        sys.exit(1)
    print("READ DUCKDB E2E: PASSED")
