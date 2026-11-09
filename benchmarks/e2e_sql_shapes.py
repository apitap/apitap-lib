"""SQL shapes TPC-H does not cover, through apitap.sql(), against known answers.

Small tables on the bench Postgres and MySQL (prefix `shp_`), each case a query
whose right answer is written here by hand: a NULL-producing scalar subquery,
a case-insensitive MySQL column read plainly and ordered across parts, two IN
over one CTE with different filters, LEFT JOIN shapes, COUNT(DISTINCT) with
NULLs, a timestamp scalar subquery. A case expects rows (in order when the
query orders them) or a refusal; a refusal where rows are expected, rows
where a refusal is expected, or wrong rows fail the leg.

These came from the 0.67 review (reports are local): each was a silent wrong
answer, a server error or a missing refusal before its fix.

Usage: e2e_sql_shapes.py [case,case,...]
"""

import os
import sys

import apitap
import duckdb

PG = "postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
PG_DUCK = "host=127.0.0.1 port=5544 dbname=apitap_bench_src user=postgres password=bench"
MY_DUCK = "host=127.0.0.1 port=3307 database=bench user=root password=bench"

PG_SETUP = """
DROP TABLE IF EXISTS shp_c, shp_o, shp_t6, shp_s6, shp_cust, shp_ord, shp_dx, shp_tt, shp_ut, shp_nl, shp_lk, shp_dt, shp_tz, shp_g1, shp_g2, shp_lo, shp_g3, shp_g4;
CREATE TABLE shp_g3(id bigint primary key, grp int); INSERT INTO shp_g3 SELECT i::bigint * 1000003, 100000 + i FROM generate_series(1, 5000) i;
CREATE TABLE shp_g4(gid bigint); INSERT INTO shp_g4 SELECT (1 + i % 5000)::bigint * 1000003 FROM generate_series(1, 20000) i; ANALYZE shp_g3;
CREATE TABLE shp_lo(o_id int, oc_id int, st text); INSERT INTO shp_lo VALUES (10,1,'F'),(11,1,'O'),(12,3,NULL),(13,3,'F');
CREATE TABLE shp_g1(id int primary key, name text, grp int); INSERT INTO shp_g1 SELECT i, 'name-' || i, 100000 + i FROM generate_series(1, 5000) i;
CREATE TABLE shp_g2(gid int); INSERT INTO shp_g2 SELECT 1 + i % 5000 FROM generate_series(1, 20000) i; ANALYZE shp_g1;
CREATE TABLE shp_tz(tz timestamptz); INSERT INTO shp_tz VALUES ('2024-01-02 10:00:00+00');
CREATE TABLE shp_nl(t text); INSERT INTO shp_nl VALUES ('x'),(NULL),('x'),(NULL),('y');
CREATE TABLE shp_lk(k int, v text); INSERT INTO shp_lk VALUES (1,'a'),(NULL,'b'),(7,'c');
CREATE TABLE shp_dt(g int, s text); INSERT INTO shp_dt VALUES (1,'a'),(1,'a'),(1,NULL),(1,'b'),(2,NULL);
CREATE TABLE shp_c(k int); INSERT INTO shp_c VALUES (1),(2);
CREATE TABLE shp_o(k int, x int); INSERT INTO shp_o VALUES (1,10);
CREATE TABLE shp_t6(a int, b int); INSERT INTO shp_t6 VALUES (1,2),(1,9),(5,2);
CREATE TABLE shp_s6(x int, y text); INSERT INTO shp_s6 VALUES (1,'A'),(2,'B');
CREATE TABLE shp_cust(c_id int primary key, nation text); INSERT INTO shp_cust VALUES (1,'X'),(2,'X'),(3,'Y');
CREATE TABLE shp_ord(o_id int primary key, oc_id int); INSERT INTO shp_ord VALUES (10,1),(11,1),(12,3);
CREATE TABLE shp_dx(g int, x int); INSERT INTO shp_dx VALUES (1,NULL),(1,NULL),(2,5),(2,5),(2,7);
CREATE TABLE shp_tt(ts timestamp); INSERT INTO shp_tt VALUES ('2024-01-02 10:00:00');
CREATE TABLE shp_ut(id int, ts timestamp); INSERT INTO shp_ut VALUES (1,'2024-01-01 00:00:00'),(2,'2024-01-03 00:00:00');
ANALYZE shp_cust; ANALYZE shp_ord;
"""

MY_SETUP = [
    "DROP TABLE IF EXISTS shp_n, shp_ci",
    "CREATE TABLE shp_n(n varchar(10) COLLATE utf8mb4_general_ci)",
    "INSERT INTO shp_n VALUES ('a'),('A')",
    "CREATE TABLE shp_ci(id int primary key, name varchar(10) COLLATE utf8mb4_general_ci, v int)",
    "INSERT INTO shp_ci VALUES (1,'B',1),(2,'a',1),(3,'c',1),(4,'D',1),(5,'e',1),(6,'F',1)",
]

ROWS, REFUSED = "rows", "refused"
# The answer DuckDB gives for the same query (pg. read through its postgres
# extension); DUCK_OR_REFUSED also takes a refusal holding the fragment.
DUCK, DUCK_OR_REFUSED = "duck", "duck_or_refused"

# (name, query, expected kind, expected rows or a refusal message fragment,
#  ordered?, env)
CASES = [
    # B5: under IS NULL the subquery's NULL keeps the row, which an inner
    # join would drop ([] at b8dee28, correct [2]): refused, not decorrelated.
    ("scalar_is_null", "SELECT c.k FROM pg.shp_c c WHERE (SELECT MAX(x) FROM pg.shp_o o WHERE o.k = c.k) IS NULL",
     REFUSED, "correlated subquery", False, {}),
    # B5: a NULL-rejecting comparison still decorrelates (unchanged).
    ("scalar_cmp", "SELECT c.k FROM pg.shp_c c WHERE (SELECT MAX(x) FROM pg.shp_o o WHERE o.k = c.k) > 5",
     ROWS, [(1,)], False, {}),
    # B8: a plain SELECT returns 'a' and 'A' of a _ci column, not 'a' twice.
    ("ci_plain_select", "SELECT n FROM my.shp_n", ROWS, [("A",), ("a",)], False, {}),
    # A1: two IN over one CTE with different filters each get their own keys.
    ("cte_two_in", "WITH c AS (SELECT x, y FROM pg.shp_s6) SELECT a, b FROM pg.shp_t6 "
     "WHERE a IN (SELECT x FROM c WHERE y = 'A') AND b IN (SELECT x FROM c WHERE y = 'B')",
     ROWS, [(1, 2)], False, {}),
    # B2: grouped by the preserved key: answered.
    ("left_join_by_key", "SELECT c.c_id, COUNT(o.o_id) AS n FROM pg.shp_cust c LEFT JOIN pg.shp_ord o ON c.c_id = o.oc_id "
     "GROUP BY c.c_id", ROWS, [(1, 2), (2, 0), (3, 1)], False, {}),
    # B2: grouped by the key and another column of the preserved table: answered.
    ("left_join_by_key_and_col", "SELECT c_id, nation, COUNT(o_id) AS n FROM pg.shp_cust c LEFT JOIN pg.shp_ord o ON c.c_id = o.oc_id "
     "GROUP BY c_id, nation", ROWS, [(1, "X", 2), (2, "X", 0), (3, "Y", 1)], False, {}),
    # B2: grouped coarser than the key: the halves would not merge; refused.
    ("left_join_coarse_group", "SELECT c.nation, COUNT(o.o_id) AS n FROM pg.shp_cust c LEFT JOIN pg.shp_ord o ON c.c_id = o.oc_id "
     "GROUP BY c.nation", REFUSED, "does not hold its key", False, {}),
    # B3: HAVING over a LEFT JOIN: refused.
    ("left_join_having", "SELECT c.c_id, COUNT(o.o_id) AS n FROM pg.shp_cust c LEFT JOIN pg.shp_ord o ON c.c_id = o.oc_id "
     "GROUP BY c.c_id HAVING COUNT(o.o_id) > 1", REFUSED, "HAVING or DISTINCT over a LEFT JOIN", False, {}),
    # B4: a group whose values are all NULL counts 0, not absent.
    ("count_distinct_nulls", "SELECT g, COUNT(DISTINCT x) AS n FROM pg.shp_dx GROUP BY g",
     ROWS, [(1, 0), (2, 2)], False, {}),
    # B6: OFFSET on a staged path: refused, not dropped.
    ("staged_offset", "SELECT g, COUNT(DISTINCT x) AS n FROM pg.shp_dx GROUP BY g ORDER BY g LIMIT 1 OFFSET 1",
     REFUSED, "OFFSET", False, {}),
    # B12: a nested WITH naming an outer CTE again: refused.
    ("cte_shadowing", "WITH a AS (SELECT k FROM pg.shp_c) SELECT COUNT(*) FROM (WITH a AS (SELECT k FROM pg.shp_o) SELECT k FROM a) d",
     REFUSED, "shadowing", False, {}),
    # B13: IN over a stage under OR: refused here, never sent to the server.
    ("cte_in_under_or", "WITH a AS (SELECT k FROM pg.shp_c) SELECT COUNT(*) FROM pg.shp_o t WHERE t.k IN (SELECT k FROM a) OR t.x > 100",
     REFUSED, "subquery under OR", False, {}),
    # B17: a timestamp scalar compared with a timestamp column: a refusal
    # (the planner takes no timestamp literal yet), never a wrong row.
    ("timestamp_scalar", "SELECT id FROM pg.shp_ut WHERE ts > (SELECT ts FROM pg.shp_tt)", REFUSED, "timestamp value used as a literal", False, {}),
    # B9: NULLs through the repeat, top level (in memory) and in a stage.
    ("plain_select_nulls", "SELECT t FROM pg.shp_nl", ROWS, [("x",), ("x",), (None,), (None,), ("y",)], False, {}),
    ("cte_plain_nulls", "WITH c AS (SELECT t FROM pg.shp_nl) SELECT COUNT(*) AS n, COUNT(t) AS m FROM c",
     ROWS, [(5, 3)], False, {}),
    # B9: a LIMIT that cuts a repeated group, top level and in a stage.
    ("plain_limit_in_group", "SELECT t FROM pg.shp_nl WHERE t = 'x' LIMIT 1", ROWS, [("x",)], False, {}),
    ("cte_limit_in_group", "WITH c AS (SELECT t FROM pg.shp_nl WHERE t = 'x' LIMIT 1) SELECT COUNT(*) AS n FROM c",
     ROWS, [(1,)], False, {}),
    # B2: a NULL key on the preserved side: one unmatched group of its own.
    ("left_join_null_key", "SELECT l.k, COUNT(o.o_id) AS n FROM pg.shp_lk l LEFT JOIN pg.shp_ord o ON l.k = o.oc_id GROUP BY l.k",
     ROWS, [(1, 2), (7, 0), (None, 0)], False, {}),
    # B4: COUNT(DISTINCT) of a text column, NULLs skipped, an all-NULL group 0.
    ("count_distinct_text", "SELECT g, COUNT(DISTINCT s) AS n FROM pg.shp_dt GROUP BY g", ROWS, [(1, 2), (2, 0)], False, {}),
    # A3: build-side groups past the group budget with no integer column to
    # part by: a clear refusal (at sf10 the build was OOM-killed instead).
    ("build_groups_over", "SELECT g.name, COUNT(*) AS n FROM pg.shp_g2 x JOIN pg.shp_g1 g ON x.gid = g.id GROUP BY g.name",
     REFUSED, "groups past their memory budget", False, {"APITAP_SQL_BUILD_BYTES": "64K"}),
    # A timestamptz scalar has no session-independent literal: refused.
    ("timestamptz_scalar", "SELECT id FROM pg.shp_ut WHERE ts > (SELECT tz FROM pg.shp_tz)",
     REFUSED, "timestamp value used as a literal", False, {}),
    # A3, the 75-100% band: 5,000 build groups (~390 KB) under a 460K build
    # budget pass 3/4 of it (353 KB): parts by g.grp, one pass each, exact
    # (a range split cannot shrink groups the passes share).
    ("build_groups_band", "SELECT g.grp, COUNT(*) AS n FROM pg.shp_g2 x JOIN pg.shp_g1 g ON x.gid = g.id GROUP BY g.grp",
     ROWS, [(100000 + i, 4) for i in range(1, 5001)], False, {"APITAP_SQL_BUILD_BYTES": "460K"}),
    # A4: ORDER BY a _ci column across parts orders by its collation.
    ("ci_order_in_parts", "SELECT id, name, SUM(v) AS s FROM my.shp_ci GROUP BY id, name ORDER BY name LIMIT 4",
     ROWS, [(2, "a", 1), (1, "B", 1), (3, "c", 1), (4, "D", 1)], True, {"APITAP_SQL_GROUP_BYTES": "256"}),
    # C1: a LEFT JOIN aggregate over an expression, not a bare column of the
    # joined table: the unmatched keys' value cannot be told from the
    # function's name (0 for a SUM of CASE, the row count for a COUNT of
    # "IS NULL"); DuckDB's answer, or refused.
    ("left_join_sum_case", "SELECT c.c_id, SUM(CASE WHEN o.st = 'F' THEN 1 ELSE 0 END) AS n FROM pg.shp_cust c "
     "LEFT JOIN pg.shp_lo o ON c.c_id = o.oc_id GROUP BY c.c_id", DUCK_OR_REFUSED, "aggregate", False, {}),
    ("left_join_count_case_null", "SELECT c.c_id, COUNT(CASE WHEN o.o_id IS NULL THEN 1 END) AS n FROM pg.shp_cust c "
     "LEFT JOIN pg.shp_lo o ON c.c_id = o.oc_id GROUP BY c.c_id", DUCK_OR_REFUSED, "aggregate", False, {}),
    ("left_join_sum_case_null", "SELECT c.c_id, SUM(CASE WHEN o.o_id IS NULL THEN 1 ELSE 0 END) AS n FROM pg.shp_cust c "
     "LEFT JOIN pg.shp_lo o ON c.c_id = o.oc_id GROUP BY c.c_id", DUCK_OR_REFUSED, "aggregate", False, {}),
    # C1: bare columns of the joined table, matched and unmatched keys:
    # DuckDB's answer exactly.
    ("left_join_bare_aggs", "SELECT c.c_id, COUNT(*) AS a, COUNT(o.st) AS b, SUM(o.o_id) AS s, MIN(o.o_id) AS lo, "
     "MAX(o.o_id) AS hi, AVG(o.o_id) AS av FROM pg.shp_cust c LEFT JOIN pg.shp_lo o ON c.c_id = o.oc_id GROUP BY c.c_id",
     DUCK, None, False, {}),
    # C4: a build with no server (a CTE) whose 5,000 groups (~386 KB) sit
    # under 3/4 of a 540K budget but whose groups and hash map pass it:
    # parts by g.grp, exact (refused at p106, answered at b8dee28).
    ("serverless_build_band", "WITH g AS (SELECT id, grp FROM pg.shp_g3) SELECT g.grp, COUNT(*) AS n FROM pg.shp_g4 x "
     "JOIN g ON x.gid = g.id GROUP BY g.grp", ROWS, [(100000 + i, 4) for i in range(1, 5001)], False,
     {"APITAP_SQL_BUILD_BYTES": "540K"}),
    # C2: OFFSET / FETCH nested in an IN, an EXISTS or a derived table:
    # refused, never dropped.
    ("in_offset", "SELECT a FROM pg.shp_t6 WHERE a IN (SELECT x FROM pg.shp_s6 OFFSET 1)", REFUSED, "OFFSET", False, {}),
    ("in_fetch", "SELECT a FROM pg.shp_t6 WHERE a IN (SELECT x FROM pg.shp_s6 FETCH FIRST 1 ROWS ONLY)", REFUSED, "FETCH", False, {}),
    ("exists_offset", "SELECT a FROM pg.shp_t6 t WHERE EXISTS (SELECT s.x FROM pg.shp_s6 s WHERE s.x = t.a OFFSET 1)",
     REFUSED, "OFFSET", False, {}),
    ("derived_fetch", "SELECT COUNT(*) AS n FROM (SELECT a FROM pg.shp_t6 FETCH FIRST 1 ROWS ONLY) d", REFUSED, "FETCH", False, {}),
]


def setup():
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres; INSTALL mysql; LOAD mysql;")
    con.execute(f"ATTACH '{PG_DUCK}' AS pgw (TYPE postgres)")
    con.execute(f"ATTACH '{MY_DUCK}' AS myw (TYPE mysql)")
    # One statement per call: newer DuckDB prepares the text, and a prepared
    # statement takes one command (the setup holds no ';' inside a literal).
    for stmt in (x.strip() for x in PG_SETUP.split(";")):
        if stmt:
            con.execute("CALL postgres_execute('pgw', ?)", [stmt])
    for s in MY_SETUP:
        con.execute("CALL mysql_execute('myw', ?)", [s])
    con.close()


def norm(v):
    import decimal
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (float, decimal.Decimal)):
        return round(float(v), 9)
    return v


def duck_rows(q):
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_DUCK}' AS pgw (TYPE postgres, READ_ONLY)")
    rows = con.execute(q.replace("pg.", "pgw.")).fetchall()
    con.close()
    return [tuple(norm(x) for x in r) for r in rows]


def limit_zero():
    """C3: LIMIT 0 over a plain SELECT, top level and in a CTE: no rows and
    every column, read as an Arrow stream (polars, DuckDB); dest= ends clean."""
    ok_all = True
    for q in ["SELECT a, b FROM pg.shp_t6 LIMIT 0",
              "WITH c AS (SELECT a, b FROM pg.shp_t6 LIMIT 0) SELECT a, b FROM c"]:
        got = []
        try:
            df = apitap.sql(q, sources={"pg": PG}).to_polars()
            got.append(("polars", list(df.columns), df.height))
        except Exception as e:  # noqa: BLE001
            got.append(("polars", "ERR " + str(e).splitlines()[0][:120], None))
        try:
            res = apitap.sql(q, sources={"pg": PG})
            rel = duckdb.sql("SELECT * FROM res")
            got.append(("duckdb", list(rel.columns), len(rel.fetchall())))
        except Exception as e:  # noqa: BLE001
            got.append(("duckdb", "ERR " + str(e).splitlines()[0][:120], None))
        # dest=: the run ends clean with 0 rows (a 0-row answer creates no
        # table, as on 0.66.0; nothing more to read back).
        try:
            r = apitap.sql(q, sources={"pg": PG}, dest=PG, table="shp_lim0_out")
            got.append(("dest", ["a", "b"] if getattr(r, "rows", None) == 0 else f"landed {r}", 0))
        except Exception as e:  # noqa: BLE001
            got.append(("dest", "ERR " + str(e).splitlines()[0][:120], None))
        ok = all(cols == ["a", "b"] and n == 0 for _, cols, n in got)
        ok_all &= ok
        print(f"  {'OK   ' if ok else 'WRONG'} limit_zero: {q[:40]}…: {got}")
    return ok_all


def pin_by_table():
    """C5: a statement pins the Postgres sources its tables name, not every
    source whose name appears in the text: an alias `a.` with a source `a`
    that is down does not touch it."""
    q = "SELECT a.k FROM pg.shp_c a"
    try:
        rows = sorted(tuple(r) for r in apitap.sql(q, sources={"pg": PG, "a": "postgresql://postgres:x@127.0.0.1:1/none"}).to_polars().rows())
        ok, shown = rows == [(1,), (2,)], f"rows {rows}"
    except Exception as e:  # noqa: BLE001
        ok, shown = False, "refused: " + str(e).splitlines()[0][:160]
    print(f"  {'OK   ' if ok else 'WRONG'} pin_by_table: {shown}")
    return ok


def snapshots_per_statement():
    """A2: one snapshot per Postgres server for the whole statement, none per
    run: a LEFT JOIN (two halves, stages) and a GROUP BY answered in parts."""
    import subprocess
    out = []
    for q, env in [
        ("SELECT c.c_id, COUNT(o.o_id) AS n FROM pg.shp_cust c LEFT JOIN pg.shp_ord o ON c.c_id = o.oc_id GROUP BY c.c_id", {}),
        ("SELECT g, SUM(x) AS s FROM pg.shp_dx GROUP BY g", {"APITAP_SQL_GROUP_BYTES": "64"}),
    ]:
        code = f"import apitap\napitap.sql({q!r}, sources={{'pg': {PG!r}, 'my': {MY!r}}}).to_polars()\n"
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, stdin=subprocess.DEVNULL,
                           env={**os.environ, "APITAP_DEBUG": "1", **env}, timeout=300)
        lines = [l for l in r.stderr.splitlines() if "snapshot(s) pinned" in l]
        ok = r.returncode == 0 and len(lines) == 1 and "for the statement" in lines[0]
        out.append(ok)
        print(f"  {'OK   ' if ok else 'WRONG'} snapshot_per_statement: {lines or r.stderr[-200:]}")
    return all(out)


def main():
    only = set(sys.argv[1].split(",")) if len(sys.argv) > 1 else None
    setup()
    bad = []
    if not only or "snapshot_per_statement" in only:
        if not snapshots_per_statement():
            bad.append("snapshot_per_statement")
    if not only or "limit_zero" in only:
        if not limit_zero():
            bad.append("limit_zero")
    if not only or "pin_by_table" in only:
        if not pin_by_table():
            bad.append("pin_by_table")
    for name, q, kind, want, ordered, env in CASES:
        if only and name not in only:
            continue
        saved = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            try:
                got = [tuple(norm(x) for x in r) for r in apitap.sql(q, sources={"pg": PG, "my": MY}).to_polars().rows()]
                err = None
            except Exception as e:  # noqa: BLE001 — a refusal is a result here
                got, err = None, str(e).splitlines()[0][:200]
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        if kind in (DUCK, DUCK_OR_REFUSED):
            truth = sorted(duck_rows(q), key=repr)
            if kind == DUCK_OR_REFUSED and err is not None:
                ok = want in err
            else:
                ok = err is None and sorted(got, key=repr) == truth
            shown = (f"refused: {err}" if err else f"rows {got}") + ("" if ok else f"  (DuckDB {truth})")
        elif kind == REFUSED:
            ok = err is not None and want in err
            shown = f"refused: {err}" if err else f"rows {got}"
        else:
            ok = err is None and (got == want if ordered else sorted(got, key=repr) == sorted(want, key=repr))
            shown = f"refused: {err}" if err else f"rows {got}"
        print(f"  {'OK   ' if ok else 'WRONG'} {name}: {shown}" + ("" if ok else f"  (want {kind} {want})"))
        if not ok:
            bad.append(name)
    print(f"  wrong: {bad or 'none'}")
    if bad:
        print("SQL SHAPES E2E: FAILED")
        sys.exit(1)
    print("SQL SHAPES E2E: PASSED")


if __name__ == "__main__":
    main()
