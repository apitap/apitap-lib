"""Postgres source/sink hardening: what a replace keeps, what a parallel read reads.

Four P2 findings (docs/review/2026-10-18-p2-triage.md), PG16 :5544 / :5545 / CH :8124:
  notnull   a replace re-created the destination without its NOT NULL
            columns (attnotnull is no pg_constraint row): every one but the
            key's became nullable.
  pub       a replace dropped the destination out of its FOR TABLE
            publication (the membership belonged to the dropped OID), column
            list and row filter included: its subscribers went quiet.
  nullcursor a parallel read over an integer cursor ran closed ranges
            [min, max]: rows whose cursor is NULL matched no span and were
            never copied.
  negscale  numeric(5,-2) (PG15+, values up to 9999900) was declared
            Decimal(5, 0) in ClickHouse: out of its own precision.
  longname  a 64-byte destination name was cut to 63 by Postgres: every run
            looked for the full name, never found it, and re-bootstrapped.
  fkref     a replace into a table another table references failed at the
            swap's DROP, after the whole load was staged.
  keepalive pool sockets had no TCP keepalive: a half-open one parked a COPY
            read forever. `ss -o` shows the timer on apitap's connections.
  multigroup a per-table mode map whose later group failed to start raised
            one error and dropped the report of the group that had landed.
Usage: e2e_pg_hardening.py notnull|pub|nullcursor|negscale|longname|fkref|keepalive|multigroup
"""
import os
import subprocess
import time
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(c, db, sql):
    o = sh(["docker", "exec", c, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst_state_wipe(t):
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{t}', 'public.{t}'); END IF; END $$")


def notnull():
    t = "pg_replace_notnull"
    src(f"DROP TABLE IF EXISTS {t}")
    dst(f"DROP TABLE IF EXISTS {t}")
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, a int, b int); INSERT INTO {t} VALUES (1, 1, 1), (2, 2, 2)")
        dst(f"CREATE TABLE {t} (id int PRIMARY KEY, a int NOT NULL, b int)")
        apitap.transfer(PG, DST, table=t, mode="replace")
        got = dst(f"SELECT string_agg(attname || '=' || attnotnull, ',' ORDER BY attnum) FROM pg_attribute "
                  f"WHERE attrelid = '{t}'::regclass AND attnum > 0")
        assert got == "id=true,a=true,b=false", f"the replace must keep a's NOT NULL, got {got}"
        print(f"  replace over a NOT NULL column: {got}")
    finally:
        src(f"DROP TABLE IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {t}")


def pub():
    t, p = "pg_replace_pub", "pg_replace_pub_p"
    src(f"DROP TABLE IF EXISTS {t}")
    dst(f"DROP PUBLICATION IF EXISTS {p}")
    dst(f"DROP TABLE IF EXISTS {t}")
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, a int, b int); INSERT INTO {t} VALUES (1, 1, 1), (2, 2, 2)")
        dst(f"CREATE TABLE {t} (id int PRIMARY KEY, a int, b int)")
        dst(f"CREATE PUBLICATION {p} FOR TABLE {t} (id, a) WHERE (a > 1)")
        apitap.transfer(PG, DST, table=t, mode="replace")
        got = dst(f"SELECT array_to_string(attnames, ' ') || ' | ' || coalesce(rowfilter, '') "
                  f"FROM pg_publication_tables WHERE pubname = '{p}' AND tablename = '{t}'")
        assert got == "id a | (a > 1)", f"the replace must keep the table in {p} with its columns and filter, got {got!r}"
        print(f"  replace of a published table: still in {p}: {got}")
    finally:
        dst(f"DROP PUBLICATION IF EXISTS {p}")
        src(f"DROP TABLE IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {t}")


def nullcursor():
    t = "pg_null_cursor"
    src(f"DROP TABLE IF EXISTS {t}")
    dst(f"DROP TABLE IF EXISTS {t}")
    dst_state_wipe(t)
    try:
        src(f"CREATE TABLE {t} (n int, c int); "
            f"INSERT INTO {t} SELECT g, CASE WHEN g % 10 = 0 THEN NULL ELSE g END FROM generate_series(1, 1000) g")
        apitap.transfer(PG, DST, table=t, mode="append", cursor="c", parallel=4)
        got = dst(f"SELECT count(*) || ':' || count(c) FROM {t}")
        assert got == "1000:900", f"every row, NULL cursors included (rows:non-null), got {got}"
        print(f"  a parallel read over a cursor with 100 NULLs: {got}")
    finally:
        src(f"DROP TABLE IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {t}")
        dst_state_wipe(t)


def negscale():
    t = "pg_numeric_negscale"
    src(f"DROP TABLE IF EXISTS {t}")
    ch(f"DROP TABLE IF EXISTS {t}")
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, v numeric(5,-2), w numeric(2,5)); "
            f"INSERT INTO {t} VALUES (1, 9999900, 0.00012), (2, -1234500, -0.00099)")
        apitap.transfer(PG, CH, table=t, mode="replace")
        ty = ch(f"SELECT arrayStringConcat(groupArray(type), ',') FROM (SELECT type FROM system.columns "
                f"WHERE database = 'default' AND table = '{t}' AND name IN ('v', 'w') ORDER BY name)")
        assert ty == "Nullable(Decimal(7, 0)),Nullable(Decimal(5, 5))", f"numeric(5,-2) / numeric(2,5) as Decimal(7,0) / Decimal(5,5), got {ty}"
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(v), '/', toString(w))), ',') "
                 f"FROM (SELECT v, w FROM {t} ORDER BY id)")
        assert got == "9999900/0.00012,-1234500/-0.00099", f"values as the source holds them, got {got}"
        print(f"  numeric(5,-2), numeric(2,5) -> {ty}: {got}")
    finally:
        src(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")


def longname():
    t, long = "pg_long_dest_src", "x" * 64
    src(f"DROP TABLE IF EXISTS {t}")
    dst(f"DROP TABLE IF EXISTS {long[:63]}")
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY); INSERT INTO {t} SELECT generate_series(1, 10)")
        try:
            apitap.transfer(PG, DST, table=t, dest_table=long, mode="append", cursor="id")
            m = None
        except Exception as e:  # noqa: BLE001
            m = str(e)
        made = dst(f"SELECT count(*) FROM pg_class WHERE relname = '{long[:63]}'")
        assert m and "64 bytes" in m, f"a 64-byte destination must be refused, got {m!r} (cut table exists: {made})"
        assert made == "0", "nothing may be created under the cut name"
        print(f"  dest_table of 64 bytes: refused ({m[:70]}...)")
    finally:
        src(f"DROP TABLE IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {long[:63]}")
        dst_state_wipe(long)


def fkref():
    p, c = "pg_fk_parent", "pg_fk_child"
    src(f"DROP TABLE IF EXISTS {p}")
    dst(f"DROP TABLE IF EXISTS {c}; DROP TABLE IF EXISTS {p}")
    try:
        src(f"CREATE TABLE {p} (id int PRIMARY KEY); INSERT INTO {p} SELECT generate_series(1, 10)")
        dst(f"CREATE TABLE {p} (id int PRIMARY KEY); CREATE TABLE {c} (pid int REFERENCES {p})")
        try:
            apitap.transfer(PG, DST, table=p, mode="replace")
            m = None
        except Exception as e:  # noqa: BLE001
            m = str(e)
        staged = dst("SELECT count(*) FROM pg_class WHERE relname LIKE '%apitap_staging%'")
        assert m and "Nothing has been copied" in m and c in m, f"refused before the load, naming {c}: {m!r}"
        assert staged == "0", f"no staging table may exist: {staged}"
        print(f"  replace into a referenced table: refused up front ({m[:70]}...)")
    finally:
        src(f"DROP TABLE IF EXISTS {p}")
        dst(f"DROP TABLE IF EXISTS {c}; DROP TABLE IF EXISTS {p}")


def keepalive():
    import threading
    t = "pg_keepalive_v"
    src(f"DROP VIEW IF EXISTS {t}")
    dst(f"DROP TABLE IF EXISTS {t}")
    try:
        # A slow source (about 8 s): the connections stay open while we look.
        src(f"CREATE VIEW {t} AS SELECT g AS id, (SELECT 1 FROM pg_sleep(0.002 + g * 0)) AS s FROM generate_series(1, 4000) g")
        err = []
        th = threading.Thread(target=lambda: _try(lambda: apitap.transfer(PG, DST, table=t, mode="replace"), err))
        th.start()
        time.sleep(3)
        me = os.getpid()
        ss = sh(["ss", "-tnoip", "state", "established"]).stdout
        ours = [line for line in ss.splitlines()
                if f"pid={me}," in line and (":5544 " in line or ":5545 " in line)]
        th.join()
        assert not err, err
        assert ours, f"no connection of this process to Postgres found:\n{ss[:600]}"
        timed = [line for line in ours if "timer:(keepalive" in line]
        assert len(timed) == len(ours), \
            f"every pool socket must carry a keepalive timer ({len(timed)} of {len(ours)}):\n" + "\n".join(ours[:4])
        print(f"  {len(ours)} pool socket(s) mid-run, each with timer:(keepalive,...)")
    finally:
        src(f"DROP VIEW IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {t}")


def multigroup():
    ok, missing = "pg_multi_ok", "pg_multi_no_such_table"
    src(f"DROP TABLE IF EXISTS {ok}")
    dst(f"DROP TABLE IF EXISTS {ok}")
    try:
        src(f"CREATE TABLE {ok} (id int PRIMARY KEY); INSERT INTO {ok} SELECT generate_series(1, 7)")
        try:
            apitap.transfer(PG, DST, tables={ok: "replace", missing: "append"})
            raise AssertionError("a missing table must fail its group")
        except apitap.MultiTransferError as e:
            by = {t.table: t for t in e.report.tables}
        except Exception as e:  # noqa: BLE001
            raise AssertionError(f"the landed group must be reported, got a bare {type(e).__name__}: {e}")
        assert by[ok].error is None and by[ok].rows == 7, f"{ok} landed and says so: {by[ok]}"
        assert by[missing].error, f"{missing} carries its group's error: {by[missing]}"
        assert dst(f"SELECT count(*) FROM {ok}") == "7"
        print(f"  replace + a failing append group: {ok} reported 7 rows, {missing} its error")
    finally:
        src(f"DROP TABLE IF EXISTS {ok}")
        dst(f"DROP TABLE IF EXISTS {ok}")


def _try(f, err):
    try:
        f()
    except Exception as e:  # noqa: BLE001
        err.append(str(e))


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"notnull": notnull, "pub": pub, "nullcursor": nullcursor, "negscale": negscale, "longname": longname, "fkref": fkref, "keepalive": keepalive, "multigroup": multigroup}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_pg_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ Postgres hardening: {which[0]}")
