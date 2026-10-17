"""An unqualified Postgres destination stays in the schema it lives in.

With search_path = "$user", public, a schema named after the user, and the
table in public, the guard/lease/state resolved the table to public while
the staging and final names were left unqualified and followed search_path:
staging was created in "$user", the replace dropped public.t, and the table
moved schemas — every reader of public.t lost it (MASTER review M-19).

Proof: in a database with that search_path, replace an unqualified table
that lives in public — twice, and an append — and it is still in public,
holding the new rows, with nothing created in the user's schema.
"""
import subprocess

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DB = "qa_search_path"
DST = f"postgres://postgres:bench@127.0.0.1:5545/{DB}"
T = "pg_sp_tbl"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql, db=DB):
    return psql("apitap-bench-pg-dst", db, sql)


def where():
    return dst(f"SELECT coalesce(string_agg(schemaname || '.' || tablename, ',' ORDER BY 1), '') "
               f"FROM pg_tables WHERE tablename LIKE '{T}%'")


def main():
    if dst(f"SELECT count(*) FROM pg_database WHERE datname = '{DB}'", db="postgres") == "0":
        dst(f"CREATE DATABASE {DB}", db="postgres")
    dst(f"ALTER DATABASE {DB} SET search_path = \"$user\", public", db="postgres")
    dst("CREATE SCHEMA IF NOT EXISTS postgres")
    assert dst("SELECT current_schema()") == "postgres", "the user's schema must be first on the path"
    src(f"DROP TABLE IF EXISTS {T}")
    for s in ("public", "postgres"):
        dst(f"DROP TABLE IF EXISTS {s}.{T}")

    dst(f"CREATE TABLE public.{T} (id int PRIMARY KEY, v text)")
    dst(f"INSERT INTO public.{T} VALUES (1, 'old')")
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} SELECT g, 'new' FROM generate_series(1, 5) g")
    for i in (1, 2):
        apitap.transfer(SRC, DST, table=T, mode="replace")
        assert where() == f"public.{T}", f"replace {i}: the table must stay in public, found {where()!r}"
        assert dst(f"SELECT count(*) || ':' || max(v) FROM public.{T}") == "5:new"
    print("  replace x2: still public.%s, 5 new rows, nothing in schema postgres" % T)

    src(f"INSERT INTO {T} VALUES (6, 'new')")
    apitap.transfer(SRC, DST, table=T, mode="append", cursor="id")
    assert where() == f"public.{T}", f"append: found {where()!r}"
    assert dst(f"SELECT count(*) FROM public.{T}") == "6", "append lands the new row in public"
    print("  append: lands in public")

    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS public.{T}")
    dst(f"DELETE FROM public._apitap_state WHERE dest_table LIKE '%{T}'") if dst(
        "SELECT to_regclass('public._apitap_state') IS NOT NULL") == "t" else None
    print("✓ an unqualified Postgres destination stays in the schema it lives in")


if __name__ == "__main__":
    main()
