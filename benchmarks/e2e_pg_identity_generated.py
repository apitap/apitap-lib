"""A Postgres append and merge land in a table with identity and generated columns.

A destination pre-created from the source's schema (pg_dump --schema-only,
an app migration) can have `id GENERATED ALWAYS AS IDENTITY` and a STORED
generated column. The finalize INSERT carried neither OVERRIDING SYSTEM
VALUE nor a column list without the generated column: every run staged the
whole delta and then failed with 428C9 (deep review 2, N20).

Proof (pg -> pg, cursor=id): append, then merge, into such a table. Every
source row lands with its own id, and the generated column computes.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_identity_generated"


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


def wipe():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def rows():
    return dst(f"SELECT string_agg(id || ':' || qty || ':' || total, ',' ORDER BY id) FROM {T}")


def main():
    try:
        for mode in ("append", "merge"):
            wipe()
            # The destination is the source's own schema, as pg_dump makes it.
            src(f"CREATE TABLE {T} (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, qty int, price int, "
                f"total int GENERATED ALWAYS AS (qty * price) STORED); "
                f"INSERT INTO {T} (id, qty, price) OVERRIDING SYSTEM VALUE VALUES (10, 1, 5), (20, 2, 5)")
            dst(f"CREATE TABLE {T} (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, qty int, price int, "
                f"total int GENERATED ALWAYS AS (qty * price) STORED)")
            apitap.transfer(PG, DST, table=T, mode=mode, cursor="id")
            got = rows()
            assert got == "10:1:5,20:2:10", f"[{mode}] destination {got!r}, want the source ids and computed totals"
            print(f"  [{mode}] into identity + generated columns: {got}")
    finally:
        wipe()
    print("✓ a Postgres append and merge land in a table with identity and generated columns")


if __name__ == "__main__":
    main()
