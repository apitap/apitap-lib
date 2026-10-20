"""A Postgres replace restores every DDL statement it can, even when one fails.

A replace swaps in a fresh table and then re-applies the old one's DDL:
NOT NULLs, constraints (FKs last), indexes, grants, publication membership.
The loop stopped at the first failure, so an FK the new rows break left the
index and the grant behind it unrestored, and the next replace captured the
degraded table: gone for good (deep review D24).

Proof (pg -> pg replace): the destination `orders` has an FK to
`customers`, an index and a GRANT; the source holds an order whose customer
the destination does not have. The run must fail naming the FK, and the
index and the grant must be back.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_restore_ddl_orders"
P = "pg_restore_ddl_customers"
ROLE = "restore_ddl_reader"


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
    dst(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {P}")
    dst(f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN "
        f"DROP ROLE {ROLE}; END IF; END $$")
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def facts():
    idx = dst(f"SELECT count(*) FROM pg_indexes WHERE tablename = '{T}' AND indexname = '{T}_cust_idx'")
    grant = dst(f"SELECT has_table_privilege('{ROLE}', 'public.{T}', 'SELECT')")
    return idx, grant


def main():
    wipe()
    try:
        src(f"CREATE TABLE {T} (id int PRIMARY KEY, customer_id int); INSERT INTO {T} VALUES (1, 1), (2, 999)")
        dst(f"CREATE TABLE {P} (id int PRIMARY KEY); INSERT INTO {P} VALUES (1); "
            f"CREATE TABLE {T} (id int PRIMARY KEY, customer_id int REFERENCES {P}(id)); "
            f"INSERT INTO {T} VALUES (1, 1); CREATE INDEX {T}_cust_idx ON {T} (customer_id); "
            f"CREATE ROLE {ROLE}; GRANT SELECT ON {T} TO {ROLE}")
        assert facts() == ("1", "t"), f"(rig) the destination starts with its index and grant: {facts()}"
        try:
            apitap.transfer(PG, DST, table=T, mode="replace")
            raise AssertionError("(rig) restoring the FK must fail: the source has an orphan order")
        except AssertionError:
            raise
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "FOREIGN KEY" in msg, f"failed for the wrong reason: {msg}"
            print(f"  the FK restore fails as it must: {msg[:110]}...")
        assert dst(f"SELECT count(*) FROM {T}") == "2", "the rows must have landed"
        idx, grant = facts()
        assert idx == "1", "the index behind the failed FK was never restored"
        assert grant == "t", "the GRANT behind the failed FK was never restored"
        print("  the index and the GRANT behind it are restored")
    finally:
        wipe()
    print("✓ a Postgres replace restores every DDL statement it can")


if __name__ == "__main__":
    main()
