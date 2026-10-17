"""A Postgres CDC drain runs as a role that does not own the publication.

Every drain ran `ALTER PUBLICATION ... SET (publish_via_partition_root =
true)` on PG 13+, whether or not the option was already set — and ALTER
PUBLICATION needs ownership, so every drain was fatal for a REPLICATION role
that may read the publication but does not own it (P2). The ALTER now runs
only when the option is off.

Proof: bootstrap as the owner (which creates the publication), then drain as
a REPLICATION role with SELECT on the table: the drain lands the change.
"""
import subprocess

import apitap

OWNER = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
RO = "postgres://cdc_ro:ropw@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_nonowner_drain"


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
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    src("DROP ROLE IF EXISTS cdc_ro")
    dst(f"DROP TABLE IF EXISTS {T}")
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def main():
    wipe()
    try:
        src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} VALUES (1, 'a')")
        src("CREATE ROLE cdc_ro LOGIN REPLICATION PASSWORD 'ropw'")
        src(f"GRANT SELECT ON {T} TO cdc_ro")
        apitap.transfer(OWNER, DST, table=T, mode="log_based")
        src(f"INSERT INTO {T} VALUES (2, 'b')")
        apitap.transfer(RO, DST, table=T, mode="log_based")
        got = dst(f"SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM {T}")
        assert got == "1:a,2:b", f"the non-owner drain must land the change, got {got}"
        print(f"  drain as a REPLICATION role that does not own the publication: {got}")
    finally:
        wipe()
    print("✓ a Postgres CDC drain runs as a role that does not own the publication")


if __name__ == "__main__":
    main()
