"""A Postgres CDC bootstrap survives idle_in_transaction_session_timeout.

A log_based first run creates its slot with EXPORT_SNAPSHOT and full-loads
every table pinned to that snapshot. The walsender that exported it then
sits idle in that transaction for the whole load, and a role or database
idle_in_transaction_session_timeout (common on managed servers) killed it:
the next table's load failed "invalid snapshot identifier", the bootstrap
dropped its slot, and every retry failed the same way (deep review 2, N8;
D31 had fixed only the bulk read's own holder).

Proof (pg -> pg log_based, first run of a two-table group, as a role whose
idle_in_transaction_session_timeout is 2 s): each table takes longer than
2 s to load, so the second imports the snapshot after the walsender has
been idle past the limit. Both tables must land whole.
"""
import subprocess

import apitap

ROLE = "apitap_itx_cdc"
SRC = f"postgres://{ROLE}:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
TABLES = ["pg_cdc_idle_a", "pg_cdc_idle_b"]
N = 2_000_000


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db,
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def cleanup():
    for slot in src(f"SELECT slot_name FROM pg_replication_slots WHERE NOT active AND slot_name IN "
                    f"(SELECT replace(pubname, '_pub', '') FROM pg_publication_tables "
                    f"WHERE tablename IN ('{TABLES[0]}', '{TABLES[1]}'))").split():
        src(f"SELECT pg_drop_replication_slot('{slot}')")
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables "
                       f"WHERE tablename IN ('{TABLES[0]}', '{TABLES[1]}')").split()):
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    for t in TABLES:
        src(f"DROP TABLE IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {t}")
        dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
            f"DELETE FROM public._apitap_state WHERE dest_table IN ('{t}', 'public.{t}'); END IF; END $$")
    src(f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN "
        f"REVOKE CREATE ON DATABASE apitap_bench_src FROM {ROLE}; DROP ROLE {ROLE}; END IF; END $$")


def main():
    cleanup()
    try:
        src(f"CREATE ROLE {ROLE} LOGIN REPLICATION PASSWORD 'bench'")
        src(f"GRANT CREATE ON DATABASE apitap_bench_src TO {ROLE}")
        for t in TABLES:
            src(f"CREATE TABLE {t} (id int PRIMARY KEY, v text); "
                f"INSERT INTO {t} SELECT g, md5(g::text) || md5(g::text) FROM generate_series(1, {N}) g; "
                f"ALTER TABLE {t} OWNER TO {ROLE}")
        src(f"ALTER ROLE {ROLE} SET idle_in_transaction_session_timeout = '2s'")
        got = src(f"SELECT setting FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole, "
                  f"unnest(s.setconfig) setting WHERE r.rolname = '{ROLE}'")
        assert "idle_in_transaction_session_timeout=2s" in got, f"(rig) role setting missing: {got!r}"
        apitap.transfer(SRC, DST, tables=TABLES, mode="log_based")
        for t in TABLES:
            n = int(dst(f"SELECT count(*) FROM {t}"))
            assert n == N, f"{t} landed {n:,} rows, want {N:,}"
        print(f"  two-table CDC bootstrap under a 2 s idle-in-transaction timeout: {N:,} rows each")
    finally:
        cleanup()
    print("✓ a Postgres CDC bootstrap survives idle_in_transaction_session_timeout")


if __name__ == "__main__":
    main()
