"""A parallel Postgres read survives idle_in_transaction_session_timeout.

Every span of a parallel read imports one exported snapshot (MASTER M-18).
The connection that exports it sends nothing until the last span is done, so
the server sees it idle in transaction for the whole read. A role or database
idle_in_transaction_session_timeout, common on managed servers, killed it; the
snapshot went with it and every later span failed "invalid snapshot
identifier", late and on every retry (deep review D31).

Proof: a role whose idle_in_transaction_session_timeout is 2 s reads a
10M-row table with parallel=4 (several seconds, 24 spans). The read must
finish with every row; the role and the destination are dropped after.
"""
import subprocess

import apitap

ROLE = "apitap_itx"
SRC = f"postgres://{ROLE}:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
TABLE = "bench_data_10m"
T = "pg_snapshot_idle_probe"


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
    dst(f"DROP TABLE IF EXISTS {T}")
    src(f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN "
        f"REVOKE SELECT ON {TABLE} FROM {ROLE}; DROP ROLE {ROLE}; END IF; END $$")


def main():
    n = int(src(f"SELECT count(*) FROM {TABLE}"))
    assert n >= 5_000_000, f"(rig) {TABLE} must be the 10M seed, has {n} rows"
    cleanup()
    src(f"CREATE ROLE {ROLE} LOGIN PASSWORD 'bench'")
    src(f"GRANT SELECT ON {TABLE} TO {ROLE}")
    src(f"ALTER ROLE {ROLE} SET idle_in_transaction_session_timeout = '2s'")
    try:
        got = src(f"SELECT setting FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole, "
                  f"unnest(s.setconfig) setting WHERE r.rolname = '{ROLE}'")
        assert "idle_in_transaction_session_timeout=2s" in got, f"(rig) role setting missing: {got!r}"
        r = apitap.transfer(SRC, DST, table=TABLE, dest_table=T, mode="replace", parallel=4)
        landed = int(dst(f"SELECT count(*) FROM {T}"))
        assert r.rows == n and landed == n, f"rows={r.rows} landed={landed}, want {n}"
        print(f"  parallel=4 read under a 2 s idle-in-transaction timeout: {landed:,} rows")
    finally:
        cleanup()
    print("✓ a parallel read survives idle_in_transaction_session_timeout")


if __name__ == "__main__":
    main()
