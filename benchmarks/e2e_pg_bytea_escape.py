"""A Postgres CDC drain reads bytea as hex whatever bytea_output the server sets.

pgoutput renders a text-mode cell with the type's output function, and
byteaout reads the session's bytea_output. The replication session pinned
TimeZone, DateStyle, IntervalStyle and extra_float_digits, not bytea_output,
so a role or database with `bytea_output = 'escape'` (a legacy app setting)
streamed `\\000A` instead of `\\x0041`: the MySQL, Iceberg and BigQuery lanes
refused every window, and ClickHouse stored the other spelling (deep review
D33).

Proof (pg -> mysql, the text plane): bootstrap as the owner, then drain as
a REPLICATION role whose bytea_output is escape. The destination bytes must
equal the source bytes.
"""
import subprocess

import apitap

OWNER = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
ESC = "postgres://cdc_bya:byapw@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "pg_bytea_escape"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def src(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    src("DROP ROLE IF EXISTS cdc_bya")
    my(f"DROP TABLE IF EXISTS {T}")
    if my("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'bench' "
          "AND table_name = '_apitap_state'") != "0":
        my(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")


def main():
    wipe()
    try:
        src(f"CREATE TABLE {T} (id int PRIMARY KEY, b bytea); "
            f"INSERT INTO {T} VALUES (1, '\\x00410aff'::bytea)")
        src("CREATE ROLE cdc_bya LOGIN REPLICATION PASSWORD 'byapw'")
        src(f"GRANT SELECT ON {T} TO cdc_bya")
        src("ALTER ROLE cdc_bya SET bytea_output = 'escape'")
        apitap.transfer(OWNER, MY, table=T, mode="log_based")
        src(f"INSERT INTO {T} VALUES (2, '\\x5c0001fe'::bytea); UPDATE {T} SET b = '\\x7f80'::bytea WHERE id = 1")
        apitap.transfer(ESC, MY, table=T, mode="log_based")
        want = src(f"SELECT string_agg(id || ':' || encode(b, 'hex'), ',' ORDER BY id) FROM {T}")
        got = my(f"SELECT GROUP_CONCAT(CONCAT(id, ':', LOWER(HEX(b))) ORDER BY id) FROM {T}")
        assert got == want, f"destination {got!r}, source {want!r}"
        print(f"  drain under bytea_output=escape: {got}")
    finally:
        wipe()
    print("✓ a Postgres CDC drain reads bytea as hex whatever bytea_output the server sets")


if __name__ == "__main__":
    main()
