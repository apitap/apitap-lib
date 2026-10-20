"""A changelog keeps a TOASTed value that an earlier update in the window set.

When an UPDATE leaves an out-of-line PRIMARY KEY alone, pgoutput ships the
key cell as unchanged-TOAST. The changelog read that as key `[""]`, took the
update for a move (with a fake D half), and stored the row's resolved cells
under `[""]`. A second masked update of the same row in the window then
found nothing in the window and took the destination's PRE-window value:
`<table>__current` silently reverted a column the source had changed (deep
review D7).

Proof: pg -> ch, changelog=True, a text key stored EXTERNAL (~2.4 KB,
incompressible) and an EXTERNAL body. One window: `body` set to a new
out-of-line value, then `v` alone. __current must hold the new body.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_changelog_ext_key"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP VIEW IF EXISTS {T}__current")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def fp_src():
    return pg(f"SELECT md5(id) || '|' || v || '|' || md5(body) FROM {T}")


def fp_dst():
    return ch(f"SELECT concat(lower(hex(MD5(id))), '|', toString(v), '|', lower(hex(MD5(body)))) "
              f"FROM {T}__current FORMAT TSV")


def main():
    wipe()
    try:
        # md5 chains do not compress: the key and both bodies stay out of line.
        big = "(SELECT string_agg(md5(i::text || '{tag}'), '') FROM generate_series(1, {n}) i)"
        pg(f"CREATE TABLE {T} (id text PRIMARY KEY, v int, body text)")
        pg(f"ALTER TABLE {T} ALTER COLUMN id SET STORAGE EXTERNAL, ALTER COLUMN body SET STORAGE EXTERNAL")
        pg(f"INSERT INTO {T} VALUES ({big.format(tag='k', n=75)}, 0, {big.format(tag='old', n=200)})")
        assert pg(f"SELECT pg_column_size(id) > 2000 AND pg_relation_size(reltoastrelid) > 0 "
                  f"FROM {T}, pg_class WHERE relname = '{T}'") == "t", "(rig) the key must be out of line"
        apitap.transfer(PG, CH, table=T, mode="log_based", changelog=True)
        assert fp_dst() == fp_src(), "(rig) bootstrap differs"

        pg(f"BEGIN; UPDATE {T} SET body = {big.format(tag='new', n=200)}, v = 1; "
           f"UPDATE {T} SET v = 2; COMMIT")
        apitap.transfer(PG, CH, table=T, mode="log_based", changelog=True)
        s, d = fp_src(), fp_dst()
        assert s == d, f"{T}__current after two updates: {d} vs source {s} (body reverted?)"
        print(f"  two updates of an out-of-line key in one window: __current equals the source")
    finally:
        wipe()
    print("✓ a changelog keeps the value an earlier update in the window set")


if __name__ == "__main__":
    main()
