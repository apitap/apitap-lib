"""A ClickHouse changelog window with thousands of masked rows applies.

changelog=True reads back the pre-window values of every masked TOAST cell
in one statement. Each key predicate is written twice (the WHERE and the
slot multiIf), so a single UPDATE over ~7,000 rows that left the TOASTed
column alone produced a statement past ClickHouse's max_query_size (256
KiB): "Max query size exceeded", on every replay of the same window, and the
lane never moved again (deep review D1).

Proof: pg -> ch, changelog=True, a STORAGE EXTERNAL text column; one UPDATE
touches 20,000 rows without touching it. The drain must succeed and
<table>__current must equal the source, md5 of the TOASTed value included.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_changelog_mass_mask"
N = 20000


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


def fp_src():
    return pg(f"SELECT count(*) || '|' || sum(v) || '|' || md5(string_agg(md5(body), '' ORDER BY id)) FROM {T}")


def fp_dst():
    return ch(f"SELECT concat(toString(count()), '|', toString(sum(v)), '|', "
              f"lower(hex(MD5(arrayStringConcat(arrayMap(x -> lower(hex(MD5(x.2))), "
              f"arraySort(x -> x.1, groupArray((id, body)))), ''))))) FROM {T}__current")


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


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v int, body text)")
    pg(f"ALTER TABLE {T} ALTER COLUMN body SET STORAGE EXTERNAL")
    pg(f"INSERT INTO {T} SELECT g, 0, repeat(md5(g::text), 80) FROM generate_series(1, {N}) g")
    assert pg(f"SELECT pg_relation_size(reltoastrelid) > 0 FROM pg_class WHERE relname = '{T}'") == "t", \
        "(rig) body is not stored out of line; the masked path would not run"
    try:
        apitap.transfer(PG, CH, table=T, mode="log_based", changelog=True)
        assert fp_dst() == fp_src(), "bootstrap differs"
        pg(f"UPDATE {T} SET v = v + 1")
        apitap.transfer(PG, CH, table=T, mode="log_based", changelog=True)
        s, d = fp_src(), fp_dst()
        assert s == d, f"after the mass masked update: source {s} vs destination {d}"
        print(f"  {N:,} masked rows in one window: applied, {T}__current equals the source")
    finally:
        wipe()
    print("✓ a changelog window with thousands of masked rows applies")


if __name__ == "__main__":
    main()
