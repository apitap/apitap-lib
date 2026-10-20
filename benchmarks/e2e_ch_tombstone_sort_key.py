"""A Replacing destination whose ORDER BY is not the key never takes the tombstone path.

`engine="ReplacingMergeTree(_apitap_ver, _apitap_deleted)"` switches the
apply to tombstones: a delete or update is an INSERT of a newer version,
and the engine collapses rows that share a SORTING KEY. With
`order_by="tenant_id, id"` over key id, a tombstone carries id only, lands
at (tenant_id DEFAULT, id) and never meets the live row: the delete stays
visible under FINAL, and a tenant change leaves two rows for one key,
silently (deep review D9). The shape check read only the engine string.

Proof (pg -> ch log_based, that engine and ORDER BY): bootstrap 10 rows,
then DELETE id=5 and move id=6 to another tenant. FINAL must equal the
source: 9 rows, id 6 once, at its new tenant.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_tombstone_sort_key"
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"


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


def src():
    return pg(f"SELECT string_agg(id || ':' || tenant_id, ',' ORDER BY id) FROM {T}")


def dst():
    return ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', toString(tenant_id))), ',') "
              f"FROM (SELECT id, tenant_id FROM {T} FINAL WHERE _apitap_deleted = 0 ORDER BY id, tenant_id)")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, tenant_id int NOT NULL, v text); "
           f"INSERT INTO {T} SELECT g, 42, 'v' FROM generate_series(1, 10) g")
        apitap.transfer(PG, CH, table=T, mode="log_based", engine=ENGINE, order_by="tenant_id, id")
        key = ch(f"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = '{T}'")
        assert key == "tenant_id, id", f"(rig) the destination must order by (tenant_id, id), has {key!r}"
        assert dst() == src(), "(rig) bootstrap exact"

        pg(f"DELETE FROM {T} WHERE id = 5; UPDATE {T} SET tenant_id = 43 WHERE id = 6")
        apitap.transfer(PG, CH, table=T, mode="log_based", engine=ENGINE, order_by="tenant_id, id")
        s, d = src(), dst()
        assert d == s, f"FINAL after a delete and a tenant move: {d!r}, source {s!r}"
        print(f"  ORDER BY (tenant_id, id) over key id: FINAL equals the source ({d.count(',') + 1} rows)")
    finally:
        wipe()
    print("✓ a Replacing destination whose ORDER BY is not the key never takes the tombstone path")


if __name__ == "__main__":
    main()
