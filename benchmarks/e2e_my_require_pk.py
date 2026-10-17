"""A MySQL destination with sql_require_primary_key=ON (the DigitalOcean default).

Every MySQL-destination run created its lock marker as a key-less MEMORY
table, which the server refuses under sql_require_primary_key=ON (ERROR
3750) — the run failed at its first statement. A key-less source's staging
table and the CDC apply's key temp table had no key either (MASTER review
M-52).

Proof (global sql_require_primary_key=ON on the destination for the leg's
duration, restored after): replace and append of a keyed table and of a
key-less one, and a CDC drain with updates and deletes — every run lands, the
rows equal the source, and the key-less table's generated key stays invisible.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
TK, TN, TC = "my_rpk_keyed", "my_rpk_keyless", "my_rpk_cdc"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my-dst", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def same(t, stage):
    s = pg(f"SELECT coalesce(string_agg(id || ':' || v, ',' ORDER BY id, v), '') FROM {t}")
    d = my(f"SELECT COALESCE(GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id, v SEPARATOR ','), '') FROM {t}")
    assert s == d, f"{t} {stage}: source {s!r} vs destination {d!r}"
    print(f"  {t} {stage}: equal")


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{TC}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    for t in (TK, TN, TC):
        pg(f"DROP TABLE IF EXISTS {t}")
        my(f"DROP TABLE IF EXISTS {t}")
    my(f"DELETE FROM _apitap_state WHERE dest_table IN ('{TK}', '{TN}', '{TC}')") if my(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bench' "
        "AND table_name = '_apitap_state'") != "0" else None


def main():
    was = my("SELECT @@global.sql_require_primary_key")
    wipe()
    pg(f"CREATE TABLE {TK} (id int PRIMARY KEY, v text); INSERT INTO {TK} SELECT g, 'k' FROM generate_series(1, 20) g")
    pg(f"CREATE TABLE {TN} (id int, v text); INSERT INTO {TN} SELECT g % 5, 'n' FROM generate_series(1, 20) g")
    pg(f"CREATE TABLE {TC} (id int PRIMARY KEY, v text); INSERT INTO {TC} SELECT g, 'c' FROM generate_series(1, 20) g")
    try:
        my("SET GLOBAL sql_require_primary_key = ON")
        for t in (TK, TN):
            apitap.transfer(PG, MY, table=t, mode="replace")
            same(t, "replace")
            pg(f"INSERT INTO {t} VALUES (100, 'later')")
            apitap.transfer(PG, MY, table=t, mode="append", cursor="id")
            same(t, "append")
        cols = my(f"SELECT GROUP_CONCAT(column_name) FROM information_schema.columns "
                  f"WHERE table_schema = 'bench' AND table_name = '{TN}' AND extra NOT LIKE '%INVISIBLE%'")
        assert cols in ("id,v", "v,id"), f"the generated key must stay invisible, visible columns {cols}"
        print(f"  {TN}: visible columns {cols}")

        apitap.transfer(PG, MY, table=TC, mode="log_based")
        pg(f"UPDATE {TC} SET v = 'u' WHERE id <= 5; DELETE FROM {TC} WHERE id > 15; INSERT INTO {TC} VALUES (50, 'i')")
        apitap.transfer(PG, MY, table=TC, mode="log_based")
        same(TC, "CDC drain with updates and deletes")
    finally:
        my(f"SET GLOBAL sql_require_primary_key = {was}")
        wipe()
    print("✓ a MySQL destination with sql_require_primary_key=ON takes every lane")


if __name__ == "__main__":
    main()
