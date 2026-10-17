"""Postgres CDC of a mixed-case table whose primary key has INCLUDE columns.

The drain resolved the table and looked its key up with `$1::regclass` on
unquoted text: `public.CdcMixed` case-folded to `public.cdcmixed` — with a
lower-case twin present, the drain tracked the TWIN while the bootstrap (which
quotes) copied CdcMixed. And `indkey` lists
a PRIMARY KEY's INCLUDE columns too, which were taken for key columns
(MASTER review M-60).

Proof: a lower-case twin `cdcmixed` keyed on a different column sits beside
`"CdcMixed" (id PRIMARY KEY INCLUDE (v))`; bootstrap, insert/update/delete,
drain into ClickHouse twice — the destination equals the source, keyed on id.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = 'CdcMixed'


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    for pub in set(pg("SELECT pubname FROM pg_publication_tables WHERE tablename IN ('CdcMixed', 'cdcmixed')").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg('DROP TABLE IF EXISTS "CdcMixed"; DROP TABLE IF EXISTS cdcmixed')
    for n in ch("SELECT name FROM system.tables WHERE database = 'default' AND lower(name) LIKE 'cdcmixed%'").split():
        ch(f"DROP TABLE IF EXISTS `{n}`")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch("ALTER TABLE _apitap_state DELETE WHERE lower(dest_table) LIKE 'cdcmixed%' SETTINGS mutations_sync = 1")


def drain():
    return apitap.transfer(PG, CH, table="public.CdcMixed", dest_table=T, mode="log_based")


def agree(stage):
    s = pg('SELECT coalesce(string_agg(id || \':\' || v, \',\' ORDER BY id), \'\') FROM "CdcMixed"')
    d = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', v)), ',') "
           f"FROM (SELECT id, v FROM `{T}` ORDER BY id)")
    assert s == d, f"{stage}: source {s!r} vs destination {d!r}"
    print(f"  {stage}: equal ({s})")


def main():
    wipe()
    pg("CREATE TABLE cdcmixed (other int PRIMARY KEY, id int, v text)")
    pg('CREATE TABLE "CdcMixed" (id int, v text, PRIMARY KEY (id) INCLUDE (v))')
    pg('INSERT INTO "CdcMixed" VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')')
    drain()
    agree("bootstrap")
    pg('UPDATE "CdcMixed" SET v = \'B\' WHERE id = 2; DELETE FROM "CdcMixed" WHERE id = 1; '
       'INSERT INTO "CdcMixed" VALUES (4, \'d\')')
    drain()
    agree("drain 1")
    pg('UPDATE "CdcMixed" SET v = v || \'!\'')
    drain()
    agree("drain 2")
    wipe()
    print("✓ Postgres CDC of a mixed-case table keyed with INCLUDE columns is exact")


if __name__ == "__main__":
    main()
