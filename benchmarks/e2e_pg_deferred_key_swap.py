"""A key swap on a DEFERRABLE primary key keeps both rows.

`UPDATE t SET id = 3 - id` on a DEFERRABLE key reaches the WAL as 1→2 then
2→1: for a moment two rows share key 2. The collapser read the second "old
key 2" as the row it had just moved there and nulled it — the destination
lost row r1 on a green run (MASTER review M-56, reproduced). A deferrable key
cannot be a replica identity, so such a table runs REPLICA IDENTITY FULL; the
collapser now compares the full old image with the row it holds at that key.

Proof (Postgres → ClickHouse): a swap and a 3-cycle rotation on a DEFERRABLE
key under REPLICA IDENTITY FULL, then a plain key chain on a non-deferrable
key (the control the old reading was right for). Each destination equals its
source, row for row.
"""
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
DEF, CHAIN = "dks_def", "dks_chain"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    for pub in set(pg("SELECT pubname FROM pg_publication_tables "
                      f"WHERE tablename IN ('{DEF}', '{CHAIN}')").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    for t in (DEF, CHAIN):
        pg(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")
        if ch("SELECT count() FROM system.tables WHERE database='default' "
              "AND name='_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{t}' SETTINGS mutations_sync=1")


def run(t):
    code = (f"import apitap\nr = apitap.transfer({PG!r}, {CH!r}, table={t!r}, mode='log_based')\n"
            "print(r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def same(t):
    src = pg(f"SELECT coalesce(string_agg(id || ':' || v, ',' ORDER BY id), '') FROM {t}")
    dst = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', v)), ',') "
             f"FROM (SELECT id, v FROM {t} ORDER BY id)")
    return src, dst


def main():
    wipe()
    pg(f"CREATE TABLE {DEF} (id int PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE, v text);"
       f"ALTER TABLE {DEF} REPLICA IDENTITY FULL;"
       f"INSERT INTO {DEF} SELECT g, 'r' || g FROM generate_series(1, 9) g")
    pg(f"CREATE TABLE {CHAIN} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {CHAIN} SELECT g, 'r' || g FROM generate_series(1, 9) g")
    for t in (DEF, CHAIN):
        r = run(t)
        assert r.returncode == 0, f"{t} bootstrap:\n{r.stderr[-800:]}"

    pg(f"UPDATE {DEF} SET id = 3 - id WHERE id IN (1, 2)")                        # swap
    pg(f"UPDATE {DEF} SET id = CASE id WHEN 4 THEN 5 WHEN 5 THEN 6 ELSE 4 END "
       f"WHERE id IN (4, 5, 6)")                                                 # rotation
    pg(f"UPDATE {CHAIN} SET id = 20 WHERE id = 1; UPDATE {CHAIN} SET id = 21 WHERE id = 20")
    for t, what in ((DEF, "deferred swap + rotation"), (CHAIN, "key chain (control)")):
        r = run(t)
        assert r.returncode == 0, f"{t} drain:\n{r.stderr[-800:]}"
        src, dst = same(t)
        assert src == dst, f"{what}: destination {dst!r} != source {src!r}"
        print(f"  {what}: {src}")
    wipe()
    print("✓ deferred-key swaps keep every row; a plain key chain still moves one")


if __name__ == "__main__":
    main()
