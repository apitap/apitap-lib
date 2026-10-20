"""A key deleted and re-inserted in one window must not wedge a MySQL destination.

The CDC apply loads every key it clears into the `_ap_del` twin: the window's
deletes, then the key of every upsert. A key deleted and re-inserted in one
window is in both sets. Since the twin got a PRIMARY KEY (MASTER M-52), the
second copy was skipped by LOAD DATA LOCAL with Warning 1062, `no_warnings`
turned that into an error, and the same window was cut on every rerun: the
lane was stuck on ordinary input (deep review D2).

Proof: one source transaction deletes and re-inserts two keys and updates a
third; the drain must succeed, a second drain must succeed too (no replay
residue), and the destination must equal the source.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_del_reinsert"


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


def same(stage):
    s = pg(f"SELECT coalesce(string_agg(id || ':' || v, ',' ORDER BY id), '') FROM {T}")
    d = my(f"SELECT COALESCE(GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ','), '') FROM {T}")
    assert s == d, f"{stage}: source {s!r} vs destination {d!r}"
    print(f"  {stage}: equal ({len(s.split(','))} rows)")


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    my(f"DROP TABLE IF EXISTS {T}")
    if my("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bench' "
          "AND table_name = '_apitap_state'") != "0":
        my(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); "
       f"INSERT INTO {T} SELECT g, 'seed' FROM generate_series(1, 10) g")
    try:
        apitap.transfer(PG, MY, table=T, mode="log_based")
        same("bootstrap")
        pg(f"BEGIN; DELETE FROM {T} WHERE id IN (1, 2); "
           f"INSERT INTO {T} VALUES (1, 'again'), (2, 'again'); "
           f"UPDATE {T} SET v = 'u' WHERE id = 3; COMMIT")
        apitap.transfer(PG, MY, table=T, mode="log_based")
        same("delete + re-insert in one window")
        apitap.transfer(PG, MY, table=T, mode="log_based")
        same("the next drain")
    finally:
        wipe()
    print("✓ a key deleted and re-inserted in one window applies to a MySQL destination")


if __name__ == "__main__":
    main()
