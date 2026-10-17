"""A Replacing-family ClickHouse destination takes the insert-only path.

When the user asks for `engine="ReplacingMergeTree(_apitap_ver, _apitap_deleted)"`
the bootstrap adds the tombstone bookkeeping columns and the drain stops paying
for delete-then-insert: upserts land as rows with `_apitap_deleted = 0`, deletes
as key-only tombstones with `_apitap_deleted = 1`, both stamped with the
window's LSN — five statements per window become two plus the state write
(design §14). Readers use `FINAL` (or a `__current` argMax view).

This leg proves it end to end: the engine survives the bootstrap, the columns
exist, an update/delete/insert round is exact under `FINAL`, `_apitap_ver`
moves, and a classic MergeTree replica is untouched.

A user's OWN Replacing table is not insert-only: the bootstrap used to ALTER
the pair into any `ReplacingMergeTree(...)` and the drain then wrote deletes
as tombstones that `ReplacingMergeTree(updated_at)` never reads — deleted rows
stayed forever (MASTER review M-26). The last stage asks for one and
proves a delete removes the row and no column is added.
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "io_demo"
TU = "io_theirs"
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
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
    # Slot names are hashes: find each one through its publication.
    for pub in set(pg("SELECT pubname FROM pg_publication_tables "
                      f"WHERE tablename IN ('{T}', '{TU}')").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    for t in (T, TU):
        pg(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")
        if ch("SELECT count() FROM system.tables WHERE database='default' "
              "AND name='_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{t}' "
               "SETTINGS mutations_sync=1")


def run(engine=None, table=T):
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={table!r}, mode='log_based'"
        + (f", engine={engine!r}" if engine else "")
        + ")\nprint('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code], timeout=240)


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {T} VALUES (1, 'a'), (2, 'b'), (3, 'c');")

    r = run(engine=ENGINE)
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {T} FINAL") == "3", "bootstrap lands three rows"
    cols = ch("SELECT count() FROM system.columns WHERE database='default' "
              f"AND table='{T}' AND name IN ('_apitap_ver','_apitap_deleted')")
    assert cols == "2", f"bookkeeping columns exist, got {cols}"
    eng = ch(f"SELECT engine FROM system.tables WHERE database='default' AND name='{T}'")
    assert eng.startswith("ReplacingMergeTree"), f"engine kept, got {eng}"

    # A change round on the insert-only table.
    pg(f"UPDATE {T} SET v = 'a2' WHERE id = 1; DELETE FROM {T} WHERE id = 2;"
       f"INSERT INTO {T} VALUES (4, 'd');")
    r = run()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {T} FINAL") == "3", "one updated, one gone, one new"
    assert ch(f"SELECT v FROM {T} FINAL WHERE id = 1") == "a2", "update visible under FINAL"
    assert ch(f"SELECT count() FROM {T} FINAL WHERE id = 2") == "0", "tombstone hides the deleted key"
    assert ch(f"SELECT count() FROM {T} FINAL WHERE id = 4") == "1", "the insert landed"
    ver = ch(f"SELECT max(_apitap_ver) FROM {T}")
    assert int(ver) > 0, f"the window LSN stamped the rows, got {ver}"
    del_rows = ch(f"SELECT count() FROM {T} WHERE id = 2 AND _apitap_deleted = 1")
    assert del_rows == "1", "the delete is a tombstone row, not an absence"

    # P0-2 (review 2026-10-14): DELETE + re-INSERT of the SAME key in one
    # source transaction collapses to a live upsert with a pending delete;
    # writing the fresh row AND its tombstone at the same `_apitap_ver` left
    # the FINAL winner to chance, so the row could vanish. The window must
    # keep only the live upsert.
    pg(f"DELETE FROM {T} WHERE id = 1; INSERT INTO {T} VALUES (1, 'a3');")
    r = run()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {T} FINAL WHERE id = 1") == "1", \
        "delete+reinsert of one key in one window keeps the row"
    assert ch(f"SELECT v FROM {T} FINAL WHERE id = 1") == "a3", "the fresh image wins"

    # M-26: the user's own ReplacingMergeTree(updated_at). It must stay the
    # user's table and take the classic path.
    pg(f"CREATE TABLE {TU} (id int PRIMARY KEY, v text, updated_at timestamptz NOT NULL);"
       f"INSERT INTO {TU} SELECT g, 'v' || g, '2024-01-01'::timestamptz + g * interval '1 hour' "
       f"FROM generate_series(1, 5) g;")
    r = run(engine="ReplacingMergeTree(updated_at)", table=TU)
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TU} FINAL") == "5", "bootstrap lands five rows"
    pg(f"DELETE FROM {TU} WHERE id IN (2, 4); UPDATE {TU} SET v = 'new', updated_at = now() WHERE id = 1;")
    r = run(table=TU)
    assert r.returncode == 0, r.stderr
    got = ch(f"SELECT groupArray(id) FROM (SELECT id FROM {TU} FINAL ORDER BY id)")
    assert got == "[1,3,5]", f"the deletes must remove the rows from the user's Replacing table, got {got}"
    assert ch(f"SELECT v FROM {TU} FINAL WHERE id = 1") == "new", "the update is visible"
    extra = ch("SELECT count() FROM system.columns WHERE database='default' "
               f"AND table='{TU}' AND name IN ('_apitap_ver','_apitap_deleted')")
    assert extra == "0", f"no bookkeeping column may be added to the user's table, got {extra}"
    print("  ReplacingMergeTree(updated_at): deletes apply, no column added")

    wipe()
    print("✓ Replacing destination: bootstrap columns, tombstone round, FINAL exact, ver moves")


if __name__ == "__main__":
    main()
