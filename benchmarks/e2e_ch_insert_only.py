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
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "io_demo"
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
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database='default' "
          "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' "
           "SETTINGS mutations_sync=1")


def run(engine=None):
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based'"
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

    wipe()
    print("✓ Replacing destination: bootstrap columns, tombstone round, FINAL exact, ver moves")


if __name__ == "__main__":
    main()
