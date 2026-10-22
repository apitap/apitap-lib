"""A Postgres CDC drain never drops a tracked table's changes made under another name.

pgoutput publishes by oid, so a tracked table renamed away keeps streaming,
under a re-sent Relation that carries its new name. The drain resolved it by
name, took it for an untracked table and dropped its changes, while every
commit still advanced the watermark and the slot. Renamed back, the table
passed every later check: the destination missed those changes for good, on
green runs (deep review 3, R13). Now a tracked oid under another name stops
the drain.

Proof (pg -> ch): bootstrap ids 1-2; RENAME the table away, insert 3,
RENAME it back, insert 4. The next drain must refuse; a drain that succeeds
must leave the destination equal to the source.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "pg_rename_away"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe(before):
    for s in set(pg("SELECT slot_name FROM pg_replication_slots WHERE NOT active").split()) - before[0]:
        pg(f"SELECT pg_drop_replication_slot('{s}')")
    for p in set(pg("SELECT pubname FROM pg_publication").split()) - before[1]:
        pg(f"DROP PUBLICATION IF EXISTS {p}")
    pg(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {T}_tmp")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def ids(q):
    return q(f"SELECT COALESCE(string_agg(id::text, ',' ORDER BY id), '') FROM {T}")


def main():
    before = (set(pg("SELECT slot_name FROM pg_replication_slots").split()),
              set(pg("SELECT pubname FROM pg_publication").split()))
    wipe(before)
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} VALUES (1, 'a'), (2, 'b')")
        apitap.transfer(PG, CH, table=T, mode="log_based")
        pg(f"ALTER TABLE {T} RENAME TO {T}_tmp; INSERT INTO {T}_tmp VALUES (3, 'c'); "
           f"ALTER TABLE {T}_tmp RENAME TO {T}; INSERT INTO {T} VALUES (4, 'd')")
        try:
            apitap.transfer(PG, CH, table=T, mode="log_based")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            assert "renamed" in str(e) and "Re-bootstrap" in str(e), f"refused for the wrong reason: {e}"
            print(f"  refused: {str(e)[:120]}...")
        else:
            src = ids(pg)
            dst = ch(f"SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM {T} ORDER BY id)")
            assert dst == src, f"a green drain over a rename away and back: destination {dst!r}, source {src!r}"
            print("  the drain followed the rename exactly")
    finally:
        wipe(before)
    print("✓ a Postgres CDC drain never drops a tracked table's changes made under another name")


if __name__ == "__main__":
    main()
