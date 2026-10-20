"""MySQL CDC refuses a DDL that removes a tracked table's rows without row events.

MySQL logs every DDL as a QUERY event, even under binlog_format=ROW, so
`ALTER TABLE t DROP PARTITION p` removes rows that never reach the drain as
deletes. The drain read it as schema-only and moved on: the destination kept
every row of the dropped partition, on a green run, forever (deep review D13).
The binlog does not say which rows went, so the run must stop and say so.

Proof (mysql -> clickhouse): a table range-partitioned on m, 5 rows per
partition, bootstrapped. Then DROP PARTITION p1 and one more insert. The next
drain must refuse and name the statement; a drain that succeeds must leave
the destination equal to the source, so a green run that kept p1 fails.
"""
import subprocess

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "my_partition_ddl"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
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
    my(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def ids_src():
    return my(f"SELECT COALESCE(GROUP_CONCAT(id ORDER BY id), '') FROM {T}")


def ids_dst():
    return ch(f"SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM {T} ORDER BY id)")


def main():
    wipe()
    try:
        my(f"CREATE TABLE {T} (id int NOT NULL, m int NOT NULL, v varchar(10), PRIMARY KEY (id, m)) "
           f"PARTITION BY RANGE (m) (PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (3)); "
           f"INSERT INTO {T} VALUES (1,1,'a'),(2,1,'a'),(3,1,'a'),(4,1,'a'),(5,1,'a'),"
           f"(6,2,'b'),(7,2,'b'),(8,2,'b'),(9,2,'b'),(10,2,'b')")
        apitap.transfer(MY, CH, table=T, mode="log_based")
        assert ids_dst() == ids_src(), "(rig) bootstrap exact"
        print(f"  bootstrap: {ids_dst()}")

        my(f"ALTER TABLE {T} DROP PARTITION p1; INSERT INTO {T} VALUES (11,2,'c')")
        try:
            apitap.transfer(MY, CH, table=T, mode="log_based")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "DROP PARTITION" in msg and T in msg, f"refused for the wrong reason: {msg}"
            print(f"  DROP PARTITION refused: {msg[:110]}...")
        else:
            src, dst = ids_src(), ids_dst()
            assert dst == src, f"a green drain after DROP PARTITION: source {src!r}, destination {dst!r}"
            print("  the drain applied DROP PARTITION exactly")
    finally:
        wipe()
    print("✓ MySQL CDC never ignores a DDL that removes tracked rows")


if __name__ == "__main__":
    main()
