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


def refused(label, ddl_sql, table, dest=None):
    """Bootstrap `table` (already created), run `ddl_sql`, drain: refused, or exact."""
    kw = {"dest_table": dest} if dest else {}
    apitap.transfer(MY, CH, table=table, mode="log_based", **kw)
    my(ddl_sql)
    try:
        apitap.transfer(MY, CH, table=table, mode="log_based", **kw)
    except Exception as e:  # noqa: BLE001 — the message is what is checked
        assert "removes or swaps rows" in str(e), f"[{label}] refused for the wrong reason: {e}"
        print(f"  [{label}] refused: {str(e)[:100]}...")
    else:
        raise AssertionError(f"[{label}] a green drain after {ddl_sql!r}: the destination keeps the old rows")


def more():
    """N5: the forms the first parser let through; N16: a rename rotation."""
    # The tracked table is the WITH TABLE side of an exchange.
    t2 = "my_partition_ddl_x"
    my(f"DROP TABLE IF EXISTS {t2}; DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {t2}")
    try:
        my(f"CREATE TABLE {T} (id int NOT NULL, m int NOT NULL, PRIMARY KEY (id, m)) "
           f"PARTITION BY RANGE (m) (PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (3)); "
           f"INSERT INTO {T} VALUES (1,1),(2,1); "
           f"CREATE TABLE {t2} (id int NOT NULL, m int NOT NULL, PRIMARY KEY (id, m)); "
           f"INSERT INTO {t2} VALUES (7,1)")
        refused("exchange WITH TABLE", f"ALTER TABLE {T} EXCHANGE PARTITION p1 WITH TABLE {t2}", t2)
    finally:
        my(f"DROP TABLE IF EXISTS {t2}")
        ch(f"DROP TABLE IF EXISTS {t2}")
        if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{t2}' SETTINGS mutations_sync = 1")
    # A rotation renames another table onto the tracked name (N16).
    tr = "my_ddl_rotate"
    my(f"DROP TABLE IF EXISTS {tr}; DROP TABLE IF EXISTS {tr}_new; DROP TABLE IF EXISTS {tr}_old; "
       f"CREATE TABLE {tr} (id int PRIMARY KEY); INSERT INTO {tr} VALUES (1),(2); "
       f"CREATE TABLE {tr}_new (id int PRIMARY KEY); INSERT INTO {tr}_new VALUES (5)")
    ch(f"DROP TABLE IF EXISTS {tr}")
    try:
        refused("RENAME rotation", f"RENAME TABLE {tr} TO {tr}_old, {tr}_new TO {tr}", tr)
    finally:
        my(f"DROP TABLE IF EXISTS {tr}; DROP TABLE IF EXISTS {tr}_new; DROP TABLE IF EXISTS {tr}_old")
        ch(f"DROP TABLE IF EXISTS {tr}")
        if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{tr}' SETTINGS mutations_sync = 1")
    # The tracked table's whole database goes, and comes back.
    # Named across databases from `bench`: the dropped one cannot be dialled.
    db, td = "apitap_ddl_db", "my_ddl_db_t"
    my(f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}; "
       f"CREATE TABLE {db}.{td} (id int PRIMARY KEY); INSERT INTO {db}.{td} VALUES (1),(2)")
    ch(f"DROP TABLE IF EXISTS {td}")
    try:
        # Recreated before the next drain: a plain drop is caught at startup
        # (the table is not in the catalog); this shape is the silent one.
        refused("DROP DATABASE", f"DROP DATABASE {db}; CREATE DATABASE {db}; "
                f"CREATE TABLE {db}.{td} (id int PRIMARY KEY); INSERT INTO {db}.{td} VALUES (9)",
                f"{db}.{td}", td)
    finally:
        my(f"DROP DATABASE IF EXISTS {db}")
        ch(f"DROP TABLE IF EXISTS {td}")
        if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{td}' SETTINGS mutations_sync = 1")


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
    more()
    print("✓ MySQL CDC never ignores a DDL that removes tracked rows")


if __name__ == "__main__":
    main()
