"""MySQL CDC under binlog_checksum=NONE, and with a STATEMENT-format writer.

checksum: the decoder assumed CRC32 and cut 4 bytes off every event. Under
binlog_checksum=NONE a ROTATE's file name lost its tail (the watermark went
backwards) and a TRUNCATE's table name was cut (MASTER review M-39). Each
FORMAT_DESCRIPTION now says which.

statement format: a session with binlog_format=STATEMENT writes its DML as
QUERY text, no rows event — those changes were ignored and vanished from the
destination (M-38). A statement naming a tracked table is now refused.

Proof (MySQL 8, global binlog_checksum=NONE for the leg's duration, restored
after): bootstrap, then inserts/updates/deletes, a binlog rotate in the middle
and a TRUNCATE, drained into ClickHouse twice — the destination equals the
source each time. Then a STATEMENT-format session inserts into the tracked
table: the drain is refused naming it; a STATEMENT-format write to an
untracked table is not ours and is not refused.
"""
import subprocess

import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "my_ckn"
U = "my_ckn_other"


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


def drain():
    return apitap.transfer(MY, CH, table=T, mode="log_based")


def agree(stage):
    s = my(f"SELECT GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ',') FROM {T}") or ""
    if s == "NULL":
        s = ""
    d = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', v)), ',') "
           f"FROM (SELECT id, v FROM {T} ORDER BY id)")
    assert s == d, f"{stage}: source {s[:200]!r} vs destination {d[:200]!r}"
    print(f"  {stage}: {len(s.split(',')) if s else 0} rows equal")


def wipe():
    my(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {U}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    was = my("SELECT @@global.binlog_checksum")
    wipe()
    try:
        my("SET GLOBAL binlog_checksum = 'NONE'")       # rotates to a NONE file
        my(f"CREATE TABLE {T} (id int PRIMARY KEY, v varchar(40))")
        my(f"CREATE TABLE {U} (id int PRIMARY KEY)")
        my(f"INSERT INTO {T} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        drain()
        agree("bootstrap")
        my(f"INSERT INTO {T} VALUES (4, 'd'), (5, 'e'); UPDATE {T} SET v = 'B' WHERE id = 2")
        my("FLUSH BINARY LOGS")                          # a real ROTATE mid-window
        my(f"DELETE FROM {T} WHERE id = 1; INSERT INTO {T} VALUES (6, 'f')")
        drain()
        agree("drain across a rotate")
        my(f"TRUNCATE TABLE {T}; INSERT INTO {T} VALUES (7, 'g'), (8, 'h')")
        drain()
        agree("drain across a TRUNCATE")
        drain()
        agree("empty drain")

        my(f"SET SESSION binlog_format = 'STATEMENT'; INSERT INTO {U} VALUES (1)")
        drain()
        agree("a STATEMENT write to an untracked table is not ours")
        my(f"SET SESSION binlog_format = 'STATEMENT'; INSERT INTO {T} VALUES (9, 'stmt')")
        try:
            drain()
            raise AssertionError("a STATEMENT-format write to a tracked table must be refused")
        except RuntimeError as e:
            assert "statement text" in str(e) and T in str(e), e
        print("  a STATEMENT-format INSERT into the tracked table: refused naming it")
    finally:
        my(f"SET GLOBAL binlog_checksum = '{was}'")
        wipe()
    print("✓ MySQL CDC under binlog_checksum=NONE is exact; a STATEMENT-format writer is refused")


if __name__ == "__main__":
    main()
