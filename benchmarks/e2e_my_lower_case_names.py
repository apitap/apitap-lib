"""MySQL CDC on a server with lower_case_table_names=1 follows a mixed-case table.

With lower_case_table_names=1 the server stores and logs every table name in
lower case, so the binlog's TABLE_MAP says `bench.orders`. The drain tracked
the table as the user named it — `bench.Orders` — never matched, and applied
zero changes, forever, after a correct bootstrap (MASTER review M-44).

Proof, on a throwaway MySQL 8.0 started with --lower-case-table-names=1:
`table="Orders"` bootstraps, then an insert, an update and a delete drain
into ClickHouse — the destination equals the source.
"""
import subprocess
import time

import apitap

NAME = "apitap-qa-my-lcn"
PORT = 18311
MY = f"mysql://root:bench@127.0.0.1:{PORT}/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "Orders"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def my(sql):
    o = sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch_wipe():
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def agree(stage):
    s = my("SELECT COALESCE(GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id), '') FROM Orders")
    d = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', v)), ',') "
           f"FROM (SELECT id, v FROM `{T}` ORDER BY id)")
    assert s == d, f"{stage}: source {s!r} vs destination {d!r}"
    print(f"  {stage}: equal ({s})")


def main():
    sh(["docker", "rm", "-f", NAME])
    r = sh(["docker", "run", "-d", "--rm", "--name", NAME, "-p", f"127.0.0.1:{PORT}:3306",
            "-e", "MYSQL_ROOT_PASSWORD=bench", "-e", "MYSQL_DATABASE=bench", "mysql:8.0",
            "--lower-case-table-names=1", "--server-id=7", "--log-bin=binlog",
            "--binlog-format=ROW", "--binlog-row-image=FULL"])
    assert r.returncode == 0, r.stderr
    ch_wipe()
    try:
        for _ in range(120):
            if sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "-e", "SELECT 1"]).returncode == 0:
                break
            time.sleep(1)
        assert my("SELECT @@lower_case_table_names") == "1"
        my("CREATE TABLE Orders (id int PRIMARY KEY, v varchar(20)); INSERT INTO Orders VALUES (1, 'a'), (2, 'b')")
        apitap.transfer(MY, CH, table=T, mode="log_based")
        agree("bootstrap")
        my("INSERT INTO Orders VALUES (3, 'c'); UPDATE Orders SET v = 'B' WHERE id = 2; DELETE FROM Orders WHERE id = 1")
        apitap.transfer(MY, CH, table=T, mode="log_based")
        agree("drain")
    finally:
        sh(["docker", "rm", "-f", NAME])
        ch_wipe()
    print("✓ MySQL CDC with lower_case_table_names=1 follows a mixed-case table")


if __name__ == "__main__":
    main()
