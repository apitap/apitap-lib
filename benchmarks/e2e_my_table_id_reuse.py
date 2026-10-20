"""MySQL CDC never applies an untracked table's rows to a tracked one after a restart.

A binlog rows event names its table by a numeric id, which the TABLE_MAP before
it binds to a name. The drain kept the id -> map binding for tracked tables
only, and never dropped it: after a server restart the ids start again, an
UNTRACKED table's TABLE_MAP took a tracked table's old id, and its rows were
decoded with the tracked table's map and applied to that table's destination,
silently (deep review D14, K8).

Proof, on a throwaway MySQL 8.0 (so the restarts touch nothing else): tracked
`t` and untracked `u`, same columns. Bootstrap `t`. Restart and insert into
`t` first; restart and insert into `u` first: both get the first id the
server hands out after a start (the leg checks that in the binlog, or the rig
proved nothing). One drain over that backlog. The destination must hold `t`'s
rows only.
"""
import re
import subprocess
import time

import apitap

NAME = "apitap-qa-my-tid"
PORT = 18312
MY = f"mysql://root:bench@127.0.0.1:{PORT}/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "tid_t"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def my(sql):
    o = sh(["docker", "exec", NAME, "mysql", "-h127.0.0.1", "-uroot", "-pbench", "-N", "bench", "-e", sql])
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


def wait_up():
    # Over TCP: the image's init server answers the socket and then restarts.
    for _ in range(120):
        if sh(["docker", "exec", NAME, "mysql", "-h127.0.0.1", "-uroot", "-pbench",
               "-e", "SELECT 1"]).returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("(rig) the throwaway MySQL never came up")


def restart():
    assert sh(["docker", "restart", NAME]).returncode == 0
    wait_up()


def map_ids(table):
    """The table ids the binlog's TABLE_MAP events gave `bench.<table>`, from the
    server (`SHOW BINLOG EVENTS`: the image ships no mysqlbinlog)."""
    ids = set()
    for line in my("SHOW BINARY LOGS").split("\n"):
        events = my(f"SHOW BINLOG EVENTS IN '{line.split(chr(9))[0]}'")
        ids |= set(re.findall(rf"table_id: (\d+) \(bench\.{table}\)", events))
    return ids


def main():
    sh(["docker", "rm", "-f", NAME])
    r = sh(["docker", "run", "-d", "--name", NAME, "-p", f"127.0.0.1:{PORT}:3306",
            "-e", "MYSQL_ROOT_PASSWORD=bench", "-e", "MYSQL_DATABASE=bench", "mysql:8.0",
            "--server-id=7", "--log-bin=binlog", "--binlog-format=ROW", "--binlog-row-image=FULL"])
    assert r.returncode == 0, r.stderr
    ch_wipe()
    try:
        wait_up()
        my(f"CREATE TABLE {T} (id int PRIMARY KEY, v varchar(20)); "
           f"CREATE TABLE tid_u (id int PRIMARY KEY, v varchar(20)); "
           f"INSERT INTO {T} VALUES (1, 't1')")
        apitap.transfer(MY, CH, table=T, mode="log_based")

        restart()
        my(f"INSERT INTO {T} VALUES (2, 't2')")
        restart()
        my("INSERT INTO tid_u VALUES (100, 'u100'), (101, 'u101')")

        tid, uid = map_ids(T), map_ids("tid_u")
        if not tid & uid:
            raise RuntimeError(f"(rig) the restart did not reuse an id: {T} {tid}, tid_u {uid}")
        print(f"  after two restarts tid_u took {T}'s table id {sorted(tid & uid)}")

        apitap.transfer(MY, CH, table=T, mode="log_based")
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', v)), ',') "
                 f"FROM (SELECT id, v FROM {T} ORDER BY id)")
        assert got == "1:t1,2:t2", f"{T} at the destination holds {got!r}, want only its own rows 1:t1,2:t2"
        print(f"  the drain applied {T}'s rows only: {got}")
    finally:
        sh(["docker", "rm", "-f", NAME])
        ch_wipe()
    print("✓ MySQL CDC never applies an untracked table's rows to a tracked one")


if __name__ == "__main__":
    main()
