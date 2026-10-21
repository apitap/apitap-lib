"""MySQL CDC refuses a tracked table whose rows a foreign key cascade changes.

InnoDB applies ON DELETE/UPDATE CASCADE, SET NULL and SET DEFAULT inside
the storage engine, and the binlog carries only the parent's row events.
Tracking the child table, a parent delete left every cascaded-away child row
at the destination, on a green run (deep review 2, N17).

Proof (mysql -> clickhouse log_based): order_items REFERENCES orders ON
DELETE CASCADE; track order_items. The run must refuse and name the
constraint; a run that succeeds must, after a parent delete, leave the
destination equal to the source.
"""
import subprocess

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
P, T = "my_fk_orders", "my_fk_items"


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
    my(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {P}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def ids(q):
    return q(f"SELECT COALESCE(GROUP_CONCAT(id ORDER BY id), '') FROM {T}") if q is my else \
        q(f"SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM {T} ORDER BY id)")


def main():
    wipe()
    try:
        my(f"CREATE TABLE {P} (id int PRIMARY KEY); INSERT INTO {P} VALUES (1), (2); "
           f"CREATE TABLE {T} (id int PRIMARY KEY, order_id int, "
           f"FOREIGN KEY (order_id) REFERENCES {P}(id) ON DELETE CASCADE); "
           f"INSERT INTO {T} VALUES (10, 1), (11, 1), (20, 2)")
        try:
            apitap.transfer(MY, CH, table=T, mode="log_based")
            my(f"DELETE FROM {P} WHERE id = 1")
            apitap.transfer(MY, CH, table=T, mode="log_based")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "ON DELETE CASCADE" in msg and P in msg, f"refused for the wrong reason: {msg}"
            print(f"  a tracked child of a cascading foreign key is refused: {msg[:100]}...")
        else:
            s, d = ids(my), ids(ch)
            assert d == s, f"after a parent delete: destination {d!r}, source {s!r} (cascaded rows kept)"
            print("  the cascade reached the destination")
    finally:
        wipe()
    print("✓ MySQL CDC never misses a foreign key cascade silently")


if __name__ == "__main__":
    main()
