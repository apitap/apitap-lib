"""A MySQL -> ClickHouse CDC bootstrap orders by a key with any legal name.

The D16 fix ordered a MySQL bootstrap by its primary key, written raw into
the user-validated order_by: a key named `user$id` or `número` (legal,
unquoted, in MySQL) was refused "order_by contains unexpected character" for
an order_by nobody passed, and a name with a space became a syntax error. On
0.62.1 the same table bootstrapped (deep review 3, R1). The key is now
quoted, and order_by accepts backtick-quoted names.

Proof (mysql -> ch, log_based): PRIMARY KEY (`user$id`, `línea`); the
bootstrap and a drain with an insert land every row, ordered by both.
"""
import subprocess

import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "my_ch_odd_pk_names"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "--default-character-set=utf8mb4", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    my(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        my(f"CREATE TABLE {T} (`user$id` int NOT NULL, `línea` int NOT NULL, v varchar(10), "
           f"PRIMARY KEY (`user$id`, `línea`)); INSERT INTO {T} VALUES (1, 1, 'a'), (1, 2, 'b')")
        apitap.transfer(MY, CH, table=T, mode="log_based")
        my(f"INSERT INTO {T} VALUES (2, 1, 'c')")
        apitap.transfer(MY, CH, table=T, mode="log_based")
        n = ch(f"SELECT count() FROM {T}")
        assert n == "3", f"{n} rows in ClickHouse, want 3"
        key = ch(f"SELECT sorting_key FROM system.tables WHERE database = 'default' AND name = '{T}'")
        assert "user$id" in key and "línea" in key, f"sorting key {key!r}"
        print(f"  PRIMARY KEY (`user$id`, `línea`): 3 rows, ORDER BY ({key})")
    finally:
        wipe()
    print("✓ a MySQL -> ClickHouse CDC bootstrap orders by a key with any legal name")


if __name__ == "__main__":
    main()
