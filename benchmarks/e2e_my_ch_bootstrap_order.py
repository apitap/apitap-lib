"""A MySQL CDC bootstrap into ClickHouse orders the table by its key.

The Postgres bootstrap defaults a ClickHouse destination's ORDER BY to the
replica identity; the MySQL bootstrap never did. A composite or text key
landed `ORDER BY tuple()`: every window's key delete scanned the whole
table, and with a Replacing engine every row shared one empty sorting key,
so merges collapsed the table toward a single row (deep review D16).

Proof (mysql -> clickhouse log_based, PRIMARY KEY (order_id, line_no), no
order_by=): the destination's sorting key is the key. With
engine='ReplacingMergeTree(_apitap_ver, _apitap_deleted)', 50 rows survive
OPTIMIZE FINAL.
"""
import subprocess

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "my_ch_bootstrap_order"
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"


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
    ch(f"DROP VIEW IF EXISTS {T}__current")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    for engine in (None, ENGINE):
        wipe()
        try:
            my(f"CREATE TABLE {T} (order_id int NOT NULL, line_no int NOT NULL, v varchar(10), "
               f"PRIMARY KEY (order_id, line_no)); "
               f"INSERT INTO {T} WITH RECURSIVE g(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM g WHERE i < 49) "
               f"SELECT i DIV 5, i MOD 5, 'v' FROM g")
            kw = {"engine": engine} if engine else {}
            apitap.transfer(MY, CH, table=T, mode="log_based", **kw)
            key = ch(f"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = '{T}'")
            assert key == "order_id, line_no", f"[{engine or 'default'}] ORDER BY ({key!r}), want the key"
            if engine:
                ch(f"OPTIMIZE TABLE {T} FINAL")
                n = ch(f"SELECT count() FROM {T} FINAL")
                assert n == "50", f"[{engine}] {n} rows after OPTIMIZE FINAL, want 50"
            print(f"  [{engine or 'MergeTree'}] ORDER BY ({key})" + (", 50 rows after OPTIMIZE FINAL" if engine else ""))
        finally:
            wipe()
    print("✓ a MySQL CDC bootstrap into ClickHouse orders the table by its key")


if __name__ == "__main__":
    main()
