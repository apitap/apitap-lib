"""A ClickHouse append without cursor= never auto-picks a sorting-key column.

A ClickHouse PRIMARY KEY is the sparse sorting prefix, not a unique key, but
the source flagged its integer column as the table's key. With ORDER BY
(user_id, ts) an append without cursor= tracked user_id, so run 2 read
`user_id > max(user_id)` and never copied a new event of an existing user,
silently (deep review D26).

Proof (ch -> ch): bootstrap with mode='append' and no cursor=, add an event
for an existing user, append again. The run must refuse and ask for a
cursor; a run that succeeds must have copied the event. A replace of the
same table still copies every row.
"""
import urllib.parse
import urllib.request

import apitap

SRC = "clickhouse://default:bench@127.0.0.1:8124/default"
DST = "clickhouse://default:bench@127.0.0.1:8125/default"
T = "ch_sort_key_cursor"


def http(port, sql):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/?" + urllib.parse.urlencode({"query": sql}),
                                 headers={"X-ClickHouse-User": "default", "X-ClickHouse-Key": "bench"},
                                 method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read().decode().strip()


def wipe():
    http(8124, f"DROP TABLE IF EXISTS {T}")
    http(8125, f"DROP TABLE IF EXISTS {T}")
    if http(8125, "SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        http(8125, f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def main():
    wipe()
    http(8124, f"CREATE TABLE {T} (user_id UInt64, ts DateTime, e String) ENGINE = MergeTree ORDER BY (user_id, ts)")
    http(8124, f"INSERT INTO {T} VALUES (1, '2026-10-20 08:00:00', 'a'), (3, '2026-10-20 08:00:00', 'b'), "
               f"(9, '2026-10-20 08:00:00', 'c')")
    try:
        refused = None
        try:
            apitap.transfer(SRC, DST, table=T, mode="append")
            http(8124, f"INSERT INTO {T} VALUES (3, '2026-10-20 09:00:00', 'd')")
            apitap.transfer(SRC, DST, table=T, mode="append")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            refused = str(e)
        if refused is not None:
            assert "cursor" in refused, f"refused for the wrong reason: {refused}"
            print(f"  append without cursor= on ORDER BY (user_id, ts): refused ({refused[:80]}...)")
        else:
            n = http(8125, f"SELECT count() FROM {T} WHERE user_id = 3 AND e = 'd'")
            assert n == "1", "a green append skipped a new event of an existing user (cursor = user_id)"
            print("  append without cursor= copied the new event")

        http(8125, f"DROP TABLE IF EXISTS {T}")
        apitap.transfer(SRC, DST, table=T, mode="replace", parallel=2)
        assert http(8125, f"SELECT count() FROM {T}") == http(8124, f"SELECT count() FROM {T}"), \
            "a replace of the same table lost rows"
        print("  replace with parallel=2: every row")
    finally:
        wipe()
    print("✓ a ClickHouse append never auto-picks a sorting-key column")


if __name__ == "__main__":
    main()
