"""A ch -> ch replace orders the new table by the source's sorting key.

With no order_by=, a ClickHouse destination is ordered by the read's cursor.
A ClickHouse source's cursor is only its leading sorting-key column (a split
hint), and after the D26 fix it is none when that column is not an integer:
ORDER BY (event_date, id) produced ORDER BY tuple(), and ORDER BY
(tenant_id, id) produced ORDER BY tenant_id. Under ReplacingMergeTree both
merge rows that are not duplicates: everything into one row, or one row per
tenant, silently (deep review 2, N26). The source's own key orders the
destination now, and a collapsing engine over an empty key is refused.

Proof (ch -> ch, mode='replace', engine='ReplacingMergeTree', no order_by=):
for each source key, the destination's sorting key equals the source's and
all 100 rows survive OPTIMIZE FINAL.
"""
import urllib.parse
import urllib.request

import apitap

SRC = "clickhouse://default:bench@127.0.0.1:8124/default"
DST = "clickhouse://default:bench@127.0.0.1:8125/default"
T = "ch_replace_sort_key"


def http(port, sql):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/?" + urllib.parse.urlencode({"query": sql}),
                                 headers={"X-ClickHouse-User": "default", "X-ClickHouse-Key": "bench"},
                                 method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read().decode().strip()


def wipe():
    http(8124, f"DROP TABLE IF EXISTS {T}")
    http(8125, f"DROP TABLE IF EXISTS {T}")


def main():
    shapes = [
        ("event_date Date, id UInt64, v String", "event_date, id",
         "toDate('2026-10-20') + intDiv(number, 10), number, 'v'"),
        ("tenant_id UInt32, id UInt64, v String", "tenant_id, id", "intDiv(number, 25), number, 'v'"),
    ]
    try:
        for cols, key, select in shapes:
            wipe()
            http(8124, f"CREATE TABLE {T} ({cols}) ENGINE = MergeTree ORDER BY ({key})")
            http(8124, f"INSERT INTO {T} SELECT {select} FROM numbers(100)")
            apitap.transfer(SRC, DST, table=T, mode="replace", engine="ReplacingMergeTree")
            got = http(8125, f"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() "
                             f"AND name = '{T}'")
            assert got == key, f"ORDER BY ({key}) at the source became ({got}) at the destination"
            http(8125, f"OPTIMIZE TABLE {T} FINAL")
            n = http(8125, f"SELECT count() FROM {T} FINAL")
            assert n == "100", f"ORDER BY ({key}): {n} rows after OPTIMIZE FINAL, want 100"
            print(f"  ORDER BY ({key}): kept, 100 rows after OPTIMIZE FINAL")
    finally:
        wipe()
    print("✓ a ch -> ch replace orders the new table by the source's sorting key")


if __name__ == "__main__":
    main()
