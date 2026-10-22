"""A ch -> ch copy keyed on a Nullable source column lands exact, or fails on a NULL.

The ClickHouse sink makes the cursor, the order_by columns and the engine's
arguments non-Nullable. The ClickHouse source still selected such a column
raw, so a Nullable(Int64) source column put its null-flag byte into a stream
framed without it: rows landed as shifted garbage, or the span failed with a
misleading "server aborted the query after sending a 200" (deep review 3,
R5/R28/R31). The source now CASTs it to the bare type, and a real NULL fails
the read.

Proof (ch :8124 -> ch :8125, replace with order_by='user_id', user_id
Nullable(Int64)): 16,384 rows with no NULL land equal to the source; one
NULL fails the run and names it, and the previous destination is untouched.
"""
import urllib.parse
import urllib.request

import apitap

SRC = "clickhouse://default:bench@127.0.0.1:8124/default"
DST = "clickhouse://default:bench@127.0.0.1:8125/default"
T = "ch_nullable_sort_col"


def http(port, sql):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/?" + urllib.parse.urlencode({"query": sql}),
                                 headers={"X-ClickHouse-User": "default", "X-ClickHouse-Key": "bench"},
                                 method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read().decode().strip()


def digest(port):
    return http(port, f"SELECT count(), sum(cityHash64(user_id, id, v)) FROM {T}")


def wipe():
    http(8124, f"DROP TABLE IF EXISTS {T}")
    http(8125, f"DROP TABLE IF EXISTS {T}")


def main():
    wipe()
    try:
        http(8124, f"CREATE TABLE {T} (user_id Nullable(Int64), id Int64, v String) ENGINE = MergeTree "
                   f"ORDER BY id")
        http(8124, f"INSERT INTO {T} SELECT number % 97, number, toString(number) FROM numbers(16384)")
        apitap.transfer(SRC, DST, table=T, mode="replace", order_by="user_id, id")
        want, got = digest(8124), digest(8125)
        assert got == want, f"destination {got!r}, source {want!r}"
        print(f"  16,384 rows ordered by a Nullable(Int64) source column: equal ({got.split()[0]} rows)")

        http(8124, f"INSERT INTO {T} VALUES (NULL, 99999, 'null')")
        try:
            apitap.transfer(SRC, DST, table=T, mode="replace", order_by="user_id, id")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            assert "NULL" in str(e), f"refused for the wrong reason: {e}"
            print(f"  a NULL in it fails the run: {str(e)[:110]}...")
        else:
            raise AssertionError("a NULL in a non-Nullable sort column landed")
        assert digest(8125) == got, "the failed run changed the destination"
    finally:
        wipe()
    print("✓ a ch -> ch copy keyed on a Nullable source column lands exact, or fails on a NULL")


if __name__ == "__main__":
    main()
