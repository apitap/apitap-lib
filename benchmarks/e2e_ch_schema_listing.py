"""schema= on a ClickHouse source moves every user table, including ones that merely look like apitap's.

The catalog skipped apitap's own objects with `name NOT LIKE '__apitap%'` —
and `_` is LIKE's one-character wildcard, so any user table whose third
character starts "apitap" (`xyapitap_orders`) was silently left out of a
schema-wide transfer (P2). The underscores are escaped now.

Proof: a ClickHouse database with `orders`, `xyapitap_orders` and an apitap
lock-style `__apitap_marker`; schema= into a second ClickHouse moves the two
user tables and not the marker.
"""
import subprocess

import apitap

SRC = "clickhouse://default:bench@127.0.0.1:8124/qa_listing"
DST = "clickhouse://default:bench@127.0.0.1:8125/qa_listing"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def ch(box, sql):
    o = sh(["docker", "exec", box, "clickhouse-client", "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    for box in ("apitap-bench-ch", "apitap-bench-ch2"):
        ch(box, "DROP DATABASE IF EXISTS qa_listing")
        ch(box, "CREATE DATABASE qa_listing")
    for t in ("orders", "xyapitap_orders", "__apitap_marker"):
        ch("apitap-bench-ch", f"CREATE TABLE qa_listing.{t} (id Int64) ENGINE = MergeTree ORDER BY id")
        ch("apitap-bench-ch", f"INSERT INTO qa_listing.{t} SELECT number FROM numbers(10)")
    try:
        apitap.transfer(SRC, DST, schema="qa_listing", mode="replace")
        got = ch("apitap-bench-ch2", "SELECT arrayStringConcat(groupArray(name), ',') FROM "
                 "(SELECT name FROM system.tables WHERE database = 'qa_listing' "
                 "AND name NOT LIKE '%apitap_state%' AND name NOT LIKE '%apitap_lease%' ORDER BY name)")
        assert got == "orders,xyapitap_orders", f"schema= must move both user tables and not the marker, got {got!r}"
        print(f"  moved: {got}")
    finally:
        for box in ("apitap-bench-ch", "apitap-bench-ch2"):
            ch(box, "DROP DATABASE IF EXISTS qa_listing")
    print("✓ schema= on ClickHouse moves every user table")


if __name__ == "__main__":
    main()
