"""A ClickHouse-source DateTime cursor with its own time zone resumes exactly.

ch -> ch append with cursor ts DateTime('America/Chicago'): the destination
column is DateTime64(6, 'UTC'), so the watermark is UTC wall-clock text. Run
2 embedded it as `ts > '<utc text>'`, and ClickHouse parses a bare string
compared with a zoned DateTime in the COLUMN's zone: the threshold moved 5-6
hours later and every row in between was skipped, silently (deep review D5).
A positive-offset zone (Asia/Jakarta) moves it earlier and re-sends rows.

Proof: per zone, bootstrap 3 rows, add 3 rows one to three hours past the
newest, append again. The destination must hold exactly the 6 source rows.
"""
import urllib.parse
import urllib.request

import apitap

SRC = "clickhouse://default:bench@127.0.0.1:8124/default"
DST = "clickhouse://default:bench@127.0.0.1:8125/default"
T = "ch_tz_cursor"


def http(port, sql):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/?" + urllib.parse.urlencode({"query": sql}),
                                 headers={"X-ClickHouse-User": "default", "X-ClickHouse-Key": "bench"},
                                 method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read().decode().strip()


def src(sql):
    return http(8124, sql)


def dst(sql):
    return http(8125, sql)


def wipe():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    if dst("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        dst(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def leg(zone):
    wipe()
    src(f"CREATE TABLE {T} (id UInt64, ts DateTime('{zone}')) ENGINE = MergeTree ORDER BY id")
    src(f"INSERT INTO {T} VALUES (1, '2026-10-20 08:00:00'), (2, '2026-10-20 09:00:00'), "
        f"(3, '2026-10-20 10:00:00')")
    apitap.transfer(SRC, DST, table=T, mode="append", cursor="ts")
    assert dst(f"SELECT count() FROM {T}") == "3", f"[{zone}] bootstrap"
    src(f"INSERT INTO {T} VALUES (4, '2026-10-20 11:00:00'), (5, '2026-10-20 12:00:00'), "
        f"(6, '2026-10-20 13:00:00')")
    apitap.transfer(SRC, DST, table=T, mode="append", cursor="ts")
    got = dst(f"SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM {T} ORDER BY id)")
    assert got == "1,2,3,4,5,6", f"[{zone}] after the append the destination holds ids {got!r}, want 1..6"
    print(f"  [{zone}] append resumed exactly: ids {got}")


def main():
    try:
        for zone in ("America/Chicago", "Asia/Jakarta"):
            leg(zone)
    finally:
        wipe()
    print("✓ a zoned DateTime cursor resumes exactly")


if __name__ == "__main__":
    main()
