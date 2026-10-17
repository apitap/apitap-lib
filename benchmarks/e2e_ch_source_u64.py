"""A parallel ClickHouse read on a UInt64 cursor past i64::MAX reads every row.

The span planner parsed the cursor's min/max as i64; a UInt64 key above
9223372036854775807 failed every parallel read of the table (MASTER review
M-49). Such a range now reads as one span.

Proof: a 300-row MergeTree keyed on UInt64 values straddling 2^63, read
into a second ClickHouse with parallel=2 — every row, the same sum.
"""
import subprocess

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
DST = "clickhouse://default:bench@127.0.0.1:8125/default"
T = "ch_u64_cursor"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch2(sql):
    o = sh(["docker", "exec", "apitap-bench-ch2", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch2_wipe():
    ch2(f"DROP TABLE IF EXISTS {T}")
    if ch2("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch2(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    ch(f"DROP TABLE IF EXISTS {T}")
    ch2_wipe()
    ch(f"CREATE TABLE {T} (id UInt64, v String) ENGINE = MergeTree ORDER BY id")
    ch(f"INSERT INTO {T} SELECT toUInt64(9223372036854775657) + number, 'v' FROM numbers(300)")
    want = ch(f"SELECT concat(toString(count()), ':', toString(sum(toUInt128(id)) % 1000000007)) FROM {T}")
    apitap.transfer(CH, DST, table=T, mode="append", cursor="id", parallel=2)
    got = ch2(f"SELECT concat(toString(count()), ':', toString(sum(toUInt128(id)) % 1000000007)) FROM {T}")
    assert got == want, f"parallel read on a UInt64 cursor past i64::MAX: got {got}, want {want}"
    print(f"  300 rows straddling 2^63, parallel=2: {got} on both sides")
    ch(f"DROP TABLE IF EXISTS {T}")
    ch2_wipe()
    print("✓ a parallel ClickHouse read on a UInt64 cursor past i64::MAX reads every row")


if __name__ == "__main__":
    main()
