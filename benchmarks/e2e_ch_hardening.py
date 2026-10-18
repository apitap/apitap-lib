"""ClickHouse hardening: DateTime64(9) and an escaped String cursor.

Two P2 findings (docs/review/2026-10-18-p2-triage.md), each on CH 24.8:
  dt64     a DateTime64(9) source column was cast to DateTime64(6): its
           nanoseconds were dropped silently. It now travels as exact text.
  cursor   the watermark reads used escaped TabSeparated and `.trim()`: a
           String cursor (ClickHouse source) holding a backslash was stored
           escaped, and the next append compared against the escaped literal.
           Raw TSV now.
Usage: e2e_ch_hardening.py dt64|cursor
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
CH2 = "clickhouse://default:bench@127.0.0.1:8125/default"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def chq(box, sql):
    o = sh(["docker", "exec", box, "clickhouse-client", "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return chq("apitap-bench-ch", sql)


def ch2(sql):
    return chq("apitap-bench-ch2", sql)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def state_wipe(q, t):
    if q("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        q(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{t}' SETTINGS mutations_sync = 1")


def dt64():
    t = "ch_dt64_nanos"
    ch(f"DROP TABLE IF EXISTS {t}")
    ch2(f"DROP TABLE IF EXISTS {t}")
    try:
        ch(f"CREATE TABLE {t} (id Int64, ts DateTime64(9, 'UTC')) ENGINE = MergeTree ORDER BY id")
        ch(f"INSERT INTO {t} VALUES (1, '2024-01-01 00:00:00.123456789'), (2, '2024-01-01 00:00:00.000000001')")
        apitap.transfer(CH, CH2, table=t, mode="replace")
        got = ch2(f"SELECT arrayStringConcat(groupArray(toString(ts)), ',') FROM (SELECT ts FROM {t} ORDER BY id)")
        assert got == "2024-01-01 00:00:00.123456789,2024-01-01 00:00:00.000000001", \
            f"DateTime64(9) must keep its nanoseconds, got {got!r}"
        print(f"  DateTime64(9) CH -> CH: {got}")
    finally:
        ch(f"DROP TABLE IF EXISTS {t}")
        ch2(f"DROP TABLE IF EXISTS {t}")


def cursor():
    t = "ch_str_cursor"
    for q in (ch, ch2):
        q(f"DROP TABLE IF EXISTS {t}")
        state_wipe(q, t)
    try:
        ch(f"CREATE TABLE {t} (k String, n Int64) ENGINE = MergeTree ORDER BY k")
        ch(f"INSERT INTO {t} VALUES ('a\\\\b', 1), ('a', 2)")
        apitap.transfer(CH, CH2, table=t, mode="append", cursor="k")
        wm = ch2(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table = '{t}' FORMAT TabSeparatedRaw")
        assert wm == "a\\b", f"the watermark must be the value itself, a\\b, got {wm!r}"
        ch(f"INSERT INTO {t} VALUES ('a\\\\c', 3), ('b', 4)")
        apitap.transfer(CH, CH2, table=t, mode="append", cursor="k")
        got = ch2(f"SELECT arrayStringConcat(groupArray(k), '|') FROM (SELECT k FROM {t} ORDER BY k) FORMAT TabSeparatedRaw")
        want = ch(f"SELECT arrayStringConcat(groupArray(k), '|') FROM (SELECT k FROM {t} ORDER BY k) FORMAT TabSeparatedRaw")
        assert got == want, f"each row exactly once: source {want!r}, destination {got!r}"
        print(f"  String cursor with a backslash: watermark {wm!r}, rows {got!r}")
    finally:
        for q in (ch, ch2):
            q(f"DROP TABLE IF EXISTS {t}")
            state_wipe(q, t)


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"dt64": dt64, "cursor": cursor}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_ch_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ ClickHouse hardening: {which[0]}")
