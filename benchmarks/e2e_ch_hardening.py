"""ClickHouse hardening: DateTime64(9) and an escaped String cursor.

Two P2 findings (docs/review/2026-10-18-p2-triage.md), each on CH 24.8:
  dt64     a DateTime64(9) source column was cast to DateTime64(6): its
           nanoseconds were dropped silently. It now travels as exact text.
  cursor   the watermark reads used escaped TabSeparated and `.trim()`: a
           String cursor (ClickHouse source) holding a backslash was stored
           escaped, and the next append compared against the escaped literal.
           Raw TSV now.
  clock    _apitap_state is ReplacingMergeTree(synced_at), and synced_at
           defaulted to the server's now: a row stamped by a clock running
           ahead outranked every newer watermark until real time caught up.
Usage: e2e_ch_hardening.py dt64|cursor|clock
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


def clock():
    t = "ch_clock_step"
    pg(f"DROP TABLE IF EXISTS {t}")
    ch(f"DROP TABLE IF EXISTS {t}")
    state_wipe(ch, t)
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{t}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    try:
        pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v int); INSERT INTO {t} VALUES (1, 1)")
        apitap.transfer(PG, CH, table=t, mode="log_based")
        q = (f"SELECT argMax(watermark, synced_at) FROM _apitap_state "
             f"WHERE dest_table = '{t}' AND mode = 'log_based' FORMAT TabSeparatedRaw")
        sid, cur, wm = ch(f"SELECT any(source_id), any(cursor_col), argMax(watermark, synced_at) FROM _apitap_state "
                          f"WHERE dest_table = '{t}' AND mode = 'log_based' FORMAT TSV").split("\t")
        # The current watermark, re-stamped as a writer whose clock ran an
        # hour ahead would have left it: a newer watermark must still win.
        ch(f"INSERT INTO _apitap_state (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) "
           f"VALUES ('{t}', '{sid}', '{cur}', '{wm}', 'log_based', 0, now64(6) + INTERVAL 1 HOUR)")
        pg(f"INSERT INTO {t} VALUES (2, 2)")
        apitap.transfer(PG, CH, table=t, mode="log_based")
        got = ch(q)
        assert int(got) > int(wm), f"the drain's newer watermark must win over the future-stamped {wm}, read back {got}"
        print(f"  watermark {wm} stamped an hour ahead, then a drain: {got} wins")
    finally:
        for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{t}'").split()):
            pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
               f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
            pg(f"DROP PUBLICATION IF EXISTS {pub}")
        pg(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")
        state_wipe(ch, t)


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"dt64": dt64, "cursor": cursor, "clock": clock}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_ch_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ ClickHouse hardening: {which[0]}")
