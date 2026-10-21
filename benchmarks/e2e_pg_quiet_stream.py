"""A replication stream the server leaves quiet is asked for a keepalive.

The drain ended any stream that said nothing for its silence budget (120 s)
and never asked the server to speak: every status it sent carried
request_reply = false. A server with wal_sender_timeout = 0 sends no
keepalive of its own after the first, one above 240 s pings less often than
the budget, and one decoding WAL for tables the slot does not track says
nothing at all. Each of those runs died "replication stream silent for 120s
... half-open connection?" on a healthy connection, and a one-shot lane
stuck behind such a backlog died the same way on every run (deep review 2,
N10).

Proof (pg -> ch, log_based, APITAP_FOLLOW_SECS=14, silence budget 4 s, the
source's wal_sender_timeout set to 0): the follow session sits on an idle
stream past four budgets, a row inserted 8 s in lands, and the run ends
cleanly at its deadline. The busy catch-up shape is not timed here: how long
a backlog takes to decode is not a thing a leg can pin, and it is the same
missing request.
"""
import os
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLE = "pg_quiet_stream"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, stdin=subprocess.DEVNULL, **kw)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    pg(f"DROP TABLE IF EXISTS {TABLE}")
    ch(f"DROP TABLE IF EXISTS {TABLE}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{TABLE}' SETTINGS mutations_sync = 1")


def drain(env=None):
    code = ("import apitap\n"
            f"apitap.transfer({PG!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n")
    return subprocess.Popen([sys.executable, "-c", code], env={**os.environ, **(env or {})},
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def main():
    before = pg("SHOW wal_sender_timeout")
    wipe()
    try:
        pg(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text); INSERT INTO {TABLE} VALUES (1, 'a')")
        boot = drain()
        _, err = boot.communicate(timeout=120)
        assert boot.returncode == 0, err
        pg("ALTER SYSTEM SET wal_sender_timeout = 0")
        pg("SELECT pg_reload_conf()")
        time.sleep(1)
        assert pg("SHOW wal_sender_timeout") == "0", "the source kept its wal_sender_timeout"

        t0 = time.monotonic()
        child = drain({"APITAP_FOLLOW_SECS": "14", "APITAP_REPLICATION_SILENCE_SECS": "4"})
        time.sleep(8)
        pg(f"INSERT INTO {TABLE} VALUES (2, 'b')")
        try:
            _, err = child.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            child.kill()
            raise AssertionError("the follow session never ended")
        took = time.monotonic() - t0
        assert child.returncode == 0, f"a quiet, healthy stream failed the run:\n{err[-600:]}"
        n = ch(f"SELECT count() FROM {TABLE}")
        assert n == "2", f"{n} rows in ClickHouse, want 2"
        print(f"  wal_sender_timeout=0, budget 4 s: follow ran {took:.0f} s on a quiet stream, both rows landed")
    finally:
        pg(f"ALTER SYSTEM SET wal_sender_timeout = '{before}'")
        pg("SELECT pg_reload_conf()")
        wipe()
    print("✓ a quiet replication stream is asked for a keepalive instead of being called dead")


if __name__ == "__main__":
    main()
