"""A one-shot MySQL drain gets past a large untracked transaction.

The window deadline waited for the end of a transaction only when it had
buffered TRACKED rows, so inside an untracked one (a retention DELETE, a
staging LOAD) the buffer stayed empty and the deadline broke mid-stream with
nothing gained: every scheduled run re-read the same bytes and broke again,
and the tracked tables' later changes never landed, on green runs (deep
review 3, R18). The deadline now waits for the end of any transaction.

Proof (mysql -> ch, APITAP_MY_WINDOW_SECS=1): bootstrap a tracked table,
then one ~2M-row, 500-byte-padded transaction into an untracked table
(over 1 GB of row events, more than a second to read even uncapped), then
one tracked insert. Within three one-shot runs the tracked row must land.
"""
import os
import subprocess
import sys

MY = "mysql://root:bench@127.0.0.1:3307/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T, U = "my_deadline_tracked", "my_deadline_untracked"


def sh(a, **kw):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **kw)


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def run():
    code = ("import apitap\n"
            f"apitap.transfer({MY!r}, {CH!r}, table={T!r}, mode='log_based')\n")
    return sh([sys.executable, "-c", code], timeout=600, env={**os.environ, "APITAP_MY_WINDOW_SECS": "1"})


def wipe():
    my(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {U}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        my(f"CREATE TABLE {T} (id int PRIMARY KEY, v varchar(10)); INSERT INTO {T} VALUES (1, 'a'); "
           f"CREATE TABLE {U} (id int PRIMARY KEY, pad varchar(520))")
        r = run()
        assert r.returncode == 0, r.stderr[-800:]
        # One transaction, ~2M rows of a 500-byte-padded untracked table: a
        # little over 1 GB of row events, far more than one second to read.
        # Sized on an UNCAPPED 16-core rig, where 2M narrow rows streamed in
        # under the 1 s deadline and the leg passed both ways — worthless: a
        # 100-byte pad read through in ~0.9 s, a 500-byte one takes ~3.5 s.
        my(f"SET SESSION cte_max_recursion_depth = 3000000; START TRANSACTION; "
           f"INSERT INTO {U} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 2000000) "
           f"SELECT n, REPEAT('p', 500) FROM s; COMMIT")
        my(f"INSERT INTO {T} VALUES (2, 'b')")
        for i in range(1, 4):
            r = run()
            assert r.returncode == 0, r.stderr[-800:]
            if ch(f"SELECT count() FROM {T}") == "2":
                print(f"  the tracked row landed on run {i} past a 2M-row untracked transaction")
                break
        else:
            raise AssertionError("three one-shot runs never got past the untracked transaction")
    finally:
        wipe()
    print("✓ a one-shot MySQL drain gets past a large untracked transaction")


if __name__ == "__main__":
    main()
