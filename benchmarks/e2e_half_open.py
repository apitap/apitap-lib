"""A replication connection that stops talking is not waited on forever.

A half-open socket — a NAT that reaped the flow, a middlebox that stopped
forwarding — makes every read pending forever, and the old drain parked there:
the run never finished, never failed, and the schedule stacked on top of it
(system review 2026-10-07, G0.5). The drain now ends a stream that says
nothing for its silence budget, and the walsender socket carries TCP keepalive
under it.

This leg makes the half-open state for real with `docker pause`: the server's
processes freeze mid-stream, so keepalives stop arriving; a drain whose
APITAP_REPLICATION_SILENCE_SECS is 5 must fail with the silence message rather
than hang, and after the unpause a fresh run must land everything the frozen
server had already accepted.
"""
import os
import subprocess
import sys
import time

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLE = "half_open"
BIG = 300001  # ~120 MiB of row frames: the drain is mid-stream when the pause lands


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe_state():
    if ch("SELECT count() FROM system.tables WHERE database='default' "
         "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{TABLE}' "
           "SETTINGS mutations_sync=1")


def drain(env_extra=None, timeout=180):
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    env = dict(os.environ)
    env.update(env_extra or {})
    return sh([sys.executable, "-c", code], env=env, timeout=timeout)


def main():
    wipe_state()
    pg(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    pg(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text);")
    pg(f"INSERT INTO {TABLE} VALUES (1, 'a');")

    r = drain()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TABLE}") == "1", "bootstrap lands the baseline"

    pg(f"INSERT INTO {TABLE} SELECT g, repeat('x', 400) "
       f"FROM generate_series(2, {BIG}) g;")

    # Freeze the server mid-stream. The child would park forever without the
    # silence budget; it must fail with the message instead.
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import apitap\n"
         f"r = apitap.transfer({PG!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"],
        env={**os.environ, "APITAP_REPLICATION_SILENCE_SECS": "5"},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    time.sleep(3)
    assert sh(["docker", "pause", "apitap-bench-pg-src"]).returncode == 0, "pause"
    try:
        out, err = child.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        child.kill()
        sh(["docker", "unpause", "apitap-bench-pg-src"])
        raise AssertionError("the drain parked on a frozen server despite the silence budget")
    finally:
        sh(["docker", "unpause", "apitap-bench-pg-src"])
    assert child.returncode != 0, "a silent stream must fail the run"
    text = (out or "") + (err or "")
    for want in ("silent for", "half-open"):
        assert want in text, f"the failure must say {want!r}; got:\n{text}"

    # Recovery: the data the frozen server had already accepted still lands.
    ok = None
    for _ in range(6):
        ok = drain(timeout=240)
        if ok.returncode == 0:
            break
        time.sleep(5)  # the dead walsender takes a moment to be reaped
    assert ok is not None and ok.returncode == 0, f"recovery drain:\n{ok.stderr}"
    n = ch(f"SELECT count() FROM {TABLE}")
    assert n == str(BIG), f"every row landed after recovery, got {n!r}"

    pg(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    wipe_state()
    print("✓ a frozen source fails the drain within its silence budget; the next run recovers")


if __name__ == "__main__":
    main()
