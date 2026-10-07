"""A Postgres drain says how many rows it has moved, while it moves them.

Only the MySQL lane charged rows to the progress counters; a pg CDC drain
printed progress lines whose row count stayed 0 no matter how much it moved,
so an operator could not tell a stuck run from a slow one (system review
2026-10-07, G0.9). The counters are the same ones the final line uses, so the
proof is to read the child's stderr AS IT RUNS and see a non-zero row count
before the process exits.
"""
import os
import re
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLE = "pg_prog"
ROWS = 200001  # small rows, a good handful of progress intervals


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


def drain():
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code], env=dict(os.environ), timeout=240)


ROW_RE = re.compile(r"(?:rows|changes)=(\d+)")


def main():
    wipe_state()
    pg(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    pg(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {TABLE} VALUES (1, 'a');")

    r = drain()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TABLE}") == "1", "bootstrap lands the baseline"

    pg(f"INSERT INTO {TABLE} SELECT g, repeat('x', 120) "
       f"FROM generate_series(2, {ROWS}) g;")

    # Read the child's stderr as it runs: a row count above zero arriving
    # while the process is still alive is a LIVE progress line.
    child = subprocess.Popen(
        [sys.executable, "-c",
         "import apitap\n"
         f"r = apitap.transfer({PG!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"],
        env={**os.environ, "APITAP_PROGRESS_INTERVAL": "1"},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    live, last_rows = [], 0
    assert child.stderr is not None
    for line in child.stderr:
        m = ROW_RE.search(line)
        if m:
            n = int(m.group(1))
            last_rows = max(last_rows, n)
            if n > 0 and child.poll() is None:
                live.append(line.strip())
    child.wait()
    assert child.returncode == 0, "the drain must succeed"
    assert live, "no non-zero count was reported while the drain still ran"
    assert last_rows >= (ROWS - 1) // 2, f"the live count reached the batch, got {last_rows}"
    assert ch(f"SELECT count() FROM {TABLE}") == str(ROWS), "every row landed"

    pg(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    wipe_state()
    print(f"✓ a pg drain reports its changes live, {len(live)} mid-run line(s), last {last_rows}")


if __name__ == "__main__":
    main()
