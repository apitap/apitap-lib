"""A ClickHouse replace publishes and cleans up past max_table_size_to_drop.

ClickHouse refuses to DROP a table larger than max_table_size_to_drop
(default 50 GB). A replace with engine= into an existing table dropped its
staging BEFORE the EXCHANGE, and staging still held every part (ATTACH FROM
hard-links them): over the limit the run failed unpublished and left two
artifacts that refused every later run (deep review D20). apitap's drops of
its own tables now lift the limit for that one statement.

Proof, on a throwaway ClickHouse 24.8 whose limit is 1 byte (so every
non-empty table is "too big"): pg -> ch mode='replace' with
engine='ReplacingMergeTree' twice (the second goes through the shadow and
the EXCHANGE), the source changed in between. The second run must publish
the new rows, and no apitap artifact may be left.
"""
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

import apitap

NAME = "apitap-qa-ch-dropcap"
PORT = 18127
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = f"clickhouse://default:bench@127.0.0.1:{PORT}/default"
T = "ch_replace_big_drop"
CAP = "<clickhouse><max_table_size_to_drop>1</max_table_size_to_drop></clickhouse>\n"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}/?" + urllib.parse.urlencode({"query": sql}),
                                 headers={"X-ClickHouse-User": "default", "X-ClickHouse-Key": "bench"},
                                 method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read().decode().strip()


def up(confdir):
    sh(["docker", "rm", "-f", NAME])
    r = sh(["docker", "run", "-d", "--name", NAME, "-p", f"127.0.0.1:{PORT}:8123",
            "-e", "CLICKHOUSE_PASSWORD=bench", "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
            "-v", f"{confdir}/cap.xml:/etc/clickhouse-server/config.d/cap.xml:ro",
            "clickhouse/clickhouse-server:24.8"])
    assert r.returncode == 0, r.stderr
    for _ in range(120):
        try:
            if ch("SELECT 1") == "1":
                return
        except Exception:  # noqa: BLE001 — not up yet
            pass
        time.sleep(1)
    raise RuntimeError("(rig) the throwaway ClickHouse never came up")


def main():
    with tempfile.TemporaryDirectory() as d:
        os.chmod(d, 0o755)
        with open(os.path.join(d, "cap.xml"), "w") as f:
            f.write(CAP)
        os.chmod(os.path.join(d, "cap.xml"), 0o644)
        up(d)
        try:
            ch("CREATE TABLE cap_probe (x UInt64) ENGINE = MergeTree ORDER BY x")
            ch("INSERT INTO cap_probe SELECT number FROM numbers(1000)")
            try:
                ch("DROP TABLE cap_probe")
                raise AssertionError("(rig) the 1-byte max_table_size_to_drop is not in force")
            except urllib.error.HTTPError:
                pass
            ch("DROP TABLE cap_probe SETTINGS max_table_size_to_drop = 0")

            pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v int); "
               f"INSERT INTO {T} SELECT g, 1 FROM generate_series(1, 1000) g")
            apitap.transfer(PG, CH, table=T, mode="replace", engine="ReplacingMergeTree")
            pg(f"UPDATE {T} SET v = 2; INSERT INTO {T} SELECT g, 2 FROM generate_series(1001, 1500) g")
            apitap.transfer(PG, CH, table=T, mode="replace", engine="ReplacingMergeTree")
            got = ch(f"SELECT concat(toString(count()), ':', toString(sum(v))) FROM {T}")
            assert got == "1500:3000", f"the second replace published {got!r}, want 1500:3000"
            left = ch(f"SELECT arrayStringConcat(groupArray(name), ',') FROM system.tables "
                      f"WHERE database = 'default' AND name LIKE '%{T}%' AND name != '{T}'")
            assert left == "", f"apitap artifacts left behind: {left}"
            print(f"  two replaces under a 1-byte drop limit: {got}, no artifacts left")
        finally:
            sh(["docker", "rm", "-f", NAME])
            pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ a ClickHouse replace publishes and cleans up past max_table_size_to_drop")


if __name__ == "__main__":
    main()
