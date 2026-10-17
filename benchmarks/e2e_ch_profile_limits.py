"""A ClickHouse profile that shortens answers cannot shorten a transfer.

A server profile may set `limit`, `offset`, `max_result_rows` with
`result_overflow_mode='break'`, and more: each one cuts an answer while
ClickHouse still says 200. The source's count() cross-check is gone, so a
ClickHouse -> X read returned the first rows and called it the table
(MASTER review M-30). Every request now pins those settings.

Proof, on a throwaway ClickHouse 24.8:
  1. default profile limit=3, offset=1 — a 200-row table read from it into
     the bench ClickHouse (parallel and not) and loaded into it from
     Postgres: every row, every time;
  2. default profile max_result_rows=5, result_overflow_mode='break' — the
     overflow mode is pinned to throw, so the read is either every row or a
     refusal naming the limit; never a short table;
  3. default profile readonly=1 — refused, naming readonly=2 (MASTER M-53).
"""
import os
import subprocess
import tempfile
import time

import apitap

PORT = 18301
NAME = "apitap-qa-ch-limits"
CH = f"clickhouse://default:lim@127.0.0.1:{PORT}/default"
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_prof_lim"
PROFILE = """<clickhouse>
  <profiles><default>{settings}</default></profiles>
  <users><default><password>lim</password><networks><ip>::/0</ip></networks></default></users>
</clickhouse>
"""


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def chq(sql):
    # clickhouse-client applies the profile too; turn it off for the checks.
    o = sh(["docker", "exec", NAME, "clickhouse-client", "--password", "lim", "--limit", "0", "--offset", "0",
            "--max_result_rows", "0", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def bench(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def bench_wipe():
    bench(f"DROP TABLE IF EXISTS {T}")
    if bench("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        bench(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def start(settings):
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "limits.xml"), "w") as f:
        f.write(PROFILE.format(settings=settings))
    os.chmod(d, 0o755)
    sh(["docker", "rm", "-f", NAME])
    r = sh(["docker", "run", "-d", "--rm", "--name", NAME, "-p", f"127.0.0.1:{PORT}:8123",
            "-v", f"{d}/limits.xml:/etc/clickhouse-server/users.d/limits.xml:ro",
            "clickhouse/clickhouse-server:24.8"])
    assert r.returncode == 0, r.stderr
    for _ in range(120):
        if sh(["curl", "-sf", f"http://127.0.0.1:{PORT}/ping"]).returncode == 0:
            return
        time.sleep(0.5)
    raise AssertionError("the throwaway ClickHouse never answered")


def main():
    start("<limit>3</limit><offset>1</offset>")
    try:
        probe = sh(["curl", "-s", f"http://default:lim@127.0.0.1:{PORT}/", "--data-binary",
                    "SELECT number FROM numbers(100)"]).stdout.split()
        assert len(probe) < 5, f"the profile must shorten a plain read, got {len(probe)} rows"

        chq(f"CREATE TABLE {T} (id Int64, v String) ENGINE = MergeTree ORDER BY id")
        chq(f"INSERT INTO {T} SELECT number + 1, 'v' FROM numbers(200)")
        for par in (1, 2):
            bench_wipe()
            apitap.transfer(CH, DST, table=T, mode="replace", parallel=par)
            got = bench(f"SELECT concat(toString(count()), ':', toString(sum(id))) FROM {T}")
            assert got == "200:20100", f"ClickHouse -> ClickHouse, parallel={par}: got {got} (want 200:20100)"
            print(f"  ClickHouse (limit=3 profile) -> ClickHouse, parallel={par}: all 200 rows")

        psql("apitap-bench-pg-src", "apitap_bench_src", f"DROP TABLE IF EXISTS {T}")
        psql("apitap-bench-pg-src", "apitap_bench_src",
             f"CREATE TABLE {T} (id bigint PRIMARY KEY, v text); INSERT INTO {T} SELECT g, 'v' FROM generate_series(1, 200) g")
        chq(f"DROP TABLE {T}")
        for _ in range(2):
            apitap.transfer(PG, CH, table=T, mode="replace")
            got = chq(f"SELECT concat(toString(count()), ':', toString(sum(id))) FROM {T}")
            assert got == "200:20100", f"Postgres -> ClickHouse (limit=3 profile): got {got}"
        print("  Postgres -> ClickHouse (limit=3 profile), replace x2: all 200 rows")

        start("<max_result_rows>5</max_result_rows><result_overflow_mode>break</result_overflow_mode>")
        chq(f"CREATE TABLE {T} (id Int64, v String) ENGINE = MergeTree ORDER BY id")
        chq(f"INSERT INTO {T} SELECT number + 1, 'v' FROM numbers(200)")
        bench_wipe()
        try:
            apitap.transfer(CH, DST, table=T, mode="replace")
            got = bench(f"SELECT concat(toString(count()), ':', toString(sum(id))) FROM {T}")
            assert got == "200:20100", f"a capped read published a short table: {got}"
            print("  max_result_rows=5 with break: every row (the cap never cut the stream)")
        except RuntimeError as e:
            assert "TOO_MANY_ROWS" in str(e), e
            assert bench(f"SELECT count() FROM system.tables WHERE database = 'default' AND name = '{T}'") == "0", \
                "nothing may be published from a capped read"
            print("  max_result_rows=5 with break: refused naming the limit, nothing published")

        # A readonly=1 user can take no per-query setting at all, and every
        # request carries some: it must be told what to change (M-53).
        start("<readonly>1</readonly>")
        try:
            apitap.transfer(CH, DST, table=T, mode="replace")
            m = None
        except Exception as e:  # noqa: BLE001 — any refusal, judged by its text
            m = str(e)
        assert m and "readonly=2" in m, f"a readonly=1 user must be told to use readonly=2: {m}"
        print("  readonly=1 profile: refused, naming readonly=2")
    finally:
        sh(["docker", "rm", "-f", NAME])
        psql("apitap-bench-pg-src", "apitap_bench_src", f"DROP TABLE IF EXISTS {T}")
        bench_wipe()
    print("✓ a ClickHouse profile that shortens answers cannot shorten a transfer")


if __name__ == "__main__":
    main()
