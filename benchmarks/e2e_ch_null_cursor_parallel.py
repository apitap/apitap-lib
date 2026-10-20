"""A parallel read into ClickHouse never drops the rows whose cursor is NULL.

The ClickHouse sink makes the cursor column non-Nullable (it orders by it),
and it rewrote that flag on the shared plan before the spans were built. The
spans then took the cursor for NOT NULL and left `OR c IS NULL` off the last
span, so with parallel > 1 every NULL-cursor row vanished from a green run,
while the serial read of the same table failed loudly on the first NULL
(deep review 2, N6/N9; shipped in 0.62.0 with a520d16).

Proof (pg -> ch, append, cursor=c, parallel=4, 100 of 1000 cursors NULL):
the run must fail loudly, as the serial read does; a run that succeeds must
have landed all 1000 rows.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_null_cursor_parallel"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (n int, c int); "
           f"INSERT INTO {T} SELECT g, CASE WHEN g % 10 = 0 THEN NULL ELSE g END FROM generate_series(1, 1000) g")
        try:
            apitap.transfer(PG, CH, table=T, mode="append", cursor="c", parallel=4)
        except Exception as e:  # noqa: BLE001 — loud is the correct outcome
            print(f"  a NULL cursor into ClickHouse fails loudly: {str(e)[:100]}...")
        else:
            n = ch(f"SELECT count() FROM {T}")
            assert n == "1000", f"a green parallel run landed {n} of 1000 rows: the NULL-cursor rows were dropped"
            print("  every row landed")
    finally:
        wipe()
    print("✓ a parallel read into ClickHouse never drops NULL-cursor rows")


if __name__ == "__main__":
    main()
