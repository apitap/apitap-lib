"""A MySQL load survives a source that takes longer than net_read_timeout.

The MySQL sink opens its LOAD DATA LOCAL before the source produces, and
bytes move only at 4 MiB boundaries. A slow or selective source (a delta
scan over a big table, a rate-limited API) left the server waiting for the
first packet past its net_read_timeout, and the load died ("Packets out of
sync", "Got timeout reading communication packets") on every run (deep
review 2, N22).

Proof (mysql -> mysql replace, parallel=1, the destination's global
net_read_timeout lowered to 1 s for the leg): the source is a view that
sleeps 2 s per row over 2 rows, so nothing arrives for 4 s. Both rows must
land.
"""
import subprocess

import apitap

MYSRC = "mysql://root:bench@127.0.0.1:3307/bench"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T, V = "my_slow_source_t", "my_slow_source"


def mysql(container, sql):
    o = subprocess.run(["docker", "exec", container, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return mysql("apitap-bench-my", sql)


def dst(sql):
    return mysql("apitap-bench-my-dst", sql)


def main():
    src(f"DROP VIEW IF EXISTS {V}; DROP TABLE IF EXISTS {T}; "
        f"CREATE TABLE {T} (id int PRIMARY KEY, n int); INSERT INTO {T} VALUES (1, 1), (2, 2); "
        f"CREATE VIEW {V} AS SELECT id, n FROM {T} WHERE SLEEP(2) = 0")
    dst(f"DROP TABLE IF EXISTS {V}")
    before = dst("SELECT @@global.net_read_timeout")
    try:
        dst("SET GLOBAL net_read_timeout = 1")
        apitap.transfer(MYSRC, MY, table=V, mode="replace", parallel=1)
        n = dst(f"SELECT COUNT(*) FROM {V}")
        assert n == "2", f"{n} rows landed, want 2"
        print("  a source silent for 4 s into a 1 s net_read_timeout destination: both rows landed")
    finally:
        dst(f"SET GLOBAL net_read_timeout = {before}")
        dst(f"DROP TABLE IF EXISTS {V}")
        src(f"DROP VIEW IF EXISTS {V}; DROP TABLE IF EXISTS {T}")
    print("✓ a MySQL load survives a source slower than net_read_timeout")


if __name__ == "__main__":
    main()
