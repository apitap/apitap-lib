"""A parallel MySQL read copies the rows whose cursor is NULL.

A parallel read over an integer cursor splits [min, max] into spans, each
`cursor >= lo AND cursor <= hi`. A NULL cursor matches no span, so with
parallel > 1 every such row was dropped from a green run, while the serial
read kept it (deep review D30, K9). Postgres got the fix earlier
(`e2e_pg_hardening.py nullcursor`). The ClickHouse source has the same spans
and the same fix, but no route reaches it with a NULL cursor today: it only
writes to ClickHouse, which orders by the cursor and refuses the NULL loudly.

Proof: 1000 rows, every tenth with a NULL cursor, mysql -> postgres append
with cursor=c and parallel=4. The destination must hold all 1000 rows, 900
of them with a cursor.
"""
import subprocess

import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
PG = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "null_cursor_spans"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def run(a):
    o = sh(a)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    return run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])


def pg(sql):
    return run(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst",
                "-v", "ON_ERROR_STOP=1", "-Atc", sql])


def wipe():
    my(f"DROP TABLE IF EXISTS {T}")
    pg(f"DROP TABLE IF EXISTS {T}")
    pg(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
       f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def main():
    wipe()
    try:
        my(f"CREATE TABLE {T} (n int NOT NULL, c int NULL); "
           f"INSERT INTO {T} WITH RECURSIVE g(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM g WHERE i < 1000) "
           f"SELECT i, IF(i % 10 = 0, NULL, i) FROM g")
        apitap.transfer(MY, PG, table=T, mode="append", cursor="c", parallel=4)
        got = pg(f"SELECT count(*) || ':' || count(c) FROM {T}")
        assert got == "1000:900", f"every row, NULL cursors included (rows:non-null), got {got}"
        print(f"  a parallel read over a cursor with 100 NULLs: {got}")
    finally:
        wipe()
    print("✓ a parallel MySQL read copies NULL-cursor rows")


if __name__ == "__main__":
    main()
