"""A MySQL/MariaDB partial date is refused by the bulk load, not folded into another day.

MariaDB's default sql_mode accepts '2024-05-00'. The binary protocol hands it
over as year 2024, month 5, day 0, and the bulk decoder's day arithmetic made
that 2024-04-30 — a real date the source never held, silently (MASTER review
M-41). A zero month does the same a month further back.

Proof: load a MariaDB table holding a partial date into Postgres — the run is
refused naming the value, and nothing lands; with the partial row gone the
same table loads and every date is the source's.
"""
import subprocess

import apitap

MA = "mysql://root:bench@127.0.0.1:3309/bench"
PG = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "my_partial_dates"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def ma(sql):
    o = sh(["docker", "exec", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    ma(f"DROP TABLE IF EXISTS {T}")
    pg(f"DROP TABLE IF EXISTS {T}")


def main():
    wipe()
    ma(f"SET sql_mode = ''; CREATE TABLE {T} (id int PRIMARY KEY, d date, dt datetime); "
       f"INSERT INTO {T} VALUES (1, '2024-05-01', '2024-05-01 10:00:00'), (2, '2024-05-00', '2024-05-02 10:00:00'), "
       f"(3, '2024-05-03', '2024-00-10 10:00:00')")
    assert ma(f"SELECT d FROM {T} WHERE id = 2") == "2024-05-00", "the server must hold the partial date"
    for bad in (2, 3):
        try:
            apitap.transfer(MA, PG, table=T, mode="replace")
            raise AssertionError("a partial date must not load")
        except RuntimeError as e:
            assert "partial binary DATE" in str(e), f"the refusal must name the partial date: {e}"
        assert pg(f"SELECT count(*) FROM pg_tables WHERE tablename = '{T}'") == "0", "nothing may land"
        ma(f"DELETE FROM {T} WHERE id = {bad}")
        print(f"  partial date in row {bad}: refused naming it, nothing landed")
    apitap.transfer(MA, PG, table=T, mode="replace")
    got = pg(f"SELECT string_agg(id || '|' || d || '|' || dt, ',' ORDER BY id) FROM {T}")
    assert got == "1|2024-05-01|2024-05-01 10:00:00", got
    print("  without them: every date is the source's")
    wipe()
    print("✓ a partial MySQL/MariaDB date is refused by the bulk load, never shifted to another day")


if __name__ == "__main__":
    main()
