"""An append onto rows whose cursor is NULL everywhere never lands them twice.

A replace built the destination (no state row) from a table whose cursor
column is NULL in every row (a newly added updated_at). The next append found
rows, no state, and MAX(cursor) NULL: "no watermark", read as "copy
everything", so every row landed a second time on a green run, and again on
every run after (deep review 3, R12; Iceberg already refused this). Now the
append refuses, naming the cursor; a merge, which upserts by key, still runs.

Proof (pg -> pg and mysql -> mysql): replace 3 rows with updated_at NULL,
then append with cursor=updated_at: refused, and the destination still holds
3 rows; on Postgres a merge with the same cursor lands, still 3 rows.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
MYS = "mysql://root:bench@127.0.0.1:3307/bench"
MYD = "mysql://root:bench@127.0.0.1:3308/bench"
T = "append_null_cursor"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(c, db, sql):
    o = sh(["docker", "exec", c, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def mysql(c, sql):
    o = sh(["docker", "exec", c, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


CASES = [
    ("pg", PG, PGD, lambda q: psql("apitap-bench-pg-src", "apitap_bench_src", q),
     lambda q: psql("apitap-bench-pg-dst", "apitap_bench_dst", q),
     f"CREATE TABLE {T} (id int PRIMARY KEY, v text, updated_at timestamptz)",
     f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
     f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$"),
    ("mysql", MYS, MYD, lambda q: mysql("apitap-bench-my", q), lambda q: mysql("apitap-bench-my-dst", q),
     f"CREATE TABLE {T} (id int PRIMARY KEY, v varchar(10), updated_at datetime(6) NULL)",
     f"DELETE FROM _apitap_state WHERE dest_table = '{T}'"),
]


def main():
    for name, src_url, dst_url, src, dst, ddl, clear in CASES:
        src(f"DROP TABLE IF EXISTS {T}")
        dst(f"DROP TABLE IF EXISTS {T}")
        try:
            src(ddl)
            src(f"INSERT INTO {T} (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            apitap.transfer(src_url, dst_url, table=T, mode="replace")
            try:
                dst(clear)
            except RuntimeError:
                pass  # no state table yet
            try:
                apitap.transfer(src_url, dst_url, table=T, mode="append", cursor="updated_at")
                refused = None
            except Exception as e:  # noqa: BLE001 — the message is what is checked
                refused = str(e)
            n = dst(f"SELECT COUNT(*) FROM {T}")
            assert n == "3", f"[{name}] append over an all-NULL cursor left {n} rows, want 3"
            assert refused and "NULL 'updated_at'" in refused, f"[{name}] refused for the wrong reason: {refused}"
            if name == "pg":  # a MySQL destination takes no merge at all
                dst(f"ALTER TABLE {T} ADD PRIMARY KEY (id)")  # merge keys on it; replace adds none
                apitap.transfer(src_url, dst_url, table=T, mode="merge", cursor="updated_at")
                n = dst(f"SELECT COUNT(*) FROM {T}")
                assert n == "3", f"[{name}] merge left {n} rows, want 3"
            print(f"  [{name}] append refused over an all-NULL cursor, still 3 rows")
        finally:
            src(f"DROP TABLE IF EXISTS {T}")
            dst(f"DROP TABLE IF EXISTS {T}")
    print("✓ an append onto rows whose cursor is NULL everywhere never lands them twice")


if __name__ == "__main__":
    main()
