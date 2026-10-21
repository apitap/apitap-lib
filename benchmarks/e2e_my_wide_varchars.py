"""A pg -> mysql copy of a table with several wide varchars fits MySQL's row.

Each bounded varchar(n) became utf8mb4 VARCHAR(n), 4n + 2 bytes of MySQL's
65,535-byte row, with no running total: two varchar(10000) columns, or five
varchar(4000), failed the staging CREATE with ERROR 1118 ("Row size too
large") on every run (deep review 2, N23).

Proof (pg -> mysql replace): id, title varchar(255), body varchar(10000),
notes varchar(10000). Every row lands with its values; columns stay
VARCHAR while the row has room, the rest become LONGTEXT.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_wide_varchars"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst(sql):
    o = sh(["docker", "exec", "apitap-bench-my-dst", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    pg(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    try:
        pg(f"CREATE TABLE {T} (id varchar(40) PRIMARY KEY, title varchar(255), body varchar(10000), "
           f"notes varchar(10000)); "
           f"INSERT INTO {T} VALUES ('a', 't', repeat('b', 9000), repeat('n', 9999)), ('b', NULL, 'x', NULL)")
        apitap.transfer(PG, MY, table=T, mode="replace")
        want = pg(f"SELECT string_agg(id || ':' || length(coalesce(body, '')) || ':' || length(coalesce(notes, '')), "
                  f"',' ORDER BY id) FROM {T}")
        got = dst(f"SELECT GROUP_CONCAT(CONCAT(id, ':', LENGTH(COALESCE(body, '')), ':', "
                  f"LENGTH(COALESCE(notes, ''))) ORDER BY id) FROM {T}")
        assert got == want, f"destination {got!r}, source {want!r}"
        types = dst(f"SELECT GROUP_CONCAT(COLUMN_NAME, ' ', DATA_TYPE ORDER BY ORDINAL_POSITION) "
                    f"FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'bench' AND TABLE_NAME = '{T}'")
        # 4n + 2 bytes each: id, title and body fit 65,535; notes would not.
        assert types == "id varchar,title varchar,body varchar,notes longtext", f"columns {types!r}"
        print(f"  columns: {types}")
        print(f"  two varchar(10000) columns next to a varchar key: {got}")
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")
        dst(f"DROP TABLE IF EXISTS {T}")
    print("✓ a pg -> mysql copy of wide varchars fits MySQL's row")


if __name__ == "__main__":
    main()
