"""A MySQL destination never publishes rows LOAD DATA silently dropped or changed.

LOAD DATA LOCAL implies IGNORE: a duplicate key, a clipped value or a bad
date is a WARNING, the row skipped or altered, and the load "succeeds".
Nothing read the warnings. A Postgres text key 'abc'/'ABC' (one key under
the server's default _ci collation) published one row of two, and under the
loader's sql_mode='' a timestamptz 'infinity' landed as 0000-00-00 — both
silently (MASTER review M-20, M-51).

Proof, Postgres -> MySQL 8 replace:
  1. case- and accent-distinct text keys all land (the key column is
     utf8mb4_bin, Postgres' byte equality);
  2. a timestamptz 'infinity' is refused loudly, nothing published;
  3. MySQL -> MySQL with a utf8mb4_bin source key into a _ci server: refused
     or exact, never short.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MYSRC = "mysql://root:bench@127.0.0.1:3307/bench"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_sink_warn"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def mysql(container, sql):
    o = sh(["docker", "exec", container, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst(sql):
    return mysql("apitap-bench-my-dst", sql)


def src(sql):
    return mysql("apitap-bench-my", sql)


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    src(f"DROP TABLE IF EXISTS {T}")
    for n in dst(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'bench' "
                 f"AND table_name LIKE '{T}%'").split():
        dst(f"DROP TABLE IF EXISTS `{n}`")


def leftovers():
    return dst(f"SELECT COALESCE(GROUP_CONCAT(table_name), '') FROM information_schema.tables "
               f"WHERE table_schema = 'bench' AND table_name LIKE '{T}%' AND table_name != '{T}'")


def refused(url, why, words=("warning",)):
    try:
        apitap.transfer(url, MY, table=T, mode="replace")
    except RuntimeError as e:
        assert any(w in str(e) for w in words), f"{why}: the refusal must say why: {e}"
        return str(e)
    raise AssertionError(f"{why}: the load must be refused")


def main():
    wipe()
    pg(f"CREATE TABLE {T} (k varchar(16) PRIMARY KEY, v int, ts timestamptz)")
    pg(f"INSERT INTO {T} VALUES ('abc', 1, now()), ('ABC', 2, now()), ('e', 3, now()), ('é', 4, now()), "
       f"('a ', 5, now()), ('a', 6, now())")
    apitap.transfer(PG, MY, table=T, mode="replace")
    got = dst(f"SELECT COUNT(*), GROUP_CONCAT(v ORDER BY v) FROM {T}")
    assert got == "6\t1,2,3,4,5,6", f"every Postgres key must land, got {got!r}"
    print("  case/accent/trailing-space distinct text keys: all 6 land")

    pg(f"INSERT INTO {T} VALUES ('inf', 7, 'infinity')")
    # Refused loudly either way: the binary lane's decoder has no infinity,
    # and the LOAD DATA warnings check is the backstop behind it.
    e = refused(PG, "timestamptz infinity", ("warning", "malformed timestamp"))
    assert dst(f"SELECT COUNT(*) FROM {T}") == "6", "the published table must be the previous one"
    assert dst(f"SELECT COUNT(*) FROM {T} WHERE CAST(ts AS CHAR) LIKE '0000%'") == "0"
    assert leftovers() == "", f"nothing may be left behind, found {leftovers()}"
    print(f"  timestamptz 'infinity': refused ({e[:80]}...), previous table intact")

    wipe()
    src(f"CREATE TABLE {T} (k varchar(10) COLLATE utf8mb4_bin PRIMARY KEY, v int)")
    src(f"INSERT INTO {T} VALUES ('abc', 1), ('ABC', 2)")
    try:
        apitap.transfer(MYSRC, MY, table=T, mode="replace")
        got = dst(f"SELECT COUNT(*) FROM {T}")
        assert got == "2", f"MySQL -> MySQL published {got} of 2 rows"
        print("  utf8mb4_bin source key: both rows land")
    except RuntimeError as e:
        assert "warning" in str(e), e
        assert leftovers() == "", f"nothing may be left behind, found {leftovers()}"
        print("  utf8mb4_bin source key into a _ci server: refused, never short")
    wipe()
    print("✓ a MySQL destination never publishes rows LOAD DATA dropped or changed")


if __name__ == "__main__":
    main()
