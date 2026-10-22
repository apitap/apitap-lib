"""A key change that only changes case never wedges a MySQL -> MySQL lane.

MySQL -> MySQL mirrors the source column's collation, so a utf8mb4_0900_ai_ci
key stays case-insensitive at the destination, and so does the delete twin
built from it. An UPDATE that only changes a key's case ('Alice@X.com' ->
'alice@x.com') collapses to a delete of the old key plus an upsert of the
new one: two byte-distinct keys, one key under the twin's collation. LOAD
DATA skipped the second with Warning 1062, the key load refused the window,
and every rerun cut the same window (deep review 2, N4).

Proof (mysql -> mysql log_based, PRIMARY KEY email in the default _ci
collation): bootstrap, then change one key's case. The drain must succeed
and the destination must hold the new spelling, once. Then 2000 such changes
in one window, past max_error_count: the same.
"""
import subprocess

import apitap

SRC = "mysql://root:bench@127.0.0.1:3307/bench"
DST = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_ci_key_change"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def mysql(container, sql):
    o = sh(["docker", "exec", container, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return mysql("apitap-bench-my", sql)


def dst(sql):
    return mysql("apitap-bench-my-dst", sql)


def rows(q):
    return q(f"SET SESSION group_concat_max_len = 10000000; "
             f"SELECT GROUP_CONCAT(CONCAT(email, ':', v) ORDER BY email COLLATE utf8mb4_bin) FROM {T}")


def wipe():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    if dst("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'bench' "
           "AND table_name = '_apitap_state'") != "0":
        dst(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")


def main():
    wipe()
    try:
        src(f"CREATE TABLE {T} (email varchar(64) PRIMARY KEY, v int) "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci; "
            f"INSERT INTO {T} VALUES ('Alice@X.com', 1), ('bob@x.com', 2)")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        assert rows(dst) == rows(src), "(rig) bootstrap exact"
        src(f"UPDATE {T} SET email = 'alice@x.com' WHERE email = 'Alice@X.com'")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        s, d = rows(src), rows(dst)
        assert d == s, f"after a case-only key change: destination {d!r}, source {s!r}"
        print(f"  a case-only key change in a _ci key: applied, {d}")
        # R7/R17: a bulk normalisation, more case-only collisions in one
        # window than max_error_count (1024 on MySQL 8, 64 on MariaDB) lists.
        src(f"SET SESSION cte_max_recursion_depth = 5000; INSERT INTO {T} SELECT CONCAT('User', n, '@X.com'), n FROM "
            f"(WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 2000) "
            f"SELECT n FROM s) q")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        src(f"UPDATE {T} SET email = LOWER(email) WHERE email LIKE 'User%'")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        s, d = rows(src), rows(dst)
        assert d == s, "after 2000 case-only key changes in one window the destination differs"
        print("  2000 case-only key changes in one window: applied")
    finally:
        wipe()
    print("✓ a key change that only changes case never wedges a MySQL -> MySQL lane")


if __name__ == "__main__":
    main()
