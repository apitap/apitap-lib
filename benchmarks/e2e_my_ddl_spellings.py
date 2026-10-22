"""MySQL CDC refuses the row-removing DDL whatever way it is spelled.

The D13/N5/N16 refusals parsed a statement word by word after its LEADING
comments only, so `rename /* gh-ost */ table ...` (gh-ost's own cut-over) and
`ALTER /* cron */ TABLE t DROP PARTITION p` passed as schema-only. So did
MariaDB's `RENAME TABLE IF EXISTS`, `RENAME TABLE t WAIT n TO`, `CREATE OR
REPLACE DATABASE`, and MySQL's per-partition `DISCARD PARTITION p
TABLESPACE`: each left the destination holding rows the source no longer has,
on a green run (deep review 3, R6, R8/R20, R9).

Proof (-> ClickHouse; each case bootstraps its table, runs the DDL, drains):
on MySQL 8, a comment after RENAME's verb, a comment after ALTER's verb with
DROP PARTITION, and DISCARD PARTITION ... TABLESPACE; on MariaDB 10.6,
RENAME TABLE IF EXISTS, RENAME ... WAIT 1 TO, and CREATE OR REPLACE
DATABASE. Every drain must refuse.
"""
import subprocess
import sys

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
SERVERS = {
    "mysql": ("apitap-bench-my", "mysql://root:bench@127.0.0.1:3307/bench"),
    "mariadb": ("apitap-bench-mariadb", "mysql://root:bench@127.0.0.1:3309/bench"),
}


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def run_sql(container, sql):
    # --comments: the CLI strips comments by default, and a driver (gh-ost's
    # own) sends them; the server logs the statement as it received it.
    o = sh(["docker", "exec", container, "mysql", "-uroot", "-pbench", "--comments", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch_clear(t):
    ch(f"DROP TABLE IF EXISTS {t}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{t}' SETTINGS mutations_sync = 1")


def refused(server, label, setup, ddl, table, dest=None, cleanup=""):
    container, url = SERVERS[server]
    dest = dest or table
    ch_clear(dest)
    try:
        run_sql(container, setup)
        kw = {"dest_table": dest} if dest != table else {}
        apitap.transfer(url, CH, table=table, mode="log_based", **kw)
        run_sql(container, ddl)
        try:
            apitap.transfer(url, CH, table=table, mode="log_based", **kw)
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            assert "removes or swaps rows" in str(e), f"[{label}] refused for the wrong reason: {e}"
            print(f"  [{server}: {label}] refused")
            return
        raise AssertionError(f"[{server}: {label}] a green drain after {ddl!r}: the destination keeps the old rows")
    finally:
        if cleanup:
            run_sql(container, cleanup)
        ch_clear(dest)


def mysql_cases():
    r = "my_ddl_sp_rot"
    refused("mysql", "comment after RENAME",
            f"DROP TABLE IF EXISTS {r}, {r}_new, {r}_old; CREATE TABLE {r} (id int PRIMARY KEY); "
            f"INSERT INTO {r} VALUES (1),(2); CREATE TABLE {r}_new (id int PRIMARY KEY); INSERT INTO {r}_new VALUES (5)",
            f"rename /* gh-ost */ table {r} to {r}_old, {r}_new to {r}", r,
            cleanup=f"DROP TABLE IF EXISTS {r}, {r}_new, {r}_old")
    p = "my_ddl_sp_part"
    part = (f"DROP TABLE IF EXISTS {p}; CREATE TABLE {p} (id int NOT NULL, m int NOT NULL, PRIMARY KEY (id, m)) "
            f"PARTITION BY RANGE (m) (PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (3)); "
            f"INSERT INTO {p} VALUES (1,1),(2,1),(3,2)")
    refused("mysql", "comment after ALTER", part, f"ALTER /* cron */ TABLE {p} DROP PARTITION p1", p,
            cleanup=f"DROP TABLE IF EXISTS {p}")
    refused("mysql", "DISCARD PARTITION TABLESPACE", part, f"ALTER TABLE {p} DISCARD PARTITION p1 TABLESPACE", p,
            cleanup=f"DROP TABLE IF EXISTS {p}")


def mariadb_cases():
    r = "ma_ddl_sp_rot"
    setup = (f"DROP TABLE IF EXISTS {r}, {r}_new, {r}_old; CREATE TABLE {r} (id int PRIMARY KEY); "
             f"INSERT INTO {r} VALUES (1),(2); CREATE TABLE {r}_new (id int PRIMARY KEY); "
             f"INSERT INTO {r}_new VALUES (5)")
    clean = f"DROP TABLE IF EXISTS {r}, {r}_new, {r}_old"
    refused("mariadb", "RENAME TABLE IF EXISTS", setup,
            f"RENAME TABLE IF EXISTS {r} TO {r}_old, {r}_new TO {r}", r, cleanup=clean)
    refused("mariadb", "RENAME ... WAIT 1 TO", setup,
            f"RENAME TABLE {r} WAIT 1 TO {r}_old, {r}_new TO {r}", r, cleanup=clean)
    db, t = "apitap_ddl_sp_db", "ma_ddl_sp_db_t"
    refused("mariadb", "CREATE OR REPLACE DATABASE",
            f"DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}; CREATE TABLE {db}.{t} (id int PRIMARY KEY); "
            f"INSERT INTO {db}.{t} VALUES (1),(2)",
            f"CREATE OR REPLACE DATABASE {db}; CREATE TABLE {db}.{t} (id int PRIMARY KEY); "
            f"INSERT INTO {db}.{t} VALUES (9)",
            f"{db}.{t}", t, cleanup=f"DROP DATABASE IF EXISTS {db}")


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "mysql"):
        mysql_cases()
    if which in ("all", "maria"):
        mariadb_cases()
    print("✓ MySQL CDC refuses the row-removing DDL whatever way it is spelled")


if __name__ == "__main__":
    main()
