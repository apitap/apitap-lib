"""A MySQL replace refuses only what the user added, never what apitap or the server did.

The N24 refusal probes the old table for objects a rebuild would drop. It
read apitap's own work as the user's: under sql_require_primary_key=ON a
key-less table gets apitap's invisible `_apitap_rowid AUTO_INCREMENT` key,
and with explicit_defaults_for_timestamp=OFF (MariaDB <= 10.9, MySQL 5.7)
the server gives a NOT NULL TIMESTAMP an implicit DEFAULT. Either way the
SECOND replace of the same table was refused (deep review 3, R10/R26,
R11/R27).

Proof: on the MySQL destination, (a) with sql_require_primary_key=ON,
pg -> mysql replaces a key-less table twice; (b) with
explicit_defaults_for_timestamp=OFF, mysql -> mysql replaces a table with
`created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP` twice. Both second
replaces land.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MYSRC = "mysql://root:bench@127.0.0.1:3307/bench"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
TA, TB = "my_replace_twice_keyless", "my_replace_twice_ts"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def mysql(c, sql):
    o = sh(["docker", "exec", c, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return mysql("apitap-bench-my", sql)


def dst(sql):
    return mysql("apitap-bench-my-dst", sql)


def wipe():
    pg(f"DROP TABLE IF EXISTS {TA}")
    src(f"DROP TABLE IF EXISTS {TB}")
    dst(f"DROP TABLE IF EXISTS {TA}; DROP TABLE IF EXISTS {TB}")


def main():
    req = dst("SELECT @@global.sql_require_primary_key")
    edt = dst("SELECT @@global.explicit_defaults_for_timestamp")
    wipe()
    try:
        pg(f"CREATE TABLE {TA} (a int, b text); INSERT INTO {TA} VALUES (1, 'x'), (2, 'y')")
        dst("SET GLOBAL sql_require_primary_key = ON")
        for i in (1, 2):
            apitap.transfer(PG, MY, table=TA, mode="replace")
        n = dst(f"SELECT COUNT(*) FROM {TA}")
        assert n == "2", f"key-less table: {n} rows after two replaces"
        print("  sql_require_primary_key=ON: a key-less table replaces twice")
        dst(f"SET GLOBAL sql_require_primary_key = {req}")

        src(f"CREATE TABLE {TB} (id int PRIMARY KEY, created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP); "
            f"INSERT INTO {TB} (id) VALUES (1), (2)")
        dst("SET GLOBAL explicit_defaults_for_timestamp = OFF")
        for i in (1, 2):
            apitap.transfer(MYSRC, MY, table=TB, mode="replace")
        n = dst(f"SELECT COUNT(*) FROM {TB}")
        assert n == "2", f"timestamp table: {n} rows after two replaces"
        print("  explicit_defaults_for_timestamp=OFF: a NOT NULL TIMESTAMP table replaces twice")
    finally:
        dst(f"SET GLOBAL sql_require_primary_key = {req}")
        dst(f"SET GLOBAL explicit_defaults_for_timestamp = {edt}")
        wipe()
    print("✓ a MySQL replace refuses only what the user added, never what apitap or the server did")


if __name__ == "__main__":
    main()
