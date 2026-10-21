"""A mysql -> mysql replace copies a long latin1 key the source indexes fine.

The unindexable-key check took every VARCHAR past 768 characters for
unindexable, the utf8mb4 cap. InnoDB's cap is 3072 BYTES: a latin1
VARCHAR(1000) key is 1000 bytes, indexed fine at the source, and every
replace and log_based bootstrap of such a table was refused with "MySQL
cannot index" since a86626a (deep review 2, N25).

Proof: a source table with PRIMARY KEY url VARCHAR(1000) CHARACTER SET
latin1, replaced into the MySQL destination: every row lands.
"""
import subprocess

import apitap

MYSRC = "mysql://root:bench@127.0.0.1:3307/bench"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_latin1_long_key"


def mysql(container, sql):
    o = subprocess.run(["docker", "exec", container, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    for c in ("apitap-bench-my", "apitap-bench-my-dst"):
        mysql(c, f"DROP TABLE IF EXISTS {T}")
    try:
        mysql("apitap-bench-my", f"CREATE TABLE {T} (url VARCHAR(1000) CHARACTER SET latin1 PRIMARY KEY, n int); "
                                 f"INSERT INTO {T} VALUES (REPEAT('a', 900), 1), ('b', 2)")
        apitap.transfer(MYSRC, MY, table=T, mode="replace")
        got = mysql("apitap-bench-my-dst", f"SELECT COUNT(*), MAX(LENGTH(url)) FROM {T}")
        assert got == "2\t900", f"the destination holds {got!r}, want 2 rows with the 900-byte key"
        print(f"  latin1 VARCHAR(1000) key replaced: {got.split()[0]} rows")
    finally:
        for c in ("apitap-bench-my", "apitap-bench-my-dst"):
            mysql(c, f"DROP TABLE IF EXISTS {T}")
    print("✓ a mysql -> mysql replace copies a long latin1 key")


if __name__ == "__main__":
    main()
