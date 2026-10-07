"""A non-transactional engine's explicit transaction ends with a QUERY `COMMIT`.

MyISAM and Aria cannot join the server's two-phase commit, so an explicit
transaction over them is closed in the binlog by a QUERY event whose whole
payload is `COMMIT` — no XID follows. Classifying that event as ordinary left
the CDC window without its commit boundary: the drain read the transaction
forever or stopped short of it, and the rows it carried never landed (system
review 2026-10-07, R1). The classifier is word-bounded now; this leg proves it
against the live server, where a wrong answer means the second run does not
finish or the destination misses the transaction.

The child gets a hard timeout: the broken behaviour is a HANG, and a leg that
hangs is a leg that can never report anything.
"""
import os
import subprocess
import sys

import apitap

MA = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLE = "r1_mi"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def ma(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot",
            "-pbench", "-N", "-D", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe_state():
    if ch("SELECT count() FROM system.tables WHERE database='default' "
         "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{TABLE}' "
           "SETTINGS mutations_sync=1")


def drain(timeout=90):
    code = (
        "import apitap\n"
        f"r = apitap.transfer({MA!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code], env=dict(os.environ), timeout=timeout)


def main():
    wipe_state()
    ma(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    ma(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text) ENGINE=MyISAM;")
    ma(f"INSERT INTO {TABLE} VALUES (1, 'a');")

    r = drain()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TABLE}") == "1", "bootstrap lands the baseline row"

    # The shape R1 is about: a transaction over a non-transactional table.
    ma(f"BEGIN; INSERT INTO {TABLE} VALUES (2, 'b'); "
       f"UPDATE {TABLE} SET v = 'a2' WHERE id = 1; COMMIT;")
    r = drain()
    assert r.returncode == 0, f"the drain must finish on the QUERY COMMIT boundary:\n{r.stderr}"
    assert ch(f"SELECT v FROM {TABLE} WHERE id = 1") == "a2", "the transaction's UPDATE landed"
    assert ch(f"SELECT count() FROM {TABLE} WHERE id = 2") == "1", "the transaction's INSERT landed"

    # And the ordinary autocommit path still works beside it.
    ma(f"INSERT INTO {TABLE} VALUES (3, 'c');")
    r = drain()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TABLE}") == "3", "the autocommit insert landed"

    ma(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    wipe_state()
    print("✓ MyISAM transaction closed by QUERY COMMIT drains; autocommit beside it too")


if __name__ == "__main__":
    main()
