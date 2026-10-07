"""A MySQL string column in a non-UTF-8 charset is refused before a CDC run.

The binlog carries a string column's bytes in the column's OWN charset, while
a bootstrap reads the same column decoded through the connection's utf8mb4
charset. For anything but UTF-8-compatible charsets the two lanes would write
DIFFERENT bytes for the same row — silent divergence between the bootstrap and
the drain (system review 2026-10-07, R2). apitap must refuse up front, name
the columns and charsets, and say the remedy.

This leg proves the refusal on a latin1 table, and that the same table drains
cleanly once the column is utf8mb4 — through the data, not just the message.
"""
import os
import subprocess
import sys

import apitap

MA = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLE = "cs_latin1"


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
    """Any earlier attempt's watermark would make the next run a drain of a
    table that no longer exists — clear it, table and state together."""
    if ch("SELECT count() FROM system.tables WHERE database='default' "
         "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{TABLE}' "
           "SETTINGS mutations_sync=1")


def drain():
    code = (
        "import apitap\n"
        f"r = apitap.transfer({MA!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code], env=dict(os.environ))


def main():
    wipe_state()
    ma(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    ma(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text CHARACTER SET latin1);")
    ma(f"INSERT INTO {TABLE} VALUES (1, 'caf{chr(233)}'), (2, 'plain');")

    r = drain()
    assert r.returncode != 0, "a latin1 string column must refuse"
    out = (r.stderr or "") + (r.stdout or "")
    for want in ("non-UTF-8 charset", "latin1", "v (latin1)", "utf8mb4"):
        assert want in out, f"refusal must name {want!r}; got:\n{out}"

    # The remedy the refusal names.
    ma(f"ALTER TABLE {TABLE} MODIFY v text CHARACTER SET utf8mb4;")
    r = drain()
    assert r.returncode == 0, f"utf8mb4 must drain:\n{r.stderr}"
    n = ch(f"SELECT count() FROM {TABLE}")
    assert n == "2", f"both rows landed, got {n!r}"
    v = ch(f"SELECT v FROM {TABLE} WHERE id = 1")
    assert v == "caf" + chr(233), f"the accented value round-tripped, got {v!r}"

    ma(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    wipe_state()
    print("✓ non-UTF-8 string columns refused with columns+charset+remedy; utf8mb4 drains clean")


if __name__ == "__main__":
    main()
