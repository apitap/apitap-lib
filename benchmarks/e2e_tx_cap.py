"""One giant transaction cannot buffer without end: the cap refuses, and the
same transaction lands once the cap fits it.

Postgres ships a v1-protocol transaction whole after its commit, so a single
transaction's row data is buffered in the drain until its commit arrives — an
UPDATE over a whole table is hundreds of megabytes in one go. apitap bounds
that buffer with a hard per-transaction cap (system review 2026-10-07, G0.1):
past it the run ends with a refusal that names the table, the buffered size
and `APITAP_TX_BUF_BYTES`, and the watermark stays untouched, so a later run
with a bigger cap re-reads the transaction.

This leg builds a transaction too big for a 4 MiB cap and proves: it refuses
with the remedy; nothing landed; the same transaction lands under a 64 MiB
cap.
"""
import os
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLE = "tx_cap"
ROWS = 30001  # ~15 MiB of row frames at 400 bytes/row, inside one transaction


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
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


def drain(env_extra=None, timeout=180):
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={TABLE!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    env = dict(os.environ)
    env.update(env_extra or {})
    return sh([sys.executable, "-c", code], env=env, timeout=timeout)


def main():
    wipe_state()
    pg(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    pg(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text);")
    pg(f"INSERT INTO {TABLE} VALUES (1, 'a');")

    r = drain()
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TABLE}") == "1", "bootstrap lands the baseline"

    # One statement = one transaction, ~15 MiB of row frames.
    pg(f"INSERT INTO {TABLE} SELECT g, repeat('x', 400) "
       f"FROM generate_series(2, {ROWS}) g;")

    r = drain(env_extra={"APITAP_TX_BUF_BYTES": "4M"})
    assert r.returncode != 0, "a transaction past the cap must refuse"
    out = (r.stderr or "") + (r.stdout or "")
    for want in (TABLE, "APITAP_TX_BUF_BYTES", "MiB"):
        assert want in out, f"refusal must name {want!r}; got:\n{out}"
    assert ch(f"SELECT count() FROM {TABLE}") == "1", \
        "the refused run must not have applied the transaction"

    r = drain(env_extra={"APITAP_TX_BUF_BYTES": "64M"})
    assert r.returncode == 0, f"the transaction must land under a fitting cap:\n{r.stderr}"
    n = ch(f"SELECT count() FROM {TABLE}")
    assert n == str(ROWS), f"all rows landed, got {n!r}"

    pg(f"DROP TABLE IF EXISTS {TABLE};")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    wipe_state()
    print("✓ a transaction past the byte cap refuses with its remedy; a fitting cap lands it whole")


if __name__ == "__main__":
    main()
