"""One source, one table, two destinations: each destination gets its own slot.

Replication-slot names used to be computed from the source alone, so two
pipelines draining the same table into two destinations agreed on one name:
the second run found the first's slot and the two fought over it — one
destination's continuity silently depended on the other's schedule (system
review 2026-10-07, P1). The name now hashes the destination's origin, so each
destination resumes its own position.

This leg proves the independence through the data: bootstrap and one resume
round into ClickHouse AND into Postgres, with both destinations seeing exactly
the same changes — a shared or stolen slot would knock one of them off.
"""
import os
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
TABLE = "two_dst"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def psql(container, db, sql):
    o = sh(["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", db, "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def pgd(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


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
    if pgd("SELECT count(*) FROM information_schema.tables "
           "WHERE table_name='_apitap_state'") != "0":
        pgd(f"DELETE FROM _apitap_state WHERE dest_table = '{TABLE}'")


def send(dst):
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {dst!r}, table={TABLE!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code], env=dict(os.environ), timeout=240)


def main():
    wipe_state()
    pg(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
    pgd(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    pg(f"CREATE TABLE {TABLE} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {TABLE} VALUES (1, 'a'), (2, 'b');")

    r = send(CH)
    assert r.returncode == 0, r.stderr
    r = send(PGD)
    assert r.returncode == 0, r.stderr
    assert ch(f"SELECT count() FROM {TABLE}") == "2", "ClickHouse bootstrapped"
    assert pgd(f"SELECT count(*) FROM {TABLE}") == "2", "Postgres bootstrapped"

    # The resume round: a shared or stolen slot would knock one side off it.
    pg(f"UPDATE {TABLE} SET v = 'a2' WHERE id = 1; INSERT INTO {TABLE} VALUES (3, 'c');")
    r = send(CH)
    assert r.returncode == 0, f"ClickHouse resume:\n{r.stderr}"
    r = send(PGD)
    assert r.returncode == 0, f"Postgres resume:\n{r.stderr}"
    for name, rows, a_val in (("ClickHouse", ch(f"SELECT count() FROM {TABLE}"), 
                               ch(f"SELECT v FROM {TABLE} WHERE id = 1")),
                              ("Postgres", pgd(f"SELECT count(*) FROM {TABLE}"),
                               pgd(f"SELECT v FROM {TABLE} WHERE id = 1"))):
        assert rows == "3", f"{name} saw every change, got {rows!r} rows"
        assert a_val == "a2", f"{name} saw the update, got {a_val!r}"

    pg(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
    pgd(f"DROP TABLE IF EXISTS {TABLE} CASCADE;")
    ch(f"DROP TABLE IF EXISTS {TABLE};")
    wipe_state()
    print("✓ one source into two destinations: both bootstrap and both resume, independently")


if __name__ == "__main__":
    main()
