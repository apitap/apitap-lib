"""A Postgres CDC drain refuses a tracked table that was dropped and recreated.

DROP TABLE removes the table from its publication and emits nothing to the
slot. The next drain found it missing, ran ALTER PUBLICATION ADD TABLE and
drained on: pgoutput decides membership by the catalog at each change, so
the new table's changes before the re-add were skipped, and the old table's
rows stayed at the destination, on a green run (deep review D17).

Proof (pg -> pg): bootstrap ids 1-3, then DROP + CREATE the source table
and insert ids 10-11. The next drain must refuse and say the table left its
publication; a drain that succeeds must leave the destination equal to the
source.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_recreated_table"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(c, db, sql):
    o = sh(["docker", "exec", c, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def wipe():
    for slot in src(f"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE '%{T}%' "
                    f"AND NOT active").split():
        src(f"SELECT pg_drop_replication_slot('{slot}')")
    for pub in src(f"SELECT pubname FROM pg_publication WHERE pubname LIKE '%{T}%'").split():
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def ids(q):
    return q(f"SELECT COALESCE(string_agg(id::text, ',' ORDER BY id), '') FROM {T}")


def main():
    wipe()
    try:
        src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} VALUES (1,'a'),(2,'a'),(3,'a')")
        apitap.transfer(PG, DST, table=T, mode="log_based")
        assert ids(dst) == "1,2,3", "(rig) bootstrap"

        src(f"DROP TABLE {T}; CREATE TABLE {T} (id int PRIMARY KEY, v text); "
            f"INSERT INTO {T} VALUES (10,'b'),(11,'b')")
        try:
            apitap.transfer(PG, DST, table=T, mode="log_based")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "no longer carries" in msg and "Re-bootstrap" in msg, f"refused for the wrong reason: {msg}"
            print(f"  the recreated table is refused: {msg[:100]}...")
        else:
            s, d = ids(src), ids(dst)
            assert d == s, f"a green drain after DROP + CREATE: destination {d!r}, source {s!r}"
            print("  the drain followed the recreated table exactly")
    finally:
        wipe()
    print("✓ a Postgres CDC drain never drains on over a dropped-and-recreated table")


if __name__ == "__main__":
    main()
