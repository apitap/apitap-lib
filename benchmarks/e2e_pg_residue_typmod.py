"""A Postgres CDC residue write never truncates a value to fit the column.

The residue path (re-keys, masked updates) binds every value as text and
casts it to the column's type — `format_type` WITH the typmod, so
`$1::character varying(3)`. An explicit cast to varchar(n) truncates
silently, where the column's own assignment, and the bulk lane, refuse a
value that does not fit (P2, dest_pg residue cast). The cast now names the
bare type and the column applies its length.

Proof: bootstrap a source varchar(10) with an out-of-line body into
Postgres, narrow the destination column to varchar(3), and update the row
to a 6-character value without touching the body (a masked update): the
drain is refused ("value too long"); the destination never holds 'abc'.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_residue_typmod"


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
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def main():
    wipe()
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v varchar(10), body text); "
        f"ALTER TABLE {T} ALTER body SET STORAGE EXTERNAL; "
        f"INSERT INTO {T} VALUES (1, 'ab', repeat('x', 5000)), (2, 'cd', repeat('y', 5000))")
    apitap.transfer(PG, DST, table=T, mode="log_based")
    dst(f"ALTER TABLE {T} ALTER v TYPE varchar(3)")
    # body untouched and out of line: the WAL omits it, so this is a MASKED
    # update — the residue path, a bound `SET v = $n::<type>`.
    src(f"UPDATE {T} SET v = 'abcdef' WHERE id = 1")
    try:
        apitap.transfer(PG, DST, table=T, mode="log_based")
        m = None
    except Exception as e:  # noqa: BLE001
        m = str(e)
    held = dst(f"SELECT coalesce(string_agg(id || ':' || v, ',' ORDER BY id), '') FROM {T}")
    assert "1:abc," not in held + ",", f"the destination holds a truncated value: {held}"
    assert m and "too long" in m, f"the re-key must be refused, value too long; got {m!r}, destination {held}"
    print(f"  a 6-char masked update into varchar(3): refused ({m[:80]}...), destination {held}")
    wipe()
    print("✓ a Postgres CDC residue write never truncates a value to fit the column")


if __name__ == "__main__":
    main()
