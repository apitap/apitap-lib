"""A Postgres merge and CDC apply land in a table whose ALWAYS identity is not its key.

N20 added OVERRIDING SYSTEM VALUE to the INSERTs, but an identity column
GENERATED ALWAYS can only be SET to DEFAULT: a merge's DO UPDATE SET, and
the CDC residue's UPDATE ... SET and DO UPDATE, still named it when it was
not the key, and failed every run with 428C9. On PostgreSQL 18 a VIRTUAL
generated column was not filtered at all (deep review 3, R25).

Proof: (a) pg -> pg merge, cursor=id, into a destination keyed by uuid with
seq bigint GENERATED ALWAYS AS IDENTITY: two runs (the second updates a
row). (b) pg -> pg log_based into the same shape, with a masked update (a
TOASTed column left unchanged) and an upsert; its bootstrap rebuilds the
destination from the source's columns, so this case guards the apply
rather than reproducing the 0.63.0 failure. (c) PG 18 -> PG 18 append and
merge between two tables with a VIRTUAL generated column. Every run lands
and the destinations equal their sources.
"""
import subprocess

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
PG18 = "postgres://postgres:bench@127.0.0.1:5547/apitap_bench_src"
T = "pg_identity_nonkey"


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


def d18(sql):
    return psql("apitap-bench-pg18", "apitap_bench_src", sql)


def clear_state(q):
    q(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
      f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def rows(q, cols="id, seq, qty, md5(big)"):
    return q(f"SELECT string_agg(concat_ws(':', {cols}), ',' ORDER BY id) FROM {T}")


# The destination is the source's own schema, as pg_dump makes it.
SHAPE = DEST_SHAPE = ("u uuid PRIMARY KEY, seq bigint GENERATED ALWAYS AS IDENTITY, "
                      "id int UNIQUE NOT NULL, qty int, big text")


def main():
    before = (set(src("SELECT slot_name FROM pg_replication_slots").split()),
              set(src("SELECT pubname FROM pg_publication").split()))

    def wipe():
        for s_ in set(src("SELECT slot_name FROM pg_replication_slots WHERE NOT active").split()) - before[0]:
            src(f"SELECT pg_drop_replication_slot('{s_}')")
        for p in set(src("SELECT pubname FROM pg_publication").split()) - before[1]:
            src(f"DROP PUBLICATION IF EXISTS {p}")
        src(f"DROP TABLE IF EXISTS {T}")
        dst(f"DROP TABLE IF EXISTS {T}")
        d18(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {T}_out")
        clear_state(dst)
        clear_state(d18)

    import sys
    which = sys.argv[1:] or ["merge", "cdc", "virtual"]
    try:
      if "merge" in which:
        # (a) merge
        wipe()
        src(f"CREATE TABLE {T} ({SHAPE}); ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL; "
            f"INSERT INTO {T} (u, id, qty, big) SELECT gen_random_uuid(), g, g, repeat(md5(g::text), 100) "
            f"FROM generate_series(1, 3) g")
        dst(f"CREATE TABLE {T} ({DEST_SHAPE})")
        apitap.transfer(SRC, DST, table=T, mode="merge", cursor="id")
        src(f"UPDATE {T} SET qty = 30 WHERE id = 3; INSERT INTO {T} (u, id, qty, big) VALUES (gen_random_uuid(), 4, 4, 'x')")
        apitap.transfer(SRC, DST, table=T, mode="merge", cursor="id")
        assert rows(dst) == rows(src), f"[merge] destination {rows(dst)!r}, source {rows(src)!r}"
        print("  [merge] a non-key ALWAYS identity: both runs land")

      if "cdc" in which:
        # (b) log_based, masked update + upsert
        wipe()
        src(f"CREATE TABLE {T} ({SHAPE}); ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL; "
            f"INSERT INTO {T} (u, id, qty, big) SELECT gen_random_uuid(), g, g, repeat(md5(g::text), 100) "
            f"FROM generate_series(1, 3) g")
        dst(f"CREATE TABLE {T} ({DEST_SHAPE})")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        src(f"UPDATE {T} SET qty = 20 WHERE id = 2; INSERT INTO {T} (u, id, qty, big) VALUES (gen_random_uuid(), 5, 5, 'y')")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        assert rows(dst) == rows(src), f"[log_based] destination {rows(dst)!r}, source {rows(src)!r}"
        print("  [log_based] masked update and upsert into a non-key ALWAYS identity: land")

      if "virtual" in which:
        # (c) PostgreSQL 18 VIRTUAL generated column, the destination the
        # source's own schema (both on the 18 server).
        wipe()
        vshape = "id int PRIMARY KEY, qty int, price int, total int GENERATED ALWAYS AS (qty * price)"
        d18(f"CREATE TABLE {T} ({vshape}); CREATE TABLE {T}_out ({vshape}); "
            f"INSERT INTO {T} (id, qty, price) VALUES (1, 1, 5), (2, 2, 5)")
        apitap.transfer(PG18, PG18, table=T, dest_table=f"{T}_out", mode="append", cursor="id")
        d18(f"INSERT INTO {T} (id, qty, price) VALUES (3, 3, 5)")
        apitap.transfer(PG18, PG18, table=T, dest_table=f"{T}_out", mode="merge", cursor="id")
        got = d18(f"SELECT string_agg(id || ':' || total, ',' ORDER BY id) FROM {T}_out")
        assert got == "1:5,2:10,3:15", f"[virtual] destination {got!r}"
        print(f"  [PG 18 VIRTUAL] append and merge land, total computed: {got}")
    finally:
        wipe()
    print("✓ a Postgres merge and CDC apply land in a table whose ALWAYS identity is not its key")


if __name__ == "__main__":
    main()
