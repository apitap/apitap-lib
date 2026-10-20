"""A char(n) key on the Postgres CDC residue path matches its row.

A masked update (an unchanged out-of-line TOAST cell) and every later op on
its key go through the residue, whose placeholders are cast to the column's
type without its typmod. cbbaf6d mapped `character` to `text`: WAL ships a
char(n) key blank-padded ('abc       '), `bpchar = text` resolves to texteq,
which trims the column but not the value, and the key never matched. The
masked update was refused as a missing row on every rerun, and a delete
after it matched nothing, silently (deep review D11).

Proof (pg -> pg, key char(10) holding 'abc', body STORAGE EXTERNAL so it is
really out of line): window 1 is a masked update; window 2 is a masked
update and then a delete of the same key. Both drains must succeed, and the
destination must equal the source after each.
"""
import subprocess

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_char_key_residue"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db,
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


FP = f"SELECT coalesce(string_agg(k || ':' || v || ':' || md5(body), ',' ORDER BY k), '') FROM {T}"


def same(stage):
    s, d = src(FP), dst(FP)
    assert s == d, f"{stage}: source {s!r} vs destination {d!r}"
    print(f"  {stage}: equal ({len(s.split(',')) if s else 0} rows)")


def wipe():
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def main():
    wipe()
    src(f"CREATE TABLE {T} (k char(10) PRIMARY KEY, v int, body text)")
    src(f"ALTER TABLE {T} ALTER COLUMN body SET STORAGE EXTERNAL")
    src(f"INSERT INTO {T} SELECT k, 0, repeat(k, 2000) FROM unnest(ARRAY['abc', 'def', 'ghi']) k")
    ext = src(f"SELECT pg_relation_size(reltoastrelid) > 0 FROM pg_class WHERE relname = '{T}'")
    assert ext == "t", "(rig) body is not stored out of line; the masked path would not run"
    try:
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        same("bootstrap")
        src(f"UPDATE {T} SET v = 1 WHERE k = 'abc'")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        same("a masked update on a char(10) key")
        src(f"BEGIN; UPDATE {T} SET v = 2 WHERE k = 'def'; DELETE FROM {T} WHERE k = 'def'; COMMIT")
        apitap.transfer(SRC, DST, table=T, mode="log_based")
        same("a masked update then a delete of the same char(10) key")
    finally:
        wipe()
    print("✓ a char(n) key on the residue path matches its row")


if __name__ == "__main__":
    main()
