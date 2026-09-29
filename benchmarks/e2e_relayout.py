"""A Postgres source table changes its columns between two drains' worth of
traffic: one window never spans the change (pg -> pg).

pgoutput re-sends a table's Relation after a DDL, and until 0.57.0 the drain
kept ONE column list per table for the whole window — the latest. A window
holding rows from before `ALTER TABLE t ADD COLUMN w` and rows from after it
rendered the old rows under the new column list, and the destination refused
the COPY ("missing data for column w") on every re-drain: the table's
watermark never moved again, even after the operator had added the column to
the destination too. 0.57.0 gives every window the layout its rows were read
under: a transaction whose Relation changes a table the window already holds
opens the next window instead.

  R1  the column is added BETWEEN transactions (and to the destination, as a
      user's migration would): the drain succeeds, the rows from both sides of
      the change land, the destination equals the source, the watermark moved
  R2  ONE transaction adds a column between two of its own row changes: no
      window boundary can separate them, so the drain refuses with the reason
      and the remedy, and writes nothing (row count and watermark unchanged)

Every answer is asked of the Postgres servers. The leg drops what it created,
pass or fail: its tables, its state rows, and only the slots and publications
that appeared while it ran.
"""
import subprocess
import sys

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "cdc_relayout"


def psql(host, db, sql):
    o = subprocess.run(["docker", "exec", host, "psql", "-U", "postgres", "-d", db, "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


src = lambda sql: psql("apitap-bench-pg-src", "apitap_bench_src", sql)
dst = lambda sql: psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def drain():
    """One log_based run in a child of this interpreter: (rc, last stderr line)."""
    code = f"import apitap\napitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')\n"
    o = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300)
    tail = [l for l in o.stderr.strip().splitlines() if l.strip()][-1:] or [""]
    return o.returncode, tail[0][-500:]


def mark():
    return dst(f"SELECT coalesce(max(watermark), '') FROM _apitap_state WHERE dest_table = '{T}'")


def rows(q, cols):
    return q(f"SELECT {cols} FROM {T} ORDER BY id")


ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail and not good else ''}")
    ok = ok and bool(good)


slots0 = set(src("SELECT slot_name FROM pg_replication_slots").split())
pubs0 = set(src("SELECT pubname FROM pg_publication").split())


def clean():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    if dst("SELECT to_regclass('public._apitap_state') IS NOT NULL") == "t":
        dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")
    for s in sorted(set(src("SELECT slot_name FROM pg_replication_slots").split()) - slots0):
        src(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
            f"WHERE slot_name = '{s}' AND NOT active")
    for p in sorted(set(src("SELECT pubname FROM pg_publication").split()) - pubs0):
        src(f"DROP PUBLICATION IF EXISTS {p}")


clean()
try:
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    src(f"INSERT INTO {T} VALUES (1,'a'),(2,'b'),(3,'c')")
    rc, err = drain()
    if rc:
        print(f"   (rig) bootstrap failed rc={rc}: {err} — FAILED")
        raise SystemExit(1)

    print("== R1: a column added between two transactions of one window ==")
    w0 = mark()
    src(f"INSERT INTO {T} VALUES (4,'d')")                      # read under (id, v)
    src(f"ALTER TABLE {T} ADD COLUMN w text")
    dst(f"ALTER TABLE {T} ADD COLUMN w text")                   # the user's migration
    src(f"INSERT INTO {T} VALUES (5,'e','x')")                  # read under (id, v, w)
    src(f"UPDATE {T} SET w = 'y' WHERE id = 1")
    rc, err = drain()
    case("the drain succeeds", rc == 0, f"rc={rc} {err}")
    cols = "id || '|' || v || '|' || coalesce(w, '<N>')"
    s, d = rows(src, cols), rows(dst, cols)
    case("the destination equals the source, rows from both sides of the ALTER",
         s == d, f"\n      src {s.split()}\n      dst {d.split()}")
    w1 = mark()
    case(f"the watermark moved ({w0} -> {w1})", w1 != "" and w1 != w0)

    print("== R2: one transaction adds a column between two of its own rows ==")
    dst(f"ALTER TABLE {T} ADD COLUMN u int")
    n0, w0 = dst(f"SELECT count(*) FROM {T}"), mark()
    src(f"BEGIN; INSERT INTO {T} VALUES (6,'f','z'); ALTER TABLE {T} ADD COLUMN u int; "
        f"INSERT INTO {T} VALUES (7,'g','z',1); COMMIT;")
    rc, err = drain()
    case("the drain refuses, saying why and what to do",
         rc != 0 and "definition changed inside one source transaction" in err
         and "Clear this table's apitap state" in err, f"rc={rc} {err}")
    n1, w1 = dst(f"SELECT count(*) FROM {T}"), mark()
    case(f"nothing was written ({n0} rows, watermark {w0})", n1 == n0 and w1 == w0,
         f"rows {n0} -> {n1}, watermark {w0} -> {w1}")
finally:
    clean()
    print("   cleaned up the table, its state rows, and this leg's slots and publications")

print("\nE2E RELAYOUT: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
