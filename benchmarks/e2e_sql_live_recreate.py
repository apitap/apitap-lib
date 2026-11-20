"""Incremental views (apitap.sql mode='log_based') over a source table that is recreated, and over an inet key.

QA, round P p20 (both pre-existing on p18):
  D1  the source table is dropped and recreated: the view's publication
      silently lost the table and every drain reported "ok, 0 change images"
      while the destination kept the old groups, for ever. Now a drain over a
      table the publication no longer carries, or a table that is not the one
      the view was built from (relation oid), is refused by name; a first run
      re-adds the table to the publication before its snapshot.
  D2  a view grouped by an inet column: the bootstrap keyed '10.0.0.2/32'
      (`col::text`), the stream's image '10.0.0.2' (the output function): the
      first UPDATE died "state out of step" and the view stayed stuck. Now
      every text rendering uses the type's output function where `::text` is
      a cast of its own.

  1  view, drop + recreate the table (no reset): the next drain is refused by
     name, never "ok" over stale groups
  2  reset as the refusal says (drop slot, state, destination): the first run
     re-adds the table to the publication; an UPDATE then reaches the view
  3  view grouped by inet: UPDATE and DELETE move it, equal to Postgres

Usage: e2e_sql_live_recreate.py
"""
import os
import shutil
import subprocess
import sys
import tempfile

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + str(detail)[:240] if detail else ''}", flush=True)
    ok = ok and bool(good)


def psql(sql, container="apitap-bench-pg-src", db="apitap_bench_src"):
    o = subprocess.run(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1",
                        "-Atc", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr[-300:])
    return o.stdout.strip()


def dst(sql):
    return psql(sql, "apitap-bench-pg-dst", "apitap_bench_dst")


def view(query, table, state):
    code = (f"import apitap\nr = apitap.sql({query!r}, sources={{'pg': {PG!r}}}, dest={PGD!r}, table={table!r}, "
            f"mode='log_based')\nprint('VIEWOK', r)\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       env=dict(os.environ, APITAP_DEBUG="1", APITAP_STATE_DIR=state))
    return r.returncode, r.stdout + r.stderr


SLOTS0 = set(psql("SELECT slot_name FROM pg_replication_slots").split())
PUBS0 = set(psql("SELECT pubname FROM pg_publication").split())


def drop_new_slots():
    for s in set(psql("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS0:
        psql(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots WHERE slot_name = '{s}' AND NOT active")
    for p in set(psql("SELECT pubname FROM pg_publication").split()) - PUBS0:
        psql(f'DROP PUBLICATION IF EXISTS "{p}"')


def make(t, ddl, rows):
    psql(f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} ({ddl}); ALTER TABLE {t} REPLICA IDENTITY FULL; INSERT INTO {t} VALUES {rows}")


def groups(t, col):
    return sorted(psql(f"SELECT {col}::text || '|' || count(*) || '|' || sum(id) FROM {t} GROUP BY {col}").splitlines())


def view_groups(out, col):
    return sorted(dst(f"SELECT {col}::text || '|' || n || '|' || s FROM {out}").splitlines())


def main():
    st1, st3 = tempfile.mkdtemp(prefix="lrc-1-"), tempfile.mkdtemp(prefix="lrc-3-")
    T, OUT, T3, OUT3 = "lrc_src", "lrc_out", "lrc_inet", "lrc_inet_out"
    try:
        print("════════ D1: the source table dropped and recreated ════════", flush=True)
        make(T, "id int PRIMARY KEY, g int", "(1, 1), (2, 1), (3, 2)")
        q = f"SELECT g, COUNT(*) AS n, SUM(id) AS s FROM pg.{T} GROUP BY g"
        rc, out = view(q, OUT, st1)
        case("1: the first run lands", rc == 0, out[-200:])
        make(T, "id int PRIMARY KEY, g int", "(1, 1), (2, 1), (3, 2)")
        psql(f"UPDATE {T} SET g = 3 WHERE id = 1")
        rc, out = view(q, OUT, st1)
        stale = view_groups(OUT, "g") != groups(T, "g")
        case("1: the drain over the recreated table is refused by name, never 'ok' over stale groups",
             rc != 0 and ("recreated" in out or "no longer carries" in out), (out.strip().splitlines() or [""])[-1][:240])
        case("1: (and the destination is not silently stale-but-ok)", rc != 0 or not stale, f"rc {rc} stale {stale}")
        # Reset as the refusal says: slot, state, destination.
        drop_new_slots_view = [s for s in set(psql("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS0]
        for s in drop_new_slots_view:
            psql(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots WHERE slot_name = '{s}' AND NOT active")
        shutil.rmtree(st1)
        st1 = tempfile.mkdtemp(prefix="lrc-1b-")
        dst(f"DROP TABLE IF EXISTS {OUT}")
        rc, out = view(q, OUT, st1)
        case("2: after the reset, the first run lands", rc == 0, out[-200:])
        members = psql(f"SELECT count(*) FROM pg_publication_tables WHERE tablename = '{T}' "
                       f"AND pubname LIKE 'apitap_v_%'")
        case("2: the publication carries the table again", members not in ("", "0"), members)
        psql(f"UPDATE {T} SET g = 4 WHERE id = 2; DELETE FROM {T} WHERE id = 3")
        rc, out = view(q, OUT, st1)
        case("2: the drain moves the view", rc == 0 and view_groups(OUT, "g") == groups(T, "g"),
             f"view {view_groups(OUT, 'g')} pg {groups(T, 'g')} {out[-160:] if rc else ''}")

        print("════════ D2: a view grouped by an inet column ════════", flush=True)
        make(T3, "id int PRIMARY KEY, ip inet", "(1, '10.0.0.2'), (2, '10.0.0.2'), (3, '10.0.0.10/8'), (4, '::1')")
        q3 = f"SELECT ip, COUNT(*) AS n, SUM(id) AS s FROM pg.{T3} GROUP BY ip"
        rc, out = view(q3, OUT3, st3)
        case("3: the first run lands", rc == 0, out[-200:])
        psql(f"UPDATE {T3} SET ip = '10.0.0.3' WHERE id = 1; DELETE FROM {T3} WHERE id = 4")
        rc, out = view(q3, OUT3, st3)
        case("3: UPDATE and DELETE move the view (not 'state out of step')", rc == 0, (out.strip().splitlines() or [""])[-1][:240])
        case("3: equal to Postgres", view_groups(OUT3, "ip::inet") == groups(T3, "ip"),
             f"view {view_groups(OUT3, 'ip::inet')} pg {groups(T3, 'ip')}")
    finally:
        drop_new_slots()
        for t in (T, T3):
            psql(f"DROP TABLE IF EXISTS {t}")
        for t in (OUT, OUT3):
            dst(f"DROP TABLE IF EXISTS {t}")
        shutil.rmtree(st1, ignore_errors=True)
        shutil.rmtree(st3, ignore_errors=True)
    print("SQL LIVE RECREATE E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
