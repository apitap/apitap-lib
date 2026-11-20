"""Key moves through the changelog bodies: views and changelog=True (round P, p12).

p12 made a changelog body's key move cheaper (L14b): the U half's `moved_from`
keeps the old key's bytes only when the row has an unchanged-TOAST cell (mask
resolution reads them there), any other move keeps an empty marker, and the D
half takes the old image instead of a copy. A view of a table without a
primary key moves on EVERY update (its identity is every column), and the
copies doubled the body at the commit (QA case M, exit 137).

Every case is one transaction of key moves, drained once, and checked against
the server:

  V1  view over a PK table: UPDATE id = id + 100000 leaving the STORAGE
      EXTERNAL text alone (a masked move), plus moves that rewrite the text
      and plain updates. The view groups by the text, so the moved rows'
      untouched value must come from the D half's old image. Through p11
      this was refused ("an unchanged TOAST value with no old image").
  V2  view over a table with no primary key (REPLICA IDENTITY FULL): every
      update is a move, no TOAST cell masked (the case-M shape, small).
  C1  pg -> ClickHouse changelog=True on the PK table: the same transaction;
      `<table>__current` equals the source (the masked moves are read back at
      the OLD key: the kept key bytes).
  C2  pg -> ClickHouse changelog=True: moves without a masked cell only (the
      empty marker).

Usage: PKG=dir-holding-apitap e2e_changelog_key_move.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
N = 3000
ok = True


def sh(a, **kw):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **kw)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)

def view_slots(run, tables):
    """The apitap_v_ slots (= their publications' names) of views over `tables`:
    a view's publication is FOR TABLE its Postgres table. Only these are this
    leg's to drop; another run's views (QA's, a user's) are never touched.
    Call before the tables are dropped (that empties the publication)."""
    tl = ", ".join(f"'{t}'" for t in tables)
    return set(run(f"SELECT DISTINCT pubname FROM pg_publication_tables WHERE pubname LIKE 'apitap\\_v\\_%' "
                   f"AND tablename IN ({tl})").split())


def drop_view_slots(run, tables):
    for n in view_slots(run, tables):
        run(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{n}' AND NOT active")
        run(f'DROP PUBLICATION IF EXISTS "{n}"')



def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def case(label, good, detail):
    global ok
    ok &= bool(good)
    print(f"  {'ok ' if good else 'BAD'} {label}: {detail}", flush=True)


def _pkg_dir():
    import importlib.util
    return os.environ.get("PKG") or os.path.dirname(os.path.dirname(importlib.util.find_spec("apitap").origin))


def py(code, state=None):
    env = dict(os.environ, PYTHONPATH=_pkg_dir())
    env.pop("APITAP_DEBUG", None)
    if state:
        env["APITAP_STATE_DIR"] = state
    r = sh([sys.executable, "-c", code], env=env)
    out = [l for l in r.stdout.splitlines() if l.startswith(("ROWS", "ERR"))]
    return out[0] if out else ("ERR " + r.stderr.strip()[-300:])


def view(state, query, table):
    return py("import apitap\ntry:\n"
              f"    r = apitap.sql({query!r}, sources={{'pg': {PG!r}}}, dest={PGD!r}, table={table!r}, mode='log_based')\n"
              "    print('ROWS', r.rows)\nexcept Exception as e:\n    print('ERR', str(e)[:300])\n", state)


def transfer(table):
    return py("import apitap\ntry:\n"
              f"    r = apitap.transfer({PG!r}, {CH!r}, table={table!r}, mode='log_based', changelog=True)\n"
              "    print('ROWS', r.rows)\nexcept Exception as e:\n    print('ERR', str(e)[:300])\n")


def cleanup():
    # The changelog lanes' slots, by their publications' tables; the views'
    # slots and publications, those this run made (QA's are left alone).
    for pub in set(src("SELECT pubname FROM pg_publication_tables WHERE tablename LIKE 'km\\_%'").split()):
        if pub.startswith("apitap_v_"):
            continue
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f'DROP PUBLICATION IF EXISTS "{pub}"')
    drop_view_slots(src, ["km_pk", "km_nopk"])
    src("DROP TABLE IF EXISTS km_pk, km_nopk, km_ch1, km_ch2")
    dst("DROP TABLE IF EXISTS km_v1, km_v2")
    for t in ("km_ch1", "km_ch2"):
        ch(f"DROP VIEW IF EXISTS {t}__current")
        ch(f"DROP TABLE IF EXISTS {t}")
        for s in ("_apitap_state", "_apitap_cdc_pending"):
            if ch(f"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{s}'") != "0":
                ch(f"ALTER TABLE {s} DELETE WHERE dest_table = '{t}' SETTINGS mutations_sync = 1")




def make(t, pk):
    key = "PRIMARY KEY" if pk else ""
    # 3.2 KB a value, STORAGE EXTERNAL: out of line and uncompressed, so an
    # UPDATE that leaves it alone really sends it as unchanged-TOAST.
    src(f"CREATE TABLE {t} (id int {key}, grp int, v int, tag text); ALTER TABLE {t} ALTER tag SET STORAGE EXTERNAL; "
        f"ALTER TABLE {t} REPLICA IDENTITY FULL; "
        f"INSERT INTO {t} SELECT i, i % 10, i, repeat(md5((i % 10)::text), 100) FROM generate_series(1, {N}) i")


def moves(t, masked=True):
    """One transaction of key moves (masked ones when `masked`), rewrites and plain updates."""
    m = f"UPDATE {t} SET id = id + 100000 WHERE id % 3 = 0; " if masked else ""
    src(f"BEGIN; {m}UPDATE {t} SET id = id + 200000, tag = tag || 'x' WHERE id % 3 = 1 AND id < 100000; "
        f"UPDATE {t} SET v = v + 1 WHERE id % 3 = 2 AND id < 100000; COMMIT")


def main():
    cleanup()
    for t, pk in (("km_pk", True), ("km_nopk", False), ("km_ch1", True), ("km_ch2", True)):
        make(t, pk)
    st1 = tempfile.mkdtemp(prefix="km-v1-", dir=os.path.expanduser("~/w1"))
    st2 = tempfile.mkdtemp(prefix="km-v2-", dir=os.path.expanduser("~/w1"))
    q1 = "SELECT tag, COUNT(*) AS n, SUM(v) AS s FROM pg.km_pk GROUP BY tag"
    q2 = "SELECT grp, COUNT(*) AS n, SUM(v) AS s FROM pg.km_nopk GROUP BY grp"
    print(f"  bootstrap V1: {view(st1, q1, 'km_v1')}  V2: {view(st2, q2, 'km_v2')}  "
          f"C1: {transfer('km_ch1')}  C2: {transfer('km_ch2')}", flush=True)

    moves("km_pk")
    src("UPDATE km_nopk SET v = v + 1 WHERE id % 2 = 0")
    moves("km_ch1")
    moves("km_ch2", masked=False)
    out = view(st1, q1, "km_v1")
    want = src("SELECT md5(tag) || '|' || count(*) || '|' || sum(v) FROM km_pk GROUP BY tag ORDER BY 1")
    got = dst("SELECT md5(tag) || '|' || n || '|' || s FROM km_v1 ORDER BY 1")
    case("V1 view, PK table: masked key moves + rewrites in one transaction", out.startswith("ROWS") and got == want,
         f"{out[:160]}, equal to the server: {got == want}")
    out = view(st2, q2, "km_v2")
    want = src("SELECT grp || '|' || count(*) || '|' || sum(v) FROM km_nopk GROUP BY grp ORDER BY grp")
    got = dst("SELECT grp || '|' || n || '|' || s FROM km_v2 ORDER BY grp")
    case("V2 view, no-PK table: every update a move, no masked cell", out.startswith("ROWS") and got == want,
         f"{out[:160]}, equal to the server: {got == want}")
    for t, label in (("km_ch1", "C1 changelog=True to ClickHouse: masked and unmasked key moves"),
                     ("km_ch2", "C2 changelog=True to ClickHouse: unmasked key moves only")):
        out = transfer(t)
        want = src(f"SELECT count(*) || '|' || sum(id) || '|' || sum(v) || '|' || md5(string_agg(id || ':' || md5(tag), ',' ORDER BY id)) FROM {t}")
        got = ch(f"SELECT toString(count()) || '|' || toString(sum(id)) || '|' || toString(sum(v)) || '|' || "
                 f"lower(hex(MD5(arrayStringConcat(arrayMap(x -> concat(toString(x.1), ':', lower(hex(MD5(x.2)))), "
                 f"arraySort(x -> x.1, groupArray((id, ifNull(tag, ''))))), ',')))) FROM {t}__current")
        case(label, out.startswith("ROWS") and got == want, f"{out[:160]}, __current equal to the source: {got == want}"
             + ("" if got == want else f" (src {want[:60]} / ch {got[:60]})"))
    cleanup()
    for d in (st1, st2):
        shutil.rmtree(d, ignore_errors=True)
    print("CHANGELOG KEY MOVE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
