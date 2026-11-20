"""apitap.sql(..., mode="log_based") keeps an aggregate current from the WAL (C2).

Through 0.65.0 an aggregate in a destination could only be recomputed whole:
every run re-read the table. With mode="log_based" the first run reads the
table once at a replication slot's snapshot; every later run reads only the
slot's changes since the last run and moves the answer by each change's old
and new image. Every check below compares the destination's table with the
answer Postgres itself computes at that moment.

  1  the first run lands the server's answer (bootstrap at the slot snapshot)
  2  five rounds of random inserts, deletes, updates that change the group,
     updates that cross the WHERE, and NULL amounts: after each, the view
     equals the server, and the run read changes, not the table
  3  a run with nothing new lands the same answer from zero changes
  4  a torn state file is not trusted: the view is rebuilt and still equal
  5  refused, naming the cause: MIN with mode="log_based", and a table
     without REPLICA IDENTITY FULL
  6  the same query viewed into a second table keeps a slot of its own:
     both views stay equal to the server, drained one after the other
     (through 0.67.2 they shared one slot, and the view drained second lost
     the changes the first had consumed, with no error)
  7  a copy of a view's state (another state dir, another host) whose slot
     another run has moved past it is refused by name, not answered short
  8  the same view's FIRST run from another state dir finds the slot under
     its own name and refuses by name (through p12 it dropped the slot and
     the owner had to rebuild); the owner keeps draining, exact
"""
import os
import random
import subprocess
import sys
import tempfile
import time
from decimal import Decimal

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "sql_live_orders"
T2 = "sql_live_plain"
OUT = "sql_live_out"
OUT2 = "sql_live_out2"
Q = (f"SELECT status, COUNT(*) AS n, SUM(amount) AS amt, SUM(qty) AS q, AVG(amount) AS av "
     f"FROM pg.{T} WHERE qty > 2 GROUP BY 1 ORDER BY 1")
TRUTH = (f"SELECT status, COUNT(*), SUM(amount), SUM(qty), AVG(amount) FROM {T} "
         f"WHERE qty > 2 GROUP BY 1 ORDER BY 1")
STATE = tempfile.mkdtemp(prefix="apitap-live-")
STATE_B = tempfile.mkdtemp(prefix="apitap-live-b-")
STATE_C = tempfile.mkdtemp(prefix="apitap-live-c-")
STATE_D = tempfile.mkdtemp(prefix="apitap-live-d-")
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def psql(container, db, sql):
    o = subprocess.run(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1",
                        "-Atc", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
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



def norm(lines):
    out = []
    for l in lines.splitlines():
        s, n, a, q, av = l.split("|")
        out.append((s, int(n), Decimal(a) if a else None, int(q), round(float(av), 6) if av else None))
    return out


def truth():
    return norm(src(TRUTH))


def landed(table=OUT):
    return norm(dst(f"SELECT status, n, amt, q, av FROM {table} ORDER BY 1"))


def view(query=Q, table=OUT, state=STATE):
    """One run in its own process: stderr carries the debug lines."""
    code = ("import apitap, sys\n"
            f"r = apitap.sql({query!r}, sources={{'pg': {PG!r}}}, dest={PGD!r}, table={table!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       env=dict(os.environ, APITAP_DEBUG="1", APITAP_STATE_DIR=state))
    return r


def lines(r, frag):
    return [l for l in r.stderr.splitlines() if "[sql] view" in l and frag in l]


def slot_pos():
    """The view slots over T: name, confirmed_flush_lsn, restart_lsn; and the
    server's current WAL position (round P, case 8 evidence)."""
    s = src("SELECT string_agg(slot_name || ' flush=' || coalesce(confirmed_flush_lsn::text, '-') || ' restart=' || "
            "coalesce(restart_lsn::text, '-') || CASE WHEN active THEN ' ACTIVE' ELSE '' END, '; ') "
            "FROM pg_replication_slots WHERE slot_name LIKE 'apitap\\_v\\_%'")
    return f"[{s or 'no view slot'}] wal={src('SELECT pg_current_wal_lsn()')}"


def drain(state=STATE, table=OUT, query=None):
    """An owner drain with its evidence: slot positions before and after."""
    before = slot_pos()
    r = view(query or Q, table=table, state=state)
    r.ev = (f"exit {r.returncode}; bootstrap {lines(r, 'bootstrap')[:1] or 'none'}; "
            f"moved {[l.split('view ', 1)[-1][:110] for l in lines(r, 'change image(s)')][:2] or 'none'}; "
            f"slot before {before}; after {slot_pos()}")
    if r.returncode:
        r.ev += f"; stderr tail {r.stderr[-300:]!r}"
    return r


def agree(label, table=OUT, state=STATE):
    """landed vs truth; on a difference: the rows, a re-read 2 s later, and one
    more owner drain (converges = a position/visibility race, data not lost;
    0 changes and still different = lost changes)."""
    l, t = landed(table), truth()
    if l == t:
        return True, "destination equals Postgres: yes"
    ev = [f"destination equals Postgres: NO; destination only: {[r for r in l if r not in t]}; "
          f"Postgres only: {[r for r in t if r not in l]}; at {slot_pos()}"]
    time.sleep(2)
    l2, t2 = landed(table), truth()
    ev.append(f"re-read 2 s later: {'now equal' if l2 == t2 else 'still different'}")
    r = drain(state=state, table=table)
    ev.append(f"one more owner drain: {r.ev}; then {'CONVERGED' if landed(table) == truth() else 'STILL DIFFERENT'}")
    print(f"      [{label}] " + " | ".join(ev), flush=True)
    return False, " | ".join(ev)


def cleanup():
    drop_view_slots(src, [T, T2])
    src(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {T2}")
    dst(f"DROP TABLE IF EXISTS {OUT}; DROP TABLE IF EXISTS {OUT2}")


def main():
    rng = random.Random(7)
    cleanup()
    src(f"CREATE TABLE {T} (id serial PRIMARY KEY, status text, amount numeric(12,2), qty int, note text); "
        f"ALTER TABLE {T} REPLICA IDENTITY FULL; "
        f"INSERT INTO {T} (status, amount, qty, note) SELECT (ARRAY['new','paid','shipped','void'])[1 + g % 4], "
        f"CASE WHEN g % 17 = 0 THEN NULL ELSE (g * 3.17)::numeric(12,2) END, g % 7, repeat('x', g % 50) "
        f"FROM generate_series(1, 5000) g")
    r = view()
    # The bootstrap pushes the WHERE down: it reads only the rows that pass.
    passing = src(f"SELECT COUNT(*) FROM {T} WHERE qty > 2")
    case("1: the first run lands the server's answer, reading only the rows its WHERE keeps",
         r.returncode == 0 and landed() == truth() and lines(r, f" {passing} rows at pg:"),
         (r.stderr[-300:] if r.returncode else f"{len(truth())} groups, {passing} of 5000 rows read"))

    for rnd in range(1, 6):
        stmts = []
        for _ in range(rng.randint(40, 120)):
            k = rng.random()
            if k < 0.3:
                stmts.append(f"INSERT INTO {T} (status, amount, qty) VALUES "
                             f"('{rng.choice(['new', 'paid', 'shipped', 'void', 'held'])}', "
                             f"{'NULL' if rng.random() < 0.1 else round(rng.uniform(-50, 900), 2)}, {rng.randint(0, 9)})")
            elif k < 0.5:
                stmts.append(f"DELETE FROM {T} WHERE id = (SELECT id FROM {T} ORDER BY random() LIMIT 1)")
            elif k < 0.75:
                stmts.append(f"UPDATE {T} SET status = '{rng.choice(['new', 'paid', 'shipped', 'void', 'held'])}' "
                             f"WHERE id % 97 = {rng.randint(0, 96)}")
            elif k < 0.9:
                stmts.append(f"UPDATE {T} SET qty = {rng.randint(0, 9)} WHERE id % 53 = {rng.randint(0, 52)}")
            else:
                stmts.append(f"UPDATE {T} SET amount = amount + 1.01 WHERE id % 31 = {rng.randint(0, 30)}")
        # Half of each round in one transaction, the rest one statement each.
        mid = len(stmts) // 2
        src("BEGIN; " + "; ".join(stmts[:mid]) + "; COMMIT")
        for s in stmts[mid:]:
            src(s)
        r = drain()
        moved = lines(r, "change image(s)")
        eq, eqev = agree(f"2.{rnd}")
        good = r.returncode == 0 and eq and bool(moved) and not lines(r, "bootstrap")
        case(f"2: round {rnd}: the view equals the server, moved by changes", good,
             (moved or ["no move line"])[0][:120] if good else f"{r.ev}; {eqev}")

    r = view()
    case("3: a run with nothing new lands the same answer", r.returncode == 0 and landed() == truth()
         and not lines(r, "bootstrap"), r.stderr[-200:] if r.returncode else "")

    files = [f for f in os.listdir(STATE) if f.endswith(".state")]
    p = os.path.join(STATE, files[0]) if files else None
    if p:
        b = bytearray(open(p, "rb").read())
        b[len(b) // 2] ^= 0xFF
        open(p, "wb").write(bytes(b))
    src(f"UPDATE {T} SET status = 'paid' WHERE id % 11 = 3")
    r = view()
    case("4: a torn state file is rebuilt, not trusted", p is not None and r.returncode == 0 and lines(r, "bootstrap")
         and "content hash" in r.stderr and landed() == truth(), r.stderr[-300:])

    # 6: the same query into a second table, drained after the first.
    r = view(table=OUT2, state=STATE_B)
    src(f"UPDATE {T} SET qty = qty + 1 WHERE id % 13 = 5; DELETE FROM {T} WHERE id % 29 = 7")
    ra = drain()
    rb = drain(table=OUT2, state=STATE_B)
    eqa, eva = agree("6a")
    eqb, evb = agree("6b", table=OUT2, state=STATE_B)
    good6 = r.returncode == 0 and ra.returncode == 0 and rb.returncode == 0 and eqa and eqb
    case("6: the same query into two tables: both equal the server, drained one after the other", good6,
         f"second view {len(landed(OUT2))} groups" if good6 else f"first run exit {r.returncode}; owner A: {ra.ev}; {eva}; owner B: {rb.ev}; {evb}")

    # 7: a copy of the first view's state, drained after the first moved the slot.
    import shutil
    for f in os.listdir(STATE):
        shutil.copy(os.path.join(STATE, f), os.path.join(STATE_C, f))
    src(f"UPDATE {T} SET status = 'held' WHERE id % 19 = 2")
    ra = drain()
    before_c = slot_pos()
    rc = view(state=STATE_C)
    after_c = slot_pos()
    # Each condition with its evidence (round P p23b: one gate failure here
    # printed only the stale copy's refusal, not which condition failed).
    eq7, ev7 = agree("7")
    c7 = {"the owner's drain exits 0": ra.returncode == 0, "the owner did not re-bootstrap": not lines(ra, "bootstrap"),
          "the stale copy exits non-zero": rc.returncode != 0,
          "the refusal says 'advanced'": "advanced" in rc.stderr, "the destination equals Postgres": eq7}
    ev = [f"{k}: {'yes' if v else 'NO'}" for k, v in c7.items()]
    ev.append(f"owner: {ra.ev}")
    ev.append(f"stale copy: slot before {before_c}; after {after_c}")
    if rc.returncode == 0:
        ev.append("the stale copy answered")
    elif "advanced" not in rc.stderr:
        ev.append(f"stale copy stderr tail: {rc.stderr[-400:]!r}")
    if not eq7:
        ev.append(ev7)
    case("7: a stale copy of a view's state whose slot moved past it is refused by name", all(c7.values()), "; ".join(ev))

    # 8: the same view (same query, same destination) first run from another
    # state dir, as another host would: it finds the slot under its own name
    # and has no record of it. Through p12 it dropped that slot and
    # bootstrapped, and the owner lost its position; now it refuses by name
    # and the owner keeps draining exactly.
    before_d = slot_pos()
    rd = view(state=STATE_D)
    after_d = slot_pos()
    src(f"UPDATE {T} SET qty = qty + 2 WHERE id % 23 = 4")
    ra = drain()
    eq8, ev8 = agree("8")
    c8 = {"the second state dir exits non-zero": rd.returncode != 0, "it says 'has no record of it'": "has no record of it" in rd.stderr,
          "it names pg_drop_replication_slot": "pg_drop_replication_slot" in rd.stderr,
          "the owner's drain exits 0": ra.returncode == 0, "the owner did not re-bootstrap": not lines(ra, "bootstrap"),
          "the destination equals Postgres": eq8}
    ev = [f"{k}: {'yes' if v else 'NO'}" for k, v in c8.items()]
    ev.append(f"second state dir: slot before {before_d}; after {after_d}; " +
              (rd.stderr.strip().splitlines() or [""])[-1][:160] if rd.returncode else "second state dir: it bootstrapped")
    ev.append(f"owner: {ra.ev}")
    if not eq8:
        ev.append(ev8)
    case("8: a first run in another state dir finds the view's slot and refuses; the owner drains on, exact", all(c8.values()), "; ".join(ev))

    bad = Q.replace("AVG(amount) AS av", "MIN(amount) AS av")
    r = view(bad)
    case("5: MIN with mode=log_based is refused, naming it", r.returncode != 0 and "MIN with mode" in r.stderr and "cannot move by changes alone" in r.stderr, r.stderr[-160:])
    src(f"CREATE TABLE {T2} (id int PRIMARY KEY, status text, qty int)")
    r = view(f"SELECT status, COUNT(*) FROM pg.{T2} GROUP BY 1")
    case("5: a table without REPLICA IDENTITY FULL is refused, naming the fix", r.returncode != 0
         and "REPLICA IDENTITY FULL" in r.stderr, r.stderr[-160:])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"   XX {type(e).__name__}: {e}")
        ok = False
    finally:
        try:
            cleanup()
        except Exception as e:  # noqa: BLE001
            print(f"   (cleanup: {e})")
    print("SQL LIVE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
