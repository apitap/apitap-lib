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
"""
import os
import random
import subprocess
import sys
import tempfile
from decimal import Decimal

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "sql_live_orders"
T2 = "sql_live_plain"
OUT = "sql_live_out"
Q = (f"SELECT status, COUNT(*) AS n, SUM(amount) AS amt, SUM(qty) AS q, AVG(amount) AS av "
     f"FROM pg.{T} WHERE qty > 2 GROUP BY 1 ORDER BY 1")
TRUTH = (f"SELECT status, COUNT(*), SUM(amount), SUM(qty), AVG(amount) FROM {T} "
         f"WHERE qty > 2 GROUP BY 1 ORDER BY 1")
STATE = tempfile.mkdtemp(prefix="apitap-live-")
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


def norm(lines):
    out = []
    for l in lines.splitlines():
        s, n, a, q, av = l.split("|")
        out.append((s, int(n), Decimal(a) if a else None, int(q), round(float(av), 6) if av else None))
    return out


def truth():
    return norm(src(TRUTH))


def landed():
    return norm(dst(f"SELECT status, n, amt, q, av FROM {OUT} ORDER BY 1"))


def view(query=Q, table=OUT):
    """One run in its own process: stderr carries the debug lines."""
    code = ("import apitap, sys\n"
            f"r = apitap.sql({query!r}, sources={{'pg': {PG!r}}}, dest={PGD!r}, table={table!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       env=dict(os.environ, APITAP_DEBUG="1", APITAP_STATE_DIR=STATE))
    return r


def lines(r, frag):
    return [l for l in r.stderr.splitlines() if "[sql] view" in l and frag in l]


def cleanup():
    src(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {T2}")
    for name in src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_v_%'").split():
        src(f"SELECT pg_drop_replication_slot('{name}')")
    for name in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_v_%'").split():
        src(f'DROP PUBLICATION "{name}"')
    dst(f"DROP TABLE IF EXISTS {OUT}")


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
        r = view()
        moved = lines(r, "change image(s)")
        case(f"2: round {rnd}: the view equals the server, moved by changes", r.returncode == 0 and landed() == truth()
             and moved and not lines(r, "bootstrap"), (r.stderr[-300:] if r.returncode else (moved or ["no move line"])[0][:120]))

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
