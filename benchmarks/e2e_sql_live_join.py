"""apitap.sql(mode="log_based") keeps a Postgres x MySQL join aggregate current (C3/C4).

Through 0.65.0 there was no join view: a cross-database aggregate had to be
recomputed whole on every run. Now the first run reads each table once at a
point its change stream starts from exactly (a Postgres slot's exported
snapshot; MySQL's FLUSH TABLES WITH READ LOCK + consistent snapshot + binlog
position), and each later run moves the answer by both streams' old and new
images. Every check compares the destination with a recompute from both
servers' own rows at that moment.

  1  the first run lands the recompute
  2  six rounds of random traffic on BOTH sides: line inserts, deletes, price
     and NULL changes, lines moving to another order; orders inserted (giving
     orphan lines their order), deleted, moved to another priority (the
     group), crossing the date WHERE, and re-keyed. After each round the view
     equals the recompute and the run read changes, not tables
  3  a run with nothing new lands the same answer
  4  a torn state file is rebuilt, not trusted, and still equal
  5  the first run reads MySQL on several snapshot-pinned connections while
     a writer keeps committing to it: the next run (catching up from the
     position the bootstrap recorded) still equals the recompute, so no
     commit was both read and replayed, or lost between them
"""
import os
import random
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from decimal import Decimal

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
TL, TO, OUT = "slj_line", "slj_ord", "slj_out"
Q = (f"SELECT o.prio, SUM(l.price * (1 - l.disc)) AS rev, COUNT(*) AS n, COUNT(l.price) AS np "
     f"FROM pg.{TL} l JOIN my.{TO} o ON l.ok = o.ok "
     f"WHERE l.qty > 1 AND o.od < DATE '2020-01-20' GROUP BY 1 ORDER BY 1")
STATE = tempfile.mkdtemp(prefix="apitap-livej-")
PRIOS = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-LOW", "5-NONE"]
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def sh(args, sql):
    o = subprocess.run(args + [sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg(sql):
    return sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
               "-v", "ON_ERROR_STOP=1", "-Atc"], sql)


def pgd(sql):
    return sh(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst", "-Atc"], sql)


def my(sql):
    return sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "bench", "-N", "-e"], sql)


def truth(tl=TL, to=TO):
    orders = {}
    for l in my(f"SELECT ok, prio FROM {to} WHERE od < '2020-01-20'").splitlines():
        k, p = l.split("\t")
        orders[int(k)] = p
    agg = defaultdict(lambda: [Decimal(0), 0, 0])
    for l in pg(f"SELECT ok, price, disc FROM {tl} WHERE qty > 1 AND ok IS NOT NULL").splitlines():
        k, price, disc = l.split("|")
        if int(k) not in orders:
            continue
        a = agg[orders[int(k)]]
        a[1] += 1
        if price != "":
            a[0] += Decimal(price) * (1 - Decimal(disc))
            a[2] += 1
    return [(p, a[0] if a[2] else None, a[1], a[2]) for p, a in sorted(agg.items())]


def landed(out_t=OUT):
    out = []
    for l in pgd(f"SELECT prio, rev, n, np FROM {out_t} ORDER BY 1").splitlines():
        p, r, n, c = l.split("|")
        out.append((p, Decimal(r) if r else None, int(n), int(c)))
    return out


def view(q=Q, out_t=OUT, state=STATE):
    code = ("import apitap\n"
            f"apitap.sql({q!r}, sources={{'pg': {PG!r}, 'my': {MY!r}}}, dest={PGD!r}, table={out_t!r}, mode='log_based')\n")
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                          env=dict(os.environ, APITAP_DEBUG="1", APITAP_STATE_DIR=state))


def lines(r, frag):
    return [l for l in r.stderr.splitlines() if "[sql] view" in l and frag in l]


def cleanup():
    pg(f"DROP TABLE IF EXISTS {TL}, {TL}5")
    my(f"DROP TABLE IF EXISTS {TO}, {TO}5")
    pgd(f"DROP TABLE IF EXISTS {OUT}, {OUT}5")
    for s in pg("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_v_%'").split():
        pg(f"SELECT pg_drop_replication_slot('{s}')")
    for p in pg("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_v_%'").split():
        pg(f'DROP PUBLICATION "{p}"')


def main():
    rng = random.Random(11)
    cleanup()
    pg(f"CREATE TABLE {TL} (id serial PRIMARY KEY, ok bigint, price numeric(12,2), disc numeric(4,2), qty int); "
       f"ALTER TABLE {TL} REPLICA IDENTITY FULL; "
       f"INSERT INTO {TL} (ok, price, disc, qty) SELECT 1 + g % 450, "
       f"CASE WHEN g % 23 = 0 THEN NULL ELSE (g * 1.73)::numeric(12,2) END, ((g % 9) / 100.0)::numeric(4,2), g % 5 "
       f"FROM generate_series(1, 3000) g")
    my(f"CREATE TABLE {TO} (ok bigint PRIMARY KEY, prio varchar(12), od date); SET SESSION cte_max_recursion_depth = 1000; "
       f"INSERT INTO {TO} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 400) "
       f"SELECT n, ELT(1 + n % 5, '1-URGENT', '2-HIGH', '3-MEDIUM', '4-LOW', '5-NONE'), "
       f"DATE_ADD('2020-01-01', INTERVAL (n % 30) DAY) FROM s")
    r = view()
    case("1: the first run lands the recompute", r.returncode == 0 and landed() == truth()
         and len(lines(r, "bootstrap")) == 2, r.stderr[-300:] if r.returncode else f"{len(truth())} groups")

    next_ok = 1000
    for rnd in range(1, 7):
        pg_stmts, my_stmts = [], []
        for _ in range(rng.randint(30, 70)):
            k = rng.random()
            if k < 0.25:
                pg_stmts.append(f"INSERT INTO {TL} (ok, price, disc, qty) VALUES ({rng.randint(1, 520)}, "
                                f"{'NULL' if rng.random() < 0.1 else round(rng.uniform(1, 999), 2)}, "
                                f"{rng.choice(['0.00', '0.05', '0.10'])}, {rng.randint(0, 4)})")
            elif k < 0.4:
                pg_stmts.append(f"DELETE FROM {TL} WHERE id = (SELECT id FROM {TL} ORDER BY random() LIMIT 1)")
            elif k < 0.55:
                pg_stmts.append(f"UPDATE {TL} SET price = CASE WHEN random() < 0.1 THEN NULL ELSE price + 1.11 END, "
                                f"qty = {rng.randint(0, 4)} WHERE id % 61 = {rng.randint(0, 60)}")
            elif k < 0.65:
                # A line moves to another order (the join key on the probe side).
                pg_stmts.append(f"UPDATE {TL} SET ok = {rng.randint(1, 520)} WHERE id % 89 = {rng.randint(0, 88)}")
            elif k < 0.73:
                n = rng.randint(401, 520)
                my_stmts.append(f"INSERT IGNORE INTO {TO} VALUES ({n}, '{rng.choice(PRIOS)}', "
                                f"DATE_ADD('2020-01-01', INTERVAL {rng.randint(0, 29)} DAY))")
            elif k < 0.8:
                my_stmts.append(f"DELETE FROM {TO} WHERE ok = {rng.randint(1, 520)}")
            elif k < 0.9:
                # An order moves to another priority: its lines move group.
                my_stmts.append(f"UPDATE {TO} SET prio = '{rng.choice(PRIOS)}' WHERE ok % 37 = {rng.randint(0, 36)}")
            elif k < 0.96:
                # Orders cross the date WHERE.
                my_stmts.append(f"UPDATE {TO} SET od = DATE_ADD(od, INTERVAL {rng.choice([-15, 15])} DAY) "
                                f"WHERE ok % 41 = {rng.randint(0, 40)}")
            else:
                # An order re-keyed (its join key, on the GROUP BY side).
                a = rng.randint(1, 520)
                my_stmts.append(f"UPDATE IGNORE {TO} SET ok = {next_ok} WHERE ok = {a}")
                next_ok += 1
        half = len(pg_stmts) // 2
        if pg_stmts[:half]:
            pg("BEGIN; " + "; ".join(pg_stmts[:half]) + "; COMMIT")
        for s in pg_stmts[half:]:
            pg(s)
        if my_stmts:
            my("START TRANSACTION; " + "; ".join(my_stmts) + "; COMMIT")
        r = view()
        moved = lines(r, "change image(s) in")
        case(f"2: round {rnd}: the view equals the recompute, moved by both streams",
             r.returncode == 0 and landed() == truth() and moved and not lines(r, "bootstrap"),
             (r.stderr[-300:] if r.returncode else (moved or ["no move line"])[0].split("] ", 1)[-1][:110]))

    r = view()
    case("3: a run with nothing new lands the same answer", r.returncode == 0 and landed() == truth()
         and not lines(r, "bootstrap"), r.stderr[-200:] if r.returncode else "")

    files = [f for f in os.listdir(STATE) if f.endswith(".state")]
    if files:
        p = os.path.join(STATE, files[0])
        b = bytearray(open(p, "rb").read())
        b[len(b) // 2] ^= 0xFF
        open(p, "wb").write(bytes(b))
    my(f"UPDATE {TO} SET prio = '2-HIGH' WHERE ok % 7 = 1")
    r = view()
    case("4: a torn state file is rebuilt, not trusted", bool(files) and r.returncode == 0
         and len(lines(r, "bootstrap")) == 2 and "content hash" in r.stderr and landed() == truth(), r.stderr[-300:])

    # 5: a bootstrap racing a MySQL writer. 300K orders read on several
    # pinned connections while ~40K single-row commits land (priority moves,
    # date crossings, deletes, inserts).
    tl, to, out = TL + "5", TO + "5", OUT + "5"
    pg(f"CREATE TABLE {tl} (id serial PRIMARY KEY, ok bigint, price numeric(12,2), disc numeric(4,2), qty int); "
       f"ALTER TABLE {tl} REPLICA IDENTITY FULL; "
       f"INSERT INTO {tl} (ok, price, disc, qty) SELECT 1 + g % 330000, (g % 997 + 1.25)::numeric(12,2), "
       f"((g % 9) / 100.0)::numeric(4,2), g % 5 FROM generate_series(1, 600000) g")
    my(f"CREATE TABLE {to} (ok bigint PRIMARY KEY, prio varchar(12), od date); SET SESSION cte_max_recursion_depth = 400000; "
       f"INSERT INTO {to} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 300000) "
       f"SELECT n, ELT(1 + n % 5, '1-URGENT', '2-HIGH', '3-MEDIUM', '4-LOW', '5-NONE'), "
       f"DATE_ADD('2020-01-01', INTERVAL (n % 30) DAY) FROM s")
    q5 = Q.replace(f"pg.{TL} ", f"pg.{tl} ").replace(f"my.{TO} ", f"my.{to} ")
    st5 = tempfile.mkdtemp(prefix="apitap-livej5-")
    stmts = []
    for i in range(40000):
        k = rng.randint(1, 330000)
        x = rng.random()
        if x < 0.6:
            stmts.append(f"UPDATE {to} SET prio = '{rng.choice(PRIOS)}' WHERE ok = {k};")
        elif x < 0.8:
            stmts.append(f"UPDATE {to} SET od = DATE_ADD(od, INTERVAL {rng.choice([-15, 15])} DAY) WHERE ok = {k};")
        elif x < 0.9:
            stmts.append(f"DELETE FROM {to} WHERE ok = {k};")
        else:
            stmts.append(f"INSERT IGNORE INTO {to} VALUES ({k}, '{rng.choice(PRIOS)}', '2020-01-0{rng.randint(1, 9)}');")
    w = subprocess.Popen(["docker", "exec", "-i", "apitap-bench-my", "mysql", "-uroot", "-pbench", "bench"],
                         stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
    w.stdin.write("SET autocommit = 1;\n" + "\n".join(stmts) + "\n")
    w.stdin.close()
    time.sleep(0.5)
    r = view(q5, out, st5)
    racing = w.poll() is None
    w.wait()
    r2 = view(q5, out, st5)
    boot = lines(r, "bootstrap")
    case("5: a bootstrap racing a MySQL writer, then a catch-up, equals the recompute",
         r.returncode == 0 and r2.returncode == 0 and racing and len(boot) == 2
         and lines(r2, "change image(s) in") and landed(out) == truth(tl, to),
         (r.stderr[-300:] if r.returncode else r2.stderr[-300:] if r2.returncode else
          f"writer still running after the bootstrap: {racing}; " + (lines(r2, "change image(s) in") or ["-"])[0].split("] ", 1)[-1][:90]))


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
    print("SQL LIVE JOIN E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
