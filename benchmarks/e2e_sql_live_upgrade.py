"""An incremental view made by an earlier release keeps its slot after the upgrade.

Through 0.67.2 (and the W1 wheels) a view's slots, publications and state
file were named by its query alone (`apitap_v_{fp16}_{side}`, `{fp16}.state`).
Round P names them by the query AND the destination (L7), so a view made
before the upgrade would bootstrap again under the new name and leave its old
slot behind, holding WAL on the source until someone drops it. The upgrade
now adopts the old state: when the old slot is idle and stands exactly where
the old state recorded, the view drains on with it (no new slot, no rebuild);
otherwise it bootstraps under the new name and warns, naming the old slot and
the statement that drops it.

Cases (OLD = the earlier wheel, NEW = this build, each run its own process):
  1  OLD makes the view, the source moves, NEW drains it: no new slot, the
     note says adopted, the answer equals the server's; a second NEW run moves
     it again on the same slot
  2  the same query into another table (same state dir) under NEW: a slot of
     its own, both answers equal the server
  3  an old state whose slot has moved past it (a copy taken before another
     OLD run): NEW does not adopt it, warns naming the old slot and
     pg_drop_replication_slot, bootstraps a new slot, and is exact

Usage: OLD_PKG=dir NEW_PKG=dir e2e_sql_live_upgrade.py   (dirs holding apitap/)
RED control: APITAP_TEST_NO_VIEW_ADOPT=1 (test-only) runs NEW without the adoption.
"""
import os
import shutil
import subprocess
import sys
import tempfile
from decimal import Decimal

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "sql_live_up"
OUTS = ["sql_live_up_a", "sql_live_up_b", "sql_live_up_c"]
Q = f"SELECT status, COUNT(*) AS n, SUM(amount) AS amt FROM pg.{T} GROUP BY 1 ORDER BY 1"
TRUTH = f"SELECT status, COUNT(*), SUM(amount) FROM {T} GROUP BY 1 ORDER BY 1"
Q3 = f"SELECT status, COUNT(*) AS n, SUM(amount) AS amt FROM pg.{T} WHERE amount > 10 GROUP BY 1 ORDER BY 1"
TRUTH3 = f"SELECT status, COUNT(*), SUM(amount) FROM {T} WHERE amount > 10 GROUP BY 1 ORDER BY 1"
OLD, NEW = os.environ["OLD_PKG"], os.environ["NEW_PKG"]
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
        s, n, a = l.split("|")
        out.append((s, int(n), Decimal(a) if a else None))
    return out


def slots():
    return set(src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_v_%'").split())


def view(pkg, state, table, query=Q):
    code = ("import apitap\n"
            f"r = apitap.sql({query!r}, sources={{'pg': {PG!r}}}, dest={PGD!r}, table={table!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    env = dict(os.environ, APITAP_STATE_DIR=state, PYTHONPATH=pkg, APITAP_PROGRESS="plain")
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)


def landed(table):
    return norm(dst(f"SELECT status, n, amt FROM {table} ORDER BY 1"))


def cleanup():
    src(f"DROP TABLE IF EXISTS {T}")
    for name in slots():
        src(f"SELECT pg_drop_replication_slot('{name}')")
    for name in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_v_%'").split():
        src(f'DROP PUBLICATION "{name}"')
    for t in OUTS:
        dst(f"DROP TABLE IF EXISTS {t}")


def err(r):
    return r.stderr.strip()[-300:]


def main():
    cleanup()
    s1, s3, s3copy = (tempfile.mkdtemp(prefix=f"apitap-up{i}-") for i in range(3))
    src(f"CREATE TABLE {T} (id serial PRIMARY KEY, status text, amount numeric(12,2)); "
        f"ALTER TABLE {T} REPLICA IDENTITY FULL; "
        f"INSERT INTO {T} (status, amount) SELECT (ARRAY['new','paid','void'])[1 + g % 3], (g * 1.25)::numeric(12,2) "
        f"FROM generate_series(1, 3000) g")
    truth = lambda: norm(src(TRUTH))  # noqa: E731

    # 1
    r = view(OLD, s1, OUTS[0])
    before = slots()
    case("1a: OLD makes the view", r.returncode == 0 and landed(OUTS[0]) == truth() and len(before) == 1,
         err(r) if r.returncode else f"slots {sorted(before)}")
    src(f"UPDATE {T} SET status = 'paid' WHERE id % 7 = 1; DELETE FROM {T} WHERE id % 11 = 3")
    r = view(NEW, s1, OUTS[0])
    case("1b: NEW drains it on the old slot (adopted, no new slot, exact)",
         r.returncode == 0 and slots() == before and "adopted" in r.stderr and landed(OUTS[0]) == truth(),
         err(r) if r.returncode else f"slots {sorted(slots())}, adopted: {'adopted' in r.stderr}")
    src(f"INSERT INTO {T} (status, amount) VALUES ('held', 9.99), ('new', 1.01)")
    r = view(NEW, s1, OUTS[0])
    case("1c: a second NEW run moves it again on the same slot",
         r.returncode == 0 and slots() == before and landed(OUTS[0]) == truth() and "warn" not in r.stderr.lower(),
         err(r) if r.returncode else f"slots {sorted(slots())}")

    # 2
    r = view(NEW, s1, OUTS[1])
    after = slots()
    case("2: the same query into another table takes a slot of its own",
         r.returncode == 0 and len(after) == 2 and before <= after and landed(OUTS[1]) == truth(),
         err(r) if r.returncode else f"slots {sorted(after)}")
    src(f"UPDATE {T} SET amount = amount + 2 WHERE id % 5 = 0")
    ra, rb = view(NEW, s1, OUTS[0]), view(NEW, s1, OUTS[1])
    case("2b: both drain to the server's answer", ra.returncode == 0 and rb.returncode == 0
         and landed(OUTS[0]) == truth() and landed(OUTS[1]) == truth(), err(ra) or err(rb))

    # 3
    old_slots = slots()
    r = view(OLD, s3, OUTS[2], Q3)
    q3_slot = slots() - old_slots
    for f in os.listdir(s3):
        p = os.path.join(s3, f)
        (shutil.copytree if os.path.isdir(p) else shutil.copy)(p, os.path.join(s3copy, f))
    src(f"UPDATE {T} SET status = 'void' WHERE id % 13 = 4")
    r2 = view(OLD, s3, OUTS[2], Q3)
    src(f"INSERT INTO {T} (status, amount) VALUES ('held', 99.5)")
    r3 = view(NEW, s3copy, OUTS[2], Q3)
    name = next(iter(q3_slot), "?")
    new_slot = slots() - old_slots - q3_slot
    case("3: a moved-past old state is not adopted: warned by name, new slot, exact",
         r.returncode == 0 and r2.returncode == 0 and r3.returncode == 0 and len(new_slot) == 1
         and name in r3.stderr and "pg_drop_replication_slot" in r3.stderr
         and landed(OUTS[2]) == norm(src(TRUTH3)),
         err(r3) if r3.returncode else f"new {sorted(new_slot)}, warned: {name in r3.stderr}")

    cleanup()
    for d in (s1, s3, s3copy):
        shutil.rmtree(d, ignore_errors=True)
    print("SQL LIVE UPGRADE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
