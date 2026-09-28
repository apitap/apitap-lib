"""CDC into a GTID-enforced MySQL.

`enforce_gtid_consistency=ON` refuses statements that cannot be logged as
one GTID transaction — historically CREATE/DROP TEMPORARY TABLE inside a
transaction (1787), and CREATE TABLE ... SELECT. The MySQL apply builds its
key and row twins as TEMPORARY tables, runs a WAL TRUNCATE as owned DDL, and
fences every window on its lease row; this leg runs all three against a
GTID-enforced server and asks the server whether each window became GTIDs.

  1  bootstrap, then a window of updates, deletes and inserts: applied, the
     counts agree, and `@@gtid_executed` advanced
  2  a window carrying a TRUNCATE: applied O(1) under the fence, counts agree,
     `@@gtid_executed` advanced

    APITAP_MY_GTID_URL=mysql://root:bench@127.0.0.1:3311/bench \\
        python benchmarks/e2e_my_gtid_dest.py

Source Postgres :5544. The destination is `apitap-bench-my-gtid`
(benchmarks/run-server.sh start_my_gtid).
"""
import os
import subprocess
import sys

import _rig

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = os.environ.get("APITAP_MY_GTID_URL", "mysql://root:bench@127.0.0.1:3311/bench")
GTID = ("apitap-bench-my-gtid", "bench")
T = "gtid_dest"
ok = True
_SLOTS = set(_rig.psql("SELECT slot_name FROM pg_replication_slots", _rig.PG_SRC).split())


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def my(sql):
    return _rig.mysql(sql, GTID)


def pg(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def gtids():
    return my("SELECT @@GLOBAL.gtid_executed").replace("\\n", "").replace("\n", "")


def drain():
    return subprocess.run([sys.executable, "-c",
                           f"import apitap; apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')"],
                          capture_output=True, text=True, timeout=600)


def last(stderr):
    return (stderr.strip().splitlines() or ["(nothing)"])[-1][:200]


def agree():
    s = pg(f"SELECT count(*) || '|' || coalesce(sum(id), 0) || '|' || coalesce(sum(length(v)), 0) FROM {T}")
    d = my(f"SELECT CONCAT(COUNT(*), '|', COALESCE(SUM(id), 0), '|', COALESCE(SUM(LENGTH(v)), 0)) FROM `{T}`")
    return s == d, f"source {s} / destination {d}"


def clean():
    for p in pg(f"SELECT DISTINCT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split():
        pg(f"DROP PUBLICATION IF EXISTS {p}")
    pg(f"DROP TABLE IF EXISTS {T} CASCADE")
    for s in set(pg("SELECT slot_name FROM pg_replication_slots").split()) - _SLOTS:
        pg(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots WHERE slot_name = '{s}' AND NOT active")
    for n in my(f"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() "
                f"AND table_name LIKE '{T}%'").split():
        my(f"DROP TABLE IF EXISTS `{n}`")
    for t, w in (("_apitap_lease", f"dest_key = 'bench.{T}'"), ("_apitap_state", f"dest_table = '{T}'")):
        if my(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
              f"AND table_name = '{t}'") != "0":
            my(f"DELETE FROM {t} WHERE {w}")


print("== reset ==", flush=True)
if my("SELECT @@gtid_mode, @@enforce_gtid_consistency") != "ON\tON":
    _rig.rig_fail("the destination does not enforce GTID consistency")
clean()
try:
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1, 500) g")
    r = drain()
    case("bootstrapped into a GTID-enforced server", r.returncode == 0, last(r.stderr))

    print("== 1: updates, deletes and inserts ==", flush=True)
    g0 = gtids()
    pg(f"UPDATE {T} SET v = 'u'||id WHERE id <= 100")
    pg(f"DELETE FROM {T} WHERE id BETWEEN 101 AND 150")
    pg(f"INSERT INTO {T} SELECT g, 'n'||g FROM generate_series(501, 700) g")
    r = drain()
    case("1: applied", r.returncode == 0, last(r.stderr))
    case("1: destination agrees with the source", *agree())
    case("1: the window became GTIDs", gtids() != g0, gtids()[-60:])

    print("== 2: a window with a TRUNCATE ==", flush=True)
    g1 = gtids()
    pg(f"TRUNCATE {T}")
    pg(f"INSERT INTO {T} SELECT g, 't'||g FROM generate_series(1, 30) g")
    r = drain()
    case("2: applied", r.returncode == 0, last(r.stderr))
    case("2: destination agrees with the source", *agree())
    case("2: the window became GTIDs", gtids() != g1, gtids()[-60:])
finally:
    print("== cleanup ==", flush=True)
    clean()

print("\nMY GTID DEST E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
