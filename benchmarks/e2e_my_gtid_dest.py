"""CDC into a GTID-enforced MySQL.

`enforce_gtid_consistency=ON` refuses statements that cannot be logged as
one GTID transaction — CREATE/DROP TEMPORARY TABLE inside a transaction
(1787 on 5.7, 3748 on 8.0), and CREATE TABLE ... SELECT. The MySQL apply
builds its key and row twins as TEMPORARY tables, runs a WAL TRUNCATE as
owned DDL, and fences every window on its lease row; this leg runs all three
against a GTID-enforced server and asks the server whether each window became
GTIDs.

  1  bootstrap, then a window of updates, deletes and inserts: applied, the
     counts agree, and `@@gtid_executed` advanced
  2  a window carrying a TRUNCATE: applied O(1) under the fence, counts agree,
     `@@gtid_executed` advanced
  3  a window under statement-based binlogs: applied, counts agree,
     `@@gtid_executed` advanced, and the binlog holds the drain's twins as
     statements (the server's word that the session really logged statements)

Since 8.0.13 the temporary-DDL refusal applies only when the session logs
statements (`binlog_format=STATEMENT`); with ROW or MIXED a twin created
inside the transaction is legal, so cases 1 and 2 pass on either side of that
line. Case 3 is the one that separates: 0.56.0 created its twins inside the
transaction and fails it with 3748. It switches the server's global format for
the case and puts ROW back whatever happens.

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


def binlog_pos():
    f, p = my("SHOW MASTER STATUS").split("\t")[:2]
    return f, p


def logged_since(pos):
    """(event type, info) of every binlog event written since `pos`."""
    f, p = pos
    return [tuple(r.split("\t")[2:6:3]) for r in my(f"SHOW BINLOG EVENTS IN '{f}' FROM {p}").splitlines() if r]


def window_logged(pos):
    """Did the window's own writes reach the binlog inside a GTID transaction?

    `@@gtid_executed` alone cannot say: the run's lease, markers and state
    rows are GTIDs too, and advance it even when the window itself fails.
    This looks for a write to the table — a row event's table map, or a
    statement naming it — after a GTID event.
    """
    gtid = False
    for t, info in logged_since(pos):
        gtid |= t == "Gtid"
        if gtid and (f"(bench.{T})" in info or f"`bench`.`{T}` " in info):
            return True, f"{t}: {info[:70]}"
    return False, "no write to the table in the binlog since the window began"


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
# A run that died inside case 3 left the server logging statements.
my("SET GLOBAL binlog_format = 'ROW'")
clean()
try:
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1, 500) g")
    r = drain()
    case("bootstrapped into a GTID-enforced server", r.returncode == 0, last(r.stderr))

    print("== 1: updates, deletes and inserts ==", flush=True)
    g0, pos = gtids(), binlog_pos()
    pg(f"UPDATE {T} SET v = 'u'||id WHERE id <= 100")
    pg(f"DELETE FROM {T} WHERE id BETWEEN 101 AND 150")
    pg(f"INSERT INTO {T} SELECT g, 'n'||g FROM generate_series(501, 700) g")
    r = drain()
    case("1: applied", r.returncode == 0, last(r.stderr))
    case("1: destination agrees with the source", *agree())
    case("1: gtid_executed advanced", gtids() != g0, gtids()[-60:])
    case("1: the window's writes are GTID transactions", *window_logged(pos))

    print("== 2: a window with a TRUNCATE ==", flush=True)
    g1, pos = gtids(), binlog_pos()
    pg(f"TRUNCATE {T}")
    pg(f"INSERT INTO {T} SELECT g, 't'||g FROM generate_series(1, 30) g")
    r = drain()
    case("2: applied", r.returncode == 0, last(r.stderr))
    case("2: destination agrees with the source", *agree())
    case("2: gtid_executed advanced", gtids() != g1, gtids()[-60:])
    case("2: the window's writes are GTID transactions", *window_logged(pos))

    print("== 3: statement-based binlogs ==", flush=True)
    # New sessions take the global format, and every drain opens its own.
    my("SET GLOBAL binlog_format = 'STATEMENT'")
    case("(rig) the server logs statements for new sessions",
         my("SELECT @@GLOBAL.binlog_format") == "STATEMENT", my("SELECT @@GLOBAL.binlog_format"))
    g2, pos = gtids(), binlog_pos()
    pg(f"UPDATE {T} SET v = 's'||id WHERE id <= 10")
    pg(f"DELETE FROM {T} WHERE id BETWEEN 11 AND 15")
    pg(f"INSERT INTO {T} SELECT g, 'm'||g FROM generate_series(31, 60) g")
    r = drain()
    case("3: applied", r.returncode == 0, last(r.stderr))
    case("3: destination agrees with the source", *agree())
    case("3: gtid_executed advanced", gtids() != g2, gtids()[-60:])
    case("3: the window's writes are GTID transactions", *window_logged(pos))
    # Temporary DDL reaches the binlog only when it is logged as a statement:
    # this is the server saying the drain's session ran statement-based, so
    # the twins really met the 3748 rule — outside the transaction.
    twins = [i for t, i in logged_since(pos) if t == "Query" and "TEMPORARY TABLE" in i.upper()]
    case("(rig) the drain's twins were logged as statements", bool(twins),
         twins[0][:80] if twins else "no temporary DDL in the binlog since the window began")
finally:
    print("== cleanup ==", flush=True)
    my("SET GLOBAL binlog_format = 'ROW'")
    clean()

print("\nMY GTID DEST E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
