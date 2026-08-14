"""changelog=True e2e → ClickHouse.

Proves the three things that make an append-only CDC destination trustworthy:
  1. EVERY operation is captured (a key updated 3x lands 3 rows, not 1),
  2. `<table>__current` equals the source table, and
  3. a REPLAYED window does not corrupt the current state (duplicates are inert).
"""
import subprocess, sys
import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cl_demo"


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
                        "--user", "default", "--password", "bench", "-q", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def drain():
    return apitap.transfer(PG, CH, table=T, mode="log_based", changelog=True)


def current_matches(stage):
    p = pg(f"SELECT id||'|'||COALESCE(v,'<N>') FROM {T} ORDER BY id")
    c = ch(f"SELECT toString(id)||'|'||ifNull(v,'<N>') FROM {T}__current ORDER BY id")
    if p == c:
        n = len([x for x in p.split("\n") if x])
        print(f"   ✓ {stage}: __current matches pg ({n} rows)")
        return True
    print(f"   ✗ {stage}: MISMATCH\n   pg:\n{p}\n   ch:\n{c}")
    return False


ok = True
print("== reset ==")
pg(f"DROP TABLE IF EXISTS {T} CASCADE")
pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
for t in (T, f"{T}__current", f"{T}__apitap_cl"):
    ch(f"DROP TABLE IF EXISTS {t}")
ch(f"DROP VIEW IF EXISTS {T}__current")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1") if ch(
    "SELECT count() FROM system.tables WHERE name='_apitap_state'") != "0" else None

pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text, body text)")  # body TOASTs later
pg(f"INSERT INTO {T} VALUES (1,'a'),(2,'b'),(3,'c')")

print("== bootstrap (baseline rows get op B) ==")
r = drain()
print(f"   bootstrap rows={r.rows}")
base = ch(f"SELECT count() FROM {T} WHERE _apitap_op='B'")
print(f"   baseline rows tagged B: {base}")
ok &= base == "3"
ok &= current_matches("bootstrap")

print("== window 1: 3 updates on ONE key + insert + delete ==")
pg(f"UPDATE {T} SET v='a1' WHERE id=1")
pg(f"UPDATE {T} SET v='a2' WHERE id=1")
pg(f"UPDATE {T} SET v='a3' WHERE id=1")
pg(f"INSERT INTO {T} VALUES (4,'d')")
pg(f"DELETE FROM {T} WHERE id=2")
r = drain()
print(f"   window1 events={r.rows}")
u1 = ch(f"SELECT count() FROM {T} WHERE id=1 AND _apitap_op='U'")
print(f"   'U' records for id=1: {u1}  (collapse would have left 1)")
ok &= u1 == "3"
d2 = ch(f"SELECT count() FROM {T} WHERE _apitap_op='D'")
print(f"   'D' records: {d2}")
ok &= d2 == "1"
ok &= current_matches("window1")

print("== window 2: REPLAY the same window (idempotence) ==")
before = ch(f"SELECT count() FROM {T}")
r = drain()          # nothing new in the WAL — must be a no-op
after = ch(f"SELECT count() FROM {T}")
print(f"   log rows {before} -> {after} (empty drain must not append)")
ok &= before == after
ok &= current_matches("window2-replay")

print("== unchanged-TOAST: an UPDATE that skips a big column must not blank it ==")
# A >2 KB value is stored out of line, and an UPDATE that does not touch it
# omits it from the WAL entirely. Written as NULL, it would vanish from
# __current — the single worst thing this mode could do.
BIG = "x" * 40000
pg(f"UPDATE {T} SET body = repeat('x', 40000) WHERE id = 1")
drain()                                   # window carrying the full body
pg(f"UPDATE {T} SET v = 'toast-probe' WHERE id = 1")   # body NOT touched
drain()                                   # the WAL omits body here
got = ch(f"SELECT length(ifNull(body,'')) FROM {T}__current WHERE id=1")
print(f"   body length in __current after a body-less UPDATE: {got} (want {len(BIG)})")
ok &= got == str(len(BIG))
newest = ch(f"SELECT length(ifNull(body,'')) FROM {T} WHERE id=1 AND _apitap_op='U' "
            f"ORDER BY _apitap_lsn DESC, _apitap_seq DESC LIMIT 1")
print(f"   …and the newest U record itself carries it: {newest}")
ok &= newest == str(len(BIG))
ok &= current_matches("after-toast")

print("== a REPLICA run onto a changelog table is refused ==")
try:
    apitap.transfer(PG, CH, table=T, mode="log_based")      # changelog=False
    print("   ✗ a replica run was allowed to write onto the log")
    ok = False
except Exception as e:
    good = "CHANGELOG" in str(e) and "changelog=True" in str(e)
    print(f"   {'✓' if good else '✗'} refused: {str(e)[:140]}")
    ok &= good

print("== and the mirror: changelog onto a REPLICA table is refused ==")
R = f"{T}_rep"
pg(f"DROP TABLE IF EXISTS {R} CASCADE")
pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{R}")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
ch(f"DROP TABLE IF EXISTS {R}")
if ch("SELECT count() FROM system.tables WHERE name='_apitap_state'") != "0":
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{R}' SETTINGS mutations_sync=1")
pg(f"CREATE TABLE {R} (id int PRIMARY KEY, v text)")
pg(f"INSERT INTO {R} VALUES (1,'a')")
apitap.transfer(PG, CH, table=R, mode="log_based")          # a plain replica
try:
    apitap.transfer(PG, CH, table=R, mode="log_based", changelog=True)
    print("   ✗ changelog was grafted onto a replica")
    ok = False
except Exception as e:
    good = "REPLICA" in str(e) and "changelog=True" in str(e)
    print(f"   {'✓' if good else '✗'} refused: {str(e)[:140]}")
    ok &= good
pg(f"DROP TABLE IF EXISTS {R} CASCADE")
pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{R}")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
ch(f"DROP TABLE IF EXISTS {R}")

print("== cleanup ==")
pg(f"DROP TABLE IF EXISTS {T} CASCADE")
pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
ch(f"DROP VIEW IF EXISTS {T}__current")
ch(f"DROP TABLE IF EXISTS {T}")

print("\n   ===== CH CHANGELOG E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
