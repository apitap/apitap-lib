"""changelog=True e2e → ClickHouse, MySQL BINLOG source.

Same contract as the Postgres edition (`e2e_changelog_ch.py`), run through the
other capture plane, because the binlog lane builds its changelog in
`mysource.rs` — a different accumulator call site with its own key handling:
  1. EVERY operation is captured (a key updated 3x lands 3 rows, not 1),
  2. a PK-CHANGING update lands `D` on the old identity then `U` on the new one,
  3. `<table>__current` equals the source table, and
  4. an empty drain appends nothing.
"""
import subprocess
import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cl_my_demo"


def my(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-e", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def drain():
    return apitap.transfer(MY, CH, table=T, mode="log_based", changelog=True)


def current_matches(stage):
    p = my(f"SELECT CONCAT(id,'|',IFNULL(v,'<N>')) FROM bench.{T} ORDER BY id")
    c = ch(f"SELECT toString(id)||'|'||ifNull(v,'<N>') FROM {T}__current ORDER BY id")
    if p == c:
        print(f"   ✓ {stage}: __current matches mysql ({len(p.splitlines())} rows)")
        return True
    print(f"   ✗ {stage}: MISMATCH\n   my:\n{p}\n   ch:\n{c}")
    return False


ok = True
print("== reset ==")
my(f"DROP TABLE IF EXISTS bench.{T}")
for t in (T, f"{T}__apitap_cl", f"{T}__apitap_cdc_del"):
    ch(f"DROP TABLE IF EXISTS {t}")
ch(f"DROP VIEW IF EXISTS {T}__current")
if ch("SELECT count() FROM system.tables WHERE name='_apitap_state'") != "0":
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")

my(f"CREATE TABLE bench.{T} (id INT PRIMARY KEY, v VARCHAR(64)) ENGINE=InnoDB")
my(f"INSERT INTO bench.{T} VALUES (1,'a'),(2,'b'),(3,'c')")

print("== bootstrap (baseline rows get op B) ==")
r = drain()
print(f"   bootstrap rows={r.rows}")
base = ch(f"SELECT count() FROM {T} WHERE _apitap_op='B'")
print(f"   baseline rows tagged B: {base}")
ok &= base == "3"
ok &= current_matches("bootstrap")

print("== window 1: 3 updates on ONE key + insert + delete ==")
my(f"UPDATE bench.{T} SET v='a1' WHERE id=1")
my(f"UPDATE bench.{T} SET v='a2' WHERE id=1")
my(f"UPDATE bench.{T} SET v='a3' WHERE id=1")
my(f"INSERT INTO bench.{T} VALUES (4,'d')")
my(f"DELETE FROM bench.{T} WHERE id=2")
r = drain()
print(f"   window1 events={r.rows}")
u1 = ch(f"SELECT count() FROM {T} WHERE id=1 AND _apitap_op='U'")
print(f"   'U' records for id=1: {u1}  (collapse would have left 1)")
ok &= u1 == "3"
d2 = ch(f"SELECT count() FROM {T} WHERE _apitap_op='D'")
print(f"   'D' records: {d2}")
ok &= d2 == "1"
ok &= current_matches("window1")

print("== window 2: a PK-CHANGING update is D(old) then U(new) ==")
my(f"UPDATE bench.{T} SET id=99 WHERE id=3")
r = drain()
d3 = ch(f"SELECT count() FROM {T} WHERE id=3 AND _apitap_op='D'")
u99 = ch(f"SELECT count() FROM {T} WHERE id=99 AND _apitap_op='U'")
print(f"   D on old id=3: {d3}   U on new id=99: {u99}")
ok &= d3 == "1" and u99 == "1"
ok &= current_matches("window2-pk-change")

print("== window 3: empty drain appends nothing ==")
before = ch(f"SELECT count() FROM {T}")
drain()
after = ch(f"SELECT count() FROM {T}")
print(f"   log rows {before} -> {after}")
ok &= before == after
ok &= current_matches("window3-empty")

print("== cleanup ==")
my(f"DROP TABLE IF EXISTS bench.{T}")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"DROP VIEW IF EXISTS {T}__current")

print("\n   ===== MYSQL CHANGELOG E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
