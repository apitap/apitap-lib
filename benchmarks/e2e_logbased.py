"""log_based E2E: bootstrap → CDC drains → validation, on the server rig.

Covers: pinned-snapshot bootstrap, insert/update/delete, PK-changing update,
empty-string vs NULL, unchanged-TOAST masked update, multi-transaction
windows, idempotent empty drain, TRUNCATE capture, and state/slot hygiene.
"""
import subprocess, time

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "cdc_demo"

def psql(db_port, db, sql):
    host = {"5544": "apitap-bench-pg-src", "5545": "apitap-bench-pg-dst"}[db_port]
    out = subprocess.run(
        ["docker", "exec", host, "psql", "-U", "postgres", "-d", db, "-Atc", sql],
        capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    return out.stdout.strip()

src = lambda sql: psql("5544", "apitap_bench_src", sql)
dst = lambda sql: psql("5545", "apitap_bench_dst", sql)

def run(label):
    t0 = time.time()
    r = apitap.transfer(SRC, DST, table=T, mode="log_based")
    print(f"{label}: rows={r.rows:,} in {time.time()-t0:.1f}s")
    return r

def check(label):
    a = src(f"SELECT count(*), coalesce(sum(id),0) FROM {T}")
    b = dst(f"SELECT count(*), coalesce(sum(id),0) FROM {T}")
    rows_src = src(f"SELECT id||'|'||coalesce(v,'<N>')||'|'||coalesce(big,'<N>') FROM {T} ORDER BY id")
    rows_dst = dst(f"SELECT id||'|'||coalesce(v,'<N>')||'|'||coalesce(big,'<N>') FROM {T} ORDER BY id")
    ok = a == b and rows_src == rows_dst
    print(f"CHECK {label}: {'MATCH' if ok else 'MISMATCH'} (src {a} / dst {b})")
    if not ok:
        print("  src:", rows_src[:400])
        print("  dst:", rows_dst[:400])
        raise SystemExit(1)

# ── fresh start ─────────────────────────────────────────────────────────────
src(f"DROP TABLE IF EXISTS {T} CASCADE")
dst(f"DROP TABLE IF EXISTS {T} CASCADE")
dst("DELETE FROM _apitap_state WHERE dest_table = 'cdc_demo'") if dst(
    "SELECT count(*) FROM information_schema.tables WHERE table_name='_apitap_state'") != "0" else None
for s in src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'").splitlines():
    if s:
        src(f"SELECT pg_drop_replication_slot('{s}')")
for p in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'").splitlines():
    if p:
        src(f"DROP PUBLICATION {p}")

src(f"""CREATE TABLE {T} (
      id int PRIMARY KEY, v text, big text,
      ts timestamp DEFAULT now())""")
src(f"INSERT INTO {T}(id, v, big) SELECT g, 'v'||g, NULL FROM generate_series(1, 100000) g")
# One row with a real TOASTed value (forces external storage).
src(f"UPDATE {T} SET big = repeat('x', 200000) WHERE id = 42")

# ── run 1: bootstrap ────────────────────────────────────────────────────────
r = run("run1 bootstrap")
assert r.rows == 100000, r.rows
check("after bootstrap")
assert dst("SELECT mode FROM _apitap_state WHERE dest_table='cdc_demo'") == "log_based"

# ── window 1: the full op mix across several transactions ───────────────────
src(f"INSERT INTO {T}(id, v) VALUES (100001, 'new'), (100002, '')")   # tx1 (incl empty string)
src(f"UPDATE {T} SET v = 'updated' WHERE id <= 5")                     # tx2
src(f"DELETE FROM {T} WHERE id BETWEEN 10 AND 19")                     # tx3
src(f"UPDATE {T} SET id = 999999 WHERE id = 7")                        # tx4: PK change
src(f"UPDATE {T} SET v = 'toast-kept' WHERE id = 42")                  # tx5: unchanged TOAST
src(f"INSERT INTO {T}(id, v) VALUES (100003, 'gone'); DELETE FROM {T} WHERE id = 100003")  # tx6 net delete
r = run("run2 drain")
assert r.rows > 0
check("after mixed window")
assert dst(f"SELECT big = repeat('x', 200000) FROM {T} WHERE id = 42") == "t", "TOAST value survived the masked update"
assert dst(f"SELECT v FROM {T} WHERE id = 42") == "toast-kept"
assert dst(f"SELECT count(*) FROM {T} WHERE id = 100003") == "0"
assert dst(f"SELECT v = '' FROM {T} WHERE id = 100002") == "t", "empty string stayed a value"

# ── window 2: idempotent empty drain ────────────────────────────────────────
r = run("run3 empty")
assert r.rows == 0, r.rows
check("after empty drain")

# ── window 3: TRUNCATE then repopulate ──────────────────────────────────────
src(f"TRUNCATE {T}")
src(f"INSERT INTO {T}(id, v) VALUES (1, 'reborn'), (2, '')")
r = run("run4 truncate")
check("after truncate window")
assert dst(f"SELECT count(*) FROM {T}") == "2"

# ── window 4: heavy delta for a feel of speed ───────────────────────────────
src(f"INSERT INTO {T}(id, v) SELECT g, 'bulk'||g FROM generate_series(1000, 500999) g")
src(f"UPDATE {T} SET v = 'rewrite' WHERE id BETWEEN 1000 AND 100999")
src(f"DELETE FROM {T} WHERE id BETWEEN 400000 AND 449999")
r = run("run5 heavy")
check("after heavy window")

print("\nE2E LOG_BASED: ALL GREEN")
