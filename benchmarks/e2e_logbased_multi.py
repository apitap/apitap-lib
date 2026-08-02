"""log_based multi-table E2E: one slot group + per-table modes, on the rig.

Covers: group bootstrap pinned to ONE snapshot (one apitap_g% slot, one
publication, equal LSNs in state), a window touching only SOME members
(quiet tables still advance their watermark), TRUNCATE of one member,
idempotent empty drain, and the {table: mode} dict call mixing a CDC group
with a bulk replace in one transfer.
"""
import subprocess, time

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
GROUP = ["cdc_m1", "cdc_m2", "cdc_m3"]


def sh(args):
    out = subprocess.run(args, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr or out.stdout)
    return out.stdout.strip()


def src(sql):
    return sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
               "-d", "apitap_bench_src", "-Atc", sql])


def dst(sql):
    return sh(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres",
               "-d", "apitap_bench_dst", "-Atc", sql])


def check(label, tables=GROUP):
    for t in tables:
        a = src(f"SELECT count(*)||'|'||coalesce(sum(id),0) FROM {t}")
        b = dst(f"SELECT count(*)||'|'||coalesce(sum(id),0) FROM {t}")
        if a != b:
            print(f"CHECK {label}: MISMATCH on {t} (src {a} / dst {b})")
            raise SystemExit(1)
    print(f"CHECK {label}: MATCH ({', '.join(tables)})")


def group_lsns():
    rows = dst("SELECT dest_table||'='||watermark FROM _apitap_state "
               "WHERE dest_table LIKE 'cdc_m%' AND mode='log_based' ORDER BY dest_table")
    return rows.splitlines()


# ── fresh start ─────────────────────────────────────────────────────────────
for t in GROUP + ["cdc_mr"]:
    src(f"DROP TABLE IF EXISTS {t} CASCADE")
    dst(f"DROP TABLE IF EXISTS {t}")
dst("DELETE FROM _apitap_state WHERE dest_table LIKE 'cdc_m%'") if dst(
    "SELECT count(*) FROM information_schema.tables WHERE table_name='_apitap_state'") != "0" else None
for s in src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'").splitlines():
    if s:
        src(f"SELECT pg_drop_replication_slot('{s}')")
for p in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'").splitlines():
    if p:
        src(f"DROP PUBLICATION {p}")

src("CREATE TABLE cdc_m1 (id int PRIMARY KEY, v text)")
src("CREATE TABLE cdc_m2 (id int PRIMARY KEY, n bigint, note text)")
src("CREATE TABLE cdc_m3 (id int PRIMARY KEY, ts timestamp DEFAULT now())")
src("CREATE TABLE cdc_mr (id int PRIMARY KEY, x text)")
src("INSERT INTO cdc_m1 SELECT g, 'v'||g FROM generate_series(1,100000) g")
src("INSERT INTO cdc_m2 SELECT g, g*7, 'n'||g FROM generate_series(1,50000) g")
src("INSERT INTO cdc_m3(id) SELECT g FROM generate_series(1,10000) g")
src("INSERT INTO cdc_mr SELECT g, 'x'||g FROM generate_series(1,5000) g")

# ── run 1: group bootstrap (ONE slot, ONE snapshot) ─────────────────────────
t0 = time.time()
r = apitap.transfer(SRC, DST, tables=GROUP, mode="log_based")
print(f"run1 group bootstrap: rows={r.rows:,} in {time.time()-t0:.1f}s "
      f"({[(t.table, t.rows) for t in r.tables]})")
assert r.rows == 160000, r.rows
check("after bootstrap")
slots = [s for s in src("SELECT slot_name FROM pg_replication_slots").splitlines() if s]
assert len(slots) == 1 and slots[0].startswith("apitap_g"), slots
lsns = group_lsns()
assert len(lsns) == 3 and len({l.split("=")[1] for l in lsns}) == 1, lsns
print(f"one slot ({slots[0]}), equal LSNs across the group")

# ── window 1: traffic on m1+m2 only — m3 must still advance ─────────────────
src("INSERT INTO cdc_m1 VALUES (100001, 'new')")
src("UPDATE cdc_m1 SET v='upd' WHERE id <= 10")
src("DELETE FROM cdc_m1 WHERE id BETWEEN 50 AND 59")
src("UPDATE cdc_m1 SET id = 999999 WHERE id = 77")                 # PK change
src("UPDATE cdc_m2 SET n = n+1 WHERE id % 5 = 0")
src("DELETE FROM cdc_m2 WHERE id > 49000")
r = apitap.transfer(SRC, DST, tables=GROUP, mode="log_based")
print(f"run2 drain: rows={r.rows:,} ({[(t.table, t.rows) for t in r.tables]})")
check("after window 1")
lsns = group_lsns()
assert len({l.split("=")[1] for l in lsns}) == 1, f"quiet member lagged: {lsns}"
print("quiet table advanced with the group")

# ── window 2: TRUNCATE one member + traffic on the quiet one ────────────────
src("TRUNCATE cdc_m2")
src("INSERT INTO cdc_m2 VALUES (1, 42, 'post-trunc')")
src("INSERT INTO cdc_m3(id) SELECT g FROM generate_series(10001, 12000) g")
r = apitap.transfer(SRC, DST, tables=GROUP, mode="log_based")
print(f"run3 truncate+quiet: rows={r.rows:,}")
check("after window 2")

# ── empty drain is a no-op ──────────────────────────────────────────────────
r = apitap.transfer(SRC, DST, tables=GROUP, mode="log_based")
assert r.rows == 0, r.rows
check("after empty drain")

# ── per-table modes in ONE call: CDC group + a bulk replace ─────────────────
src("INSERT INTO cdc_m1 VALUES (100002, 'mixed-call')")
src("UPDATE cdc_mr SET x = 'replaced' WHERE id <= 100")
specs = {t: "log_based" for t in GROUP}
specs["cdc_mr"] = "replace"
r = apitap.transfer(SRC, DST, tables=specs)
print(f"run5 mixed modes: rows={r.rows:,} ({[(t.table, t.rows) for t in r.tables]})")
check("after mixed call", GROUP + ["cdc_mr"])

# ── partial-state guard: a changed group must refuse loudly ─────────────────
try:
    apitap.transfer(SRC, DST, tables=GROUP + ["cdc_mr"], mode="log_based")
    print("GUARD FAIL: changed membership was accepted")
    raise SystemExit(1)
except ValueError as e:
    assert "membership" in str(e) or "state for" in str(e), e
    print(f"membership guard: refused loudly ({str(e)[:80]}…)")

print("ALL GREEN")

# ── cleanup (standing rule) ─────────────────────────────────────────────────
for t in GROUP + ["cdc_mr"]:
    src(f"DROP TABLE IF EXISTS {t} CASCADE")
    dst(f"DROP TABLE IF EXISTS {t}")
dst("DELETE FROM _apitap_state WHERE dest_table LIKE 'cdc_m%'")
for s in src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'").splitlines():
    if s:
        src(f"SELECT pg_drop_replication_slot('{s}')")
for p in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'").splitlines():
    if p:
        src(f"DROP PUBLICATION {p}")
print("cleaned up")
