"""What a replace is about to destroy, said BEFORE it copies anything.

Two operational papercuts, both on Postgres destinations:

  * A view on the destination makes `mode="replace"` fail at the very END —
    the swap drops the old table and Postgres refuses while a view depends on
    it. The rows were already copied by then. Failing at probe time costs the
    user nothing they had (the run failed either way) and saves the whole
    transfer's work.

  * A replace loads into a bare staging table (faster) and must then give the
    destination back what it had: secondary indexes, constraints and grants.
    That preservation is easy to break silently, so it is asserted here — and
    the claim in an outside review that apitap drops them, which our own docs
    repeated, turned out to be false.
"""
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
SRC = "bench_data_1m"
T = "hz_demo"


def sh(args):
    return subprocess.run(args, capture_output=True, text=True)


def db(sql, which="dst"):
    box = "apitap-bench-pg-dst" if which == "dst" else "apitap-bench-pg-src"
    name = "apitap_bench_dst" if which == "dst" else "apitap_bench_src"
    o = sh(["docker", "exec", "-i", box, "psql", "-U", "postgres", "-d", name, "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr.strip())
    return o.stdout.strip()


def transfer(mode="replace"):
    return sh([sys.executable, "-c", f"""
import apitap
r = apitap.transfer({PG!r}, {PGD!r}, table={SRC!r}, dest_table={T!r}, mode={mode!r})
print("ROWS", r.rows)
"""])


ok = True
db(f"DROP VIEW IF EXISTS {T}_v")
db(f"DROP TABLE IF EXISTS {T}")

print("== a first replace creates the destination ==")
r = transfer()
if r.returncode:
    ok = False
    print(f"   ✗ first load failed: {r.stderr[-300:]}")
else:
    n = db(f"SELECT count(*) FROM {T}")
    print(f"   ✓ {n} rows landed")

print("== indexes and grants must SURVIVE a replace ==")
# This started as a test for a warning, on the review's claim that a replace
# discards them. The engine already captures and re-applies them, so the claim —
# and our own docs — were out of date. Test the real behaviour instead.
db(f"CREATE INDEX {T}_idx ON {T} (regular_int)")
db("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='apitap_reader') "
   "THEN CREATE ROLE apitap_reader; END IF; END $$")
db(f"GRANT SELECT ON {T} TO apitap_reader")
idx_before = db(f"SELECT count(*) FROM pg_index WHERE indrelid = to_regclass('{T}') AND NOT indisprimary")
grant_before = db(f"SELECT count(*) FROM information_schema.role_table_grants "
                  f"WHERE table_name = '{T}' AND grantee = 'apitap_reader'")
r = transfer()
idx_after = db(f"SELECT count(*) FROM pg_index WHERE indrelid = to_regclass('{T}') AND NOT indisprimary")
grant_after = db(f"SELECT count(*) FROM information_schema.role_table_grants "
                 f"WHERE table_name = '{T}' AND grantee = 'apitap_reader'")
if r.returncode == 0 and (idx_after, grant_after) == (idx_before, grant_before):
    print(f"   ✓ index and grant both survived the swap "
          f"({idx_before}→{idx_after} indexes, {grant_before}→{grant_after} grants)")
else:
    ok = False
    print(f"   ✗ rc={r.returncode}, indexes {idx_before}→{idx_after}, "
          f"grants {grant_before}→{grant_after}\n{r.stderr[-400:]}")

print("== a dependent view must be refused at PROBE, before any copying ==")
db(f"CREATE VIEW {T}_v AS SELECT id FROM {T}")
rows_before = db(f"SELECT count(*) FROM {T}")
r = transfer()
msg = r.stderr
if r.returncode == 0:
    ok = False
    print("   ✗ the replace succeeded — Postgres should not have allowed the swap")
elif "depend" in msg and "Nothing has been copied yet" in msg:
    print("   ✓ refused, and the message names the view and says nothing was copied")
    line = [l for l in msg.splitlines() if "depend" in l]
    print(f"     {line[0].strip()[:150]}…")
else:
    ok = False
    print(f"   ✗ failed for the wrong reason (or too late):\n{msg[-700:]}")
rows_after = db(f"SELECT count(*) FROM {T}")
staging = db(f"SELECT count(*) FROM pg_class WHERE relname = '{T}__apitap_staging'")
if rows_after == rows_before and staging == "0":
    print(f"   ✓ destination untouched ({rows_after} rows) and no staging table was even created")
else:
    ok = False
    print(f"   ✗ rows {rows_before}→{rows_after}, staging tables left: {staging}")

print("== append into the same table is unaffected (it never drops) ==")
r = transfer(mode="append")
if r.returncode == 0:
    print("   ✓ append succeeded with the view still in place")
else:
    ok = False
    print(f"   ✗ append should not care about views: {r.stderr[-300:]}")

print("== cleanup ==")
db(f"DROP VIEW IF EXISTS {T}_v")
db(f"DROP TABLE IF EXISTS {T}")
db(f"DROP TABLE IF EXISTS {T}__apitap_staging")
db(f"DELETE FROM _apitap_state WHERE dest_table LIKE '%{T}%'")
print("   dropped the view, the table and its state")

print("\n" + ("REPLACE-HAZARD E2E: ALL GREEN" if ok else "REPLACE-HAZARD E2E: FAILED"))
raise SystemExit(0 if ok else 1)
