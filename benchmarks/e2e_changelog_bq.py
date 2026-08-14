"""changelog=True e2e → BigQuery.

The ClickHouse changelog e2e, run against the other analytical destination, so
the two engines are held to the SAME contract:
  1. EVERY operation is captured (a key updated 3x lands 3 rows, not 1),
  2. `<table>__current` equals the source table, and
  3. an empty drain appends nothing.

Plus the two things that are BigQuery-specific:
  4. the rebuilt table is partitioned MONTHLY on `_apitap_at`, and
  5. `partition_by` on a non-time column is refused with a useful message.

BigQuery is a destination, not a read source, so the readback goes through
google-auth + REST exactly like the replica-mode e2e does.

ENV: BQ_SA (service-account JSON path), BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import apitap
import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DATASET = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = f"bigquery://{PROJECT}/{DATASET}?credentials={os.environ['BQ_SA']}"
T = os.environ.get("T", "cl_bq_demo")
DS = DATASET

_creds = service_account.Credentials.from_service_account_file(
    os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def bq(sql):
    """Rows as lists of strings. The SA key stays inside google-auth."""
    _creds.refresh(gt.Request())
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
        headers={"Authorization": f"Bearer {_creds.token}"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 120000, "location": "US"})
    j = r.json()
    if r.status_code >= 400 or j.get("jobComplete") is False:
        raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
    return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]


def drain(**kw):
    return apitap.transfer(PG, BQ, table=T, mode="log_based", changelog=True, **kw)


def current_matches(stage):
    p = pg(f"SELECT id||'|'||COALESCE(v,'<N>') FROM {T} ORDER BY id")
    rows = bq(f"SELECT CONCAT(CAST(id AS STRING),'|',IFNULL(v,'<N>')) "
              f"FROM `{DS}.{T}__current` ORDER BY id")
    c = "\n".join(r[0] for r in rows)
    if p == c:
        print(f"   ✓ {stage}: __current matches pg ({len(p.splitlines())} rows)")
        return True
    print(f"   ✗ {stage}: MISMATCH\n   pg:\n{p}\n   bq:\n{c}")
    return False


def count(sql):
    return int(bq(f"SELECT COUNT(*) FROM `{DS}.{T}` WHERE {sql}")[0][0])


ok = True
print("== reset ==")
pg(f"DROP TABLE IF EXISTS {T} CASCADE")
pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM "
   "pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
for t in (f"{T}__current",):
    try:
        bq(f"DROP VIEW IF EXISTS `{DS}.{t}`")
    except Exception:
        pass
for t in (T, f"{T}__apitap_cl", f"{T}__apitap_cdc"):
    try:
        bq(f"DROP TABLE IF EXISTS `{DS}.{t}`")
    except Exception:
        pass
try:
    bq(f"DELETE FROM `{DS}._apitap_state` WHERE dest_table='{T}'")
except Exception:
    pass

pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text, body text)")  # body TOASTs later
pg(f"INSERT INTO {T} VALUES (1,'a'),(2,'b'),(3,'c')")

print("== bootstrap (baseline rows get op B) ==")
r = drain()
print(f"   bootstrap rows={r.rows}")
base = count("_apitap_op='B'")
print(f"   baseline rows tagged B: {base}")
ok &= base == 3
ok &= current_matches("bootstrap")

print("== partitioning: MONTHLY on _apitap_at ==")
part = bq(
    f"SELECT ddl FROM `{DS}.INFORMATION_SCHEMA.TABLES` WHERE table_name='{T}'")[0][0]
monthly = "TIMESTAMP_TRUNC(_apitap_at, MONTH)" in part.replace("`", "")
print(f"   partitioned monthly on _apitap_at: {monthly}")
ok &= monthly

print("== window 1: 3 updates on ONE key + insert + delete ==")
pg(f"UPDATE {T} SET v='a1' WHERE id=1")
pg(f"UPDATE {T} SET v='a2' WHERE id=1")
pg(f"UPDATE {T} SET v='a3' WHERE id=1")
pg(f"INSERT INTO {T} VALUES (4,'d')")
pg(f"DELETE FROM {T} WHERE id=2")
r = drain()
print(f"   window1 events={r.rows}")
u1 = count("id=1 AND _apitap_op='U'")
print(f"   'U' records for id=1: {u1}  (collapse would have left 1)")
ok &= u1 == 3
d2 = count("_apitap_op='D'")
print(f"   'D' records: {d2}")
ok &= d2 == 1
ok &= current_matches("window1")

print("== window 2: empty drain appends nothing ==")
before = count("TRUE")
drain()
after = count("TRUE")
print(f"   log rows {before} -> {after}")
ok &= before == after
ok &= current_matches("window2-empty")

print("== unchanged-TOAST: an UPDATE that skips a big column must not blank it ==")
BIG = 40000
pg(f"UPDATE {T} SET body = repeat('x', {BIG}) WHERE id = 1")
drain()                                                 # window carrying body
pg(f"UPDATE {T} SET v = 'toast-probe' WHERE id = 1")    # body NOT touched
drain()                                                 # the WAL omits body
got = int(bq(f"SELECT LENGTH(IFNULL(body,'')) FROM `{DS}.{T}__current` WHERE id=1")[0][0])
print(f"   body length in __current after a body-less UPDATE: {got} (want {BIG})")
ok &= got == BIG
newest = int(bq(f"SELECT LENGTH(IFNULL(body,'')) FROM `{DS}.{T}` WHERE id=1 AND "
                f"_apitap_op='U' ORDER BY _apitap_lsn DESC, _apitap_seq DESC LIMIT 1")[0][0])
print(f"   …and the newest U record itself carries it: {newest}")
ok &= newest == BIG
ok &= current_matches("after-toast")

print("== partition_by on a non-time column is refused ==")
pg(f"DROP TABLE IF EXISTS {T}_p CASCADE")
pg(f"CREATE TABLE {T}_p (id int PRIMARY KEY, v text)")
pg(f"INSERT INTO {T}_p VALUES (1,'a')")
try:
    apitap.transfer(PG, BQ, table=f"{T}_p", mode="log_based", changelog=True,
                    partition_by="v")
    print("   ✗ a STRING partition column was accepted")
    ok = False
except Exception as e:
    good = "partition" in str(e).lower() and "time" in str(e).lower()
    print(f"   {'✓' if good else '✗'} refused: {str(e)[:150]}")
    ok &= good

print("\n   ===== BQ CHANGELOG E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
