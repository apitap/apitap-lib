"""Group CDC e2e + timing: bootstrap 10 heavy tables in ONE slot, verify each
matches pg, drain a backlog (concurrent apply), verify again. Runs on the host
(uses docker exec psql); the transfer itself is what we time."""
import os, subprocess, time
import apitap, requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = "bigquery://apitap/apitap_cdc_e2e?credentials=/home/ubuntu/apitap.json"
TABLES = [f"cdc_gg{i:02d}" for i in range(1, 11)]
N = 50000

_c = service_account.Credentials.from_service_account_file(
    "/home/ubuntu/apitap.json", scopes=["https://www.googleapis.com/auth/bigquery"])


def bq(sql):
    _c.refresh(gt.Request())
    r = requests.post("https://bigquery.googleapis.com/bigquery/v2/projects/apitap/queries",
                      headers={"Authorization": f"Bearer {_c.token}"},
                      json={"query": sql, "useLegacySql": False, "location": "US"}).json()
    if r.get("jobComplete") is False or "error" in r:
        raise RuntimeError(r.get("error", r))
    return [[c.get("v") for c in row.get("f", [])] for row in r.get("rows", [])]


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def verify(stage):
    bad = 0
    for t in TABLES:
        pn = int(pg(f"SELECT count(*) FROM {t}"))
        ps = int(pg(f"SELECT COALESCE(sum(regular_int::bigint),0) FROM {t}"))
        bn = int(bq(f"SELECT count(*) FROM `apitap.apitap_cdc_e2e.{t}`")[0][0])
        bs = int(bq(f"SELECT COALESCE(CAST(sum(regular_int) AS INT64),0) FROM `apitap.apitap_cdc_e2e.{t}`")[0][0])
        if (pn, ps) != (bn, bs):
            print(f"   ✗ {t}: pg=({pn},{ps}) bq=({bn},{bs})"); bad += 1
    print(f"   {'✓' if not bad else '✗'} {stage}: {len(TABLES)-bad}/{len(TABLES)} tables match")
    return bad == 0


print("== reset ==", flush=True)
for t in TABLES:
    pg(f"DROP TABLE IF EXISTS {t} CASCADE")
    for suff in ("", "__apitap_cdc", "__apitap_cl"):
        bq(f"DROP TABLE IF EXISTS `apitap.apitap_cdc_e2e.{t}{suff}`")
    bq(f"DELETE FROM `apitap.apitap_cdc_e2e._apitap_state` WHERE dest_table='{t}'")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
for t in TABLES:
    pg(f"CREATE TABLE {t} AS SELECT * FROM heavy_t01 LIMIT {N}; ALTER TABLE {t} ADD PRIMARY KEY (id);")

print("== bootstrap group (10 tables, ONE slot) ==", flush=True)
t0 = time.time()
apitap.transfer(PG, BQ, tables=TABLES, mode="log_based")
print(f"   bootstrap wall {time.time()-t0:.1f}s", flush=True)
ok = verify("bootstrap")

print("== backlog + drain (concurrent apply) ==", flush=True)
for t in TABLES:
    pg(f"UPDATE {t} SET regular_int = regular_int + 1 WHERE id % 3 = 0")
t0 = time.time()
apitap.transfer(PG, BQ, tables=TABLES, mode="log_based")
print(f"   drain wall {time.time()-t0:.1f}s", flush=True)
ok = verify("drain") and ok

for t in TABLES:
    pg(f"DROP TABLE IF EXISTS {t} CASCADE")
    for suff in ("", "__apitap_cdc"):
        bq(f"DROP TABLE IF EXISTS `apitap.apitap_cdc_e2e.{t}{suff}`")
    bq(f"DELETE FROM `apitap.apitap_cdc_e2e._apitap_state` WHERE dest_table='{t}'")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
print("\n   ===== GROUP CDC: " + ("ALL GREEN" if ok else "MISMATCH") + " =====", flush=True)
