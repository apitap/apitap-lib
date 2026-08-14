"""Per-table partition_by for a changelog GROUP — and the torn-group guard.

Three tables, three DIFFERENT time columns, one call, one slot. Then the case
that used to leave the group half-bootstrapped: a clause naming a column only
some members own.

ENV: DEST=ch|bq. For bq: BQ_SA, BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import apitap

DEST = os.environ.get("DEST", "ch")
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = ["pc_orders", "pc_events", "pc_audit"]
COL = {"pc_orders": "created_at", "pc_events": "occurred_at", "pc_audit": "logged_at"}

if DEST == "bq":
    import requests
    from google.oauth2 import service_account
    import google.auth.transport.requests as gt
    PROJECT = os.environ.get("BQ_PROJECT", "apitap")
    DATASET = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
    DST = f"bigquery://{PROJECT}/{DATASET}?credentials={os.environ['BQ_SA']}"
    _creds = service_account.Credentials.from_service_account_file(
        os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])
else:
    DST = "clickhouse://default:bench@127.0.0.1:8124/default"


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-Atc", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst(sql):
    if DEST == "bq":
        _creds.refresh(gt.Request())
        r = requests.post(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
            headers={"Authorization": f"Bearer {_creds.token}"},
            json={"query": sql, "useLegacySql": False, "timeoutMs": 120000,
                  "location": "US"})
        j = r.json()
        if r.status_code >= 400 or j.get("jobComplete") is False:
            raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
        return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]
    q = sql + (" FORMAT TabSeparated" if sql.lstrip().upper().startswith("SELECT") else "")
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", q],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [ln.split("\t") for ln in o.stdout.strip().split("\n") if ln]


def reset():
    for t in T:
        pg(f"DROP TABLE IF EXISTS {t} CASCADE")
        if DEST == "bq":
            for s in ("__current",):
                try:
                    dst(f"DROP VIEW IF EXISTS `{DATASET}.{t}{s}`")
                except Exception:
                    pass
            for s in ("", "__apitap_cl", "__apitap_cdc"):
                try:
                    dst(f"DROP TABLE IF EXISTS `{DATASET}.{t}{s}`")
                except Exception:
                    pass
        else:
            dst(f"DROP VIEW IF EXISTS `{t}__current`")
            for s in ("", "__apitap_cl", "__apitap_cdc_del"):
                dst(f"DROP TABLE IF EXISTS `{t}{s}`")
    pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots")
    if DEST == "bq":
        dst(f"DELETE FROM `{DATASET}._apitap_state` WHERE dest_table LIKE 'pc_%'")
    else:
        dst("ALTER TABLE _apitap_state DELETE WHERE dest_table LIKE 'pc_%' "
            "SETTINGS mutations_sync=1")


def states():
    """CDC watermarks only.

    Not every row of `_apitap_state` is a watermark: the bulk load that
    bootstraps each table also writes a `source_id='*'` replace-barrier
    (`cursor_col` NULL, `mode='replace-barrier'`) whose whole job is to
    invalidate an older CDC watermark. Counting those as state made this test
    claim a torn group where there was none — `read_state` only ever accepts a
    row with `cursor_col='_lsn'`.
    """
    if DEST == "bq":
        rows = dst(f"SELECT dest_table FROM `{DATASET}._apitap_state` "
                   f"WHERE dest_table LIKE 'pc_%' AND cursor_col = '_lsn'")
    else:
        rows = dst("SELECT dest_table FROM _apitap_state FINAL "
                   "WHERE dest_table LIKE 'pc_%' AND cursor_col = '_lsn'")
    return sorted({r[0] for r in rows})


ok = True
print(f"== reset (dest={DEST}) ==")
reset()
for t in T:
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, "
       f"{COL[t]} timestamptz NOT NULL DEFAULT now())")
    pg(f"INSERT INTO {t} (id, v) VALUES (1,'a'),(2,'b')")

print("== a clause naming a column only ONE table owns: refused, nothing written ==")
try:
    apitap.transfer(PG, DST, tables=T, mode="log_based", changelog=True,
                    partition_by=("toYYYYMM(created_at)" if DEST == "ch" else "created_at"))
    print("   ✗ accepted a clause two members cannot satisfy")
    ok = False
except Exception as e:
    print(f"   ✓ refused: {str(e)[:150]}")
left = states()
print(f"   CDC watermarks left behind: {left} (want [])")
ok &= left == []

print("== per-table partition_by: three tables, three columns, one call ==")
reset()
for t in T:
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, "
       f"{COL[t]} timestamptz NOT NULL DEFAULT now())")
    pg(f"INSERT INTO {t} (id, v) VALUES (1,'a'),(2,'b')")

if DEST == "ch":
    pb = {t: f"toYYYYMM({COL[t]})" for t in T[:2]}      # audit left to the default
else:
    pb = {t: COL[t] for t in T[:2]}
r = apitap.transfer(PG, DST, tables=T, mode="log_based", changelog=True, partition_by=pb)
print(f"   group bootstrap rows={r.rows}")

for t in T:
    if DEST == "bq":
        ddl = dst(f"SELECT ddl FROM `{DATASET}.INFORMATION_SCHEMA.TABLES` "
                  f"WHERE table_name='{t}'")[0][0].replace("`", "")
        key = "TIMESTAMP_TRUNC(%s, MONTH)" % (COL[t] if t in pb else "_apitap_at")
    else:
        ddl = dst(f"SELECT partition_key FROM system.tables WHERE name='{t}' "
                  f"AND database=currentDatabase()")[0][0].replace("`", "")
        key = "toYYYYMM(%s)" % (COL[t] if t in pb else "_apitap_at")
    good = key in ddl
    print(f"   {t:<10} partitioned on {key:<38} -> {good}")
    ok &= good

print("== and one window still lands every member at the same LSN ==")
pg("UPDATE pc_orders SET v='a1' WHERE id=1")
pg("INSERT INTO pc_events (id, v) VALUES (3,'c')")
apitap.transfer(PG, DST, tables=T, mode="log_based", changelog=True, partition_by=pb)
if DEST == "bq":
    rows = dst(f"SELECT dest_table, watermark FROM `{DATASET}._apitap_state` "
               f"WHERE dest_table LIKE 'pc_%' ORDER BY dest_table, synced_at DESC")
else:
    rows = dst("SELECT dest_table, watermark FROM _apitap_state FINAL "
               "WHERE dest_table LIKE 'pc_%' ORDER BY dest_table")
seen = {}
for row in rows:
    seen.setdefault(row[0], row[1])
print(f"   watermarks: {seen}")
ok &= len(seen) == len(T) and len(set(seen.values())) == 1

print("== cleanup ==")
reset()
print(f"\n   ===== PER-TABLE PARTITION E2E ({DEST}): "
      + ("ALL GREEN" if ok else "FAILED") + " =====")
