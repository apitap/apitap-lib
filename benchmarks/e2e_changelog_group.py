"""changelog=True across a multi-table GROUP — one slot, one window, N logs.

The single-table e2e suites never exercise the group path, which is where the
changelog work actually differs per destination: ClickHouse applies each table
serially or through the lane pool, BigQuery stages every table concurrently and
commits the whole group's INSERTs in as few script jobs as it can.

What this proves, for BOTH analytical destinations:
  1. every member gets its own changelog with its own baseline,
  2. one window carries changes for SEVERAL tables and each lands only its own,
  3. a QUIET member (no changes at all) still advances with the group,
  4. `partition_by` / `order_by` given once apply to EVERY member, and
  5. every member's `__current` equals its source table.

ENV: DEST=ch|bq. For bq: BQ_SA, BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import apitap

DEST = os.environ.get("DEST", "ch")
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLES = ["clg_a", "clg_b", "clg_quiet"]

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
    DST = CH


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-Atc", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst(sql):
    """Rows as a list of lists of strings, from whichever destination."""
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
    # FORMAT belongs to SELECTs only — appending it to a DROP/ALTER makes the
    # statement a syntax error, which a try/except around a reset turns into a
    # silent no-op. That is how a "clean" rig ends up dirty.
    q = sql + (" FORMAT TabSeparated" if sql.lstrip().upper().startswith("SELECT") else "")
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", q],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [ln.split("\t") for ln in o.stdout.strip().split("\n") if ln]


def fq(t):
    return f"`{DATASET}.{t}`" if DEST == "bq" else f"`{t}`"


def count(t, where):
    return int(dst(f"SELECT count(*) FROM {fq(t)} WHERE {where}")[0][0])


def current_matches(t, stage):
    p = pg(f"SELECT id||'|'||COALESCE(v,'<N>') FROM {t} ORDER BY id")
    if DEST == "bq":
        rows = dst(f"SELECT CONCAT(CAST(id AS STRING),'|',IFNULL(v,'<N>')) "
                   f"FROM {fq(t + '__current')} ORDER BY id")
    else:
        rows = dst(f"SELECT toString(id)||'|'||ifNull(v,'<N>') "
                   f"FROM {fq(t + '__current')} ORDER BY id")
    c = "\n".join(r[0] for r in rows)
    if p == c:
        print(f"   ✓ {stage} {t}: __current matches pg ({len(p.splitlines())} rows)")
        return True
    print(f"   ✗ {stage} {t}: MISMATCH\n   pg:\n{p}\n   dst:\n{c}")
    return False


def drop_dst(t):
    try:
        dst(f"DROP VIEW IF EXISTS {fq(t + '__current')}")
    except Exception:
        pass
    for suffix in ("", "__apitap_cl", "__apitap_cdc", "__apitap_cdc_del"):
        try:
            dst(f"DROP TABLE IF EXISTS {fq(t + suffix)}")
        except Exception:
            pass


ok = True
print(f"== reset (dest={DEST}) ==")
for t in TABLES:
    pg(f"DROP TABLE IF EXISTS {t} CASCADE")
    drop_dst(t)
pg("DROP PUBLICATION IF EXISTS apitap_pub_grp")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM "
   "pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
if DEST == "bq":
    dst(f"DELETE FROM `{DATASET}._apitap_state` WHERE dest_table LIKE 'clg_%'")
else:
    dst("ALTER TABLE _apitap_state DELETE WHERE dest_table LIKE 'clg_%' "
        "SETTINGS mutations_sync=1")

for t in TABLES:
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, ts timestamptz NOT NULL DEFAULT now())")
    pg(f"INSERT INTO {t} (id, v) VALUES (1,'a'),(2,'b')")

# partition_by / order_by given ONCE — they must reach every member. `ts` is a
# column all three tables share, so a per-table DDL is well-formed either way.
KW = dict(mode="log_based", changelog=True, partition_by="ts")
if DEST == "ch":
    KW["partition_by"] = "toYYYYMM(ts)"      # ClickHouse takes an expression
    KW["order_by"] = "id, _apitap_lsn, _apitap_seq"

print("== bootstrap the whole group in one call ==")
r = apitap.transfer(PG, DST, tables=TABLES, **KW)
print(f"   group bootstrap rows={r.rows}")
for t in TABLES:
    b = count(t, "_apitap_op='B'")
    print(f"   {t}: baseline rows tagged B = {b}")
    ok &= b == 2
    ok &= current_matches(t, "bootstrap")

print("== partition_by reached every member ==")
for t in TABLES:
    if DEST == "bq":
        ddl = dst(f"SELECT ddl FROM `{DATASET}.INFORMATION_SCHEMA.TABLES` "
                  f"WHERE table_name='{t}'")[0][0]
        good = "TIMESTAMP_TRUNC(ts, MONTH)" in ddl.replace("`", "")
    else:
        ddl = dst(f"SELECT partition_key FROM system.tables WHERE name='{t}' "
                  f"AND database=currentDatabase()")[0][0]
        good = "toYYYYMM(ts)" in ddl.replace("`", "")
    print(f"   {t}: partitioned on ts -> {good}")
    ok &= good

print("== one window, changes in TWO members, the third quiet ==")
pg("UPDATE clg_a SET v='a1' WHERE id=1")
pg("UPDATE clg_a SET v='a2' WHERE id=1")
pg("INSERT INTO clg_b VALUES (3,'c', now())")
pg("DELETE FROM clg_b WHERE id=2")
r = apitap.transfer(PG, DST, tables=TABLES, **KW)
print(f"   window events={r.rows}")

ua = count("clg_a", "id=1 AND _apitap_op='U'")
print(f"   clg_a 'U' records for id=1: {ua}  (collapse would have left 1)")
ok &= ua == 2
db = count("clg_b", "_apitap_op='D'")
print(f"   clg_b 'D' records: {db}")
ok &= db == 1
# Cross-contamination: clg_a must carry none of clg_b's events and vice versa.
xa = count("clg_a", "_apitap_op='D'")
xb = count("clg_b", "_apitap_op='U'")
print(f"   cross-check — clg_a D records: {xa} (want 0), clg_b U records: {xb} (want 0)")
ok &= xa == 0 and xb == 0
q = count("clg_quiet", "TRUE")
print(f"   clg_quiet rows: {q} (want 2 — the baseline, nothing appended)")
ok &= q == 2
for t in TABLES:
    ok &= current_matches(t, "window")

print("== the quiet member advanced with the group ==")
if DEST == "bq":
    marks = dst(f"SELECT dest_table, watermark FROM `{DATASET}._apitap_state` "
                f"WHERE dest_table LIKE 'clg_%' ORDER BY dest_table, synced_at DESC")
else:
    marks = dst("SELECT dest_table, watermark FROM _apitap_state FINAL "
                "WHERE dest_table LIKE 'clg_%' ORDER BY dest_table")
seen = {}
for row in marks:
    seen.setdefault(row[0], row[1])
print(f"   watermarks: {seen}")
ok &= len(set(seen.values())) == 1 and len(seen) == len(TABLES)

print("== cleanup ==")
for t in TABLES:
    pg(f"DROP TABLE IF EXISTS {t} CASCADE")
    drop_dst(t)
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM "
   "pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")

print(f"\n   ===== GROUP CHANGELOG E2E ({DEST}): "
      + ("ALL GREEN" if ok else "FAILED") + " =====")
