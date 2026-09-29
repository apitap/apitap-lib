"""log_based CDC → BigQuery e2e (real BigQuery, project apitap).

Self-contained: creates its own pg-src table, bootstraps, drains a window that
exercises the full op mix (multi-row insert with '' and NULL, PK-changing
update, delete, unchanged-TOAST masked update, bytea update, net-delete tx),
then a TRUNCATE window — and compares a per-row digest of the BigQuery target
against Postgres ground truth after each.

Then one key, deleted twice in one window (T4, audit §3.11). The MERGE refuses a
target row that two staging rows match, and 0.56.0 staged one 'D' row per
delete the collapser had pushed — `DELETE 7; INSERT 7; DELETE 7` pushed two, so
the whole window failed "must match at most one source row" on every retry and
the watermark never moved. Asked of BigQuery: the key is gone, the table equals
the source, and the newest state row's watermark moved.

And the same key taken through the collapser's ordered tail (T4b): an UPDATE that
leaves an out-of-line value alone arrives as unchanged-TOAST and sends the key
there, and a delete in the tail folds to `Gone`. 0.56.0 staged one 'D' for the
fold's `Gone` and one more for the delete set — so a delete set without
duplicates still staged this key twice. `Resolved::rows()` yields it once.

The leg drops what it created on exit, pass or fail.

Reads BigQuery back with google-auth + REST (no bq CLI: it can't tell '' from
NULL). The SA private key is loaded inside google-auth and never printed.
"""
import atexit, os, subprocess, sys, time
import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = "bigquery://{}/{}?credentials={}".format(
    os.environ.get("BQ_PROJECT", "apitap"),
    os.environ.get("BQ_DATASET", "apitap_cdc_e2e"),
    os.environ["BQ_SA"],
)
T = "cdc_bq_demo"

import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

_creds = service_account.Credentials.from_service_account_file(
    os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])
PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DATASET = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")


def bqtoken():
    _creds.refresh(gt.Request())
    return _creds.token


def bq(sql):
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
        headers={"Authorization": f"Bearer {bqtoken()}"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 60000, "location": "US"})
    j = r.json()
    if r.status_code >= 400 or j.get("jobComplete") is False:
        raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
    return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]


def pg(sql):
    out = subprocess.run(
        ["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-Atc", sql], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    return out.stdout


# The digest both sides must agree on: id|v|big|flag(1/0)|blob, NULLs as <N>,
# ordered by id. The bulk bootstrap stored pg boolean as INT64 (1/0) and pg
# bytea as STRING in its WAL '\xHEX' text form — the CDC path must reproduce
# both exactly, so the ground truth renders bytea the same way ('\x'||hex).
def pg_digest():
    rows = pg(
        "SELECT id, COALESCE(v,'<N>'), COALESCE(big,'<N>'), "
        "COALESCE(flag::int::text,'<N>'), "
        "COALESCE('\\x'||encode(blob,'hex'),'<N>'), "
        "COALESCE(extract(epoch from ts)::bigint::text,'<N>') "
        f"FROM {T} ORDER BY id").strip("\n")
    return rows


def bq_digest():
    rows = bq(
        "SELECT STRING_AGG(line, '\\n' ORDER BY id) FROM ("
        "SELECT id, CONCAT(CAST(id AS STRING), '|', IFNULL(v,'<N>'), '|', "
        "IFNULL(big,'<N>'), '|', IFNULL(CAST(flag AS STRING),'<N>'), "
        "'|', IFNULL(blob,'<N>'), '|', IFNULL(CAST(UNIX_SECONDS(ts) AS STRING),'<N>')) AS line "
        f"FROM `{PROJECT}.{DATASET}.{T}`)")
    return (rows[0][0] if rows and rows[0] and rows[0][0] else "")


def pg_lines():
    d = pg_digest()
    return "\n".join("|".join(r.split("|")) for r in d.split("\n")) if d else ""


def check(stage):
    p, b = pg_lines(), bq_digest()
    if p == b:
        n = len([x for x in p.split("\n") if x]) if p else 0
        print(f"   ✓ {stage}: MATCH ({n} rows)")
    else:
        print(f"   ✗ {stage}: MISMATCH")
        print("   --- pg ---\n" + "\n".join("     " + l for l in p.split("\n")[:20]))
        print("   --- bq ---\n" + "\n".join("     " + l for l in b.split("\n")[:20]))
        sys.exit(1)


def drain():
    r = apitap.transfer(PG, BQ, table=T, mode="log_based")
    return r


def watermark():
    """The drain's newest state row per source for T — what every reader
    resolves. (The bootstrap's replace also leaves a source '*' barrier row
    with no watermark; it is not the drain's.)"""
    rows = bq(
        "SELECT IFNULL(STRING_AGG(CONCAT(source_id, '=', IFNULL(watermark, '<N>')), ',' "
        "ORDER BY source_id), '') FROM (SELECT source_id, watermark "
        f"FROM `{PROJECT}.{DATASET}._apitap_state` WHERE dest_table = '{T}' AND mode = 'log_based' "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY synced_at DESC) = 1)")
    return rows[0][0] if rows and rows[0] and rows[0][0] else ""


def one_key_window(stage, key, statements):
    """Run `statements` at the source, drain them as one window, and ask
    BigQuery what it holds: `key` gone, the watermark moved, every row equal."""
    before = watermark()
    for sql in statements:
        pg(sql)
    try:
        r = drain()
    except Exception as e:  # the 0.56.0 failure is a raise; say what it left
        after = watermark()
        print(f"   ✗ {stage}: the drain raised: {str(e)[-400:]}")
        print(f"     watermark {'UNMOVED' if after == before else 'moved'}: {before} -> {after}")
        sys.exit(1)
    print(f"   {stage} rows(events)={r.rows}")
    n = int(bq(f"SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{T}` WHERE id = {key}")[0][0])
    after = watermark()
    print(f"   {'✓' if n == 0 else '✗'} {stage}: COUNT(*) WHERE id={key} = {n}")
    moved = after != before and "<N>" not in after and after != ""
    print(f"   {'✓' if moved else '✗'} {stage}: watermark {before} -> {after}")
    if n != 0 or not moved:
        sys.exit(1)
    check(stage)


def cleanup():
    steps = [lambda t=t: bq(f"DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.{t}`")
             for t in (T, f"{T}__apitap_cdc")]
    steps += [
        lambda: bq(f"DELETE FROM `{PROJECT}.{DATASET}._apitap_state` WHERE dest_table='{T}'"),
        lambda: pg(f"DROP TABLE IF EXISTS {T} CASCADE"),
        lambda: pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}"),
        lambda: pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                   f"WHERE slot_name LIKE 'apitap_%{T}%' AND NOT active"),
    ]
    for step in steps:  # each on its own: one failure must not keep the rest
        try:
            step()
        except Exception as e:
            print(f"   cleanup: {str(e)[:200]}")
    print("   cleaned up")


print("== fresh start ==")
for tbl in (T,):
    pg(f"DROP TABLE IF EXISTS {tbl} CASCADE")
    pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{tbl}")
pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
   f"WHERE slot_name LIKE 'apitap_%{T}%'").strip()
for t in (T, f"{T}__apitap_cdc"):
    bq(f"DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.{t}`")
bq(f"DELETE FROM `{PROJECT}.{DATASET}._apitap_state` WHERE dest_table='{T}'"
   ) if bq(f"SELECT COUNT(*) FROM `{PROJECT}.{DATASET}`.INFORMATION_SCHEMA.TABLES "
           f"WHERE table_name='_apitap_state'")[0][0] != "0" else None

pg(f"""CREATE TABLE {T} (
  id int PRIMARY KEY, v text, big text, flag boolean, blob bytea,
  ts timestamptz)""")
# Seed: '' vs NULL, a long TOAST-able 'big', bytea, bool.
pg(f"INSERT INTO {T} VALUES "
   "(1,'a',repeat('x',4000),true,'\\xdeadbeef','2020-01-01 10:00:00+00'),"
   "(2,'',NULL,false,NULL,'2020-02-02 02:02:02+00'),"
   "(3,'c',repeat('y',5000),true,'\\x00','2020-03-03 03:03:03+00')")

atexit.register(cleanup)

print("== bootstrap ==")
r = drain()
print(f"   bootstrap rows={r.rows}")
check("bootstrap")

print("== window 1: full op mix ==")
pg(f"INSERT INTO {T} VALUES (4,'d','short',false,'\\xcafe','2021-04-04 04:04:04+00')")
pg(f"UPDATE {T} SET v='A2', flag=false WHERE id=1")          # plain update
pg(f"UPDATE {T} SET v='pkmoved' WHERE id=2")                 # will PK-change next
pg(f"UPDATE {T} SET id=99 WHERE id=2")                       # PK change 2 -> 99
pg(f"UPDATE {T} SET v='toastkept' WHERE id=3")               # unchanged-TOAST 'big'
pg(f"UPDATE {T} SET blob='\\xffff' WHERE id=3")              # bytea update
pg(f"DELETE FROM {T} WHERE id=99")                           # net: 2 gone entirely
r = drain()
print(f"   window1 rows(events)={r.rows}")
check("window1")

print("== window 2: idempotent empty drain ==")
r = drain()
print(f"   empty drain rows={r.rows}")
check("window2-empty")

print("== window 3: TRUNCATE + reload ==")
pg(f"TRUNCATE {T}")
pg(f"INSERT INTO {T} VALUES (7,'post',repeat('z',3000),true,'\\x01','2022-07-07 07:07:07+00')")
r = drain()
print(f"   truncate rows={r.rows}")
check("window3-truncate")

print("== window 4 (T4): delete / insert / delete on one key ==")
# id 7 is on both sides (window 3). The window nets to one delete of 7's
# pre-window row and nothing landed.
one_key_window("window4-delete-insert-delete", 7, [
    f"DELETE FROM {T} WHERE id=7",
    f"INSERT INTO {T} VALUES (7,'again',NULL,false,NULL,NULL)",
    f"DELETE FROM {T} WHERE id=7",
])

print("== windows 5-6 (T4b): delete / insert / masked update / delete on one key ==")
# md5 text barely compresses, so 40000 bytes of it stays out of line and an
# UPDATE that does not touch it really arrives as unchanged-TOAST.
BIG = "(SELECT string_agg(md5(g::text), '') FROM generate_series(1, 1250) g)"
pg(f"INSERT INTO {T} VALUES (8,'toasted',{BIG},true,NULL,NULL)")
r = drain()
check("window5-seed")
stored = int(pg(f"SELECT pg_column_size(big) FROM {T} WHERE id=8").strip())
if stored <= 2100:
    print(f"   ✗ rig: id=8's big is {stored} bytes, inline — the UPDATE would not be masked")
    sys.exit(2)
print(f"   ✓ rig: id=8's big is stored out of line ({stored} bytes)")
one_key_window("window6-delete-insert-masked-delete", 8, [
    f"DELETE FROM {T} WHERE id=8",
    f"INSERT INTO {T} VALUES (8,'again',{BIG},true,NULL,NULL)",
    f"UPDATE {T} SET v='masked' WHERE id=8",
    f"DELETE FROM {T} WHERE id=8",
])

print("\n   ===== BQ CDC E2E: ALL GREEN =====")
