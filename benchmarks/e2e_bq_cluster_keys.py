"""R22: the CDC bootstrap clusters a BigQuery target on its key, whatever type it is.

BigQuery clusters only on STRING, INT64, NUMERIC, BIGNUMERIC, DATE, DATETIME,
TIMESTAMP, BOOL and GEOGRAPHY. A float8 key lands FLOAT64, and so does an
unconstrained NUMERIC before 0.65.0 (it lands BIGNUMERIC since). 0.64.0 clustered
on the key regardless: a changelog=True bootstrap failed at any size (its
rebuild defaults CLUSTER BY to the key), a replica at 1M rows and up, and
every rerun repeated the full load and failed the same way.

  cl        changelog=True on a float8 key: bootstraps, applies a window
  order_by  changelog=True with order_by naming the float column: refused,
            naming the column and its type
  replica   1M rows keyed (float8, int): bootstraps, applies a window,
            clustered on `id` alone

ENV: BQ_SA (service-account JSON path), BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import sys

import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DS = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = f"bigquery://{PROJECT}/{DS}?credentials={os.environ['BQ_SA']}"
TC, TO, TR = "bq_clkey_cl", "bq_clkey_ob", "bq_clkey_rep"
ROWS = int(os.environ.get("ROWS", "1000000"))

_creds = service_account.Credentials.from_service_account_file(
    os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def bq(sql):
    _creds.refresh(gt.Request())
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
        headers={"Authorization": f"Bearer {_creds.token}"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 120000, "location": "US"})
    j = r.json()
    if r.status_code >= 400 or j.get("jobComplete") is False:
        raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
    return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]


def drain(t, **kw):
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {BQ!r}, table={t!r}, mode='log_based', **{kw!r})\n"
            "print('ROWS', r.rows, flush=True)\n")
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)


def drop_bq():
    for t in (TC, TO, TR):
        try:
            bq(f"DROP VIEW IF EXISTS `{DS}.{t}__current`")
        except Exception:
            pass
        for x in (t, f"{t}__apitap_cl", f"{t}__apitap_cdc"):
            try:
                bq(f"DROP TABLE IF EXISTS `{DS}.{x}`")
            except Exception:
                pass
        for st in ("_apitap_state", "_apitap_cdc_pending"):
            try:
                bq(f"DELETE FROM `{DS}.{st}` WHERE dest_table = '{t}'")
            except Exception:
                pass


SLOTS_BEFORE = set(pg("SELECT slot_name FROM pg_replication_slots").split())


def drop_our_slots():
    for s in sorted(set(pg("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
           f"WHERE slot_name = '{s}' AND NOT active")
        pg(f'DROP PUBLICATION IF EXISTS "{s}_pub"')


def clustering(t):
    r = bq(f"SELECT STRING_AGG(column_name, ',' ORDER BY clustering_ordinal_position) "
           f"FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{t}' "
           f"AND clustering_ordinal_position IS NOT NULL")
    return r[0][0] if r and r[0][0] else ""


drop_bq()
drop_our_slots()
try:
    print("════════ R22 cl: changelog=True on a float8 key ════════", flush=True)
    pg(f"DROP TABLE IF EXISTS {TC}")
    pg(f"CREATE TABLE {TC} (k float8 PRIMARY KEY, v text)")
    pg(f"INSERT INTO {TC} VALUES (1.5, 'a'), (2.25, 'b'), (-3.0, 'c')")
    r = drain(TC, changelog=True)
    case("cl: the bootstrap succeeds", r.returncode == 0, r.stderr.strip()[-400:])
    if r.returncode == 0:
        pg(f"UPDATE {TC} SET v = 'b2' WHERE k = 2.25")
        pg(f"INSERT INTO {TC} VALUES (9.75, 'd')")
        r = drain(TC, changelog=True)
        case("cl: a window applies", r.returncode == 0, r.stderr.strip()[-400:])
        got = ",".join(x[0] for x in bq(f"SELECT CONCAT(CAST(k AS STRING), ':', v) FROM `{DS}.{TC}__current` ORDER BY k"))
        want = pg(f"SELECT string_agg(k::text || ':' || v, ',' ORDER BY k) FROM {TC}")
        case("cl: __current equals the source", got == want, f"bq {got!r} pg {want!r}")

    print("════════ R22 order_by: an explicit float clustering column ════════", flush=True)
    pg(f"DROP TABLE IF EXISTS {TO}")
    pg(f"CREATE TABLE {TO} (id int PRIMARY KEY, f float8, v text)")
    pg(f"INSERT INTO {TO} VALUES (1, 0.5, 'a')")
    r = drain(TO, changelog=True, order_by="f")
    err = r.stderr.strip()
    case("order_by: refused, naming the column and its type",
         r.returncode != 0 and "'f' is FLOAT64" in err, err[-400:])
    drop_our_slots()

    print(f"════════ R22 replica: {ROWS} rows keyed (float8, int) ════════", flush=True)
    pg(f"DROP TABLE IF EXISTS {TR}")
    pg(f"CREATE TABLE {TR} (k float8, id int, v text, PRIMARY KEY (k, id))")
    pg(f"INSERT INTO {TR} SELECT g / 7.0, g, 'v' || g FROM generate_series(1, {ROWS}) g")
    r = drain(TR)
    case("replica: the bootstrap succeeds", r.returncode == 0, r.stderr.strip()[-400:])
    if r.returncode == 0:
        case("replica: clustered on the clusterable key column alone", clustering(TR) == "id",
             f"clustering {clustering(TR)!r}")
        pg(f"UPDATE {TR} SET v = 'changed' WHERE id = 7")
        r = drain(TR)
        case("replica: a window applies", r.returncode == 0, r.stderr.strip()[-400:])
        got = bq(f"SELECT v FROM `{DS}.{TR}` WHERE id = 7")
        case("replica: the update landed", got == [["changed"]], f"{got}")
finally:
    drop_bq()
    for t in (TC, TO, TR):
        pg(f"DROP TABLE IF EXISTS {t}")
    drop_our_slots()

print("R22 CLUSTER KEYS E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
