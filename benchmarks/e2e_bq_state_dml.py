"""D19: bulk state rows into BigQuery must not spend `_apitap_state`'s load-job quota.

Every table of a dataset writes its state into the one `_apitap_state`
table. 0.64.0 wrote each table's row with its own load job, and BigQuery
allows 1,500 table modifications per table per day (load and copy jobs
count; DML does not) and five metadata updates per ten seconds (DML counts
but is not held to it). 70 tables synced hourly, or 6 every five minutes,
ran out by the evening, and a wide run finishing at once exhausted the
retries — after the copy job had committed the data.

The run: 12 tables appended in one multi-table run, twice. Asked of
BigQuery (INFORMATION_SCHEMA.JOBS_BY_USER): no LOAD job wrote
`_apitap_state` during either run; every table has its state row; the
second run carried only the rows added since the first.

ENV: BQ_SA (service-account JSON path), BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import sys
import time

import apitap
import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DS = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = f"bigquery://{PROJECT}/{DS}?credentials={os.environ['BQ_SA']}"
N = 12
TABLES = [f"bq_state_dml_{i}" for i in range(N)]

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


def server_now():
    return bq("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', CURRENT_TIMESTAMP())")[0][0]


def state_loads(since):
    """LOAD jobs into _apitap_state since `since` (JOBS_BY_USER shows jobs
    a few seconds late, so it is asked until it settles)."""
    q = ("SELECT COUNT(*) FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER "
         f"WHERE creation_time > TIMESTAMP '{since}' AND job_type = 'LOAD' "
         f"AND destination_table.dataset_id = '{DS}' "
         "AND destination_table.table_id = '_apitap_state'")
    last = None
    for _ in range(8):
        n = int(bq(q)[0][0])
        if n == last:
            return n
        last = n
        time.sleep(4)
    return last


def run():
    return apitap.transfer(SRC, BQ, tables=TABLES, mode="append", cursor="id")


def drop_bq():
    """The tables and every artifact a run of them left (a staging, a lock
    announcement): a killed run's announcement would refuse the next one."""
    for (name,) in bq(f"SELECT table_name FROM `{DS}`.INFORMATION_SCHEMA.TABLES "
                      f"WHERE STARTS_WITH(table_name, 'bq_state_dml_')"):
        try:
            bq(f"DROP TABLE IF EXISTS `{DS}.{name}`")
        except Exception:
            pass
    try:
        names = ", ".join(f"'{t}'" for t in TABLES)
        bq(f"DELETE FROM `{DS}._apitap_state` WHERE dest_table IN ({names})")
    except Exception:
        pass


drop_bq()
for t in TABLES:
    pg(f"DROP TABLE IF EXISTS {t}")
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {t} SELECT g, 'v' || g FROM generate_series(1, 50) g")
try:
    print(f"════════ D19: {N} tables' state rows, without load jobs on _apitap_state ════════", flush=True)
    t0 = server_now()
    r1 = run()
    case(f"run 1 landed all {N * 50} rows", r1.rows == N * 50, f"rows={r1.rows}")
    loads = state_loads(t0)
    case("run 1 wrote _apitap_state with no load job", loads == 0, f"{loads} load job(s)")

    names = ", ".join(f"'{t}'" for t in TABLES)
    have = bq(f"SELECT COUNT(DISTINCT dest_table) FROM `{DS}._apitap_state` "
              f"WHERE dest_table IN ({names}) AND mode = 'append' AND watermark = '50'")[0][0]
    case(f"every table has its state row at watermark 50", have == str(N), f"{have} of {N}")

    for t in TABLES:
        pg(f"INSERT INTO {t} SELECT g, 'v' || g FROM generate_series(51, 55) g")
    t1 = server_now()
    r2 = run()
    case(f"run 2 carried only the {N * 5} new rows", r2.rows == N * 5, f"rows={r2.rows}")
    loads = state_loads(t1)
    case("run 2 wrote _apitap_state with no load job", loads == 0, f"{loads} load job(s)")
    dupes = bq(" UNION ALL ".join(
        f"SELECT COUNT(*) - COUNT(DISTINCT id) FROM `{DS}.{t}`" for t in TABLES[:3]))
    case("no row landed twice", all(d[0] == "0" for d in dupes), f"{dupes}")
finally:
    drop_bq()
    for t in TABLES:
        pg(f"DROP TABLE IF EXISTS {t}")

print("D19 STATE DML E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
