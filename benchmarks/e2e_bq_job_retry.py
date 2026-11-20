"""A BigQuery bulk job that finishes with a transient error runs again as a new job.

0.67.2 polled each bulk job (a file's load job, the copy of the stagings into
the final table) once: one `backendError` — BigQuery's own text says
"Retrying may solve the problem" — failed the whole replace (gate 2026-11-19,
e2e_guard_matrix bq, the control replace). 0.68.0 runs such a job again under
a fresh job id with load_rows' backoff (5 retries), keeps the job id on a lost
answer, and never retries a non-transient error.

The failure is injected with the test lever APITAP_TEST_BQ_FAIL=<load|copy>:
<reason>:<n>: the next n jobs of that kind finish as if BigQuery had answered
DONE with that errorResult, without being started (nothing committed, exactly
as a real failed job).

  0  control: a replace with no lever lands exact
  1  copy:backendError:1  -> the replace lands exact, one retry as a new job
  2  load:backendError:1  -> the replace lands exact, one retry as a new job
  3  copy:invalid:1       -> refused by name, no retry, the target untouched
  4  load:invalid:1       -> refused by name, no retry, the target untouched
  5  copy:backendError:6  -> bounded: 5 retries, then refused by name
  6  a malformed lever is refused by name before the run starts
  every case: no staging table of the leg left behind

RED (0.67.2 / p18): the lever does not exist there, so 1-2 show no retry and
3-5 land instead of failing. ENV: BQ_SA, BQ_PROJECT, BQ_DATASET.
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
T = "bq_jr"

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


def src_fp():
    return pg(f"SELECT count(*) || ',' || sum(id) || ',' || count(DISTINCT id) || ',' || "
              f"md5(string_agg(s, '|' ORDER BY id)) FROM {T}")


def bq_fp():
    r = bq(f"SELECT CONCAT(CAST(COUNT(*) AS STRING), ',', CAST(SUM(id) AS STRING), ',', "
           f"CAST(COUNT(DISTINCT id) AS STRING), ',', TO_HEX(MD5(STRING_AGG(s, '|' ORDER BY id)))) "
           f"FROM `{DS}.{T}`")
    return r[0][0]


def leftovers():
    r = bq(f"SELECT table_name FROM `{DS}`.INFORMATION_SCHEMA.TABLES "
           f"WHERE STARTS_WITH(table_name, '{T}') AND table_name != '{T}'")
    return [x[0] for x in r]


def replace(lever, parallel):
    env = dict(os.environ)
    env.pop("APITAP_TEST_BQ_FAIL", None)
    env["APITAP_PROGRESS"] = "plain"
    if lever is not None:
        env["APITAP_TEST_BQ_FAIL"] = lever
    code = (f"import apitap\n"
            f"apitap.transfer({SRC!r}, {BQ!r}, table={T!r}, mode='replace', parallel={parallel})\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    return r.returncode, r.stderr


def grow(n):
    pg(f"INSERT INTO {T} SELECT g, 'row ' || g || repeat('x', g % 17) FROM generate_series("
       f"(SELECT coalesce(max(id), 0) + 1 FROM {T}), {n}) g")


def drop_bq():
    for x in [T] + leftovers():
        try:
            bq(f"DROP TABLE IF EXISTS `{DS}.{x}`")
        except Exception:
            pass
    try:
        bq(f"DELETE FROM `{DS}._apitap_state` WHERE dest_table = '{T}'")
    except Exception:
        pass


drop_bq()
pg(f"DROP TABLE IF EXISTS {T}")
pg(f"CREATE TABLE {T} (id int PRIMARY KEY, s text)")
grow(1000)
try:
    print("════════ BigQuery bulk jobs: a transient failure runs again ════════", flush=True)
    rc, err = replace(None, 2)
    case("0: control replace lands", rc == 0, err.strip()[-300:])
    case("0: exact", bq_fp() == src_fp(), f"bq {bq_fp()} pg {src_fp()}")

    for n, (lever, par) in enumerate([("copy:backendError:1", 2), ("load:backendError:1", 2)], 1):
        grow(1000 + 500 * n)
        rc, err = replace(lever, par)
        case(f"{n}: {lever}: the replace lands", rc == 0, err.strip()[-300:])
        case(f"{n}: exact (no row lost or doubled)", bq_fp() == src_fp(), f"bq {bq_fp()} pg {src_fp()}")
        case(f"{n}: one retry, as a new job", err.count("as a new job") == 1 and "retry 1/5" in err,
             f"{err.count('as a new job')} retry note(s)")
        case(f"{n}: no staging left", leftovers() == [], f"{leftovers()}")

    landed = bq_fp()
    grow(3000)
    for n, (lever, par, retries) in enumerate(
            [("copy:invalid:1", 2, 0), ("load:invalid:1", 2, 0), ("copy:backendError:6", 2, 5)], 3):
        reason = lever.split(":")[1]
        rc, err = replace(lever, par)
        case(f"{n}: {lever}: the replace fails", rc != 0, err.strip()[-200:])
        case(f"{n}: by name", reason in err and "injected by APITAP_TEST_BQ_FAIL" in err, err.strip()[-200:])
        case(f"{n}: {retries} retr{'y' if retries == 1 else 'ies'}", err.count("as a new job") == retries,
             f"{err.count('as a new job')} retry note(s)")
        case(f"{n}: the target is untouched", bq_fp() == landed, f"bq {bq_fp()} before {landed}")
        case(f"{n}: no staging left", leftovers() == [], f"{leftovers()}")

    rc, err = replace("copy:backendError", 2)
    case("6: a malformed lever is refused", rc != 0 and "APITAP_TEST_BQ_FAIL" in err, err.strip()[-200:])
    case("6: before the run (the target untouched)", bq_fp() == landed, f"bq {bq_fp()} before {landed}")
finally:
    drop_bq()
    pg(f"DROP TABLE IF EXISTS {T}")

print("BQ BULK JOB RETRY E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
