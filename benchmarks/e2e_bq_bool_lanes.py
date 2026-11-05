"""N19: a Postgres boolean's BigQuery type must not depend on the lane.

The bulk lane into BigQuery flips with the pipe count: up to 3 pipes it is
the text (TSV -> CSV) lane, which delivers a boolean as 0/1, and from 4 it is
the binary (parquet) lane, which delivers a boolean. 0.64.0 typed the column
from the delivery, so the same table was INT64 on a small worker and BOOL on
a big one: an append across the two failed on the copy job, and a replace
silently retyped the column.

  1  replace on the text lane makes the column BOOL
  2  an append on the binary lane then lands, and the values are right
  3  a target an older release made INT64 keeps working on the text lane
  4  ...and the binary lane refuses it with the remedy, before any load

ENV: BQ_SA (service-account JSON path), BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import sys

import apitap
import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DS = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = f"bigquery://{PROJECT}/{DS}?credentials={os.environ['BQ_SA']}"
T = "bq_bool_lanes"

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


def col_type():
    r = bq(f"SELECT data_type FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS "
           f"WHERE table_name = '{T}' AND column_name = 'flag'")
    return r[0][0] if r else None


def run(mode, parallel):
    return apitap.transfer(SRC, BQ, table=T, mode=mode, parallel=parallel,
                           **({"cursor": "id"} if mode == "append" else {}))


def flags():
    """id:flag as the destination holds it, spelled as booleans either way."""
    return ",".join(f"{r[0]}:{r[1]}" for r in bq(
        f"SELECT CAST(id AS STRING), IF(CAST(flag AS INT64) = 1, 'true', 'false') "
        f"FROM `{DS}.{T}` ORDER BY id"))


def src_flags():
    return pg(f"SELECT string_agg(id || ':' || flag::text, ',' ORDER BY id) FROM "
              f"(SELECT id, CASE WHEN flag THEN 'true' ELSE 'false' END AS flag FROM {T}) s")


def drop_bq():
    try:
        bq(f"DROP TABLE IF EXISTS `{DS}.{T}`")
    except Exception:
        pass
    try:
        bq(f"DELETE FROM `{DS}._apitap_state` WHERE dest_table = '{T}'")
    except Exception:
        pass


drop_bq()
pg(f"DROP TABLE IF EXISTS {T}")
pg(f"CREATE TABLE {T} (id int PRIMARY KEY, flag bool NOT NULL, note text)")
pg(f"INSERT INTO {T} SELECT g, g % 2 = 0, 'n' || g FROM generate_series(1, 6) g")
try:
    print("════════ N19: one BigQuery type for a Postgres boolean, whatever the lane ════════")
    run("replace", 2)
    case("1: a replace on the text lane makes the column BOOL", col_type() == "BOOL", f"got {col_type()}")

    pg(f"INSERT INTO {T} SELECT g, g % 3 = 0, 'n' || g FROM generate_series(7, 12) g")
    try:
        run("append", 8)
        case("2: an append on the binary lane lands", True)
    except Exception as e:
        case("2: an append on the binary lane lands", False, str(e)[:300])
    case("2: every value is the source's", flags() == src_flags(), f"bq {flags()}  pg {src_flags()}")

    # An older release's table: the text lane made the column INT64.
    bq(f"CREATE OR REPLACE TABLE `{DS}.{T}` AS "
       f"SELECT id, CAST(flag AS INT64) AS flag, note FROM `{DS}.{T}`")
    pg(f"INSERT INTO {T} SELECT g, g % 2 = 1, 'n' || g FROM generate_series(13, 15) g")
    try:
        run("append", 2)
        case("3: a legacy INT64 target still takes a text-lane append", col_type() == "INT64",
             f"type now {col_type()}")
    except Exception as e:
        case("3: a legacy INT64 target still takes a text-lane append", False, str(e)[:300])
    case("3: every value is the source's", flags() == src_flags(), f"bq {flags()}  pg {src_flags()}")

    pg(f"INSERT INTO {T} VALUES (16, true, 'n16')")
    try:
        run("append", 8)
        case("4: the binary lane refuses a legacy INT64 boolean", False, "it ran")
    except Exception as e:
        m = str(e)
        case("4: the binary lane refuses a legacy INT64 boolean, naming the remedy",
             "mode='replace'" in m and "flag" in m, m[:300])
    case("4: and landed nothing", bq(f"SELECT COUNT(*) FROM `{DS}.{T}` WHERE id = 16")[0][0] == "0")
finally:
    drop_bq()
    pg(f"DROP TABLE IF EXISTS {T}")

print("N19 BOOL LANES E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
