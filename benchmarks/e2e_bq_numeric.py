"""An unconstrained Postgres numeric lands in BigQuery exact, as BIGNUMERIC.

0.64.0 typed the column from what the lane delivered — a double — so it was
FLOAT64, and the value was rounded with no error: 99999999999999999.99
landed as 1e+17 and 12345678901234567890.123456789 as 1.2345678901234567e+19.
The docs said BIGNUMERIC. From 0.65.0 the decimal text travels as it is and
BigQuery parses it into BIGNUMERIC.

  1  replace (parallel=2 and parallel=8): BIGNUMERIC, every value exact
  2  a value past BIGNUMERIC's 38 fractional digits never lands rounded:
     the load refuses it, loudly
  3  a target an older release made FLOAT64 stays FLOAT64 on an append
  4  log_based: the bootstrap makes BIGNUMERIC, an update through the WAL
     lands exact

ENV: BQ_SA (service-account JSON path), BQ_PROJECT, BQ_DATASET.
"""
import os
import subprocess
import sys
import decimal
from decimal import Decimal

# Python's default context keeps 28 digits and would round the very values
# under test; compare them whole.
decimal.getcontext().prec = 200

import apitap
import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DS = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = f"bigquery://{PROJECT}/{DS}?credentials={os.environ['BQ_SA']}"
T, TW, TC = "bq_num", "bq_num_wide", "bq_num_cdc"
VALUES = ["12345678901234567890.123456789", "99999999999999999.99", "0.1",
          "-0.000000000000000000000000000000000001", "-987654321.000000001", "0", "NULL"]

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


def col_type(t):
    r = bq(f"SELECT data_type FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS "
           f"WHERE table_name = '{t}' AND column_name = 'n'")
    return r[0][0] if r else None


def norm(v):
    return "N" if v in (None, "") else str(Decimal(v).normalize())


def src_vals(t):
    return [(int(i), norm(n or None)) for i, n in
            (line.split("|") for line in pg(f"SELECT id, n FROM {t} ORDER BY id").splitlines())]


def bq_vals(t):
    return [(int(i), norm(n)) for i, n in bq(f"SELECT id, CAST(n AS STRING) FROM `{DS}.{t}` ORDER BY id")]


def drop_bq():
    for t in (T, TW, TC):
        for x in (t, f"{t}__apitap_cdc"):
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


def run(t, mode, parallel):
    try:
        apitap.transfer(SRC, BQ, table=t, mode=mode, parallel=parallel,
                        **({"cursor": "id"} if mode == "append" else {}))
        return None
    except Exception as e:
        return str(e)[:300]


drop_bq()
drop_our_slots()
for t in (T, TW, TC):
    pg(f"DROP TABLE IF EXISTS {t}")
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, n numeric)")
pg(f"INSERT INTO {T} VALUES " + ", ".join(f"({i}, {v})" for i, v in enumerate(VALUES, 1)))
try:
    print("════════ unconstrained numeric -> BigQuery BIGNUMERIC ════════", flush=True)
    for par in (2, 8):
        err = run(T, "replace", par)
        case(f"1: replace with parallel={par} runs", err is None, err or "")
        case(f"1: the column is BIGNUMERIC (parallel={par})", col_type(T) == "BIGNUMERIC", f"got {col_type(T)}")
        case(f"1: every value is exact (parallel={par})", bq_vals(T) == src_vals(T),
             f"bq {bq_vals(T)}  pg {src_vals(T)}")

    pg(f"INSERT INTO {TW} VALUES (1, 0.1234567890123456789012345678901234567891)")
    err = run(TW, "replace", 2)
    if err is None:
        got = bq_vals(TW)
        case("2: a value past 38 fractional digits never lands rounded", got == src_vals(TW),
             f"bq {got}  pg {src_vals(TW)}")
    else:
        case("2: a value past 38 fractional digits is refused, loudly", True, err[:160])

    # An older release's table: the column FLOAT64.
    bq(f"CREATE OR REPLACE TABLE `{DS}.{T}` AS SELECT id, CAST(n AS FLOAT64) AS n FROM `{DS}.{T}`")
    pg(f"INSERT INTO {T} VALUES (100, 2.5)")
    err = run(T, "append", 8)
    case("3: a legacy FLOAT64 target takes an append", err is None, err or "")
    case("3: and stays FLOAT64", col_type(T) == "FLOAT64", f"got {col_type(T)}")
    got = bq(f"SELECT CAST(n AS STRING) FROM `{DS}.{T}` WHERE id = 100")
    case("3: the appended value landed", got == [["2.5"]], f"{got}")

    print("════════ unconstrained numeric through the WAL ════════", flush=True)
    pg(f"INSERT INTO {TC} VALUES (1, 1.5), (2, 99999999999999999.99)")
    code = f"import apitap\napitap.transfer({SRC!r}, {BQ!r}, table={TC!r}, mode='log_based')\n"
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    case("4: the bootstrap runs", r.returncode == 0, r.stderr.strip()[-300:])
    case("4: the column is BIGNUMERIC", col_type(TC) == "BIGNUMERIC", f"got {col_type(TC)}")
    pg(f"UPDATE {TC} SET n = 12345678901234567890.123456789 WHERE id = 1")
    pg(f"INSERT INTO {TC} VALUES (3, -0.000000000000000000000000000000000001)")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    case("4: the window applies", r.returncode == 0, r.stderr.strip()[-300:])
    case("4: every value is exact", bq_vals(TC) == src_vals(TC), f"bq {bq_vals(TC)}  pg {src_vals(TC)}")
finally:
    drop_bq()
    for t in (T, TW, TC):
        pg(f"DROP TABLE IF EXISTS {t}")
    drop_our_slots()

print("NUMERIC BIGNUMERIC E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
