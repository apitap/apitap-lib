"""A Postgres bytea lands in BigQuery as BYTES, the bytes the source holds.

0.64.0 landed a bytea as STRING holding its `\\x`-hex text, on the text lane
(the binary lane refused the column), while the docs promised BYTES. From
0.65.0 it is BYTES on both lanes: the binary lane writes the bytes, and the
text lane's `\\x`-hex is turned into the base64 BigQuery's CSV reader takes.

  1  replace on the text lane (parallel=2): BYTES, every byte exact
  2  replace on the binary lane (parallel=8): BYTES, every byte exact
  3  appends across the two lanes land, exact
  4  a target an older release made STRING keeps its hex text and takes an
     append on any pipe count (held to the text lane)
  5  log_based: the bootstrap makes BYTES; inserts, an update and a delete
     through the WAL land exact

The bytes are hostile: NUL, 0xFF, a backslash, `\\x` spelled inside the
value, invalid UTF-8, the empty value, and NULL.

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
T, TC = "bq_bytes", "bq_bytes_cdc"

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
           f"WHERE table_name = '{t}' AND column_name = 'b'")
    return r[0][0] if r else None


def src_rows(t):
    return pg(f"SELECT string_agg(id || ':' || COALESCE(encode(b, 'hex'), 'N'), ',' ORDER BY id) FROM {t}")


def bq_rows(t):
    """id:hex of the bytes the destination holds — read as BYTES, or (a STRING
    target) decoded from its `\\x`-hex text."""
    ty = col_type(t)
    expr = "LOWER(TO_HEX(b))" if ty == "BYTES" else "LOWER(SUBSTR(b, 3))"
    r = bq(f"SELECT STRING_AGG(CONCAT(CAST(id AS STRING), ':', IFNULL({expr}, 'N')), ',' ORDER BY id) "
           f"FROM `{DS}.{t}`")
    return r[0][0] or ""


def seed(t, lo):
    pg(f"INSERT INTO {t} VALUES "
       f"({lo}, decode('00', 'hex')), ({lo + 1}, decode('ff00fe', 'hex')), "
       f"({lo + 2}, decode('5c', 'hex')), ({lo + 3}, decode('5c783030', 'hex')), "
       f"({lo + 4}, decode('c328', 'hex')), ({lo + 5}, ''::bytea), ({lo + 6}, NULL)")


def drop_bq():
    for t in (T, TC):
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


def bulk(mode, parallel):
    try:
        apitap.transfer(SRC, BQ, table=T, mode=mode, parallel=parallel,
                        **({"cursor": "id"} if mode == "append" else {}))
        return None
    except Exception as e:
        return str(e)[:300]


drop_bq()
drop_our_slots()
for t in (T, TC):
    pg(f"DROP TABLE IF EXISTS {t}")
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, b bytea)")
seed(T, 1)
try:
    print("════════ bytea -> BigQuery BYTES ════════", flush=True)
    for n, (label, par) in enumerate((("text lane", 2), ("binary lane", 8)), start=1):
        err = bulk("replace", par)
        case(f"{n}: replace on the {label} runs", err is None, err or "")
        case(f"{n}: the column is BYTES", col_type(T) == "BYTES", f"got {col_type(T)}")
        case(f"{n}: every byte is the source's", bq_rows(T) == src_rows(T),
             f"bq {bq_rows(T)}  pg {src_rows(T)}")

    seed(T, 10)
    err = bulk("append", 2)
    case("3: an append on the text lane lands", err is None, err or "")
    seed(T, 20)
    err = bulk("append", 8)
    case("3: an append on the binary lane lands", err is None, err or "")
    case("3: every byte is the source's", bq_rows(T) == src_rows(T), f"bq {bq_rows(T)}  pg {src_rows(T)}")

    # An older release's table: bytea as STRING holding `\x`-hex text.
    if col_type(T) == "BYTES":
        bq(f"CREATE OR REPLACE TABLE `{DS}.{T}` AS "
           f"SELECT id, IF(b IS NULL, NULL, CONCAT('\\\\x', LOWER(TO_HEX(b)))) AS b FROM `{DS}.{T}`")
    seed(T, 30)
    err = bulk("append", 8)
    case("4: a legacy STRING target takes an append on 8 pipes", err is None, err or "")
    case("4: and stays STRING", col_type(T) == "STRING", f"got {col_type(T)}")
    case("4: every value is the source's hex text", bq_rows(T) == src_rows(T),
         f"bq {bq_rows(T)}  pg {src_rows(T)}")

    print("════════ bytea -> BigQuery BYTES through the WAL ════════", flush=True)
    seed(TC, 1)
    code = ("import apitap\n"
            f"apitap.transfer({SRC!r}, {BQ!r}, table={TC!r}, mode='log_based')\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    case("5: the bootstrap runs", r.returncode == 0, r.stderr.strip()[-300:])
    case("5: the column is BYTES", col_type(TC) == "BYTES", f"got {col_type(TC)}")
    seed(TC, 40)
    pg(f"UPDATE {TC} SET b = decode('0001025c78ff', 'hex') WHERE id = 1")
    pg(f"DELETE FROM {TC} WHERE id = 2")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    case("5: the window applies", r.returncode == 0, r.stderr.strip()[-300:])
    case("5: every byte is the source's", bq_rows(TC) == src_rows(TC),
         f"bq {bq_rows(TC)}  pg {src_rows(TC)}")
finally:
    drop_bq()
    for t in (T, TC):
        pg(f"DROP TABLE IF EXISTS {t}")
    drop_our_slots()

print("BYTEA BYTES E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
