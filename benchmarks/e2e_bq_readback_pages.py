"""D3: a changelog window's masked readback is bigger than one BigQuery page.

Postgres does not re-send an out-of-line (TOASTed) value that an UPDATE did
not touch, so a changelog=True window rebuilds each such cell from the
destination: one SELECT per chunk of keys, chunked by SQL length (about 4,000
keys). BigQuery answers jobs.query with at most ~10 MB per response; past
that it returns jobComplete=true, a pageToken and SOME of the rows. 0.64.0
kept the first page only, so every key past it looked like "the destination
never had this row" and the window failed "torn" on every run.

The leg makes that window: 2,000 rows, each with its own 8 KiB body stored
out of line, then one UPDATE of another column on every row. The readback is
~16 MB, more than one page. Asserted, of BigQuery: the drain succeeds, every
record the window appended carries its body, and `__current` equals the
source row for row (a hash over id + md5(body), so a body read at the wrong
key or a short one shows).

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
T = "bq_readback_pages"
N = int(os.environ.get("ROWS", "2000"))
BODY = 8192  # bytes per row, out of line: N * BODY is well past one 10 MB page

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
    """Small answers only (aggregates): one page is the whole answer here."""
    _creds.refresh(gt.Request())
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
        headers={"Authorization": f"Bearer {_creds.token}"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 120000, "location": "US"})
    j = r.json()
    if r.status_code >= 400 or j.get("jobComplete") is False:
        raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
    return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]


def drop_bq_objects():
    try:
        bq(f"DROP VIEW IF EXISTS `{DS}.{T}__current`")
    except Exception:
        pass
    for t in (T, f"{T}__apitap_cl", f"{T}__apitap_cdc"):
        try:
            bq(f"DROP TABLE IF EXISTS `{DS}.{t}`")
        except Exception:
            pass
    for t in ("_apitap_state", "_apitap_cdc_pending"):
        try:
            bq(f"DELETE FROM `{DS}.{t}` WHERE dest_table = '{T}'")
        except Exception:
            pass


SLOTS_BEFORE = set(pg("SELECT slot_name FROM pg_replication_slots").split())


def drop_our_slots():
    for s in sorted(set(pg("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
           f"WHERE slot_name = '{s}' AND NOT active")
        pg(f'DROP PUBLICATION IF EXISTS "{s}_pub"')


def drain():
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {BQ!r}, table={T!r}, mode='log_based', changelog=True)\n"
            "print('ROWS', r.rows, flush=True)\n")
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)


def source_digest():
    return pg(f"SELECT count(*) || '/' || md5(string_agg(id || ':' || md5(body), ',' ORDER BY id)) FROM {T}")


def current_digest():
    r = bq(f"SELECT CONCAT(CAST(COUNT(*) AS STRING), '/', TO_HEX(MD5(STRING_AGG("
           f"CONCAT(CAST(id AS STRING), ':', TO_HEX(MD5(IFNULL(body, '')))), ',' ORDER BY id)))) "
           f"FROM `{DS}.{T}__current`")
    return r[0][0]


print(f"════════ D3: a masked readback past one BigQuery page ({N} x {BODY} B) ════════", flush=True)
drop_bq_objects()
drop_our_slots()
pg(f"DROP TABLE IF EXISTS {T}")
pg(f"CREATE TABLE {T} (id int PRIMARY KEY, title text, body text)")
pg(f"ALTER TABLE {T} ALTER COLUMN body SET STORAGE EXTERNAL")
# A different body per row, so a body carried from the wrong key shows.
pg(f"INSERT INTO {T} SELECT g, 'title' || g, repeat(md5(g::text), {BODY // 32}) "
   f"FROM generate_series(1, {N}) g")
try:
    stored = pg(f"SELECT pg_column_size(body) FROM {T} WHERE id = 1")
    case("the body is stored out of line, uncompressed", int(stored) >= BODY, f"{stored} bytes")

    r = drain()
    if r.returncode:
        case("bootstrap", False, r.stderr.strip()[-600:])
        sys.exit(1)
    case("bootstrap", True)

    pg(f"UPDATE {T} SET title = title || '!'")  # body untouched: every one arrives masked
    r = drain()
    case("the drain over a multi-page readback succeeds", r.returncode == 0, r.stderr.strip()[-600:])
    if r.returncode == 0:
        short = bq(f"SELECT COUNT(*) FROM `{DS}.{T}` WHERE _apitap_op = 'U' "
                   f"AND LENGTH(IFNULL(body, '')) <> {BODY}")[0][0]
        case(f"every one of the {N} U records carries its whole body", short == "0", f"{short} short")
        want, got = source_digest(), current_digest()
        case("__current equals the source, row for row", want == got, f"pg {want}  bq {got}")
finally:
    drop_bq_objects()
    pg(f"DROP TABLE IF EXISTS {T}")
    drop_our_slots()

print("D3 READBACK PAGES E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
