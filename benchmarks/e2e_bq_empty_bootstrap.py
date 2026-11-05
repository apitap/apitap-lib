"""D4: a log_based bootstrap of an EMPTY source table into BigQuery.

The bootstrap's full load is a bulk Replace, and every bulk sink has a 0-row
guard: an empty read never touches the destination. On BigQuery the only
thing that creates the target is that load's copy job, so an empty source
left NO target, while the bootstrap still stamped its watermark. The first
window with an event then failed "target ... does not exist" on every run,
forever (changelog=True failed inside the bootstrap itself). And a
re-bootstrap after the source was emptied kept the old destination rows
under a fresh watermark.

  replica    empty table -> drain -> 3 inserts -> drain: BigQuery holds the 3
  changelog  the same with changelog=True: __current holds the 3
  stale      a re-bootstrap (state cleared) from a source emptied since:
             the destination is empty afterwards, not the old rows

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
TR, TC = "bq_empty_boot", "bq_empty_boot_cl"

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


def table_exists(t):
    return bq(f"SELECT COUNT(*) FROM `{DS}`.INFORMATION_SCHEMA.TABLES WHERE table_name = '{t}'")[0][0] == "1"


def clear_state(t):
    for s in ("_apitap_state", "_apitap_cdc_pending"):
        try:
            bq(f"DELETE FROM `{DS}.{s}` WHERE dest_table = '{t}'")
        except Exception:
            pass


def drop_bq_objects():
    for t in (TR, TC):
        try:
            bq(f"DROP VIEW IF EXISTS `{DS}.{t}__current`")
        except Exception:
            pass
        for x in (t, f"{t}__apitap_cl", f"{t}__apitap_cdc"):
            try:
                bq(f"DROP TABLE IF EXISTS `{DS}.{x}`")
            except Exception:
                pass
        clear_state(t)


SLOTS_BEFORE = set(pg("SELECT slot_name FROM pg_replication_slots").split())


def drop_our_slots():
    for s in sorted(set(pg("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
           f"WHERE slot_name = '{s}' AND NOT active")
        pg(f'DROP PUBLICATION IF EXISTS "{s}_pub"')


def drain(t, changelog=False):
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {BQ!r}, table={t!r}, mode='log_based', "
            f"changelog={changelog!r})\n"
            "print('ROWS', r.rows, flush=True)\n")
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)


def ids(rel):
    return ",".join(r[0] for r in bq(f"SELECT CAST(id AS STRING) FROM `{DS}.{rel}` ORDER BY id"))


def src_ids(t):
    return pg(f"SELECT COALESCE(string_agg(id::text, ',' ORDER BY id), '') FROM {t}")


drop_bq_objects()
drop_our_slots()
try:
    for t, changelog in ((TR, False), (TC, True)):
        label = "changelog" if changelog else "replica"
        print(f"════════ D4 {label}: bootstrap from an empty table ════════", flush=True)
        pg(f"DROP TABLE IF EXISTS {t}")
        pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")
        r = drain(t, changelog)
        case(f"{label}: the bootstrap of an empty table succeeds", r.returncode == 0,
             r.stderr.strip()[-500:])
        case(f"{label}: the BigQuery target exists after it", table_exists(t))
        pg(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        r = drain(t, changelog)
        case(f"{label}: the first window applies", r.returncode == 0, r.stderr.strip()[-500:])
        if r.returncode == 0:
            rel = f"{t}__current" if changelog else t
            want, got = src_ids(t), ids(rel)
            case(f"{label}: BigQuery holds the source's rows", want == got, f"pg {want!r} bq {got!r}")

    print("════════ D4 stale: a re-bootstrap from a source emptied since ════════", flush=True)
    if table_exists(TR):
        before = ids(TR)
        clear_state(TR)
        drop_our_slots()
        pg(f"TRUNCATE {TR}")
        r = drain(TR)
        case("stale: the re-bootstrap succeeds", r.returncode == 0, r.stderr.strip()[-500:])
        after = ids(TR) if table_exists(TR) else "<no table>"
        case("stale: the destination is empty, not the old rows", after == "",
             f"before {before!r} after {after!r}")
    else:
        case("stale: needs the replica target from the first case", False)
finally:
    drop_bq_objects()
    for t in (TR, TC):
        pg(f"DROP TABLE IF EXISTS {t}")
    drop_our_slots()

print("D4 EMPTY BOOTSTRAP E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
sys.exit(0 if ok else 1)
