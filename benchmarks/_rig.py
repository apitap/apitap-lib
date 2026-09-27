"""What the guard, lease and fence legs ask the servers, in one place.

Every leg that proves a claim about locks, markers or leases used to carry its
own copy of "list the locks" and "count the live leases", and the copies
drifted: one counted a released ClickHouse lease as live for a day, another
listed locks in `public` only while the drain wrote them into the table's own
schema. These helpers are the one spelling of each question, per engine, and
each asks the SERVER — the catalog, the lease table, the dataset listing — never
the exit code of the run being judged.

Names follow the engine's catalog exactly. A run token is 16 characters with
its leading `_`, sits immediately before the artifact suffix, and carries its
kind in position 8: `r` replace, `a` append/merge, `l` a CDC drain. A drain's
empty `__apitap_staging` is its MARKER (announced for readers that scan staging
only); a bulk run's staging holds data and is not a marker.

Legs own their drains as `Popen` children, so `pause`/`resume` are plain
SIGSTOP/SIGCONT — a stopped process is the honest model of a drain partitioned
from its destination, and it needs no `docker pause` of anything shared.
"""
import os
import signal
import subprocess
import sys
import time

LOCK = "__apitap_lock"
STAGING = "__apitap_staging"
TOKEN_LEN = 16
CDC_LETTER = "l"

PG_DST = ("apitap-bench-pg-dst", "apitap_bench_dst")
PG_SRC = ("apitap-bench-pg-src", "apitap_bench_src")
MY_DST = ("apitap-bench-my", "bench")
CH_DST = "apitap-bench-ch"


# ── processes ─────────────────────────────────────────────────────────────

def pause(p):
    os.kill(p.pid, signal.SIGSTOP)


def resume(p):
    os.kill(p.pid, signal.SIGCONT)


def wait_for(pred, secs, step=0.2):
    """True as soon as `pred()` holds, False once `secs` pass without it."""
    end = time.monotonic() + secs
    while True:
        if pred():
            return True
        if time.monotonic() >= end:
            return False
        time.sleep(step)


def rig_fail(msg):
    """A leg that could not set up the situation it asserts about has proven
    nothing, so it fails — loudly, and labelled so nobody reads it as the
    engine's fault. `(rig)` failures are FAIL in the gate, never SKIP."""
    print(f"(rig) {msg} — FAILED", flush=True)
    sys.exit(1)


# ── names ────────────────────────────────────────────────────────────────

def token_of(name):
    """The run token of an artifact name: the 16 characters before its suffix."""
    for suffix in (LOCK, STAGING):
        if name.endswith(suffix):
            return name[: -len(suffix)][-TOKEN_LEN:]
    raise ValueError(f"{name!r} is not a lock or staging artifact")


def is_cdc_token(tok):
    return len(tok) == TOKEN_LEN and tok[0] == "_" and tok[8] == CDC_LETTER


def _mine(names, table, suffix):
    """Artifacts of exactly `table`: the name is table + token + suffix, so a
    sibling table sharing the prefix (`orders_items` beside `orders`) never
    counts — the anchored-at-one-end bug the naming module was built to kill."""
    out = []
    for n in names:
        if n.endswith(suffix) and len(n) == len(table) + TOKEN_LEN + len(suffix) \
                and n.startswith(table):
            out.append(n)
    return sorted(out)


def _markers(names, table):
    return [n for n in _mine(names, table, STAGING) if is_cdc_token(token_of(n))]


# ── Postgres ─────────────────────────────────────────────────────────────

def psql(sql, where=PG_DST):
    container, db = where
    o = subprocess.run(["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", db,
                        "-v", "ON_ERROR_STOP=1", "-Atc", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-600:])
    return o.stdout.strip()


def _pg_names(table, where):
    """Every relation named like an artifact of `table`, in ANY schema — a drain
    under a non-`public` search_path puts its lock beside the table it guards."""
    return [n for n in psql(
        "SELECT c.relname FROM pg_class c WHERE c.relkind IN ('r','p') "
        f"AND c.relname LIKE '{table}%'", where).splitlines() if n]


def locks_pg(table, where=PG_DST):
    return _mine(_pg_names(table, where), table, LOCK)


def markers_pg(table, where=PG_DST):
    return _markers(_pg_names(table, where), table)


def live_leases_pg(table, where=PG_DST):
    if psql("SELECT to_regclass('_apitap_lease') IS NULL", where) == "t":
        return []
    return [r for r in psql(
        "SELECT token FROM _apitap_lease WHERE dest_key LIKE '%.' || "
        f"'{table}' AND NOT collected AND expires_at > now()", where).splitlines() if r]


# ── MySQL ────────────────────────────────────────────────────────────────

def mysql(sql, where=MY_DST):
    container, db = where
    o = subprocess.run(["docker", "exec", "-i", container, "mysql", "-uroot", "-pbench", "-N", "-B",
                        db, "-e", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-600:])
    return o.stdout.strip()


def _my_names(table, where):
    return [n for n in mysql(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() "
        f"AND table_name LIKE '{table}%'", where).splitlines() if n]


def locks_my(table, where=MY_DST):
    return _mine(_my_names(table, where), table, LOCK)


def markers_my(table, where=MY_DST):
    return _markers(_my_names(table, where), table)


def live_leases_my(table, where=MY_DST):
    if mysql("SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
             "AND table_name = '_apitap_lease'", where) == "0":
        return []
    return [r for r in mysql(
        f"SELECT token FROM _apitap_lease WHERE dest_key LIKE CONCAT('%.', '{table}') "
        "AND collected = 0 AND expires_at > UTC_TIMESTAMP(6)", where).splitlines() if r]


# ── ClickHouse ───────────────────────────────────────────────────────────

def clickhouse(sql, container=CH_DST):
    o = subprocess.run(["docker", "exec", "-i", container, "clickhouse-client", "--user", "default",
                        "--password", "bench", "-q", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-600:])
    return o.stdout.strip()


def _ch_names(table, container):
    return [n for n in clickhouse(
        "SELECT name FROM system.tables WHERE database = currentDatabase() "
        f"AND startsWith(name, '{table}')", container).splitlines() if n]


def locks_ch(table, container=CH_DST):
    return _mine(_ch_names(table, container), table, LOCK)


def markers_ch(table, container=CH_DST):
    return _markers(_ch_names(table, container), table)


def live_leases_ch(table, container=CH_DST):
    """LIVE leases only. The ClickHouse lease table is append-only: a released
    or collected lease is a newer row, not a missing one, so the question is
    the newest row per token, and a token needs at least one row to count."""
    if clickhouse("SELECT count() FROM system.tables WHERE database = currentDatabase() "
                  "AND name = '_apitap_lease'", container) == "0":
        return []
    return [r for r in clickhouse(
        "SELECT token FROM (SELECT token, count() AS n, argMax(collected, seq) AS c, "
        "                          argMax(expires_at, seq) AS e "
        f"                   FROM `_apitap_lease` WHERE dest_key LIKE '%.{table}' "
        "                    GROUP BY token) "
        "WHERE n > 0 AND c = 0 AND e > now64(6)", container).splitlines() if r]


# ── BigQuery ─────────────────────────────────────────────────────────────

BQ_PROJECT = os.environ.get("BQ_PROJECT", "apitap")
BQ_DATASET = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
_bq_creds = None


def _bq_token():
    global _bq_creds
    import google.auth.transport.requests as gt
    from google.oauth2 import service_account
    if _bq_creds is None:
        _bq_creds = service_account.Credentials.from_service_account_file(
            os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])
    _bq_creds.refresh(gt.Request())
    return _bq_creds.token


def bq_url():
    return f"bigquery://{BQ_PROJECT}/{BQ_DATASET}?credentials={os.environ['BQ_SA']}"


def bq(sql):
    """Rows of one query, as lists of strings (BigQuery REST's `v` values).
    Job questions go to INFORMATION_SCHEMA.JOBS_BY_USER: listing every job in
    the project is not granted to the gate's service account."""
    import requests
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/queries",
        headers={"Authorization": f"Bearer {_bq_token()}"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 60000, "location": "US"})
    j = r.json()
    if r.status_code >= 400 or j.get("jobComplete") is False:
        raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
    return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]


def bq_tables():
    """The dataset listing (`tables.list`) — what a guard scan sees, including
    tables a query cannot read yet."""
    import requests
    names, page = [], None
    while True:
        params = {"maxResults": 1000, **({"pageToken": page} if page else {})}
        r = requests.get(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{BQ_PROJECT}/datasets/{BQ_DATASET}/tables",
            headers={"Authorization": f"Bearer {_bq_token()}"}, params=params)
        j = r.json()
        if r.status_code >= 400:
            raise RuntimeError(f"BQ tables.list failed: {j.get('error', j)}")
        names += [t["tableReference"]["tableId"] for t in j.get("tables", [])]
        page = j.get("nextPageToken")
        if not page:
            return names


def locks_bq(table):
    return _mine(bq_tables(), table, LOCK)


def markers_bq(table):
    return _markers(bq_tables(), table)


def live_leases_bq(table):
    if "_apitap_lease" not in bq_tables():
        return []
    return [r[0] for r in bq(
        f"SELECT token FROM `{BQ_PROJECT}.{BQ_DATASET}._apitap_lease` "
        f"WHERE ENDS_WITH(dest_key, '.{table}') AND NOT collected "
        "AND expires_at > CURRENT_TIMESTAMP()")]
