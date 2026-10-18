"""Postgres CDC hardening: wide keys, open streams, a locked refetch.

Three P2 findings (docs/review/2026-10-18-p2-triage.md), PG16 :5544 -> :5545 / Iceberg:
  toastpk    a text key stored out of line and left unchanged arrives as
             unchanged-TOAST in the new image; the drain refused it on every
             rerun although the old image carried the key.
  openstream a streamed transaction left open on the source held the
             one-shot until APITAP_WINDOW_MAX_SECS.
  lockwait   a refetch behind an ACCESS EXCLUSIVE lock waited for it,
             without bound.
  missingpg, missingmy
             a masked update (TOAST body untouched) of a row missing at the
             destination matched nothing and the window committed: the row
             stayed gone. Postgres and MySQL destinations now refuse it.
Usage: e2e_pg_cdc_hardening.py toastpk|openstream|lockwait|missingpg|missingmy
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
NS = "cdc_e2e"
ICE = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
       "&access_key_id=bench&secret_access_key=benchbench")


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


def psql_args(sql):
    return ["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql]


def src(sql):
    o = sh(psql_args(sql))
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ice_drop(t):
    req = urllib.request.Request(f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{t}?purgeRequested=true",
                                 method="DELETE")
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def ice_query(t, sql):
    import duckdb
    d = duckdb.connect()
    d.execute("INSTALL iceberg; LOAD iceberg;")
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{t}") as r:
        loc = json.load(r)["metadata-location"]
    return d.execute(sql.replace("{T}", f"iceberg_scan('{loc}')")).fetchall()


def wipe(t, ice=False):
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{t}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {t}")
    if ice:
        ice_drop(t)
    else:
        dst(f"DROP TABLE IF EXISTS {t}")
        dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
            f"DELETE FROM public._apitap_state WHERE dest_table IN ('{t}', 'public.{t}'); END IF; END $$")


def run(url_src, url_dst, t, env=None, timeout=300):
    code = f"import apitap\napitap.transfer({url_src!r}, {url_dst!r}, table={t!r}, mode='log_based')\n"
    t0 = time.time()
    r = sh([sys.executable, "-c", code], env=dict(os.environ, **(env or {})), timeout=timeout)
    return r, time.time() - t0


def toastpk():
    t = "pg_cdc_toastpk"
    wipe(t)
    try:
        src(f"CREATE TABLE {t} (k text PRIMARY KEY, v int); ALTER TABLE {t} ALTER k SET STORAGE EXTERNAL; "
            f"INSERT INTO {t} SELECT (SELECT string_agg(md5(g::text || i::text), '') "
            f"FROM generate_series(1, 75) i), 0 FROM generate_series(1, 2) g")
        assert src(f"SELECT min(length(k)) FROM {t}") == "2400"
        apitap.transfer(SRC, DST, table=t, mode="log_based")
        src(f"UPDATE {t} SET v = 1")                      # the wide key stays out of line, unchanged
        apitap.transfer(SRC, DST, table=t, mode="log_based")
        got = dst(f"SELECT count(*) || ':' || sum(v) || ':' || min(length(k)) FROM {t}")
        assert got == "2:2:2400", f"both rows updated under their 2400-char keys (rows:sum(v):len), got {got}"
        print(f"  UPDATE beside a 2400-char out-of-line key: {got}")
    finally:
        wipe(t)


def openstream():
    t = "pg_cdc_openstream"
    wipe(t)
    hold = None
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, v text); INSERT INTO {t} VALUES (0, 'seed')")
        apitap.transfer(SRC, DST, table=t, mode="log_based")
        # One implicit transaction: the INSERT, then 40 s open, then commit.
        hold = subprocess.Popen(psql_args(
            f"INSERT INTO {t} SELECT g, repeat('x', 200) FROM generate_series(1, 20000) g; SELECT pg_sleep(40)"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(3)
        r, took = run(SRC, DST, t, env={"APITAP_DECODE_WORKMEM": "64kB", "APITAP_WINDOW_MAX_SECS": "25"})
        assert r.returncode == 0, r.stderr[-800:]
        assert took < 20, f"an open streamed transaction must not hold the one-shot ({took:.1f}s)"
        assert dst(f"SELECT count(*) FROM {t}") == "1", "nothing committed yet beyond the seed"
        hold.wait(timeout=120)
        hold = None
        r, _ = run(SRC, DST, t, env={"APITAP_DECODE_WORKMEM": "64kB"})
        assert r.returncode == 0, r.stderr[-800:]
        got = dst(f"SELECT count(*) || ':' || count(DISTINCT id) FROM {t}")
        assert got == "20001:20001", f"after the commit, every row once: {got}"
        print(f"  one-shot beside an open streamed transaction: returned in {took:.1f}s; after commit {got}")
    finally:
        if hold:
            hold.kill()
        wipe(t)


def lockwait():
    t = "pg_cdc_lockwait"
    wipe(t, ice=True)
    hold = None
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, body text); ALTER TABLE {t} ALTER body SET STORAGE EXTERNAL; "
            f"INSERT INTO {t} SELECT g, 'a', repeat(md5(g::text), 100) FROM generate_series(1, 10) g")
        apitap.transfer(SRC, ICE, table=t, mode="log_based")
        src(f"UPDATE {t} SET v = 'b'")                    # masked: body is refetched
        hold = subprocess.Popen(psql_args(f"BEGIN; LOCK TABLE {t} IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(75); COMMIT"),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2)
        r, took = run(SRC, ICE, t, timeout=200)
        assert r.returncode != 0 and "lock timeout" in r.stderr, \
            f"a refetch behind the lock must give up (exit {r.returncode}, {took:.0f}s):\n{r.stderr[-600:]}"
        assert took < 70, f"it must give up before the lock is released ({took:.0f}s)"
        # Killing the docker-exec client leaves psql's backend (and the lock)
        # alive inside the container: end the backend itself.
        src(f"SELECT pg_terminate_backend(l.pid) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
            f"WHERE c.relname = '{t}' AND l.mode = 'AccessExclusiveLock'")
        hold.kill()
        hold = None
        time.sleep(1)
        r, _ = run(SRC, ICE, t)
        assert r.returncode == 0, r.stderr[-800:]
        got = ice_query(t, "SELECT count(*), sum(CASE WHEN v = 'b' THEN 1 ELSE 0 END), sum(length(body)) FROM {T}")
        assert got[0] == (10, 10, 32000), f"after the lock, every masked update lands with its body: {got}"
        print(f"  refetch behind ACCESS EXCLUSIVE: refused after {took:.0f}s; the rerun lands {got[0]}")
    finally:
        if hold:
            hold.kill()
        wipe(t, ice=True)


MY_DST = "mysql://root:bench@127.0.0.1:3308/bench"


def my_dst(sql):
    o = sh(["docker", "exec", "apitap-bench-my-dst", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def missing(dest):
    t = f"pg_cdc_missing_{dest}"
    url, q = (DST, dst) if dest == "pg" else (MY_DST, my_dst)
    wipe(t)
    if dest == "my":
        my_dst(f"DROP TABLE IF EXISTS {t}")
    try:
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, body text); ALTER TABLE {t} ALTER body SET STORAGE EXTERNAL; "
            f"INSERT INTO {t} SELECT g, 'a', repeat(md5(g::text), 100) FROM generate_series(1, 3) g")
        apitap.transfer(SRC, url, table=t, mode="log_based")
        q(f"DELETE FROM {t} WHERE id = 2")                # the destination diverged
        src(f"UPDATE {t} SET v = 'b' WHERE id = 2")       # masked: body untouched
        r, _ = run(SRC, url, t)
        assert r.returncode != 0 and "found no row at the destination" in r.stderr, \
            f"the masked update of a missing row must be refused (exit {r.returncode}):\n{r.stderr[-600:]}"
        got = q(f"SELECT count(*) FROM {t}")
        print(f"  masked update of a row missing at the {dest} destination: refused; {got} rows kept")
    finally:
        wipe(t)
        if dest == "my":
            my_dst(f"DROP TABLE IF EXISTS {t}")
            my_dst(f"DELETE FROM _apitap_state WHERE dest_table = '{t}'")


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"toastpk": toastpk, "openstream": openstream, "lockwait": lockwait, "missingpg": lambda: missing("pg"), "missingmy": lambda: missing("my")}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_pg_cdc_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ Postgres CDC hardening: {which[0]}")
