"""Four drains in one BigQuery dataset, and a transaction longer than the TTL.

The BigQuery fence is a per-run table, `_apitap_fence<token>`, and never the
shared `_apitap_lease`, for two reasons this leg asks BigQuery about:

  * every drain's keeper writes `_apitap_lease` on every tick. A transaction
    that also wrote it would conflict with the keeper of every sibling drain in
    the dataset, and BigQuery cancels one side of a conflicting transaction —
    so sibling drains aborted each other's commits;
  * a transaction longer than the TTL must still commit, which means the keeper
    may never go quiet while one is open.

What the leg asserts:

  loop    four tables, four drains (one per table) re-run back to back for
          MULTI_DRAIN_SECS (default 300) while a writer changes every table:
          0 non-zero exits; no job failed with the lease-lost mark; the number
          of jobs that met "concurrent update" is printed; after a final
          catch-up, every table equals its source (count, sum(id), sum(n))
  long    at TTL 30, one drain over a 16-table group and one window that
          updates every row of every member: the commit is ONE fenced
          transaction holding 16 MERGEs of 200k rows, run one after another,
          open longer than the TTL. (A single MERGE will not do it: 800k rows
          merged in 5.4 s, a 9 s transaction — measured on this rig.) It
          completes, rc 0, every table equals its source, the leg reads the
          transaction's length back from JOBS_BY_USER (> TTL, or the cell
          proved nothing), the keeper was never quiet while it was open (no
          TTL-long stretch of it without one of that run's renewals landing,
          from JOBS_BY_USER), and every lease sample taken inside it reads
          uncollected.

          The lease's life is printed, not judged. A renewal stamps `now + TTL`
          as it starts and counts only once BigQuery commits it; one 1-row
          renewal UPDATE on this rig committed 21 s after it executed, which
          at the 30 s minimum TTL took the life to 0 for about a second while
          the keeper was renewing every ~7 s. And after the transaction the
          drain stops its keeper by design (a renewal must never land after
          its own close) and gives its sixteen leases back one DELETE at a
          time, ~3.6 s each, so the ones not yet given back lapse meanwhile —
          harmless, the run writes nothing after its last unit closed. An
          earlier version judged every sample until the process exited and
          failed a green run on exactly that (-32 s, 60 s into the release).

    python benchmarks/e2e_bq_multi_drain.py

Rig: pg-src :5544, BigQuery dataset `apitap_cdc_e2e` (BQ_SA).
RED control: the fence as an UPDATE of `_apitap_lease` inside the script (the
design this replaced) — sibling drains cancel each other's transactions.
"""
import datetime
import json
import os
import subprocess
import sys
import time

import _rig

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PG_DSN = "host=127.0.0.1 port=5544 user=postgres password=bench dbname=apitap_bench_src"
BQ = _rig.bq_url()
P, D = _rig.BQ_PROJECT, _rig.BQ_DATASET
TABLES = [f"mdrain_{i}" for i in range(4)]
LONG = [f"mdlong_{i}" for i in range(16)]
LONG_ROWS = 200_000
SECS = int(os.environ.get("MULTI_DRAIN_SECS", "300"))
LONG_TTL = 30
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


bq = _rig.bq


def fq(t):
    return f"`{P}.{D}.{t}`"


def clean(tables):
    for t in tables:
        pg(f"DROP TABLE IF EXISTS {t} CASCADE")
        pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{t}")
    pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
       "WHERE slot_name LIKE 'apitap_%' AND NOT active")
    names = _rig.bq_tables()
    for n in names:
        if any(n == t or n.startswith(t + "_") or n == f"{t}__current" for t in tables):
            _rig.bq_delete_table(n)
    inlist = ", ".join(f"'{t}'" for t in tables)
    if "_apitap_state" in names:
        bq(f"DELETE FROM {fq('_apitap_state')} WHERE dest_table IN ({inlist})")
    if "_apitap_lease" in names:
        keys = ", ".join(f"'{D}.{t}'" for t in tables)
        bq(f"DELETE FROM {fq('_apitap_lease')} WHERE dest_key IN ({keys})")


def src_digest(t):
    return pg(f"SELECT count(*)||'|'||coalesce(sum(id::bigint),0)||'|'||coalesce(sum(n::bigint),0) FROM {t}")


def dst_digest(t):
    r = bq(f"SELECT COUNT(*), IFNULL(SUM(id), 0), IFNULL(SUM(n), 0), COUNT(DISTINCT id) FROM {fq(t)}")[0]
    return f"{r[0]}|{r[1]}|{r[2]}", int(r[0]) == int(r[3])


def create(t, rows):
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, n int NOT NULL DEFAULT 0)")
    pg(f"INSERT INTO {t} SELECT g, 'v'||g, 0 FROM generate_series(1,{rows}) g")


def drain_py(extra_env=None):
    return dict(os.environ, **(extra_env or {}))


def transfer_cmd(**kw):
    args = ", ".join(f"{k}={v!r}" for k, v in kw.items())
    return [sys.executable, "-c", f"import apitap; apitap.transfer({PG!r}, {BQ!r}, {args})"]


# A drain re-run back to back until the deadline; prints one JSON line.
LOOP = r'''
import json, sys, time, apitap
src, dst, table, deadline = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
runs, fails = 0, []
while time.time() < deadline:
    runs += 1
    try:
        apitap.transfer(src, dst, table=table, mode="log_based")
    except Exception as e:                                    # noqa: BLE001
        fails.append(f"{type(e).__name__}: {e}"[:400])
    time.sleep(1)
print(json.dumps({"table": table, "runs": runs, "fails": fails}))
'''

# Changes every table in its own transaction until the deadline.
WRITER = r'''
import random, sys, time, psycopg2
dsn, deadline, tables = sys.argv[1], float(sys.argv[2]), sys.argv[3:]
c = psycopg2.connect(dsn)
nxt = {t: 1_000_000 for t in tables}
while time.time() < deadline:
    for t in tables:
        with c, c.cursor() as cur:
            cur.execute(f"INSERT INTO {t} SELECT g, 'w'||g, 1 FROM generate_series(%s, %s) g",
                        (nxt[t], nxt[t] + 49))
            nxt[t] += 50
            cur.execute(f"UPDATE {t} SET n = n + 1, v = v || 'u' WHERE id IN "
                        f"(SELECT id FROM {t} ORDER BY random() LIMIT 20)")
            cur.execute(f"DELETE FROM {t} WHERE id IN (SELECT id FROM {t} ORDER BY random() LIMIT 5)")
    time.sleep(0.5)
'''


def jobs_since(start):
    """(concurrent-update jobs, lease-lost jobs) of this service account."""
    r = bq("SELECT COUNTIF(LOWER(error_result.message) LIKE '%concurrent update%'), "
           "COUNTIF(error_result.message LIKE '%apitap-lease-lost%') "
           "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER "
           f"WHERE creation_time >= TIMESTAMP('{start}')")[0]
    return int(r[0] or 0), int(r[1] or 0)


TS = "'%Y-%m-%d %H:%M:%E6S'"


def longest_fenced(start, like):
    """(seconds, start, end, start µs, end µs) of the longest apply transaction
    since `start` that names `like` — whatever its fence, so the RED control's
    is found too."""
    r = bq(f"SELECT TIMESTAMP_DIFF(end_time, start_time, SECOND), FORMAT_TIMESTAMP({TS}, start_time), "
           f"FORMAT_TIMESTAMP({TS}, end_time), UNIX_MICROS(start_time), UNIX_MICROS(end_time) "
           "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER "
           f"WHERE creation_time >= TIMESTAMP('{start}') AND statement_type = 'SCRIPT' AND end_time IS NOT NULL "
           "AND STARTS_WITH(query, 'BEGIN TRANSACTION') "
           f"AND query LIKE '%{like}%' ORDER BY 1 DESC LIMIT 1")
    return (int(r[0][0]), r[0][1], r[0][2], int(r[0][3]), int(r[0][4])) if r else (-1, "", "", 0, 0)


def renewals_landed(tok, t0, t1):
    """When (µs) each of the keeper's successful renewals of run `tok` LANDED
    inside [t0, t1]. Landed, not started: a renewal queued behind a
    transaction that holds `_apitap_lease` (the design this replaced) is
    created on time and lands only after that transaction ends."""
    r = bq("SELECT UNIX_MICROS(end_time) FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER "
           f"WHERE creation_time >= TIMESTAMP_SUB(TIMESTAMP_MICROS({t0}), INTERVAL 1 MINUTE) "
           "AND statement_type = 'UPDATE' AND parent_job_id IS NULL AND error_result IS NULL "
           f"AND query LIKE '%_apitap_lease%SET expires_at%{tok}%' "
           f"AND end_time BETWEEN TIMESTAMP_MICROS({t0}) AND TIMESTAMP_MICROS({t1}) ORDER BY 1")
    return [int(x[0]) for x in r]


procs = []
try:
    print("== reset and bootstrap four tables ==", flush=True)
    clean(TABLES + LONG)
    # One after another: four logical slots created at once each wait on the
    # others' exported snapshots, which is the rig's problem, not this leg's.
    for t in TABLES:
        create(t, 1000)
        r = subprocess.run(transfer_cmd(table=t, mode="log_based"), env=drain_py(),
                           capture_output=True, text=True, timeout=900)
        case(f"{t} bootstrapped", r.returncode == 0, r.stderr.strip()[-200:] if r.returncode else "rc=0")
    if not ok:
        _rig.rig_fail("the four bootstraps did not all succeed")

    start = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    deadline = time.time() + SECS
    print(f"== loop: four drains and a writer for {SECS}s ==", flush=True)
    writer = subprocess.Popen([sys.executable, "-c", WRITER, PG_DSN, str(deadline), *TABLES],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    procs.append(writer)
    loops = [subprocess.Popen([sys.executable, "-c", LOOP, PG, BQ, t, str(deadline)], env=drain_py(),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for t in TABLES]
    procs += loops
    _, werr = writer.communicate(timeout=SECS + 120)
    case("the writer ran to the deadline", writer.returncode == 0, werr.strip()[-200:] or "rc=0")
    runs, fails = 0, []
    for t, p in zip(TABLES, loops):
        out, err = p.communicate(timeout=SECS + 1800)
        try:
            s = json.loads(out.strip().splitlines()[-1])
        except Exception:                                     # noqa: BLE001
            s = {"table": t, "runs": 0, "fails": [f"no summary (rc={p.returncode}): {err.strip()[-300:]}"]}
        runs += s["runs"]
        fails += [f"{t}: {f}" for f in s["fails"]]
    case(f"0 non-zero exits over {runs} drain runs", runs > 0 and not fails,
         f"{len(fails)} failed: " + "; ".join(f[:300] for f in fails[:2]) if fails else f"{runs} runs")
    for t in TABLES:
        r = subprocess.run(transfer_cmd(table=t, mode="log_based"), env=drain_py(),
                           capture_output=True, text=True, timeout=900)
        case(f"{t} final catch-up", r.returncode == 0, r.stderr.strip()[-200:] if r.returncode else "rc=0")
        d, unique = dst_digest(t)
        case(f"{t} equals its source, one row per key", d == src_digest(t) and unique,
             f"src {src_digest(t)} vs dst {d}")
    cu, lost = jobs_since(start)
    print(f"      jobs that met 'concurrent update' (retried inside apitap): {cu}", flush=True)
    case("no job failed with the lease-lost mark", lost == 0, f"{lost} job(s)")

    print(f"== long: one fenced transaction longer than a {LONG_TTL}s TTL ==", flush=True)
    for t in LONG:
        create(t, LONG_ROWS)
    # One window for the whole group: a window never splits a source
    # transaction, and this budget holds all sixteen.
    env_long = drain_py({"APITAP_LEASE_TTL_SECS": str(LONG_TTL), "APITAP_CDC_WINDOW_BYTES": str(1 << 30)})
    r = subprocess.run(transfer_cmd(tables=LONG, mode="log_based"), env=env_long,
                       capture_output=True, text=True, timeout=1800)
    case("the group bootstrapped", r.returncode == 0, r.stderr.strip()[-200:] if r.returncode else "rc=0")
    for t in LONG:
        pg(f"UPDATE {t} SET n = n + 1")
    long_start = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    p = subprocess.Popen(transfer_cmd(tables=LONG, mode="log_based"), env=env_long,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    procs.append(p)
    # The drain's own lease, sampled on the server's clock while it runs: the
    # least life left among its rows, and how many read collected. Only the
    # samples inside the transaction are judged (see the module doc): after
    # it, the drain stops its keeper by design and gives its sixteen leases
    # back one by one, and the ones not yet given back lapse meanwhile.
    lives, tok = [], None
    while p.poll() is None:
        if tok is None:
            lk = _rig.locks_bq(LONG[0])
            tok = _rig.token_of(lk[0]) if lk else None
        if tok:
            rows = bq(f"SELECT FORMAT_TIMESTAMP({TS}, CURRENT_TIMESTAMP()), "
                      "MIN(TIMESTAMP_DIFF(expires_at, CURRENT_TIMESTAMP(), SECOND)), COUNTIF(collected) "
                      f"FROM {fq('_apitap_lease')} WHERE token = '{tok}'")
            if rows and rows[0][1] is not None:
                lives.append((rows[0][0], int(rows[0][1]), int(rows[0][2])))
        time.sleep(2)
    _, err = p.communicate()
    case("the drain completed", p.returncode == 0, err.strip()[-200:] if p.returncode else "rc=0")
    longest, t0, t1, t0us, t1us = longest_fenced(long_start, "mdlong_")
    if longest <= LONG_TTL:
        _rig.rig_fail(f"the longest fenced transaction took {longest}s, not more than the {LONG_TTL}s TTL "
                      "— the cell did not exercise what it claims")
    case("its fenced transaction outlived the TTL and committed", True, f"{longest}s > {LONG_TTL}s ({t0} .. {t1})")
    # Never quiet: every stretch of the transaction as long as the TTL saw a
    # renewal land. A keeper that goes quiet during a commit, or whose
    # renewals queue behind it, shows the whole transaction as one gap.
    landed = renewals_landed(tok, t0us, t1us)
    marks = [t0us] + landed + [t1us]
    quiet = max(b - a for a, b in zip(marks, marks[1:])) / 1e6
    case("the keeper was never quiet while it was open: no TTL-long stretch without a renewal landing",
         landed and quiet < LONG_TTL, f"{len(landed)} renewal(s) landed inside it, longest gap {quiet:.1f}s")
    during = [(left, coll) for ts, left, coll in lives if t0 <= ts <= t1]
    case("and nothing collected its lease while it was open", len(during) >= 3 and not any(c for _, c in during),
         f"{len(during)} samples, {sum(1 for _, c in during if c)} collected")
    # Printed, not judged. A renewal stamps `now + TTL` when it starts and
    # counts only once BigQuery commits it, and one 1-row UPDATE has been seen
    # to commit 21 s after it executed: at the 30 s minimum TTL that alone can
    # take the life to ~0 for a moment. The keeper did not go quiet — the
    # line above is that claim.
    after = [left for ts, left, _ in lives if ts > t1]
    print(f"      lease life inside it: min {min(l for l, _ in during) if during else None}s; after the commit "
          f"(keeper stopped, leases given back one by one): min {min(after) if after else None}s — not judged",
          flush=True)
    for t in LONG:
        d, unique = dst_digest(t)
        case(f"{t} equals its source", d == src_digest(t) and unique, f"src {src_digest(t)} vs dst {d}")
finally:
    print("== cleanup ==", flush=True)
    for p in procs:
        if p.poll() is None:
            p.kill()
            p.wait()
    try:
        clean(TABLES + LONG)
    except Exception as ex:                                   # noqa: BLE001
        print(f"   cleanup: {ex}", flush=True)

print("\nBQ MULTI DRAIN E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
