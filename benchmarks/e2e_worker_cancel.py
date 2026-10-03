#!/usr/bin/env python3
"""A failed bulk worker stops its siblings before staging is swept.

0.56.0 spawned one task per loader and joined them with `for t in tasks {
t.await?? }`: the first `Err` returned and dropped the remaining handles, which
on tokio detaches them. The siblings kept pulling from a queue that had no
notion of cancellation, kept reading the source, and kept writing into this
run's staging — while `pipeline::run`'s error arm had already run
`sink.discard()`. This leg makes the source connection of ONE worker die
(`pg_terminate_backend`) and asks the servers what the other three did:

  F1  the source lists no active `COPY` backend 2 s after the child raised;
  F2  the source read less than half of the 4M rows (0.56.0: ~0.96N, 23 of 24
      spans drained);
  F3  the destination holds no artifact of this run and no in-flight write
      (objects under the staging prefix on s3, `system.processes` on ch,
      a `LOAD` job past `RAISED` on bq), and an immediate re-run succeeds.

On MySQL (`my`) there is one more fact, because that sink's abort had nothing to
abort with: closing the stream is NOT a server-side abort. mysql_async's
local-infile handler breaks on the stream error and then unconditionally writes
the terminating empty packet, so the server sees a complete file and COMMITS the
partial rows into `<table>_<token>__apitap_staging`. Two cases ask this of the
destination server itself, before apitap is involved (the mysql CLI reading a
FIFO, so the client really does stop mid-file): A, the client stops early and the
rows land — what 0.57.0's abort did every time; B, a third connection issues
KILL QUERY mid-load and nothing lands — what the abort does now. F4 then asks the
run itself: one accepted KILL QUERY per aborted loader. The server's own record
of the aborted loads is NOT asked: `events_statements_history_long` ships
disabled and `discard` drops the staging table a moment after the abort, so there
is nothing left to read after the fact.

The child prints `RETURNED` or `RAISED <type> <msg>` and then sleeps 12 s
(D9): process exit kills detached tasks, which would hide the red control.

RED controls (against ~/gate-0560-venv):
  pg/ch  F2 reads about 0.96N;
  s3     a `part-*` under the failed token appears after `RAISED`, and the
         re-run raises `locked:`;
  bq     a staging table re-appears by +90 s, or a RUNNING load job outlives
         `RAISED`;
  my     F4: the abort sends no KILL QUERY at all, so case B's mechanism never
         runs and every aborted load commits its partial file.

Usage: e2e_worker_cancel.py pg|ch|s3|bq|my
Requires: pg source :5544; the destination engine; s3 also needs MinIO :9100
(the leg creates its own bucket), bq needs BQ_SA, my needs `apitap-bench-my-dst`
on :3308.
"""
import datetime
import os
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _rig

import apitap

ENGINE = (sys.argv[1] if len(sys.argv) > 1 else "pg").lower()
if ENGINE not in ("pg", "ch", "s3", "bq", "my"):
    print(f"usage: {sys.argv[0]} pg|ch|s3|bq|my", flush=True)
    sys.exit(2)

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
MYD = os.environ.get("MYD_URL", "mysql://root:bench@127.0.0.1:3308/bench")
MYD_C = os.environ.get("MYD_CONTAINER", "apitap-bench-my-dst")
S3_BUCKET = "apitap-e2e"
S3 = (f"s3://{S3_BUCKET}/wc?format=parquet&endpoint=http://127.0.0.1:9100"
      "&access_key_id=bench&secret_access_key=benchbench")
BQ = _rig.bq_url()
TABLE = "wc_src"
N = 4_000_000
PARALLEL = 4
SLEEP_AFTER = 12
STAGING_PREFIX = f"wc/{TABLE}__apitap_staging/"
# The child may keep writing for SLEEP_AFTER after it raises; ch and bq also
# get a later sample (the in-flight statement / the re-creatable staging).
LATE_S = {"pg": 0, "ch": 10, "s3": 60, "bq": 90, "my": 0}[ENGINE]
DST = {"pg": PGD, "ch": CH, "s3": S3, "bq": BQ, "my": MYD}[ENGINE]

ok = True
_duck = None


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def dst_pg(sql):
    return _rig.psql(sql, _rig.PG_DST)


def ch(sql):
    return _rig.clickhouse(sql)


def my(sql, check=True):
    o = subprocess.run(["docker", "exec", "-i", MYD_C, "mysql", "-uroot", "-pbench",
                        "-N", "-D", "bench", "-e", sql], capture_output=True, text=True)
    if check and o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


PROBE_T = "killprobe"
PROBE_FIFO = "/tmp/apitap_killprobe.tsv"


def _in_c(args, **kw):
    """Run something INSIDE the destination container (the file the client reads
    has to live where the client is)."""
    return subprocess.run(["docker", "exec", "-i", MYD_C] + args,
                          capture_output=True, text=True, **kw)


def probe_kill_semantics():
    """What an aborted load needs, asked of THIS server before apitap is involved.

    0.57.0 closed the client's stream and wrote that "LOAD DATA aborts instead of
    committing a partial file". That is false: a client that stops early still
    sends the terminating empty packet, so the server holds a syntactically
    complete file and COMMITS it. Reproduced here with the mysql CLI reading a
    FIFO, so the client really does stop mid-file:

      A  the writer closes and the CLI sends the terminating packet -> the rows
         land. This is what 0.57.0's abort did to staging, every time;
      B  a third connection issues KILL QUERY while the client is still
         streaming -> the statement is interrupted and its transaction rolled
         back, so nothing lands. This is what the abort does now.

    Both are asked of the destination the rest of the leg uses (8.0 by default,
    8.4 with `MYD_CONTAINER`/`MYD_URL`), because the semantics are the server's.
    """
    n = 200_000
    half = 50_000

    def loader():
        # `--local-infile` is what lets the client answer the server's request
        # for the file; apitap's own loads need it too, so the rig has it on.
        return subprocess.Popen(
            ["docker", "exec", "-i", MYD_C, "mysql", "-uroot", "-pbench",
             "--local-infile=1", "-D", "bench", "-e",
             f"LOAD DATA LOCAL INFILE '{PROBE_FIFO}' INTO TABLE {PROBE_T} "
             "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t'"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def fresh_fifo():
        my(f"TRUNCATE TABLE {PROBE_T}")
        _in_c(["sh", "-c", f"rm -f {PROBE_FIFO}; mkfifo {PROBE_FIFO}"])

    def load_conn_id():
        v = my("SELECT id FROM information_schema.processlist WHERE info LIKE "
               f"'LOAD DATA%{PROBE_T}%' AND id <> CONNECTION_ID() LIMIT 1", check=False)
        return v.splitlines()[0].strip() if v.strip() else ""

    my(f"DROP TABLE IF EXISTS {PROBE_T}")
    my(f"CREATE TABLE {PROBE_T}(id bigint PRIMARY KEY, pad text) ENGINE=InnoDB")
    try:
        # A — the 0.57.0 abort: the client stops, the server commits.
        fresh_fifo()
        p = loader()
        w = _in_c(["sh", "-c",
                   f"seq 1 {n} | awk '{{print $1\"\\tx\"}}' > {PROBE_FIFO}"])
        try:
            _, err = p.communicate(timeout=180)
        except subprocess.TimeoutExpired:
            p.kill()
            _, err = p.communicate()
        landed = int(my(f"SELECT COUNT(*) FROM {PROBE_T}"))
        case("A an early EOF COMMITS the partial file — 0.57.0's abort was not one",
             landed == n, f"{landed} of {n} rows landed (client rc={p.returncode}"
                          f"{', ' + err.strip()[-90:] if err.strip() else ''})")
        _in_c(["sh", "-c", f"rm -f {PROBE_FIFO}"])

        # B — the abort's KILL QUERY, mid-stream.
        fresh_fifo()
        p = loader()
        # The writer sends some rows and then holds the FIFO OPEN, so the server
        # is waiting for more when the kill lands.
        w = subprocess.Popen(
            ["docker", "exec", "-i", MYD_C, "sh", "-c",
             f"(seq 1 {half} | awk '{{print $1\"\\tx\"}}'; sleep 25) > {PROBE_FIFO}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # `wait_for` answers yes/no, so the id itself is read afterwards.
        # (Reading it from the first answer is how this case ran a `KILL QUERY
        # True` and measured nothing.)
        found = _rig.wait_for(lambda: load_conn_id() != "", 30, 0.2)
        cid = load_conn_id()
        if not (found and cid.isdigit()):
            p.kill()
            w.kill()
            _rig.rig_fail("the probe never saw a running LOAD DATA")
        my(f"KILL QUERY {cid}", check=False)
        try:
            _, err = p.communicate(timeout=180)
        except subprocess.TimeoutExpired:
            p.kill()
            _, err = p.communicate()
        w.kill()
        w.wait()
        landed = int(my(f"SELECT COUNT(*) FROM {PROBE_T}"))
        interrupted = "1317" in err or "interrupted" in err.lower()
        case("B1 KILL QUERY mid-load interrupts the running statement", interrupted,
             f"client rc={p.returncode}, said: {err.strip()[-120:]}")
        case("B2 and rolls it back — nothing partial is committed", landed == 0,
             f"{landed} of {half}+ rows landed after KILL QUERY {cid}")
    finally:
        for pr in (p, w):
            # `w` is a finished run() in case A and a live Popen in case B.
            if hasattr(pr, "poll") and pr.poll() is None:
                pr.kill()
        _in_c(["sh", "-c", f"rm -f {PROBE_FIFO}"])
        my(f"DROP TABLE IF EXISTS {PROBE_T}", check=False)


def duck():
    global _duck
    if _duck is None:
        import duckdb
        _duck = duckdb.connect()
        _duck.execute("INSTALL httpfs; LOAD httpfs;")
        _duck.execute(
            f"SET s3_endpoint='{_rig.S3_ENDPOINT}'; SET s3_use_ssl=false; "
            "SET s3_url_style='path'; SET s3_access_key_id='bench'; "
            f"SET s3_secret_access_key='benchbench'; SET s3_region='{_rig.S3_REGION}';")
    return _duck


def tuples_read():
    """The source's own read counter for this table, reset before each run."""
    n = src("SELECT coalesce(seq_tup_read,0)+coalesce(idx_tup_fetch,0) "
            f"FROM pg_stat_all_tables WHERE schemaname='public' AND relname='{TABLE}'")
    return int(n or 0)


def copy_pids():
    """Active `COPY (SELECT … wc_src …)` backends on the source, oldest first."""
    out = src("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
              "AND state='active' AND query LIKE 'COPY (SELECT%wc_src%' "
              "ORDER BY backend_start")
    return [p for p in out.splitlines() if p]


def dest_artifacts():
    """(clean, detail): no artifact of this run and no write still in flight."""
    if ENGINE == "pg":
        n = dst_pg("SELECT count(*) FROM pg_class WHERE relkind='r' "
                   "AND relname LIKE 'wc\\_src\\_%\\_\\_apitap\\_%'")
        return n == "0", f"{n} artifact relation(s)"
    if ENGINE == "ch":
        t = ch("SELECT count() FROM system.tables WHERE database=currentDatabase() "
               "AND name LIKE 'wc_src_%__apitap_%'")
        p = ch("SELECT count() FROM system.processes WHERE query ILIKE "
               "'INSERT INTO%wc_src_%apitap%'")
        return t == "0" and p == "0", f"{t} artifact table(s), {p} in-flight INSERT(s)"
    if ENGINE == "s3":
        keys = _rig.s3_list(STAGING_PREFIX, bucket=S3_BUCKET)
        ups = _rig.s3_list_uploads("wc/", bucket=S3_BUCKET)
        return not keys and not ups, f"{len(keys)} staging key(s) {keys[:3]}, " \
                                     f"{len(ups)} open upload(s)"
    if ENGINE == "my":
        t = my("SELECT COUNT(*) FROM information_schema.tables "
               "WHERE table_schema = DATABASE() AND table_name LIKE 'wc\\_src\\_%'")
        p = my("SELECT COUNT(*) FROM information_schema.processlist "
               "WHERE info LIKE 'LOAD DATA%' AND info LIKE '%__apitap_staging%'")
        return t == "0" and p == "0", f"{t} artifact table(s), {p} in-flight LOAD(s)"
    bad = [t for t in _rig.bq_tables() if re.match(r"wc_src.*__apitap_", t)]
    return not bad, f"{len(bad)} artifact table(s) {bad[:3]}"


def dest_count():
    if ENGINE == "pg":
        return int(dst_pg(f"SELECT count(*) FROM {TABLE}"))
    if ENGINE == "ch":
        return int(ch(f"SELECT count() FROM `{TABLE}`"))
    if ENGINE == "my":
        return int(my(f"SELECT COUNT(*) FROM `{TABLE}`"))
    if ENGINE == "s3":
        q = f"SELECT count(*) FROM read_parquet('s3://{S3_BUCKET}/wc/{TABLE}/part-*.parquet')"
        return int(duck().execute(q).fetchone()[0])
    return int(_rig.bq(f"SELECT COUNT(*) FROM `{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}.{TABLE}`")[0][0])


def bq_loads(since, state):
    """LOAD jobs the child submitted since `since` whose destination is one of
    this run's staging tables, in state `state`."""
    q = (f"SELECT COUNT(*) FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER "
         f"WHERE creation_time >= TIMESTAMP('{since}') AND job_type='LOAD' "
         f"AND state {state} AND destination_table.table_id LIKE 'wc_src%apitap_staging%'")
    return int(_rig.bq(q)[0][0])


def cleanup():
    """Drop everything this leg created, on every engine it could have used."""
    # source
    src(f"DROP TABLE IF EXISTS {TABLE} CASCADE")
    # pg destination
    for n in dst_pg("SELECT relname FROM pg_class WHERE relkind='r' "
                    "AND relname LIKE 'wc\\_src\\_%\\_\\_apitap\\_%'").splitlines():
        dst_pg(f'DROP TABLE IF EXISTS "{n}"')
    dst_pg(f"DROP TABLE IF EXISTS {TABLE} CASCADE")
    if dst_pg("SELECT to_regclass('_apitap_state') IS NOT NULL") == "t":
        dst_pg(f"DELETE FROM _apitap_state WHERE dest_table='{TABLE}'")
    if dst_pg("SELECT to_regclass('_apitap_lease') IS NOT NULL") == "t":
        dst_pg(f"DELETE FROM _apitap_lease WHERE dest_key LIKE '%.{TABLE}'")
    # clickhouse destination
    for n in ch("SELECT name FROM system.tables WHERE database=currentDatabase() "
                f"AND name LIKE '{TABLE}%'").splitlines():
        ch(f"DROP TABLE IF EXISTS `{n}`")
    if ch("SELECT count() FROM system.tables WHERE database=currentDatabase() "
          "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE `_apitap_state` DELETE WHERE dest_table='{TABLE}' "
           "SETTINGS mutations_sync=1")
    # mysql destination
    for n in my("SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name LIKE 'wc\\_src%'").splitlines():
        my(f"DROP TABLE IF EXISTS `{n}`")
    if my("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
          "AND table_name = '_apitap_state'") != "0":
        my(f"DELETE FROM `_apitap_state` WHERE dest_table='{TABLE}'")
    my(f"DROP TABLE IF EXISTS `{PROBE_T}`", check=False)
    _in_c(["sh", "-c", f"rm -f {PROBE_FIFO}"])
    # s3 destination (own bucket, own prefix); the bucket may not exist yet
    try:
        keys = _rig.s3_list("wc/", bucket=S3_BUCKET)
    except RuntimeError:
        keys = []
    for k in keys:
        _rig.s3_delete(k, bucket=S3_BUCKET)
    _rig.s3_delete_bucket(S3_BUCKET)
    # bigquery destination
    if os.environ.get("BQ_SA"):
        for t in _rig.bq_tables():
            if t == TABLE or (t.startswith(TABLE) and "__apitap_" in t):
                _rig.bq_delete_table(t)
        if "_apitap_state" in _rig.bq_tables():
            _rig.bq(f"DELETE FROM `{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}._apitap_state` "
                    f"WHERE dest_table='{TABLE}'")


def seed():
    v = int(src("SELECT current_setting('server_version_num')"))
    if v < 140000:
        _rig.rig_fail(f"needs TID range scan (server_version_num={v})")
    src(f"DROP TABLE IF EXISTS {TABLE} CASCADE")
    src(f"CREATE TABLE {TABLE}(id bigint PRIMARY KEY, pad text)")
    print(f"   seeding {TABLE}: {N:,} rows", flush=True)
    src(f"INSERT INTO {TABLE} SELECT g, repeat('x',200) "
        f"FROM generate_series(1,{N}) g")
    src(f"ANALYZE {TABLE}")
    if ENGINE == "s3":
        _rig.s3_create_bucket(S3_BUCKET)


def calibrate():
    """A full successful run (also the destination the F3 re-run compares to);
    the source counter must measure the read, or F2 proves nothing.

    The measurement is a DELTA around the run: `pg_stat_reset_single_table_
    counters` does not revoke counters other backends still hold pending, so
    the post-reset absolute can carry an earlier run's total. The delta is the
    same number when the reset does stick and stays correct when it does not."""
    src(f"SELECT pg_stat_reset_single_table_counters('{TABLE}'::regclass)")
    base = tuples_read()
    r = apitap.transfer(SRC, DST, table=TABLE, mode="replace", parallel=PARALLEL)
    if r.rows != N:
        return False, f"calibration returned rows={r.rows}, want {N}"
    time.sleep(3)
    got = tuples_read() - base
    if not (0.9 * N <= got <= 1.3 * N):
        _rig.rig_fail(f"counter does not measure the read: {got} of {N} (base {base})")
    got_n = dest_count()
    if got_n != N:
        return False, f"calibration destination holds {got_n}, want {N}"
    print(f"   calibration: rows={r.rows:,}, source counter={got:,}", flush=True)
    return True, ""


CHILD = (
    "import sys, time, apitap\n"
    "try:\n"
    f"    r = apitap.transfer({SRC!r}, {DST!r}, table={TABLE!r}, "
    f"mode='replace', parallel={PARALLEL})\n"
    "    print('RETURNED', r.rows, flush=True)\n"
    "except BaseException as e:\n"
    "    print('RAISED', type(e).__name__, str(e)[:500], flush=True)\n"
    f"    time.sleep({SLEEP_AFTER}); sys.exit(1)\n"
    f"time.sleep({SLEEP_AFTER})\n")


def one_run():
    """Kill one of the child's source COPY backends; ask the servers what the
    other workers did. Returns (raised, err)."""
    src(f"SELECT pg_stat_reset_single_table_counters('{TABLE}'::regclass)")
    base = tuples_read()
    since = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    env = dict(os.environ)
    if ENGINE == "my":
        # The abort reports what it did to the server on stderr (APITAP_DEBUG);
        # F4 is read from there.
        env["APITAP_DEBUG"] = "1"
    child = subprocess.Popen([sys.executable, "-c", CHILD], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=env)
    lines, raised, returned = [], [], []

    def pump():
        for line in child.stdout:
            lines.append(line.strip())
            if line.startswith("RAISED"):
                raised.append(time.monotonic())
            if line.startswith("RETURNED"):
                returned.append(line.strip())

    threading.Thread(target=pump, daemon=True).start()

    if not _rig.wait_for(lambda: len(copy_pids()) >= PARALLEL, 30, 0.05):
        child.kill()
        _rig.rig_fail(f"rig too fast: never saw {PARALLEL} COPY OUT backends")
    if ENGINE == "bq":
        # The run must have begun on the destination before the kill. The
        # spec asks for a RUNNING LOAD job; measured on both wheels, this
        # seed's staged parquet compresses below the loader's rotate
        # threshold, so the first job appears only at a worker's finish —
        # waiting for it moves the kill past the read (measured: child
        # RETURNED). The precondition is therefore "the destination holds
        # this run's announcement", and the job fact is asserted after the
        # kill instead: every LOAD job that did start is DONE at RAISED.
        def submitted():
            if any(re.match(r"wc_src.*__apitap_", t) for t in _rig.bq_tables()):
                return True
            return bq_loads(since, "= 'RUNNING'") > 0

        if not _rig.wait_for(submitted, 120, 0.25):
            child.kill()
            _rig.rig_fail("no staging table and no in-flight LOAD job to prove against")

    # One connection at a time, until the child raises. SELECT and signal run
    # in ONE server statement: a client-side snapshot plus a later terminate
    # races the COPY phase (a span's COPY is short and the pool replaces a
    # connection killed between spans — a measured GREEN run absorbed exactly
    # that). Only one worker is ever killed, so the siblings keep reading —
    # which is exactly what 0.56.0's red control needs to measure.
    killed = []
    killed_at = None
    deadline = time.monotonic() + 90
    while (not raised and not returned and child.poll() is None
           and time.monotonic() < deadline):
        row = src(
            "SELECT pg_terminate_backend(pid), pid FROM pg_stat_activity "
            "WHERE datname=current_database() AND state='active' "
            "AND query LIKE 'COPY (SELECT%wc_src%' AND pid <> pg_backend_pid() "
            "ORDER BY backend_start LIMIT 1")
        if not row.startswith("t|"):
            time.sleep(0.2)
            continue
        pid = row.split("|", 1)[1]
        if pid in killed:
            time.sleep(0.2)
            continue
        killed.append(pid)
        if killed_at is None:
            killed_at = tuples_read()
        print(f"   terminating COPY backend {pid}", flush=True)
        _rig.wait_for(lambda: raised or returned or child.poll() is not None, 10, 0.05)
    if not raised and not returned and child.poll() is None:
        child.kill()
        _rig.rig_fail("killing every COPY backend was absorbed; the child never raised")

    if not _rig.wait_for(lambda: raised or returned or child.poll() is not None, 180, 0.05):
        child.kill()
        _rig.rig_fail("the child neither raised nor returned in 180 s")
    t_raise = raised[0] if raised else time.monotonic()
    if returned:
        case("the child raised on the killed connection", False, returned[0][:160])
    else:
        case("the child raised on the killed connection", bool(raised),
             (lines[-1][:160] if lines else f"rc={child.poll()}"))

    if ENGINE == "bq":
        nd = bq_loads(since, "!= 'DONE'")
        case("no LOAD job is past DONE at RAISED", bool(raised) and nd == 0,
             f"{nd} non-DONE job(s)")

    time.sleep(max(0.0, t_raise + 2 - time.monotonic()))
    n_copy = len(copy_pids())
    case("F1 the source has no active COPY two seconds after the raise",
         n_copy == 0, f"{n_copy} active COPY backend(s)")

    clean, detail = dest_artifacts()
    case("F3 no run artifact and no in-flight write at RAISED+2", clean, detail)
    if LATE_S > 2:
        time.sleep(LATE_S - 2)
        clean, detail = dest_artifacts()
        case(f"F3 still clean at RAISED+{LATE_S}", clean, detail)

    try:
        child.wait(timeout=SLEEP_AFTER + 60)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
    err = child.stderr.read() if child.stderr else ""

    if ENGINE == "my":
        # F4: the engine really asked the server to stop. The server's own record
        # of the aborted loads is not readable on this rig
        # (`events_statements_history_long` ships disabled, and `discard` drops the
        # staging table a moment after the abort), so what is checked here is the
        # engine's side of it: one KILL QUERY per aborted loader, every one of
        # them accepted by the server, and none of them silently skipped. What
        # those kills DO to the rows is probe case B above, asked of this server.
        sent = [ln.strip() for ln in err.splitlines() if "KILL QUERY" in ln]
        skipped = [ln.strip() for ln in sent if "accepted" not in ln]
        case("F4 the abort stopped every load server-side", len(sent) >= PARALLEL,
             f"{len(sent)} KILL QUERY for {PARALLEL} pipes; first: "
             f"{sent[0][:90] if sent else 'none'}")
        case("F4 every KILL QUERY was accepted by the server", bool(sent) and not skipped,
             f"{len(sent) - len(skipped)} accepted, {len(skipped)} not: {skipped[:2]}")
    time.sleep(3)
    read = tuples_read() - base
    if ENGINE == "bq":
        # On BigQuery the in-flight-job precondition cannot be met before the
        # first 96 MiB LOAD job exists — by then roughly a third of the table
        # has been read — so the spec's total bound is unreachable there. The
        # invariant is the same: what the source reads AFTER the kill is the
        # cancellation contribution, and it must stay under half the table.
        pre = (killed_at - base) if killed_at is not None else 0
        case("F2 the source read less than half the table after the kill",
             read - pre < N // 2,
             f"{read - pre:,} tuples after the kill of {N:,} (bound {N // 2:,}; "
             f"total {read:,}, pre-kill {pre:,})")
    else:
        case("F2 the source read less than half the table", read < N // 2,
             f"{read:,} tuples of {N:,} (bound {N // 2:,}, base {base:,})")
    return bool(raised) and not returned, err


def rerun():
    try:
        r = apitap.transfer(SRC, DST, table=TABLE, mode="replace", parallel=PARALLEL)
    except Exception as e:  # a RED s3/bq leg raises LockedError here
        return False, f"{type(e).__name__}: {str(e)[:300]}"
    got = dest_count()
    return r.rows == N and got == N, f"rows={r.rows}, destination={got}"


def main():
    try:
        cleanup()
        if ENGINE == "my":
            # First, and before apitap is involved: what this server does with a
            # load whose client stops early, with and without a KILL QUERY.
            probe_kill_semantics()
        seed()
        good, why = calibrate()
        case("calibration is a full run the counter can see", good, why)
        if good:
            one_run()
            good, why = rerun()
            case("an immediate re-run is not refused and lands the table", good, why)
    finally:
        try:
            cleanup()
            print("   cleaned up", flush=True)
        except Exception as e:  # never mask the leg's verdict with cleanup
            print(f"   cleanup: {str(e)[:200]}", flush=True)
    print(f"\nWORKER CANCEL E2E ({ENGINE}): "
          + ("PASSED" if ok else "FAILED"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
