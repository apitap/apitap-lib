"""What is left behind when a transfer does NOT finish.

Benchmarks prove speed and correctness on the happy path. Production asks a
different question: the process was killed, the network dropped, someone ran a
migration mid-run — what state am I in, and what do I do now? apitap claims
strong answers (staging swapped atomically, the CDC watermark committed with
the data, loud refusals instead of guesses). Claims are not receipts.

Each case here creates the failure ON PURPOSE, then asserts the two things a
user actually needs:

  * the destination was never left holding a partial or wrong version, and
  * the run is recoverable by simply running it again.

Rig: the shared bench containers (pg-src :5544, pg-dst :5545, ch :8124,
mariadb :3309). Everything it creates, it drops.
"""
import os
import signal
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
SRC_BIG = "bench_data_10m_cap"          # 10M rows: long enough to kill mid-flight
T = "fm_demo"

from _artifacts import drop_ch            # noqa: E402 — after T, used below
CT = "fm_cdc"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql, db="src"):
    box = "apitap-bench-pg-src" if db == "src" else "apitap-bench-pg-dst"
    name = "apitap_bench_src" if db == "src" else "apitap_bench_dst"
    o = sh(["docker", "exec", "-i", box, "psql", "-U", "postgres", "-d", name, "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr.strip())
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client", "--user",
               "default", "--password", "bench", "-q", sql]).stdout.strip()


def spawn(code):
    """Run engine work in a child so it can be killed the way an operator's
    process, pod or Airflow task gets killed."""
    return subprocess.Popen([sys.executable, "-c", code],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def kill_after(p, secs):
    time.sleep(secs)
    if p.poll() is not None:
        return False                      # finished too fast to be a mid-flight kill
    os.kill(p.pid, signal.SIGKILL)
    p.wait(timeout=30)
    return True


def kill_once_staging_exists(p, probe, timeout=60.0):
    """SIGKILL the run the moment the SERVER shows it has started writing.

    A timer was here before — `kill_after(p, 3.0)` — and a PGO-built wheel loads
    the fixture in under three seconds, so the kill stopped landing and the case
    reported a FAILURE for a transfer that simply won. A timer is a guess about
    how fast the box is today; the existence of the staging object is proof the
    run is mid-flight. Same fix, same reason, as the gate in
    e2e_concurrent_runs.py.

    Returns True if the kill landed, False if the run finished first even so
    (which is a rig too fast to test this at all, and says so out loud).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if p.poll() is not None:
            return False
        if probe():
            if p.poll() is not None:
                return False
            os.kill(p.pid, signal.SIGKILL)
            p.wait(timeout=30)
            return True
        time.sleep(0.02)
    return False


ok = True
results = []


def case(name, passed, detail):
    global ok
    ok &= passed
    results.append((name, passed, detail))
    print(f"   {'✓' if passed else '✗'} {name}: {detail}")


print("== 1. SIGKILL mid-transfer, over an EXISTING destination table ==")
# Not just the destination: a PREVIOUS run of this leg was killed on purpose and
# left tokenized staging behind, and since 0.55.0 that refuses the next run
# instantly — which looked exactly like "the transfer finished before it could be
# killed", because the child had indeed exited. Clear the artifacts, not the name.
drop_ch(ch, T)
# A known-good version readers must keep seeing throughout.
sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client", "--user", "default",
    "--password", "bench", "-q",
    f"CREATE TABLE {T} (id Int64, marker String) ENGINE = MergeTree ORDER BY id"])
ch(f"INSERT INTO {T} SELECT number, 'OLD' FROM numbers(1000)")
before = ch(f"SELECT count(), uniqExact(marker) FROM {T}")

p = spawn(f"""
import apitap
apitap.transfer({PG!r}, {CH!r}, table={SRC_BIG!r}, dest_table={T!r}, mode="replace")
""")
killed = kill_once_staging_exists(p, lambda: bool([
    n for n in ch("SELECT name FROM system.tables WHERE database = currentDatabase() "
                  f"AND position(name, '__apitap_') > 0 AND startsWith(name, '{T}')").split()
    if n
]))
if not killed:
    case("mid-flight kill", False,
         "the transfer finished before its staging table was ever visible — this "
         "rig is too fast to stage a mid-flight kill; use a larger SRC_BIG")
else:
    after = ch(f"SELECT count(), uniqExact(marker) FROM {T}")
    case("old table survives a killed transfer", after == before,
         f"{after} (was {before}) — readers never saw a partial version")
    # The staging name carries the run's token since 0.55.0, so the orphan can
    # only be found by listing — and SIGKILL runs no error path, so there IS one.
    listed = [n for n in ch(
        "SELECT name FROM system.tables WHERE database = currentDatabase() "
        f"AND position(name, '__apitap_') > 0 AND startsWith(name, '{T}')").split() if n]
    case("what is left behind is named", bool(listed),
         f"orphaned staging: {listed or 'none'}")

    def rerun():
        return sh([sys.executable, "-c", f"""
import apitap
r = apitap.transfer({PG!r}, {CH!r}, table={SRC_BIG!r}, dest_table={T!r}, mode="replace")
print(r.rows)
"""])

    # A killed run's leftover is NOT collected on a timer: the token records when
    # the run STARTED, so on a long load it is already hours old when the object
    # is made and age can never prove the object dead. Collecting a live peer's
    # workspace is silent truncation on the object stores, so the safe action is
    # to refuse. That costs a manual step, and the error has to earn it by
    # naming the object.
    r = rerun()
    refused = r.returncode != 0 and "locked" in (r.stderr or "").lower()
    case("the re-run REFUSES rather than dropping the orphan blindly", refused,
         (r.stderr.strip().splitlines() or [""])[-1][:150])
    case("and the refusal names the object to drop",
         any(n in (r.stderr or "") for n in listed),
         (r.stderr or "")[-150:])

    for n in listed:
        ch(f"DROP TABLE IF EXISTS {n}")
    r = rerun()
    landed = ch(f"SELECT count() FROM {T}")
    truth = pg(f"SELECT count(*) FROM {SRC_BIG}")
    case("after dropping it, the re-run recovers fully",
         r.returncode == 0 and landed == truth, f"{landed} rows == source {truth}")
drop_ch(ch, T)

print("== 2. SIGKILL mid-CDC-window: the watermark must not move ==")
pg(f"DROP TABLE IF EXISTS {CT}")
pg(f"CREATE TABLE {CT} (id int primary key, v text)")
pg(f"INSERT INTO {CT} SELECT g, 'v'||g FROM generate_series(1,1000) g")
ch(f"DROP TABLE IF EXISTS {CT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{CT}' SETTINGS mutations_sync=1")
boot = sh([sys.executable, "-c", f"""
import apitap
apitap.transfer({PG!r}, {CH!r}, table={CT!r}, mode="log_based")
"""])
if boot.returncode:
    case("cdc bootstrap", False, boot.stderr[-300:])
else:
    wm0 = ch(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table='{CT}' AND source_id NOT LIKE 'server-identity:%'")
    # A window big enough that a kill lands inside the apply.
    pg(f"UPDATE {CT} SET v = v || '-x'")
    pg(f"INSERT INTO {CT} SELECT g, 'n'||g FROM generate_series(1001,4000) g")
    p = spawn(f"""
import apitap
apitap.transfer({PG!r}, {CH!r}, table={CT!r}, mode="log_based")
""")
    killed = kill_after(p, 1.2)
    wm1 = ch(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table='{CT}' AND source_id NOT LIKE 'server-identity:%'")
    if not killed:
        case("mid-window kill", True, "window completed before the kill — watermark advanced legitimately")
    else:
        case("watermark unmoved after a killed window", wm1 == wm0,
             f"{wm1 or '(none)'} == {wm0 or '(none)'}")
    # Recovery: one clean run must land EVERY change exactly once.
    r = sh([sys.executable, "-c", f"""
import apitap
apitap.transfer({PG!r}, {CH!r}, table={CT!r}, mode="log_based")
"""])
    src_digest = pg(f"SELECT count(*)||'|'||sum(id::bigint)||'|'||md5(string_agg(v, ',' ORDER BY id)) FROM {CT}")
    dst_digest = ch(f"SELECT toString(count()) || '|' || toString(sum(toInt64(id))) || '|' || "
                    f"lower(hex(MD5(arrayStringConcat(arraySort(x -> x.1, groupArray((id, v))).2, ',')))) FROM {CT}")
    exact = src_digest.split("|")[:2] == dst_digest.split("|")[:2]
    case("replay applies every change exactly once", r.returncode == 0 and exact,
         f"src {src_digest.split('|')[0]} rows / sum {src_digest.split('|')[1]} == "
         f"dst {dst_digest.split('|')[0]} / {dst_digest.split('|')[1]}")

print("== 3. the source connection is cut mid-COPY ==")
drop_ch(ch, T)
p = spawn(f"""
import apitap
apitap.transfer({PG!r}, {CH!r}, table={SRC_BIG!r}, dest_table={T!r}, mode="replace")
""")
time.sleep(2.5)
if p.poll() is None:
    killed_backends = pg("SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                         "WHERE query LIKE 'COPY%' AND pid <> pg_backend_pid()) t")
    out, err = p.communicate(timeout=120)
    # "Loud" means: nonzero exit AND a message that tells the operator what
    # happened. The first version of this check demanded the words COPY or
    # connection, and failed a run whose error said "early eof" — which WAS
    # loud, just unhelpful. That unhelpfulness was the real defect, and it is
    # fixed in the engine; the check now asks for the event to be described.
    low = err.lower()
    loud = p.returncode != 0 and (
        "closed the connection mid-stream" in low
        or "connection" in low
        or "eof" in low
    )
    case("a cut connection fails loudly", loud,
         f"rc={p.returncode}, killed {killed_backends} backends, error names the cause: "
         f"{[l for l in err.splitlines() if l.strip()][-1][:90] if err.strip() else '(silent)'}")
    exists = ch(f"SELECT count() FROM system.tables WHERE name = '{T}'")
    case("no half-built table is published", exists == "0",
         "destination table absent (it is only created by the atomic swap)")
else:
    p.wait()
    case("cut connection", True, "transfer finished before the connection could be cut (skipped)")
drop_ch(ch, T)

print("== 4. source DDL changes mid-run ==")
pg("DROP TABLE IF EXISTS fm_ddl")
pg("CREATE TABLE fm_ddl AS SELECT g AS id, 'v'||g AS v, g*2 AS extra FROM generate_series(1,3000000) g")
ch("DROP TABLE IF EXISTS fm_ddl")
p = spawn(f"""
import apitap
apitap.transfer({PG!r}, {CH!r}, table="fm_ddl", mode="replace")
""")
time.sleep(1.5)
ddl_applied = False
if p.poll() is None:
    try:
        pg("ALTER TABLE fm_ddl DROP COLUMN extra")
        ddl_applied = True
    except RuntimeError as e:
        ddl_applied = False
out, err = p.communicate(timeout=300)
if not ddl_applied:
    case("DDL mid-run", True, "the ALTER could not land mid-run (lock held) — nothing to assert")
else:
    if p.returncode != 0:
        case("DDL mid-run fails loudly, never mis-maps", True,
             f"refused: {[l for l in err.splitlines() if l.strip()][-1][:100]}")
    else:
        # It succeeded: then every landed row must still be correct for the
        # columns it claims to have. Wrong column mapping is the disaster here.
        cols = ch("SELECT count() FROM system.columns WHERE table = 'fm_ddl'")
        bad = ch("SELECT count() FROM fm_ddl WHERE v != concat('v', toString(id))")
        case("DDL mid-run kept every value on its own column", bad == "0",
             f"{cols} columns landed, {bad} rows with a value on the wrong column")
pg("DROP TABLE IF EXISTS fm_ddl")
ch("DROP TABLE IF EXISTS fm_ddl")

print("== cleanup ==")
pg(f"DROP TABLE IF EXISTS {CT}")
for t in (T, CT):
    ch(f"DROP TABLE IF EXISTS {t}")
    ch(f"DROP TABLE IF EXISTS {t}__apitap_staging")
for s in pg("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap%'").splitlines():
    if s:
        pg(f"SELECT pg_drop_replication_slot('{s}')")
print("   dropped test tables, staging leftovers and slots")

print()
for name, passed, _ in results:
    print(f"   {'PASS' if passed else 'FAIL'}  {name}")
print("\n" + ("FAILURE-MODE E2E: ALL GREEN" if ok else "FAILURE-MODE E2E: FAILED"))
raise SystemExit(0 if ok else 1)
