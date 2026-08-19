"""Progress reporting — the same transfer seen from three places it runs in.

A transfer that moves half a billion rows used to print nothing until it was
over. This checks the fix in the environments that matter: a terminal, an
orchestrator that captures a pipe (Airflow, Kubernetes, docker logs, cron),
and a log collector that wants JSON.

What must hold everywhere:
  1. lines appear WHILE the transfer runs, not only at the end — a pipe that
     buffers until exit is the classic container trap, and it is worse than no
     logging because the operator thinks the job is hung;
  2. the closing row count equals TransferReport.rows exactly — progress must
     never publish a second, disagreeing tally;
  3. a captured pipe gets no ANSI escapes and no carriage returns, or the
     orchestrator's log view fills with garbage;
  4. APITAP_PROGRESS=0 buys back the old silence.
"""
import json
import os
import re
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "progress_demo"

# A child process is the point: progress is written by the engine to stderr,
# so it can only be observed from outside.
CHILD = """
import apitap, sys
r = apitap.transfer({pg!r}, {ch!r}, table="bench_data_1m",
                    dest_table={t!r}, mode="replace")
print("REPORT_ROWS=%d" % r.rows)
""".format(pg=PG, ch=CH, t=T)


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-Atc", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True).stdout.strip()


def run(env_extra, interval="0.3"):
    """Run the transfer in a child with stderr captured — i.e. NOT a terminal,
    exactly like Airflow/K8s see it. Returns (stdout, stderr, seconds)."""
    env = dict(os.environ, APITAP_PROGRESS_INTERVAL=interval, **env_extra)
    ch(f"DROP TABLE IF EXISTS {T}")
    t0 = time.time()
    p = subprocess.run([sys.executable, "-c", CHILD], capture_output=True,
                       text=True, env=env)
    if p.returncode:
        raise RuntimeError(p.stderr[-2000:])
    return p.stdout, p.stderr, time.time() - t0


def _slots_now():
    return set(pg("SELECT slot_name FROM pg_replication_slots").split())


# Slots that existed BEFORE this leg started. Anything else is ours.
#
# The blanket `SELECT pg_drop_replication_slot(slot_name) ... WHERE NOT active`
# that used to be in the cleanup is a live grenade on a shared rig: a CDC job
# that is merely BETWEEN drains has an inactive slot, and dropping it destroys
# its WAL continuity. It took out a running 24 h soak on 2026-08-20.
_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for _s in sorted(_slots_now() - _SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{_s}') FROM pg_replication_slots "
           f"WHERE slot_name='{_s}' AND NOT active")


ok = True

print("== captured pipe (Airflow / Kubernetes / docker logs) ==")
out, err, wall = run({})
lines = [l for l in err.splitlines() if l.startswith("2") and "apitap" in l]
report_rows = re.search(r"REPORT_ROWS=(\d+)", out).group(1)
if len(lines) >= 2:
    print(f"   ✓ {len(lines)} lines during a {wall:.1f}s run, not one dump at the end")
    print(f"     {lines[0]}")
    print(f"     {lines[-1]}")
else:
    ok = False
    print(f"   ✗ only {len(lines)} progress line(s) — the pipe never showed live output")
if any("\x1b" in l or "\r" in l for l in err.splitlines()):
    ok = False
    print("   ✗ ANSI escapes or carriage returns leaked into a captured pipe")
else:
    print("   ✓ no ANSI, no carriage returns — safe for a log viewer")
done = [l for l in lines if " done " in l]
if done and f"rows={report_rows}" in done[-1]:
    print(f"   ✓ closing line agrees with TransferReport: rows={report_rows}")
else:
    ok = False
    print(f"   ✗ closing line disagrees with TransferReport (rows={report_rows}): {done[-1:]}")

print("== JSON mode (Loki / Fluentd / Airflow parsers) ==")
_, err, _ = run({"APITAP_PROGRESS": "json"})
objs = []
for line in err.splitlines():
    line = line.strip()
    if line.startswith("{"):
        try:
            objs.append(json.loads(line))
        except json.JSONDecodeError:
            ok = False
            print(f"   ✗ a line starting with {{ is not valid JSON: {line[:120]}")
# Shapes differ BY EVENT, which is what `event` is for: a progress record
# carries rows/elapsed_s, a note carries the note. Requiring every object to
# look like a progress record would forbid the engine from ever saying
# anything else — and did, the first time it had something to say.
def well_formed(o):
    if "event" not in o:
        return False
    if o["event"] == "transfer.note":
        return "note" in o
    return "rows" in o and "elapsed_s" in o


progress_objs = [o for o in objs if o.get("event") != "transfer.note"]
if objs and all(well_formed(o) for o in objs) and progress_objs:
    last = progress_objs[-1]
    print(f"   ✓ {len(objs)} JSON objects, one per line; last event={last['event']} "
          f"rows={last['rows']} bytes={last['bytes']}")
    if last["event"] != "transfer.done":
        ok = False
        print("   ✗ the final object is not transfer.done")
else:
    ok = False
    print(f"   ✗ JSON output missing or malformed ({len(objs)} objects)")

print("== operational gauges are NUMBERS, not prose ==")
# The WAL a replication slot holds on the SOURCE is the one number an
# unattended pipeline must be able to alert on — it is the difference between
# "a schedule paused" and "the source's disk filled up". It used to exist only
# inside an English sentence, and its warning form bypassed this channel
# entirely with a raw eprintln!, so in JSON mode the most important line was
# the only one that was not JSON.
# The gauge only exists on a CDC run, and `run()` above drives a bulk one —
# so this leg brings its own child rather than bending that helper.
GT = "prog_gauge"
pg(f"DROP TABLE IF EXISTS {GT}")
pg(f"CREATE TABLE {GT} (id int primary key, v text)")
pg(f"INSERT INTO {GT} SELECT g, 'v'||g FROM generate_series(1,200) g")
ch(f"DROP TABLE IF EXISTS {GT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{GT}' "
   "SETTINGS mutations_sync=1")
gp = subprocess.run(
    [sys.executable, "-c",
     "import apitap\n"
     f"apitap.transfer({PG!r}, {CH!r}, table={GT!r}, mode='log_based')\n"
     f"apitap.transfer({PG!r}, {CH!r}, table={GT!r}, mode='log_based')\n"],
    capture_output=True, text=True,
    env=dict(os.environ, APITAP_PROGRESS="json"))
gerr = gp.stderr
gauges = []
for line in gerr.splitlines():
    line = line.strip()
    if line.startswith("{"):
        try:
            o = json.loads(line)
        except json.JSONDecodeError:
            continue
        if o.get("event") == "slot.wal":
            gauges.append(o)
if not gauges:
    ok = False
    print("   ✗ no slot.wal gauge in the JSON stream")
else:
    g = gauges[-1]
    numeric = isinstance(g.get("retained_bytes"), (int, float))
    print(f"   ✓ slot.wal gauge present: retained_bytes={g.get('retained_bytes')} "
          f"warn_at_bytes={g.get('warn_at_bytes')}")
    if not numeric:
        ok = False
        print("   ✗ retained_bytes is not a number — nothing can alert on it")
    # And every line is still JSON: the warning must not slip out beside it.
    stray = [l for l in gerr.splitlines()
             if l.strip() and not l.strip().startswith("{")]
    if stray:
        ok = False
        print(f"   ✗ {len(stray)} non-JSON line(s) in JSON mode: {stray[0][:100]}")
    else:
        print("   ✓ every line in JSON mode is JSON, warnings included")

pg(f"DROP TABLE IF EXISTS {GT}")
drop_our_slots()
ch(f"DROP TABLE IF EXISTS {GT}")

print("== APITAP_PROGRESS=0 restores silence ==")
_, err, _ = run({"APITAP_PROGRESS": "0"})
noise = [l for l in err.splitlines() if "apitap" in l and ("progress" in l or "done" in l)]
if not noise:
    print("   ✓ nothing printed")
else:
    ok = False
    print(f"   ✗ {len(noise)} line(s) printed while switched off")

print("== the numbers are the transfer's own, not a second tally ==")
out, err, _ = run({"APITAP_PROGRESS": "json"})
report_rows = int(re.search(r"REPORT_ROWS=(\d+)", out).group(1))
final = [json.loads(l) for l in err.splitlines() if l.strip().startswith("{")][-1]
landed = int(ch(f"SELECT count() FROM {T}"))
if report_rows == final["rows"] == landed:
    print(f"   ✓ progress, TransferReport and ClickHouse all say {landed:,}")
else:
    ok = False
    print(f"   ✗ disagreement — progress={final['rows']} report={report_rows} clickhouse={landed}")

ch(f"DROP TABLE IF EXISTS {T}")
print("\n" + ("PROGRESS E2E: ALL GREEN" if ok else "PROGRESS E2E: FAILED"))
raise SystemExit(0 if ok else 1)
