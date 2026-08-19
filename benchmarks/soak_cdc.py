"""A long CDC soak: does anything grow that should not?

The longest measured apitap run before this was about two minutes of apply work.
Everything the failure-modes page says about a scheduled CDC job is therefore an
argument, not a measurement — file descriptors, replication-slot retention,
state-table accumulation and watermark drift are all DURATION bugs, and none of
them can be seen in two minutes.

The shape mirrors how apitap is actually run: a fresh process per drain, on a
schedule, against a source that is being written to the whole time. Nothing here
is a daemon, because apitap has no daemon mode and testing one would be testing
a thing nobody runs.

Every drain reports its OWN peak RSS from inside the child. That is not a detail:
`RUSAGE_CHILDREN.ru_maxrss` in this parent is a high-water mark over every child
it has ever reaped, so across thousands of runs it can only ever rise and would
manufacture exactly the leak this soak exists to look for.

Every N runs the writer is paused, the drain is allowed to catch up, and the two
sides are compared exactly — count AND sum(id) AND sum(touched). Checking only at
the end would tell you that drift happened without telling you when.

  CSV: /home/ubuntu/soak-<table>.csv   one row per drain
  log: whatever you redirect stdout to

Env: SOAK_HOURS (default 24), SOAK_EVERY (seconds between drains, default 30),
     SOAK_VERIFY_EVERY (drains between exact checks, default 30).
"""
import csv
import os
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
PG_C = "apitap-bench-pg-src"
DB = "apitap_bench_src"
T = os.environ.get("SOAK_TABLE", "soak_cdc")
STOP = "/tmp/soak.stop"
PAUSE = "/tmp/soak.pause"
HOURS = float(os.environ.get("SOAK_HOURS", "24"))
EVERY = float(os.environ.get("SOAK_EVERY", "30"))
VERIFY_EVERY = int(os.environ.get("SOAK_VERIFY_EVERY", "30"))
CSV_PATH = f"/home/ubuntu/soak-{T}.csv"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres", "-d", DB, "-Atc", sql])
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def i(v, d=-1):
    try:
        return int(v)
    except (TypeError, ValueError):
        return d


DRAIN = f"""
import os, resource, sys, apitap
r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')
# Read from INSIDE the child: this process's own high-water mark, not the
# parent's running maximum over every child it ever reaped.
rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
try:
    fds = len(os.listdir('/proc/self/fd'))
except OSError:
    fds = -1
print(f"RESULT {{r.rows}} {{r.elapsed_ms}} {{rss:.0f}} {{fds}}")
"""


def drain():
    t0 = time.time()
    o = sh([sys.executable, "-c", DRAIN])
    wall = time.time() - t0
    for line in o.stdout.splitlines():
        if line.startswith("RESULT "):
            _, rows, ms, rss, fds = line.split()
            return dict(ok=1, rows=i(rows), ms=i(ms), rss=float(rss), fds=i(fds),
                        wall=wall, err="")
    return dict(ok=0, rows=-1, ms=-1, rss=-1.0, fds=-1, wall=wall,
                err=o.stderr.strip().replace("\n", " ")[-300:])


def sample():
    """Server-side state — the part a fresh process per run cannot hide."""
    slots = pg("SELECT count(*) FROM pg_replication_slots")
    wal = pg("SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)),0)::bigint "
             "FROM pg_replication_slots")
    inactive = pg("SELECT count(*) FROM pg_replication_slots WHERE NOT active")
    backends = pg(f"SELECT count(*) FROM pg_stat_activity WHERE datname='{DB}'")
    src_n = pg(f"SELECT count(*) FROM {T}")
    dst_n = ch(f"SELECT count() FROM {T}")
    state_rows = ch(f"SELECT count() FROM _apitap_state WHERE dest_table='{T}'")
    sidecar = ch(f"SELECT count() FROM system.tables WHERE database='default' "
                 f"AND name='{T}__apitap_cdc_del'")
    return dict(slots=i(slots), slot_wal_bytes=i(wal), inactive_slots=i(inactive),
                pg_backends=i(backends), src_rows=i(src_n), dst_rows=i(dst_n),
                state_rows=i(state_rows), sidecar=i(sidecar))


def verify():
    """Quiesce, catch up, then compare both sides exactly.

    The writer is paused rather than stopped so the soak keeps its id stream.
    Without the pause the two counts are simply never equal and the check would
    have to be a fuzzy one, which is no check at all.
    """
    open(PAUSE, "w").close()
    try:
        time.sleep(2)
        for _ in range(6):
            d = drain()
            if not d["ok"]:
                return dict(verified=0, detail=d["err"])
            if i(pg(f"SELECT count(*) FROM {T}")) == i(ch(f"SELECT count() FROM {T}")):
                break
            time.sleep(1)
        s_n, d_n = pg(f"SELECT count(*) FROM {T}"), ch(f"SELECT count() FROM {T}")
        s_id = pg(f"SELECT coalesce(sum(id::bigint),0)::text FROM {T}")
        d_id = ch(f"SELECT sum(id) FROM {T}")
        s_t = pg(f"SELECT coalesce(sum(touched::bigint),0)::text FROM {T}")
        d_t = ch(f"SELECT sum(touched) FROM {T}")
        good = (s_n == d_n) and (s_id == d_id) and (s_t == d_t)
        return dict(verified=1 if good else 0,
                    detail=f"n {s_n}/{d_n} sum_id {s_id}/{d_id} sum_touched {s_t}/{d_t}")
    finally:
        os.path.exists(PAUSE) and os.remove(PAUSE)


# ---------------------------------------------------------------------------
for f in (STOP, PAUSE):
    if os.path.exists(f):
        os.remove(f)

print(f"soak: table={T} hours={HOURS} every={EVERY}s verify_every={VERIFY_EVERY}", flush=True)
pg(f"DROP TABLE IF EXISTS {T}")
pg(f"CREATE TABLE {T} (id int primary key, v text, touched int)")
pg(f"INSERT INTO {T} SELECT g, 'seed'||g, 0 FROM generate_series(1,1000) g")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"DROP TABLE IF EXISTS `{T}__apitap_cdc_del`")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")
pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active")

b = drain()
print(f"bootstrap: ok={b['ok']} rows={b['rows']} rss={b['rss']:.0f}MB {b['err']}", flush=True)

writer = subprocess.Popen([sys.executable, "benchmarks/soak_writer.py"],
                          cwd="/home/ubuntu/apitap-lib",
                          stdout=open("/home/ubuntu/soak-writer.log", "a"),
                          stderr=subprocess.STDOUT,
                          env=dict(os.environ, SOAK_TABLE=T))
print(f"writer pid {writer.pid}", flush=True)

cols = ["run", "iso", "elapsed_h", "ok", "rows", "ms", "wall_s", "rss_mb", "child_fds",
        "slots", "inactive_slots", "slot_wal_bytes", "pg_backends",
        "src_rows", "dst_rows", "state_rows", "sidecar", "verified", "detail", "err"]
fh = open(CSV_PATH, "w", newline="")
w = csv.DictWriter(fh, fieldnames=cols)
w.writeheader()

t_start = time.time()
run = 0
try:
    while (time.time() - t_start) < HOURS * 3600 and not os.path.exists(STOP):
        run += 1
        t_run = time.time()
        d = drain()
        s = sample()
        v = dict(verified="", detail="")
        if run % VERIFY_EVERY == 0:
            v = verify()
            s = sample()
        row = dict(run=run,
                   iso=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   elapsed_h=round((time.time() - t_start) / 3600.0, 3),
                   ok=d["ok"], rows=d["rows"], ms=d["ms"], wall_s=round(d["wall"], 2),
                   rss_mb=round(d["rss"], 1), child_fds=d["fds"],
                   err=d["err"], **s, **v)
        w.writerow(row)
        fh.flush()
        if not d["ok"] or v.get("verified") == 0:
            print(f"[run {run}] PROBLEM ok={d['ok']} verified={v.get('verified')} "
                  f"{v.get('detail','')} {d['err']}", flush=True)
        elif run % 20 == 0 or v.get("verified") == 1:
            print(f"[run {run} {row['elapsed_h']}h] rows={d['rows']} rss={d['rss']:.0f}MB "
                  f"fds={d['fds']} slots={s['slots']} wal={s['slot_wal_bytes']//1048576}MB "
                  f"state={s['state_rows']} src={s['src_rows']} dst={s['dst_rows']}"
                  + (f" VERIFIED {v['detail']}" if v.get("verified") == 1 else ""), flush=True)
        time.sleep(max(0.0, EVERY - (time.time() - t_run)))
finally:
    open(STOP, "w").close()
    try:
        writer.wait(timeout=30)
    except subprocess.TimeoutExpired:
        writer.kill()
    print("writer stopped; final catch-up and verify", flush=True)
    final = verify()
    print(f"FINAL verified={final['verified']} {final['detail']}", flush=True)
    fh.close()
    print(f"csv: {CSV_PATH}  runs: {run}", flush=True)
    print("SOAK: " + ("PASSED" if final["verified"] == 1 else "FAILED"), flush=True)
