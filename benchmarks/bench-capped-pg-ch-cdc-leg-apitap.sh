#!/usr/bin/env bash
# The apitap leg of the capped PostgreSQL -> ClickHouse CDC campaign, run INSIDE
# the capped container (--cpus=0.5 --memory=256m --memory-swap=256m):
#
#   catchup   ONE transfer call for all 30 tables in ONE CDC group — the
#             bootstrap (the pre-seeded source lands whole) and the group's first
#             drain. This is the headline number, comparable in shape to the bulk
#             arm's one-shot load.
#   drain     the same call again, on the established group: everything the slot
#             has accumulated since the last drain. run_twice=1 runs it a second
#             time in the SAME process, so an empty drain and a file-descriptor
#             count before/after are both observed across two runs of one process.
#
#   apitap.transfer(src, dst, tables=[...30...], mode="log_based")
#
# No knobs: no parallel=, no chunk_bytes, no slots=, no env tuning. One
# replication slot for the whole group is the default and the point of the test.
#
# The venv's site-packages is mounted READ-ONLY and put on PYTHONPATH, exactly
# like benchmarks/e2e_parquet_capped.py does, so the PyPI wheel is what runs and
# nothing is built here. The same mount makes the provenance assertable from
# inside the cage.
set -u

SRC="${APITAP_SRC:-postgres://postgres:bench@127.0.0.1:5548/bench}"
DST="${APITAP_DST:-clickhouse://default:bench@127.0.0.1:8128/default}"
MODE="${APITAP_MODE:-catchup}"
RUN_TWICE="${APITAP_RUN_TWICE:-0}"
# The campaign's thirty tables; the orchestrator passes them in so this script is
# never edited per run.
TABLES="${APITAP_TABLES:?APITAP_TABLES is required}"
# A background thread samples the descriptor count, so "no descriptor leak" is a
# measurement of the run rather than an assertion about it. 50 ms is inside the
# window between two of the run's own closes.
python - "$SRC" "$DST" "$MODE" "$RUN_TWICE" "$TABLES" <<'PY'
import hashlib, os, sys, threading, time

src, dst, mode, run_twice, tables = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1", sys.argv[5].split()
n_tables = len(tables)

t_imp = time.time()
import apitap
imp_s = time.time() - t_imp
so = apitap._apitap.__file__
print(f"APITAP_VERSION {apitap.__version__}", flush=True)
print(f"APITAP_SO_MD5 {hashlib.md5(open(so,'rb').read()).hexdigest()}", flush=True)
print(f"APITAP_TABLES {n_tables}", flush=True)

fd_dir = "/proc/self/fd"
fd_now = lambda: len(os.listdir(fd_dir))
fd_peak = fd_now()
stop = threading.Event()

def watch():
    global fd_peak
    while not stop.is_set():
        try:
            n = fd_now()
        except OSError:
            return
        if n > fd_peak:
            fd_peak = n
        stop.wait(0.05)

th = threading.Thread(target=watch, daemon=True)
th.start()

def drain(n):
    """One CDC transfer of the whole group; returns (rows, wall, per-table rows)."""
    t0 = time.time()
    try:
        r = apitap.transfer(src, dst, tables=tables, mode="log_based")
    except Exception as exc:                                  # noqa: BLE001
        print(f"RAISED {type(exc).__name__}: {exc}", flush=True)
        for t in getattr(getattr(exc, "report", None), "tables", ()) or ():
            print(f"  table {t.table}: rows={t.rows} err={t.error}", flush=True)
        raise SystemExit(9)
    wall = time.time() - t0
    print(f"DRAIN pass={n} rows={r.rows} wall_s={wall:.3f} budget={r.parallel} "
          f"tables_reporting={len(r.tables)} fd_now={fd_now()} fd_peak={fd_peak}", flush=True)
    # The per-table counters: one per table, never the group total handed to
    # every member (that bug shipped once and inflated a report 10x).
    per = sorted((t.table, t.rows) for t in r.tables)
    print(f"  per_table_rows {' '.join(f'{t}={v}' for t, v in per)}", flush=True)
    nz = sum(1 for _, v in per if v)
    print(f"  per_table_nonzero {nz} of {len(per)}  sum={sum(v for _, v in per)}", flush=True)
    print(f"FDCOUNT pass={n} before={fd_now()}", flush=True)
    return r.rows, wall, per

rows, wall, _ = drain(1)
print(f"FDCOUNT pass=1 after={fd_now()}", flush=True)
if run_twice:
    rows2, wall2, _ = drain(2)
    print(f"FDCOUNT pass=2 after={fd_now()}", flush=True)
    print(f"IDEMPOTENT second_drain_rows={rows2}", flush=True)
print(f"IMPORT_S {imp_s:.3f}", flush=True)
print(f"ELAPSED {wall:.3f}", flush=True)
print(f"MODE {mode}", flush=True)
stop.set()
th.join(timeout=1)
print(f"FD_PEAK {fd_peak}", flush=True)
print(f"FD_END {fd_now()}", flush=True)
PY
rc=$?
# The kernel's own peak for THIS container — read before the cgroup goes away.
echo "MEMPEAK=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
echo "MEMSTAT=$(grep -E '^(anon|file|shmem) ' /sys/fs/cgroup/memory.stat 2>/dev/null | tr '\n' ' ')"
echo "MEMEVENTS=$(grep -E '^(oom|oom_kill|high|max) ' /sys/fs/cgroup/memory.events 2>/dev/null | tr '\n' ' ')"
echo "EXITCODE $rc"
exit $rc