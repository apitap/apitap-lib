#!/usr/bin/env bash
# The drain leg of the steady-profile rig, run INSIDE the capped container:
#
#   docker run --cpus=X --memory=256m --memory-swap=256m \
#       -v ~/gate-venv/lib/python3.13/site-packages:/py:ro -e PYTHONPATH=/py \
#       -e APITAP_SRC=... -e APITAP_DST=... -e BUDGET_S=... \
#       python:3.13-slim sh /job/leg.sh
#
# One bounded drain after another in ONE process (that is how a scheduled CDC
# job runs), printing per-pass rows/wall. Stops on BUDGET_S wall, or after
# ZERO_STOP consecutive empty passes, whichever is first. The wheel is mounted
# read-only from the venv; nothing is built here. cgroup cpu.stat and
# memory.peak are read inside, so the numbers belong to this container.
set -u

BUDGET_S="${BUDGET_S:-600}"
ZERO_STOP="${ZERO_STOP:-2}"
TAG="${APITAP_TAG:-leg}"

python3 - "$APITAP_SRC" "$APITAP_DST" "$APITAP_TABLE" "$BUDGET_S" "$ZERO_STOP" "$TAG" <<'PY'
import hashlib, os, sys, time
src, dst, table, budget, zero_stop, tag = (
    sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]), int(sys.argv[5]), sys.argv[6])
t_imp = time.time()
import apitap
imp_s = time.time() - t_imp
so = apitap._apitap.__file__
print(f"APITAP_VERSION {apitap.__version__}", flush=True)
print(f"APITAP_SO_MD5 {hashlib.md5(open(so,'rb').read()).hexdigest()}", flush=True)
print(f"LEG_TAG {tag} pid={os.getpid()}", flush=True)

def stat(key, f="/sys/fs/cgroup/cpu.stat"):
    try:
        for line in open(f):
            if line.startswith(key):
                return int(line.split()[1])
    except OSError:
        pass
    return 0

def read_file(p):
    try:
        return open(p).read().strip()
    except OSError:
        return ""

cpu0 = stat("usage_usec")
cpu_user0 = stat("user_usec")
cpu_sys0 = stat("system_usec")
wall0 = time.time()
nz = 0
passes = 0
total = 0
while True:
    t0 = time.time()
    try:
        r = apitap.transfer(src, dst,
                           tables=[t for t in table.split(",") if t], mode="log_based")
    except Exception as exc:                                  # noqa: BLE001
        print(f"RAISED {type(exc).__name__}: {exc}", flush=True)
        raise SystemExit(9)
    wall = time.time() - t0
    passes += 1
    total += r.rows
    print(f"DRAIN tag={tag} pass={passes} rows={r.rows} wall_s={wall:.3f} "
          f"budget={r.parallel} fd_now={len(os.listdir('/proc/self/fd'))}",
          flush=True)
    per = sorted((t.table, t.rows) for t in r.tables)
    print(f"  per_table_rows {' '.join(f'{t}={v}' for t, v in per)}", flush=True)
    fair = time.time() - wall0
    cpu = (stat("usage_usec") - cpu0) / 1e6
    print(f"CPU_SAMPLER elapsed_s={fair:.3f} cpu_s={cpu:.3f} cap_frac={cpu / (fair * float(os.environ.get('CPUS','0.5'))):.3f}",
          flush=True)
    if r.rows == 0:
        nz += 1
    else:
        nz = 0
    if fair >= budget or nz >= zero_stop:
        break

wall = time.time() - wall0
cpu = (stat("usage_usec") - cpu0) / 1e6
user = (stat("user_usec") - cpu_user0) / 1e6
syscpu = (stat("system_usec") - cpu_sys0) / 1e6
cpus = float(os.environ.get("CPUS", "0.5"))
print(f"LEG_DONE tag={tag} passes={passes} changes={total} wall_s={wall:.3f} "
      f"cpu_s={cpu:.3f} user_s={user:.3f} sys_s={syscpu:.3f} "
      f"cap_frac={cpu / (wall * cpus):.4f} avg_cores={cpu / wall:.4f}", flush=True)
print(f"MEMPEAK={read_file('/sys/fs/cgroup/memory.peak')}", flush=True)
print(f"MEMCURRENT={read_file('/sys/fs/cgroup/memory.current')}", flush=True)
print(f"MEMEVENTS={read_file('/sys/fs/cgroup/memory.events').replace(chr(10), ' ')}", flush=True)
print(f"CPUPressure={read_file('/sys/fs/cgroup/cpu.pressure').replace(chr(10), ' ')}", flush=True)
print(f"IMPORT_S {imp_s:.3f}", flush=True)
PY
rc=$?
echo "EXITCODE $rc"
exit $rc
