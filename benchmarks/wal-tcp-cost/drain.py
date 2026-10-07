#!/usr/bin/env python3
"""CDC drain driver for the wal-tcp-cost mission.

Runs inside the capped container (or on the host under a cgroup scope) and is
the apitap 0.58.0 wheel from PyPI, nothing built. Same output shape as
benchmarks/cdc-steady-profile/leg.sh so busyrate.sh parses it, plus
APITAP_DEBUG=1 turns on the engine's per-window/applied lines.

    drain.py SRC DST TABLE DEST_TABLE BUDGET_S ZERO_STOP TAG
"""
import hashlib
import os
import sys
import time

src, dst, table, dest_table, budget, zero_stop, tag = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
    float(sys.argv[5]), int(sys.argv[6]), sys.argv[7])

t_imp = time.time()
import apitap  # noqa: E402
imp_s = time.time() - t_imp
so = apitap._apitap.__file__
print(f"APITAP_VERSION {apitap.__version__}", flush=True)
print(f"APITAP_SO_MD5 {hashlib.md5(open(so, 'rb').read()).hexdigest()}", flush=True)
print(f"LEG_TAG {tag} pid={os.getpid()}", flush=True)


def cgroup_dir():
    try:
        for line in open("/proc/self/cgroup"):
            parts = line.strip().split(":", 2)
            if len(parts) == 3 and parts[0] == "0":
                return "/sys/fs/cgroup" + parts[2]
    except OSError:
        pass
    return "/sys/fs/cgroup"


CG = cgroup_dir()


def stat(key):
    try:
        for line in open(CG + "/cpu.stat"):
            if line.startswith(key):
                return int(line.split()[1])
    except OSError:
        return 0
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
        r = apitap.transfer(src, dst, table=table, dest_table=dest_table,
                            mode="log_based")
    except Exception as exc:  # noqa: BLE001
        print(f"RAISED {type(exc).__name__}: {exc}", flush=True)
        raise SystemExit(9)
    wall = time.time() - t0
    passes += 1
    total += r.rows
    print(f"DRAIN tag={tag} pass={passes} rows={r.rows} wall_s={wall:.3f} "
          f"budget={r.parallel} fd_now={len(os.listdir('/proc/self/fd'))}",
          flush=True)
    per = sorted((t.table, t.rows) for t in (r.tables or []))
    print(f"  per_table_rows {' '.join(f'{t}={v}' for t, v in per)}", flush=True)
    fair = time.time() - wall0
    cpu = (stat("usage_usec") - cpu0) / 1e6
    print(f"CPU_SAMPLER elapsed_s={fair:.3f} cpu_s={cpu:.3f} "
          f"cap_frac={cpu / (fair * float(os.environ.get('CPUS', '0.5'))):.3f}",
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
print(f"MEMPEAK={read_file(CG + '/memory.peak')}", flush=True)
print(f"MEMCURRENT={read_file(CG + '/memory.current')}", flush=True)
print(f"MEMEVENTS={read_file(CG + '/memory.events').replace(chr(10), ' ')}", flush=True)
print(f"CPUPressure={read_file(CG + '/cpu.pressure').replace(chr(10), ' ')}", flush=True)
print(f"IMPORT_S {imp_s:.3f}", flush=True)
