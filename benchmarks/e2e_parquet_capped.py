#!/usr/bin/env python3
"""E1 (0.57.0 §2.D): parquet pipes fit the cage they are planned into.

0.56.0 priced every pipe at 10 × chunk_bytes and knew nothing about a parquet
loader's builders, part buffer and page, so a capped multi-core run was planned
into memory it did not have (audit §3.15): (128m, 2) planned 4 × 2 MiB × rg24
and (256m, 4) planned 8 × 2 MiB × rg24 — peaks above the cage. The 0.57.0
planner prices the loader's residency (`PER_ROW_GROUP` row groups per pipe) and
shrinks/gives up pipes until the model fits.

This leg is the calibration: with the brief's `PER_ROW_GROUP = 2` the fitted
(2 MiB, rg4, 2 pipes) plan for (128m, 2) still peaked 134 MB in a 128 MB cage
(model 126) — the row group's own transient is real, not free. The constant is
3, which sends the 128 MiB tier to its measured-safe one-pipe plan (4 MiB /
rg8, 75 MB peak) and keeps two pipes at the headline tier on rg8.

Each cell runs one transfer into MinIO inside `docker run --memory=X
--memory-swap=X --cpus=Y`, asks DOCKER for OOMKilled/exit, asks the KERNEL for
cgroup memory.peak, asks MINIO (through DuckDB httpfs) for the row total, and
asks the RUN's own `[plan]` line for the pipe count. A part file exists exactly
once per loader, so the part count is a server fact for the plan.

The (256m, 0.5) cell is the headline regression guard: it must be green on the
0.56.0 wheel too. 0.56.0's `num_cpus` honours the 0.5-core quota (ceil → 1), so
it plans (4 MiB, rg24, 2 pipes); this release plans (4 MiB, rg8, 2 pipes). Both
run inside the cage.

Requires: iceberg (MinIO at 127.0.0.1:9100), >=4 host cores.
Usage: e2e_parquet_capped.py
"""
import os
import re
import shlex
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _rig

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DEST = ("s3://apitap-bench/e2e_cap?format=parquet&endpoint=http://127.0.0.1:9100"
        "&access_key_id=bench&secret_access_key=benchbench")
TABLE = "e2e_cap_src"
DEST_PREFIX = f"e2e_cap/{TABLE}/"
STAGING_PREFIX = f"e2e_cap/{TABLE}__apitap_staging/"
# (docker memory, --cpus, planned pipes, planned row-group MiB or None = any).
# The asks: 2 cores → 2×2 = 4; 4 cores → 8; 0.5 → ceil(0.5) = 1, so
# to_bq_parallel(1) = 2. With PER_ROW_GROUP = 3: (128m, ask 4) → 1 pipe / rg8;
# (256m, ask 8) → 4 pipes / rg4. The 0.56.0 wheel plans 4 / 8 pipes at rg24
# and breaches those two cages, so their rung is asserted.
#
# (256m, 0.5) is the headline regression guard and must be green on BOTH
# wheels: 0.56.0 plans 2 × 4 MiB × rg24 (peak ~173 MB), this release 2 ×
# 4 MiB × rg8 (peak ~151 MB). The pipe count is the two-sided fact; the rung
# is the release's own calibration and is not asserted.
CELLS = [
    ("128m", "2", 1, 8),
    ("256m", "4", 4, 4),
    ("256m", "0.5", 2, None),
]
PEAK_FRACTION = 0.90
PART = re.compile(r"/part-\d{5}\.parquet$")
PLAN = re.compile(r"\[plan\] chunk=(\d+)KiB row_group=(\d+)MiB pipes=(\d+)")


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def cleanup():
    """Drop everything this leg publishes or stages (the seed stays)."""
    for prefix in (DEST_PREFIX, STAGING_PREFIX):
        for key in _rig.s3_list(prefix):
            _rig.s3_delete(key)


def seed():
    if int(src(f"SELECT count(*) FROM pg_class WHERE relname='{TABLE}'")) != 0:
        return
    print(f"seeding {TABLE}: 10M rows (kept for the next run)")
    src(f"CREATE TABLE {TABLE} (id bigint PRIMARY KEY, v text, n bigint)")
    src(f"INSERT INTO {TABLE} SELECT g, md5(g::text), g*7 "
        f"FROM generate_series(1, 10000000) g")
    src(f"ANALYZE {TABLE}")


_duck = None


def duck():
    global _duck
    if _duck is None:
        import duckdb
        _duck = duckdb.connect()
        _duck.execute("INSTALL httpfs; LOAD httpfs;")
        _duck.execute(
            "SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; "
            "SET s3_url_style='path'; SET s3_access_key_id='bench'; "
            "SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    return _duck


def run_cell(mem, cpus, image, name):
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {DEST!r}, table={TABLE!r})\n"
            "print('rows', r.rows)")
    script = (f"python -c {shlex.quote(code)}; "
              "echo MEMPEAK=$(cat /sys/fs/cgroup/memory.peak)")
    r = sh(["docker", "run", "--name", name, "--network", "host",
            f"--memory={mem}", f"--memory-swap={mem}", f"--cpus={cpus}",
            "-v", f"{site_packages()}:/py:ro", "-e", "PYTHONPATH=/py",
            "-e", "APITAP_DEBUG=1", image, "sh", "-c", script])
    state = sh(["docker", "inspect", "-f",
                "{{.State.OOMKilled}} {{.State.ExitCode}}", name]).stdout.strip()
    sh(["docker", "rm", name])
    return r, state


# Chosen at start so the mount matches the interpreter RUNNING this leg; the
# wheel is abi3, so a matching major.minor image is all that is needed.
SITE_PACKAGES = None
IMAGE = None


def site_packages():
    global SITE_PACKAGES
    if SITE_PACKAGES is None:
        import apitap
        SITE_PACKAGES = os.path.dirname(os.path.dirname(os.path.abspath(apitap.__file__)))
    return SITE_PACKAGES


def cell(mem, cpus, want_pipes, want_rg):
    cap = int(mem[:-1]) * (1 << 20)
    name = f"apitap-bench-e2epc-{mem}-{cpus.replace('.', 'p')}"
    cleanup()
    print(f"\n-- cell {mem} / {cpus} cpu (want {want_pipes} pipes, rg{want_rg})")
    r, state = run_cell(mem, cpus, IMAGE, name)
    out = (r.stdout or "") + (r.stderr or "")
    plan = PLAN.search(out)
    peak_m = re.search(r"MEMPEAK=(\d+)", r.stdout or "")
    st = state.split()
    oom, rc = st if len(st) == 2 else (state, "?")
    if oom != "false" or rc != "0":
        if plan:
            print(f"   plan chunk={plan.group(1)}KiB row_group={plan.group(2)}MiB "
                  f"pipes={plan.group(3)}")
        if peak_m:
            print(f"   memory.peak={int(peak_m.group(1)) / 1048576:.0f}MB "
                  f"(cage {cap / 1048576:.0f}MB)")
        return False, (f"docker says OOMKilled={oom} exit={rc} — the cage was "
                       "breached")
    if peak_m is None:
        print(out[-2000:])
        return False, f"the run printed no MEMPEAK (docker state {state})"
    peak = int(peak_m.group(1))
    if plan is not None:
        chunk_kib, rg, pipes = (int(x) for x in plan.groups())
        print(f"   plan chunk={chunk_kib}KiB row_group={rg}MiB pipes={pipes} "
              f"model output; docker: {state}; memory.peak={peak / 1048576:.0f}MB")
        if pipes != want_pipes or (want_rg is not None and rg != want_rg):
            want = f"{want_pipes} pipes" + ("" if want_rg is None else f" / rg{want_rg}")
            return False, f"planned (pipes={pipes}, rg={rg}MiB), want {want}"
    else:
        # Pre-0.57 wheels print no plan line. The two-sided cell then judges
        # only the facts a 0.56 wheel can state: no OOM, the kernel peak, the
        # parts it wrote. A cell that asserts the planned rung requires it.
        if want_rg is not None:
            print(out[-1500:])
            return False, "no [plan] line, and this cell asserts the planned rung"
        rg, pipes = None, want_pipes
        print(f"   no [plan] line (pre-0.57 wheel); memory.peak={peak / 1048576:.0f}MB")
    if peak > PEAK_FRACTION * cap:
        return False, (f"memory.peak {peak / 1048576:.0f}MB > "
                       f"{PEAK_FRACTION:.0%} of {cap / 1048576:.0f}MB — raise "
                       "PER_ROW_GROUP (bqparquet.rs) and re-derive T2/T3")
    listing = _rig.s3_list(DEST_PREFIX)
    parts = [k for k in listing if PART.search(k)]
    if len(parts) != pipes or len(parts) != len(listing):
        return False, (f"{len(parts)} part file(s) for {pipes} planned pipes; "
                       f"listing={listing[:8]}")
    left = _rig.s3_list(STAGING_PREFIX)
    if left:
        return False, f"staging not empty after the run: {left[:8]}"
    q = (f"SELECT count(*)||'|'||sum(id)||'|'||sum(n) "
         f"FROM read_parquet('s3://apitap-bench/{DEST_PREFIX}part-*.parquet')")
    got = "|".join(str(x) for x in duck().execute(q).fetchone())
    want = src(f"SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM {TABLE}")
    if got != want:
        return False, f"DuckDB readback {got} != source {want}"
    print(f"   OK: {len(parts)} parts, peak {peak * 100 // cap}% of cage, DuckDB == source")
    return True, ""


def main():
    if (os.cpu_count() or 1) < 4:
        print(f"rig: needs ≥4 host cores ({os.cpu_count()}) — FAILED", flush=True)
        return 2
    global IMAGE
    IMAGE = f"python:{sys.version_info.major}.{sys.version_info.minor}-slim"
    if sh(["docker", "image", "inspect", IMAGE]).returncode != 0:
        print(f"pulling {IMAGE} …")
        if sh(["docker", "pull", IMAGE]).returncode != 0:
            _rig.rig_fail(f"cannot get {IMAGE}")
    if sh(["docker", "container", "inspect", "apitap-bench-minio"]).returncode != 0:
        _rig.rig_fail("MinIO (apitap-bench-minio) is not running")
    seed()
    bad = []
    try:
        for mem, cpus, want_pipes, want_rg in CELLS:
            ok, why = cell(mem, cpus, want_pipes, want_rg)
            if not ok:
                bad.append(f"{mem}/{cpus}: {why}")
                print(f"   FAILED: {why}")
    finally:
        cleanup()
    if bad:
        print("\nE1 PARQUET CAPPED: FAILED")
        for b in bad:
            print(f"  !! {b}")
        return 1
    print("\nE1 PARQUET CAPPED: ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
