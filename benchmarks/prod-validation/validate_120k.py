#!/usr/bin/env python3
"""Validate apitap's 120k changes/s at half a core on a production-class box.

The bench VPS caps ~48-49k/s at 0.5 core because its per-core throughput is
~2.5x slower than production-class hardware (design 2026-10-08 sections
14.23/14.27: ~7.0 us/change there, ~3 us/change measured on GCP). This
script measures the same figure where it matters: your machine.

It runs the transfer inside its own transient cgroup limited to 50% of one
CPU, samples the process' own CPU time (the engine runs in-process), and
prints the verdict against the mandate: 120,000 changes/s at 0.5 core =
4.17 microseconds of CPU per change.

Usage:
    python3 validate_120k.py POSTGRES_URL CLICKHOUSE_URL SCHEMA.TABLE [SECONDS] [--no-cap]

Generate load in parallel with pgbench (see README.md). The table must
already exist on the destination (bootstrap runs first, then catch-up).

Requires: apitap installed, systemd user session (for the cgroup); without
systemd-run and without --no-cap it refuses to run, because an uncapped
number cannot answer the 0.5-core question.
"""

import os
import subprocess
import sys
import time

TARGET_US_PER_CHANGE = 4.17  # 120k/s on 0.5 core
CPU_QUOTA = "50%"


def _cpu_seconds() -> float:
    """utime+stime of this process, in seconds."""
    with open("/proc/self/stat", "rb") as f:
        parts = f.read().rsplit(b")", 1)[1].split()
    # After the comm field: ..., utime (14th overall), stime (15th).
    utime = float(parts[11])
    stime = float(parts[12])
    return (utime + stime) / os.sysconf("SC_CLK_TCK")


def _under_quota() -> bool:
    """Did the parent already re-exec us under the cgroup scope?"""
    return os.environ.get("APITAP_120K_QUOTA") == "1"


def _reexec_under_cgroup() -> None:
    env = dict(os.environ, APITAP_120K_QUOTA="1")
    cmd = [
        "systemd-run",
        "--user",
        "--scope",
        "--quiet",
        "-p",
        f"CPUQuota={CPU_QUOTA}",
        sys.executable,
        *sys.argv,
    ]
    try:
        sys.exit(subprocess.call(cmd, env=env))
    except FileNotFoundError:
        print(
            "error: systemd-run not found — cannot cage to 0.5 core; re-run "
            "with --no-cap only for a smoke test, never for the verdict",
            file=sys.stderr,
        )
        sys.exit(2)


def main() -> int:
    argv = [a for a in sys.argv[1:] if a != "--no-cap"]
    no_cap = "--no-cap" in sys.argv
    if len(argv) < 3:
        print(__doc__)
        return 2
    src, dst, table = argv[0], argv[1], argv[2]
    seconds = float(argv[3]) if len(argv) > 3 else 60.0

    if not no_cap and not _under_quota():
        _reexec_under_cgroup()
    if no_cap:
        print("warning: --no-cap — the verdict below is NOT a 0.5-core number", file=sys.stderr)

    import apitap

    # mode="log_based" is the whole point: the default is a bulk replace
    # (a first smoke ran a copy and measured nothing — 200k rows re-copied
    # in 0.4 s while the actual CDC never started).
    print(f"[validate] bootstrap {table} (log_based) ...", flush=True)
    apitap.transfer(src, dst, table, mode="log_based")

    # Catch-up + follow under the cage; the engine reads windows/knobs from
    # the environment, exactly as in production. `transfer` returns
    # (rows, elapsed_ms, parallel) — rows is the CHANGES this run applied,
    # the honest numerator for an update-heavy source (a destination
    # row-count does not move when pgbench UPDATEs rows).
    os.environ["APITAP_FOLLOW_SECS"] = str(int(seconds))
    print(f"[validate] catch-up/follow for {seconds:.0f}s (CPUQuota={CPU_QUOTA}) ...", flush=True)
    t0 = time.monotonic()
    cpu0 = _cpu_seconds()
    report = apitap.transfer(src, dst, table, mode="log_based")
    rows = report.rows
    wall = time.monotonic() - t0
    cpu = _cpu_seconds() - cpu0

    us_per_change = (cpu / rows * 1e6) if rows > 0 else float("inf")
    rate = rows / wall if wall > 0 else 0.0
    ok = (not no_cap) and 0 < us_per_change <= TARGET_US_PER_CHANGE
    print(
        f"[validate] changes={rows} wall_s={wall:.1f} cpu_s={cpu:.2f} "
        f"rate={rate:,.0f}/s cpu/change={us_per_change:.2f}us "
        f"target<={TARGET_US_PER_CHANGE}us -> {'PASS' if ok else 'FAIL'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
