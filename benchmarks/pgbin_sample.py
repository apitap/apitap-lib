"""Walsender CPU sampler — polls /proc inside the pg bench container.

Every 0.25 s: list processes whose cmdline contains 'walsender', read
utime+stime from /proc/<pid>/stat. Keeps the MAX per pid (CPU is monotonic)
and rewrites the output file atomically each tick, so whenever the caller
kills this process the file already holds the final totals:

    total_cpu_seconds <sum>
    pid <pid> <cpu_seconds>

One docker exec per tick (~20-50 ms) — cheap against a 30 s+ drain, and the
last sample trails the walsender's exit by at most one tick.
"""
import os
import subprocess
import sys
import time

CONTAINER = "apitap-bench-pg-src"
OUT = sys.argv[1]
CLK_TCK = 100.0

SNIPPET = (
    "for p in /proc/[0-9]*; do "
    "grep -aq walsender $p/cmdline 2>/dev/null && "
    "echo -n \"${p#/proc/} \" && cat $p/stat; done; true"
)

seen = {}  # pid -> max cpu seconds
while True:
    try:
        out = subprocess.run(
            ["docker", "exec", CONTAINER, "sh", "-c", SNIPPET],
            capture_output=True, text=True, timeout=10,
        ).stdout
    except Exception:
        out = ""
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 16:
            continue
        pid = parts[0]
        # /proc/stat fields after the parenthesised comm: utime is field 14,
        # stime 15 (1-based). comm '(postgres)' has no spaces here, so the
        # prefix is: pid_line_pid, stat_pid, (comm), state, then numbers.
        try:
            close = line.rindex(")")
            rest = line[close + 1 :].split()
            utime, stime = int(rest[11]), int(rest[12])
        except (ValueError, IndexError):
            continue
        cpu = (utime + stime) / CLK_TCK
        if cpu > seen.get(pid, 0.0):
            seen[pid] = cpu
    tmp = OUT + ".tmp"
    with open(tmp, "w") as f:
        f.write(f"total_cpu_seconds {sum(seen.values()):.2f}\n")
        for pid, cpu in sorted(seen.items()):
            f.write(f"pid {pid} {cpu:.2f}\n")
    os.replace(tmp, OUT)
    time.sleep(0.25)
