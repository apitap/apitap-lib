#!/usr/bin/env python3
"""Sum the engine's own applied-event counters over a byte range of a drain log.

    win_events.py LOG BYTE0 BYTE1

APITAP_DEBUG=1 makes the engine print, per applied window:

    [log_based] applied lsn=... events=N in X.Xs

The instrumented windows bracket the drain log by byte offset, so this is the
client-side truth for "changes applied inside the window" independent of the
drain pass boundaries.
"""
import re
import sys

log, o0, o1 = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
applied = re.compile(r"\[log_based\] applied lsn=(\S+) events=(\d+) in ([\d.]+)s")
windows = re.compile(r"\[log_based\] window=\d+ tables=(\d+) drain=([\d.]+)s "
                     r"events=(\d+) budget_hit=(\w+)")
ev = 0
n = 0
last_lsn = None
with open(log, "rb") as f:
    f.seek(o0)
    chunk = f.read(max(0, o1 - o0)).decode("utf-8", "replace")
for line in chunk.splitlines():
    m = applied.search(line)
    if m:
        ev += int(m.group(2))
        n += 1
        last_lsn = m.group(1)
w = 0
w_ev = 0
w_drain = 0.0
for line in chunk.splitlines():
    m = windows.search(line)
    if m:
        w += 1
        w_ev += int(m.group(3))
        w_drain += float(m.group(2))
print(f"applied_lines={n} applied_events={ev} last_lsn={last_lsn} "
      f"window_lines={w} window_events={w_ev} window_drain_s={w_drain:.3f}")
