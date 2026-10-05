#!/usr/bin/env bash
# Busy-segment rate from a drain log: sum rows and wall over the passes that
# applied rows. Idle discovery passes (the convergence proof) are reported
# separately, never folded into the rate.
#   busyrate.sh <drain.log>
set -uo pipefail
LOG="${1:?drain log}"
awk '
/^DRAIN / {
  rows=""; wall=""
  for (i = 1; i <= NF; i++) {
    if ($i ~ /^rows=/)   { split($i, a, "="); rows = a[2] }
    if ($i ~ /^wall_s=/) { split($i, a, "="); wall = a[2] }
  }
  if (rows + 0 > 0) { tr += rows; tw += wall; n++ }
  else              { idle++; iw += wall }
}
END {
  printf "busy_passes=%d changes=%d busy_wall_s=%.3f busy_rate=%.1f/s idle_passes=%d idle_wall_s=%.3f\n",
         n, tr, tw, (tw > 0 ? tr / tw : 0), idle, iw
}' "$LOG"
grep -E "^LEG_DONE|^MEMPEAK|^RAISED|^EXITCODE" "$LOG" | tail -6
