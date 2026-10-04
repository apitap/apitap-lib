#!/usr/bin/env bash
# What each tool pays inside the cage BEFORE it moves a single row: the floor.
# Same container, same cap, no work at all.
#   sh bench-capped-pg-ch-probes.sh import apitap
#   sh bench-capped-pg-ch-probes.sh import ws
set -u

t0=$(date +%s%N)
case "${1:-}" in
import)
    case "${2:-}" in
    apitap) python -c 'import apitap; print("APITAP_VERSION", apitap.__version__)' ;;
    ws)     walshadow-stream --version ;;
    *) echo "usage: $0 import apitap|ws" >&2; exit 2 ;;
    esac
    ;;
*) echo "usage: $0 import apitap|ws" >&2; exit 2 ;;
esac
rc=$?
echo "WALL $(awk -v a="$t0" -v b="$(date +%s%N)" 'BEGIN{printf "%.3f", (b-a)/1e9}')"
echo "MEMPEAK=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
echo "MEMSTAT=$(grep -E '^(anon|file|shmem) ' /sys/fs/cgroup/memory.stat 2>/dev/null | tr '\n' ' ')"
exit $rc