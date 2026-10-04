#!/usr/bin/env bash
# Where does walshadow's memory go?
#
# The capped legs answer "it dies" but not "because of what", and the answer
# decides how the failure is allowed to be described. This runs the same leg with
# the memory limit REMOVED and samples, from inside the container, the per-process
# RSS of everything in it at two points, next to the cgroup's own accounting. If
# the daemon alone accounts for the peak, then the shadow PostgreSQL is not the
# thing that does not fit, and no amount of tuning a database into the cage would
# change the verdict.
#
#   bash bench-capped-pg-ch-attrib.sh [uncapped|512m|1g]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-pg-ch-0.57.sh"
CAP_SEL=${1:-uncapped}
export LEG_TIMEOUT=${LEG_TIMEOUT:-600}
# its own results file, so an instrumentation run never lands in the campaign's
export RESULTS=${RESULTS:-$WORK/results-attrib.txt}
ROUND=attrib-$CAP_SEL

case "$CAP_SEL" in
uncapped) export CAP="--cpus=0.5" ;;
*)         export CAP="--cpus=0.5 --memory=$CAP_SEL --memory-swap=$CAP_SEL" ;;
esac

echo "# attribution run: CAP=$CAP, round=$ROUND"
echo

# The leg runs in the background; this loop is the instrument.
bash "$H" leg wsdefault "$ROUND" > "/tmp/attrib-$ROUND.log" 2>&1 &
LEG=$!

n=0
while [[ $n -lt 90 ]]; do
    sleep 3
    if docker ps --format '{{.Names}}' | grep -qx "cmp-wsdefault-r${ROUND//-/_}"; then :; fi
    if docker ps --format '{{.Names}}' | grep -qx "cmp-wsdefault-r${ROUND}"; then
        n=$((n + 1))
        if [[ "$n" == "4" || "$n" == "12" || "$n" == "24" ]]; then
            echo "=== ~$((n * 3))s after container start ==="
            docker exec "cmp-wsdefault-r${ROUND}" \
                sh -c 'ps -eo pid,rss,vsz,comm --sort=-rss 2>/dev/null | head -14' 2>&1
            id=$(docker inspect -f '{{.Id}}' "cmp-wsdefault-r${ROUND}" 2>/dev/null)
            cg="/sys/fs/cgroup/system.slice/docker-${id}.scope"
            if [[ -r "$cg/memory.current" ]]; then
                printf 'cgroup memory.current: '; cat "$cg/memory.current"
                grep -E '^(anon|file|shmem) ' "$cg/memory.stat"
                printf 'cgroup memory.peak:   '; cat "$cg/memory.peak" 2>/dev/null
            fi
            echo
        fi
    fi
done
wait $LEG

echo "=== the leg ==="
grep -E 'stopped_by|wall_container_s|docker_state|cgroup_memory_peak_mb|host_sampled_peak_mb' \
    "/tmp/attrib-$ROUND.log"