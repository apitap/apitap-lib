#!/usr/bin/env bash
# Was the 0.5 CPU quota DELIVERED while the uncapped writer saturated the box?
#
# WINDOW S measured "the drain applied 0 changes in the ~78 s before PostgreSQL
# invalidated the slot", and there are two explanations that need different words:
# the drain was slow, or the drain never got its half core. This measures the
# second one directly, with no CDC state involved: the same CPU-bound loop in the
# same cage, on an idle host and under the writer's full tilt, counting its own
# iterations.
#
# It also prints the cgroup's own throttling counters, which are the kernel saying
# whether the quota was enforced and whether it was exhausted.
#
#   bash bench-cdc-stress-cpu-probe.sh
set -uo pipefail
CAP=${CAP:-"--cpus=0.5 --memory=256m --memory-swap=256m"}
IMG=python:3.13-slim
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/bench-cdc-stress}
SECS=${SECS:-25}
PG_C=${PG_C:-apitap-bench-cdc-pg}
PG_DB=${PG_DB:-bench}
mkdir -p "$WORK"

# A deterministic integer loop: md5-free, no allocation, no syscalls in the hot
# path, so the only thing that varies between the two phases is the CPU it got.
cat > /tmp/burn.py <<'PY'
import sys, time
secs = float(sys.argv[1])
x = 1
t0 = time.time()
n = 0
while time.time() - t0 < secs:
    for _ in range(200000):
        x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
    n += 200000
w = time.time() - t0
print(f"iterations {n} wall_s {w:.2f} rate_per_s {n/w:.0f}")
PY

phase() {
    local label=$1 name="apitap-bench-cdc-burn-$1"
    docker rm -f "$name" >/dev/null 2>&1
    local out id cg
    out=$(docker run --name "$name" --network=host $CAP -v /tmp/burn.py:/burn.py:ro \
        "$IMG" python /burn.py "$SECS" 2>&1)
    # read the cgroup's own accounting BEFORE the container goes away: a container
    # the kernel OOM-killed takes its cgroup with it and the counters are then
    # unreadable, which is exactly how a cage-shaped peak reads as frugality
    id=$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null || true)
    cg="/sys/fs/cgroup/${id}"
    echo "  $label: $out"
    if [[ -r "$cg/cpu.stat" ]]; then
        echo "    cpu.max        $(cat "$cg/cpu.max" 2>/dev/null)"
        echo "    cpu.stat       $(tr '\n' ' ' < "$cg/cpu.stat")"
    else
        echo "    cpu.stat       (cgroup gone — the container was removed)"
    fi
    docker rm -f "$name" >/dev/null 2>&1
}

echo "== CPU delivered to the SAME 0.5-CPU cage, idle host vs under the writer =="
echo "   host: $(nproc) cores, loadavg $(cut -d' ' -f1-3 /proc/loadavg)"
phase idle

echo
echo "   now the writer's full tilt: 15 psql sessions over 30 tables, unpaced"
pids=()
for s in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14; do
    ts=""; for ((i = s * 2 + 1; i <= s * 2 + 2; i++)); do ts+="${i},"; done
    python3 "$HERE/bench-capped-pg-ch-cdc-stress-writer.py" "$s" "${ts%,}" 120 0 999001 \
        > "/tmp/probe-w-s${s}.sql"
    { echo "SET application_name = 'apitap-stress-probe-s${s}';"
      cat "/tmp/probe-w-s${s}.sql"; echo "\\echo"; } \
        | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q -f - \
            > "/tmp/probe-w-s${s}.log" 2>&1 &
    pids+=($!)
done
sleep 4
echo "   writer running: $(grep -h -c '^LEDGER ' /tmp/probe-w-s*.log 2>/dev/null | paste -sd+ | bc) committed transactions after 4 s"
echo "   host: loadavg $(cut -d' ' -f1-3 /proc/loadavg), $(top -bn1 | awk '/%Cpu/{print $2,$3,$4,$5,$6,$7,$8,$9,$10}')"
phase loaded
for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
# The writer's psql sessions live INSIDE the source container, so killing the
# pipeline would leave them committing; ask the server to end them.
docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc \
    "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
     WHERE application_name LIKE 'apitap-stress-probe-%'" 2>/dev/null
sleep 2
echo "   writer ledger total: $(grep -h -c '^LEDGER ' /tmp/probe-w-s*.log 2>/dev/null | paste -sd+ | bc) committed transactions"
echo "   host after: loadavg $(cut -d' ' -f1-3 /proc/loadavg)"
echo
echo "== what this says =="
echo "   iterations/s in the same cage, idle vs loaded, is the fraction of the"
echo "   0.5 CPU the container ACTUALLY received. The kernel's nr_throttled and"
echo "   throttled_usec say whether the quota was the binding constraint."
rm -f /tmp/probe-w-s*.sql /tmp/probe-w-s*.log