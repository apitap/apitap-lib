#!/usr/bin/env bash
# Apply-lanes sweep on the fixed wheel: for each lane count, converge the
# 30-table group, then run the same offered volume (35k/s for 15 s) with
# APITAP_CDC_APPLY_LANES set. Samples ClickHouse's running-query count during
# each keep-up so contention is visible.
#
#   lanes-sweep.sh <sp> <l1> <l2> ...     # sp = site-packages dir of the wheel
set -uo pipefail
C="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
LOGS="$WORK/logs"
SP="${1:?sp}"; shift
T30=$(seq -f "prof_pg_t%02g" 1 30 | paste -sd,)
OUT="$LOGS/lanes-sweep.$(basename "$(dirname "$(dirname "$SP")")").out"
exec > "$OUT" 2>&1
echo "LANES_SWEEP_START $(date -u +%Y-%m-%dT%H:%M:%SZ) SP=$SP"
for L in "$@"; do
  echo; echo "### lanes=$L $(date -u +%H:%M:%S)"
  SKIP_VALIDATE=1 SP="$SP" CPUS=0.5 bash "$C/converge.sh" "ls-conv-$L" pg 900 "$T30" | tail -3
  # sample the server's query concurrency every second while the drain runs
  ( for _ in $(seq 1 200); do
      n=$(docker exec apitap-bench-ch clickhouse-client --password bench -q "SELECT count() FROM system.processes" 2>/dev/null | tr -d '\n')
      echo "PROCS t=$(date -u +%H:%M:%S) n=$n"
      sleep 1
    done ) > "$LOGS/ls-$L.procs.log" 2>&1 &
  SAM=$!
  START=$(date -u '+%Y-%m-%d %H:%M:%S')
  SKIP_VALIDATE=1 APITAP_CDC_APPLY_LANES=$L SP="$SP" bash "$C/keepup.sh" "ls-$L" pg 0.5 35000 15 "$T30" 4
  END=$(date -u '+%Y-%m-%d %H:%M:%S')
  kill "$SAM" 2>/dev/null
  echo "$START|$END" > "$LOGS/ls-$L.qwindow"
  echo "--- statement census lanes=$L"
  bash "$C/windowcost.sh" "$START" "$END" "$LOGS/raw/ls-$L.cost.tsv" | tail -12
done
echo "LANES_SWEEP_DONE $(date -u +%Y-%m-%dT%H:%M:%SZ)"
