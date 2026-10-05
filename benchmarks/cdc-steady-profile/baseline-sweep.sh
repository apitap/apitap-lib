#!/usr/bin/env bash
# Baseline N-sweep: for N in 30,10,5,1, run the same offered volume
# (35k changes/s for 30 s) through an N-table CDC group at 0.5 CPU / 256 MB,
# after clearing that group's destination state and bootstrapping it under its
# own slot. Prints per-shape keep-up evidence and the ClickHouse query_log
# statement census for the drain window.
set -uo pipefail
C="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
LOGS="$WORK/logs"
mkdir -p "$LOGS"
OUT="$LOGS/baseline-sweep.out"

exec > "$OUT" 2>&1
echo "SWEEP_START $(date -u +%Y-%m-%dT%H:%M:%SZ)"

clear_state() { # comma list of bare names
  local in
  in=$(printf "'%s'," $(echo "$1" | tr ',' ' ') | sed 's/,$//')
  docker exec apitap-bench-ch clickhouse-client --password bench -q \
    "ALTER TABLE default._apitap_state DELETE WHERE dest_table IN ($in) SETTINGS mutations_sync=1"
}

for N in 30 10 5 1; do
  T=$(seq -f "prof_pg_t%02g" 1 "$N" | paste -sd,)
  echo
  echo "### N=$N $(date -u +%H:%M:%S) tables=$T"
  if [ "$N" -lt 30 ]; then
    echo "--- clear state + bootstrap"
    clear_state "$T"
    CPUS=0.5 bash "$C/converge.sh" "boot-base-n$N" pg 1200 "$T" | tail -8
  else
    echo "--- already converged (30-group state kept)"
    CPUS=0.5 bash "$C/converge.sh" "pre-base-n$N" pg 900 "$T" | tail -8
  fi
  START=$(date -u '+%Y-%m-%d %H:%M:%S')
  echo "--- keepup N=$N ql_start=$START"
  bash "$C/keepup.sh" "base-n$N-pg" pg 0.5 35000 30 "$T" 4
  END=$(date -u '+%Y-%m-%d %H:%M:%S')
  echo "$START|$END" > "$LOGS/base-n$N-pg.qwindow"
  mkdir -p "$LOGS/raw"
  bash "$C/windowcost.sh" "$START" "$END" "$LOGS/raw/base-n$N.cost.tsv" | tail -20
  docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -c CHECKPOINT >/dev/null 2>&1
done

echo "SWEEP_DONE $(date -u +%Y-%m-%dT%H:%M:%SZ)"
