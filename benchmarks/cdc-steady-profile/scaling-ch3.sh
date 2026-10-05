#!/usr/bin/env bash
# Scaling curve on the ch3 (25.8) destination: same 30-table keep-up shape at
# 1 / 2 / 4 CPU quotas with proportional offers, using the wheel in qa-venv.
# Window 32M. Each shape: converge, then writer (60 s) + drain, then validate.
set -uo pipefail
C="$(cd "$(dirname "$0")" && pwd)"
LOGS="$HOME/bench-cdc-steady/logs"
T30=$(seq -f "prof_pg_t%02g" 1 30 | paste -sd,)
export CH_DST=clickhouse://default:bench@127.0.0.1:8126/default
export CH_C=apitap-bench-ch3
SP=/home/ubuntu/qa-venv/lib/python3.13/site-packages
OUT="$LOGS/scaling-ch3.out"
exec > "$OUT" 2>&1
echo "SCALING_START $(date -u +%Y-%m-%dT%H:%M:%SZ)"

run() { # cores rate threads
  local cores="$1" rate="$2" threads="$3" tag="scale-$1"
  echo; echo "### $tag cores=$cores rate=$rate threads=$threads $(date -u +%H:%M:%S)"
  SP=$SP APITAP_CDC_WINDOW_BYTES=33554432 bash "$C/converge.sh" "scaleconv-$cores" pg 1200 "$T30" | tail -2
  START=$(date -u '+%Y-%m-%d %H:%M:%S')
  SP=$SP APITAP_CDC_WINDOW_BYTES=33554432 bash "$C/keepup.sh" "$tag" pg "$cores" "$rate" 60 "$T30" "$threads"
  END=$(date -u '+%Y-%m-%d %H:%M:%S')
  echo "$START|$END" > "$LOGS/$tag.qwindow"
  bash "$C/windowcost.sh" "$START" "$END" "$LOGS/raw/$tag.cost.tsv" | tail -12
}

run 1 70000 6
run 2 140000 8
run 4 240000 8
echo "SCALING_DONE $(date -u +%Y-%m-%dT%H:%M:%SZ)"
