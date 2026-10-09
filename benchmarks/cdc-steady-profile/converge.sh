#!/usr/bin/env bash
# Drain a route to convergence after its writer stopped, then validate.
#   converge.sh <tag> <pg|my> [budget_s] [tables_csv]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
mkdir -p "$WORK/logs"
TAG="${1:?tag}"; ROUTE="${2:?pg|my}"; BUDGET="${3:-1800}"
CPUS="${CPUS:-0.5}"
if [ "$ROUTE" = pg ]; then
  URL="postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"; TABLE="prof_pg_m"
else
  URL="mysql://root:bench@127.0.0.1:3307/bench"; TABLE="prof_my_m"
fi
TABLES="${4:-$TABLE}"
# The wheel under test; override SP to converge with a freshly built venv.
SP="${SP:-/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages}"
CH_DST="${CH_DST:-clickhouse://default:bench@127.0.0.1:8124/default}"
DLOG="$WORK/logs/$TAG.converge.log"
docker run --rm --name "prof-conv-$TAG" --network=host --cpus=$CPUS --memory=256m --memory-swap=256m \
  -v "$SP:/py:ro" -e PYTHONPATH=/py \
  -e "APITAP_SRC=$URL" -e "APITAP_DST=$CH_DST" \
  -e "APITAP_TABLE=$TABLES" -e "BUDGET_S=$BUDGET" -e ZERO_STOP=2 -e CPUS=$CPUS \
  -e "APITAP_SLOTS=${APITAP_SLOTS:-}" \
  -e "APITAP_CDC_APPLY_LANES=${APITAP_CDC_APPLY_LANES:-}" \
  -e "APITAP_CDC_WINDOW_BYTES=${APITAP_CDC_WINDOW_BYTES:-}" \
  -e "APITAP_TAG=conv-$TAG" -v "$HERE:/job:ro" python:3.13-slim sh /job/leg.sh 2>&1 | tee "$DLOG" \
  | grep -E "^DRAIN |CPU_SAMPLER|LEG_DONE|MEMPEAK|EXITCODE|RAISED"
if [ "${SKIP_VALIDATE:-0}" = "1" ]; then
  echo "[converge] validate skipped (SKIP_VALIDATE=1)"
else
  echo "[converge] digest:"
  bash "$HERE/validate30.sh" "$ROUTE" "$(printf '%s' "$TABLES" | sed 's/public\.//g')"
fi
