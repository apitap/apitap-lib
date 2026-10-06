#!/usr/bin/env bash
# One keep-up measurement on an ALREADY-CONVERGED group: the drain attaches
# first, then a paced writer offers RATE changes/s for WRITER_S seconds. The
# drain runs until it has emptied what the writer left (or its budget), and
# the busy-segment rate is the applied throughput. Writer witnesses (WAL LSN +
# pg_stat counters / binlog position + performance_schema) prove the offer.
#
#   keepup.sh <tag> <pg|my> <cores> <rate_changes_s> <writer_s> <tables_csv> [threads]
#
# Prerequisite: `converge.sh <tag-conv> <pg|my> <budget> <tables_csv>` has
# already taken the group to zero backlog — otherwise the measured rate is
# polluted by the bootstrap/backlog (the mistake the t30-pg-r1 run made).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
LOGS="$WORK/logs"
mkdir -p "$LOGS"

TAG="${1:?tag}"; ROUTE="${2:?pg|my}"; CPUS="${3:?cores}"; RATE="${4:?rate}"
WS="${5:?writer_s}"; TABLES="${6:?tables_csv}"; THREADS="${7:-4}"
# The wheel under test; override SP to measure a freshly built venv.
SP="${SP:-/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages}"
CH_DST="${CH_DST:-clickhouse://default:bench@127.0.0.1:8124/default}"
# A catch-up after a writer longer than ~60 s needs more than WS+120: the
# 180 s proof at 33.4k offered against a ~20k drain is ~130 s of tail alone.
DB="${DB_S:-$((WS + 120))}"

if [ "$ROUTE" = pg ]; then
  URL="postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
  DIALECT=pg
else
  URL="mysql://root:bench@127.0.0.1:3307/bench"
  DIALECT=mysql
fi
WLOG="$LOGS/$TAG.writer.log"; DLOG="$LOGS/$TAG.drain.log"
rm -f "$WLOG" "$DLOG"

echo "[keepup] tag=$TAG route=$ROUTE cpus=$CPUS rate=$RATE writer_s=$WS tables=$(tr ',' '\n' <<<"$TABLES" | wc -l) threads=$THREADS"

docker run --rm --name "prof-drain-$TAG" --network=host --cpus="$CPUS" --memory=256m --memory-swap=256m \
  -v "$SP:/py:ro" -e PYTHONPATH=/py \
  -e "APITAP_SRC=$URL" -e "APITAP_DST=$CH_DST" \
  -e "APITAP_TABLE=$TABLES" -e "BUDGET_S=$DB" -e ZERO_STOP=3 -e CPUS="$CPUS" \
  -e "APITAP_CDC_APPLY_LANES=${APITAP_CDC_APPLY_LANES:-}" \
  -e "APITAP_CDC_WINDOW_BYTES=${APITAP_CDC_WINDOW_BYTES:-}" \
  -e "APITAP_DEBUG=${APITAP_DEBUG:-}" \
  -e "APITAP_PG_BINARY=${APITAP_PG_BINARY:-}" \
  -e "APITAP_TAG=$TAG" -v "$HERE:/job:ro" python:3.13-slim sh /job/leg.sh > "$DLOG" 2>&1 &
DPID=$!
# artifact-first: the container exists, then the python process is up
for _ in $(seq 1 120); do docker inspect "prof-drain-$TAG" >/dev/null 2>&1 && break; sleep 0.5; done
for _ in $(seq 1 60); do
  docker top "prof-drain-$TAG" -eo pid,comm 2>/dev/null | awk '$2 ~ /^python3/{found=1} END{exit !found}' && break
  sleep 0.5
done
sleep 3

setsid nohup "$HOME/prof-venv/bin/python" "$HERE/writer.py" --dialect "$DIALECT" --url "$URL" \
  --tables "$TABLES" --threads "$THREADS" --rate "$RATE" --duration "$WS" > "$WLOG" 2>&1 < /dev/null &
WPID=$!
for _ in $(seq 1 120); do grep -q WRITER_TICK "$WLOG" 2>/dev/null && break; sleep 0.5; done
grep -q WRITER_TICK "$WLOG" || { echo "writer failed to start"; sed -n '1,30p' "$WLOG"; exit 1; }
echo "[keepup] writer: $(grep WRITER_START "$WLOG" | tail -1)"

wait "$WPID" 2>/dev/null
wait "$DPID" 2>/dev/null
wait 2>/dev/null

echo "[keepup] ===== writer ====="
grep -E "WRITER_TOTAL|WRITER_ERROR|WRITER_WAL_BYTES|WRITER_BINLOG_BYTES|WRITER_WITNESS" "$WLOG" | tail -6
echo "[keepup] ===== drain ====="
grep -E "^DRAIN |CPU_SAMPLER|LEG_DONE|MEMPEAK|MEMEVENTS|CPUPressure|EXITCODE|RAISED" "$DLOG" | tail -80
echo "[keepup] ===== busy rate ====="
bash "$HERE/busyrate.sh" "$DLOG"
if [ "${SKIP_VALIDATE:-0}" = "1" ]; then
  echo "[keepup] validate skipped (SKIP_VALIDATE=1)"
else
  echo "[keepup] ===== validate ====="
  bash "$HERE/validate30.sh" "$ROUTE" "$TABLES" | tee "$LOGS/$TAG.validate.out"
fi
