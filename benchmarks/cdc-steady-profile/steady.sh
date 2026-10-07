#!/usr/bin/env bash
# One steady-state measurement: a paced writer runs for WRITER_S while a
# capped drain container drains continuously for DRAIN_S. Optionally attaches
# perf (cpu-clock, software — this VPS blocks PMU) to the drain process and/or
# a short strace -c. Everything is an artifact under ~/bench-cdc-steady/logs.
#
#   steady.sh <tag> <pg|my> <rate_changes_s> <writer_s> <drain_budget_s> \
#             <pre_delay_s> [perf_s] [strace_s] [debug]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
mkdir -p "$WORK/logs"
TAG="${1:?tag}"; ROUTE="${2:?pg|my}"; RATE="${3:?rate}"; WS="${4:?writer_s}"
DB="${5:-0}"; DELAY="${6:-10}"; PERF_S="${7:-0}"; STRACE_S="${8:-0}"; DEBUG="${9:-}"
CPUS="${CPUS:-0.5}"
THREADS="${WRITER_THREADS:-4}"

if [ "$ROUTE" = pg ]; then
  URL="postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"; TABLE="${PROF_TABLE:-public.prof_pg_m}"
else
  URL="mysql://root:bench@127.0.0.1:3307/bench"; TABLE="${PROF_TABLE:-prof_my_m}"
fi
SP="${SP:-/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages}"
CAP="--network=host --cpus=$CPUS --memory=256m --memory-swap=256m"

WLOG="$WORK/logs/$TAG.writer.log"
DLOG="$WORK/logs/$TAG.drain.log"
rm -f "$WLOG" "$DLOG"

echo "[steady] tag=$TAG route=$ROUTE rate=$RATE writer_s=$WS drain_budget_s=$DB delay=$DELAY cpus=$CPUS threads=$THREADS"
DIALECT=$ROUTE; [ "$ROUTE" = my ] && DIALECT=mysql
setsid nohup "$HOME/prof-venv/bin/python" "$HERE/writer.py" --dialect "$DIALECT" --url "$URL" \
  --tables "$TABLE" --threads "$THREADS" --rate "$RATE" --duration "$WS" > "$WLOG" 2>&1 < /dev/null &
WPID=$!
# wait for the artifact, not the process
for _ in $(seq 1 60); do grep -q WRITER_TICK "$WLOG" 2>/dev/null && break; sleep 0.5; done
grep -q WRITER_TICK "$WLOG" || { echo "writer failed to start"; sed -n '1,20p' "$WLOG"; exit 1; }
echo "[steady] writer up: $(grep WRITER_START "$WLOG" | tail -1)"

sleep "$DELAY"
DBG_ENV=""
[ -n "$DEBUG" ] && DBG_ENV="-e APITAP_DEBUG=1"
setsid nohup docker run --rm --name "prof-drain-$TAG" $CAP \
  -v "$SP:/py:ro" -e PYTHONPATH=/py $DBG_ENV \
  -e "APITAP_SRC=$URL" -e "APITAP_DST=${PROF_DST:-clickhouse://default:bench@127.0.0.1:8124/default}" \
  -e "APITAP_TABLE=$TABLE" -e "BUDGET_S=$DB" -e ZERO_STOP=99 -e CPUS=$CPUS \
  -e "APITAP_CDC_WINDOW_BYTES=${APITAP_CDC_WINDOW_BYTES:-}" -e "APITAP_CDC_APPLY_LANES=${APITAP_CDC_APPLY_LANES:-}" \
  -e "APITAP_TAG=$TAG" -v "$HERE:/job:ro" python:3.13-slim sh /job/leg.sh > "$DLOG" 2>&1 < /dev/null &
DPID=$!
for _ in $(seq 1 120); do docker inspect prof-drain-$TAG >/dev/null 2>&1 && break; sleep 0.5; done
# perf must attach to the python process, not the container's sh
for _ in $(seq 1 60); do
  CPID=$(docker top "prof-drain-$TAG" -eo pid,comm 2>/dev/null | awk '$2 ~ /^python3/{print $1; exit}')
  [ -n "${CPID:-}" ] && break; sleep 0.5
done
CPID=${CPID:-}
echo "[steady] drain python pid=${CPID:-none}"

if [ "$PERF_S" -gt 0 ] && [ -n "$CPID" ] && [ "$CPID" != 0 ]; then
  PLOG="$WORK/logs/$TAG.perf.log"; PDATA="$WORK/logs/$TAG.perf.data"
  setsid nohup bash -c "sudo perf record -F 999 ${PERF_G:+-g} -e cpu-clock -o '$PDATA' -p $CPID -- sleep $PERF_S" > "$PLOG" 2>&1 < /dev/null &
  echo "[steady] perf recording ${PERF_S}s -> $PDATA"
fi
if [ "$STRACE_S" -gt 0 ] && [ -n "$CPID" ] && [ "$CPID" != 0 ]; then
  SLOG="$WORK/logs/$TAG.strace.log"
  setsid nohup bash -c "sudo strace -c -f -p $CPID -o '$SLOG' -- sleep $STRACE_S" >/dev/null 2>&1 < /dev/null &
  echo "[steady] strace -c ${STRACE_S}s -> $SLOG"
fi

# wait for the drain container, then the writer
for _ in $(seq 1 14400); do docker inspect prof-drain-$TAG >/dev/null 2>&1 || break; sleep 1; done
wait $DPID 2>/dev/null
[ "$PERF_S" -gt 0 ] && sleep 2
wait "$WPID" 2>/dev/null
wait 2>/dev/null

echo "[steady] ===== writer ====="
grep -E "WRITER_TOTAL|WRITER_ERROR|WRITER_WAL_BYTES|WRITER_BINLOG_BYTES|WRITER_WITNESS" "$WLOG" | tail -10
echo "[steady] ===== drain ====="
grep -E "^DRAIN |CPU_SAMPLER|LEG_DONE|MEMPEAK|MEMEVENTS|CPUPressure|EXITCODE|RAISED" "$DLOG" | tail -60
echo "[steady] logs: $WLOG $DLOG"
