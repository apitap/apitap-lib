#!/usr/bin/env bash
# Host-side drain: the same 0.58.0 wheel run directly (no container), capped at
# 0.5 CPU with a systemd scope, so the socket path is identical to the
# --network=host container but without docker's cgroup/overhead.
# Samples ss -tinm (rcvbuf/queues) while the drain is busy.
#
#   run-host.sh [tag]     CPUQUOTA env selects the cap (default 50%)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/waltcp}
mkdir -p "$WORK/logs"
TAG=${TAG:-host}
RATE=${RATE:-35000}
WS=${WS:-120}
DBUDGET=${DBUDGET:-200}
CPUQUOTA=${CPUQUOTA:-50%}
PY=${PY:-$HOME/waltcp-venv/bin/python}
SRC=${SRC:-postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src}
DST=${DST:-clickhouse://default:bench@127.0.0.1:8124/default}
TABLE=${TABLE:-public.prof_pg_m}
DEST_TABLE=${DEST_TABLE:-waltcp_pg_m}
WRITER=${WRITER:-$HOME/apitap-lib/benchmarks/cdc-steady-profile/writer.py}
WPY=${WPY:-$HOME/prof-venv/bin/python}
DLOG="$WORK/logs/$TAG.drain.log"; WLOG="$WORK/logs/$TAG.writer.log"
SSLOG="$WORK/logs/$TAG.ss.txt"
rm -f "$DLOG" "$WLOG" "$SSLOG"

echo "[host] tag=$TAG rate=$RATE writer_s=$WS budget=$DBUDGET quota=$CPUQUOTA"
setsid nohup sudo systemd-run --scope --unit="waltcp-host-$TAG" \
  -p CPUQuota="$CPUQUOTA" -p MemoryMax=256M -p MemorySwapMax=0 \
  --uid=ubuntu --gid=ubuntu \
  env APITAP_DEBUG=1 CPUS=0.5 \
  "$PY" "$HERE/drain.py" "$SRC" "$DST" "$TABLE" "$DEST_TABLE" \
  "$DBUDGET" 99 "$TAG" > "$DLOG" 2>&1 < /dev/null &
for _ in $(seq 1 60); do grep -q "LEG_TAG" "$DLOG" 2>/dev/null && break; sleep 0.5; done
grep -q "LEG_TAG" "$DLOG" || { echo "[host] drain failed to start"; sed -n '1,20p' "$DLOG"; exit 1; }
echo "[host] drain up: $(grep LEG_TAG "$DLOG" | tail -1)"

setsid nohup "$WPY" "$WRITER" --dialect pg --url "$SRC" --tables "$TABLE" \
  --threads 4 --rate "$RATE" --duration "$WS" > "$WLOG" 2>&1 < /dev/null &
WPID=$!
for _ in $(seq 1 120); do grep -q WRITER_TICK "$WLOG" 2>/dev/null && break; sleep 0.5; done
grep -q WRITER_TICK "$WLOG" || { echo "[host] writer failed"; exit 1; }
echo "[host] writer: $(grep WRITER_START "$WLOG" | tail -1)"

for i in $(seq 1 24); do
  echo "== ss sample $i t=$(date +%s.%N)" >> "$SSLOG"
  ss -tinm state established "dport = :5544" >> "$SSLOG" 2>&1
  sleep 5
done &
SSPID=$!
wait "$WPID" 2>/dev/null
for _ in $(seq 1 600); do grep -q "LEG_DONE" "$DLOG" && break; sleep 1; done
wait "$SSPID" 2>/dev/null
kill "$SSPID" 2>/dev/null

echo "[host] ===== writer ====="
grep -E "WRITER_TOTAL|WRITER_ERROR|WRITER_WAL_BYTES" "$WLOG" | tail -4
echo "[host] ===== drain ====="
grep -E "LEG_DONE|MEMPEAK|RAISED|EXITCODE|CPU_SAMPLER" "$DLOG" | tail -6
echo "[host] ===== busy rate ====="
bash "$HOME/apitap-lib/benchmarks/cdc-steady-profile/busyrate.sh" "$DLOG"
echo "[host] ss: $SSLOG"
