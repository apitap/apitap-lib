#!/usr/bin/env bash
# wal-tcp-cost main measurement run on the bench VPS.
#
#   run-main.sh conv                 -- converge (bootstrap + drain to zero)
#   run-main.sh main                 -- drain + paced writer + instruments
#   run-main.sh trickle              -- same at RATE=2000/s (coalescing check)
#
# Environment knobs: TAG RATE WS DBUDGET CPUS ZERO_STOP SP SRC DST TABLE
# DEST_TABLE PRE_DELAY TCPDUMP_S STRACE_S PERF_S
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/waltcp}
mkdir -p "$WORK/logs"
MODE=${1:-main}

SP=${SP:-/home/ubuntu/waltcp-venv/lib/python3.13/site-packages}
SRC=${SRC:-postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src}
DST=${DST:-clickhouse://default:bench@127.0.0.1:8124/default}
TABLE=${TABLE:-public.prof_pg_m}
DEST_TABLE=${DEST_TABLE:-waltcp_pg_m}
CPUS=${CPUS:-0.5}
WRITER=${WRITER:-$HOME/apitap-lib/benchmarks/cdc-steady-profile/writer.py}
WPY=${WPY:-$HOME/prof-venv/bin/python}
PG_C=${PG_C:-apitap-bench-pg-src}
PG_DB=${PG_DB:-apitap_bench_src}

docker_run() { # name budget zero_stop log
  docker run --rm --name "waltcp-$1" --network=host --cpus="$CPUS" \
    --memory=256m --memory-swap=256m \
    -v "$SP:/py:ro" -e PYTHONPATH=/py -e APITAP_DEBUG=1 -e CPUS="$CPUS" \
    -v "$HERE:/job:ro" python:3.13-slim python3 /job/drain.py \
    "$SRC" "$DST" "$TABLE" "$DEST_TABLE" "$2" "$3" "$1"
}

if [ "$MODE" = conv ]; then
  TAG=${TAG:-conv}
  CLOG="$WORK/logs/$TAG.converge.log"
  echo "[conv] table=$TABLE dest=$DEST_TABLE budget=${CONV_BUDGET:-600}"
  docker_run "conv-$TAG" "${CONV_BUDGET:-600}" 2 "$CLOG" 2>&1 | tee "$CLOG"
  echo "[conv] ===== busyrate ====="
  bash "$HOME/apitap-lib/benchmarks/cdc-steady-profile/busyrate.sh" "$CLOG"
  exit 0
fi

TAG=${TAG:-$MODE}
RATE=${RATE:-26000}
WS=${WS:-150}
DBUDGET=${DBUDGET:-240}
ZERO_STOP=${ZERO_STOP:-99}
PRE_DELAY=${PRE_DELAY:-10}
TCPDUMP_S=${TCPDUMP_S:-30}
STRACE_S=${STRACE_S:-30}
PERF_S=${PERF_S:-30}
THREADS=${THREADS:-4}
if [ "$MODE" = trickle ]; then RATE=${RATE:-2000}; WS=${WS:-60}; DBUDGET=${DBUDGET:-150}; fi

WLOG="$WORK/logs/$TAG.writer.log"
DLOG="$WORK/logs/$TAG.drain.log"
WT="$WORK/logs/$TAG.windows.tsv"
rm -f "$WLOG" "$DLOG" "$WT"

witness() {
  docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
    "select pg_current_wal_lsn()::text||' '||(select coalesce(sum(n_tup_ins+n_tup_upd+n_tup_del),0) from pg_stat_user_tables where relname='prof_pg_m')"
}
logsz() { stat -c %s "$DLOG" 2>/dev/null || echo 0; }
win_begin() { WIN="$1"; O0=$(logsz); W0=$(witness); T0=$(date +%s.%N); }
win_end() {
  T1=$(date +%s.%N); W1=$(witness); O1=$(logsz)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$WIN" "$O0" "$O1" "$W0" "$W1" "$T0" "$T1" >> "$WT"
}

echo "[main] tag=$TAG mode=$MODE rate=$RATE writer_s=$WS drain_budget=$DBUDGET cpus=$CPUS"

setsid nohup docker run --rm --name "waltcp-$TAG" --network=host --cpus="$CPUS" \
  --memory=256m --memory-swap=256m \
  -v "$SP:/py:ro" -e PYTHONPATH=/py -e APITAP_DEBUG=1 -e CPUS="$CPUS" \
  -v "$HERE:/job:ro" python:3.13-slim python3 /job/drain.py \
  "$SRC" "$DST" "$TABLE" "$DEST_TABLE" "$DBUDGET" "$ZERO_STOP" "$TAG" \
  > "$DLOG" 2>&1 < /dev/null &
DPID=$!
for _ in $(seq 1 120); do docker inspect "waltcp-$TAG" >/dev/null 2>&1 && break; sleep 0.5; done
CPID=""
for _ in $(seq 1 120); do
  CPID=$(docker top "waltcp-$TAG" -eo pid,comm 2>/dev/null | awk '$2 ~ /^python3/{print $1; exit}')
  [ -n "${CPID:-}" ] && break
  sleep 0.5
done
echo "[main] drain container up, python host pid=${CPID:-none}"

setsid nohup "$WPY" "$WRITER" --dialect pg --url "$SRC" \
  --tables "$TABLE" --threads "$THREADS" --rate "$RATE" --duration "$WS" \
  > "$WLOG" 2>&1 < /dev/null &
WPID=$!
for _ in $(seq 1 120); do grep -q WRITER_TICK "$WLOG" 2>/dev/null && break; sleep 0.5; done
grep -q WRITER_TICK "$WLOG" || { echo "[main] writer failed to start"; sed -n '1,30p' "$WLOG"; exit 1; }
echo "[main] writer up: $(grep WRITER_START "$WLOG" | tail -1)"
sleep "$PRE_DELAY"

CPORT=$(docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "select client_port from pg_stat_replication where state='streaming' order by backend_start desc limit 1")
echo "[main] walsender client_port=${CPORT:-none}"

if [ "$TCPDUMP_S" -gt 0 ]; then
  win_begin tcpdump
  ( for i in $(seq 1 $((TCPDUMP_S / 5))); do
      echo "== ss sample $i t=$(date +%s.%N)"
      ss -tinm state established "dport = :5544"
      sleep 5
    done ) >> "$WORK/logs/$TAG.ss.txt" 2>&1 &
  SSPID=$!
  sudo timeout "$TCPDUMP_S" tcpdump -i lo -nn -s 0 -w "$WORK/logs/$TAG.pcap" \
    "tcp port 5544" > "$WORK/logs/$TAG.tcpdump.txt" 2>&1
  wait "$SSPID" 2>/dev/null
  win_end tcpdump
  echo "[main] tcpdump: $(tail -2 "$WORK/logs/$TAG.tcpdump.txt" | tr '\n' ' ')"
fi

if [ "$STRACE_S" -gt 0 ] && [ -n "$CPID" ]; then
  win_begin strace
  sudo timeout -s INT "$STRACE_S" strace -c -f -p "$CPID" \
    -o "$WORK/logs/$TAG.strace.txt"
  win_end strace
  echo "[main] strace done"
fi

if [ "$PERF_S" -gt 0 ] && [ -n "$CPID" ]; then
  win_begin perf
  sudo timeout -s INT "$PERF_S" perf record -F 999 -e cpu-clock -g \
    -o "$WORK/logs/$TAG.perf.data" -p "$CPID"
  win_end perf
  echo "[main] perf done"
fi

for _ in $(seq 1 3600); do docker inspect "waltcp-$TAG" >/dev/null 2>&1 || break; sleep 1; done
wait "$DPID" 2>/dev/null
wait "$WPID" 2>/dev/null
wait 2>/dev/null

echo "[main] ===== writer ====="
grep -E "WRITER_TOTAL|WRITER_ERROR|WRITER_WAL_BYTES|WRITER_WITNESS" "$WLOG" | tail -6
echo "[main] ===== drain ====="
grep -E "^DRAIN |CPU_SAMPLER|LEG_DONE|MEMPEAK|MEMEVENTS|CPUPressure|EXITCODE|RAISED" "$DLOG" | tail -60
echo "[main] ===== busy rate ====="
bash "$HOME/apitap-lib/benchmarks/cdc-steady-profile/busyrate.sh" "$DLOG"
echo "[main] ===== windows ====="
cat "$WT"
echo "[main] logs: $WLOG $DLOG"
