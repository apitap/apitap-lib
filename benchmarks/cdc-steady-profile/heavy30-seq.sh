#!/usr/bin/env bash
# The heavy 30-table CDC bite: one capped drain (256MB / 0.5 CPU) follows two
# MIXED CDC rounds of 1M changes per table each (70% U + 15% I + 15% D, then
# 40% U + 30% I + 30% D; the insert+delete pairs are net-zero, the proven
# writer shape). Server-side SQL generates the changes so the offer is a real
# 30M-change batch per round, not a Python-ledger pace.
#
#   heavy30-seq.sh <pg|my> <tag> [drain_budget_s]
#
# Artifacts: $HOME/bench-cdc-steady/logs/<tag>.{witness,drain.log,summary}
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
LOGS="$WORK/logs"; mkdir -p "$LOGS"
ROUTE="${1:?pg|my}"; TAG="${2:?tag}"; BUDGET="${3:-7200}"
CPUS="${CPUS:-0.5}"
SP="${SP:-$HOME/gate-venv/lib/python3.13/site-packages}"
CH_DST="${CH_DST:-clickhouse://default:bench@127.0.0.1:8126/default}"
CH_CT=${CH_CT:-apitap-bench-ch3}
PG_C=${PG_C:-apitap-bench-pg-src}; PG_DB=${PG_DB:-apitap_bench_src}
MY_C=${MY_C:-apitap-bench-my}; MY_DB=${MY_DB:-bench}
N=${N:-30}

if [ "$ROUTE" = pg ]; then
  TBL=$(for i in $(seq -w 1 $N); do printf 'public.prof_pg_t%s,' "$i"; done | sed 's/,$//')
  TVAL=$(for i in $(seq -w 1 $N); do printf 'prof_pg_t%s,' "$i"; done | sed 's/,$//')
  URL="postgres://postgres:bench@127.0.0.1:5544/$PG_DB"
  SLOTS="${APITAP_SLOTS:-auto}"
else
  TBL=$(for i in $(seq -w 1 $N); do printf 'prof_my_t%s,' "$i"; done | sed 's/,$//')
  TVAL="$TBL"
  URL="mysql://root:bench@127.0.0.1:3307/$MY_DB"
  SLOTS="${APITAP_SLOTS:-}"
fi

psql() { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -v ON_ERROR_STOP=1 "$@"; }
myq()  { docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" "$@"; }

witness() {
  if [ "$ROUTE" = pg ]; then
    psql -Atc "SELECT pg_current_wal_lsn()||'|'||sum(n_tup_upd)||','||sum(n_tup_ins)||','||sum(n_tup_del) FROM pg_stat_user_tables WHERE relname LIKE 'prof_pg_t%'"
  else
    local ms f p c b
    ms=$(docker exec -i "$MY_C" mysql -uroot -pbench -N -B -e 'SHOW MASTER STATUS' 2>/dev/null | head -1)
    f=$(echo "$ms" | awk '{print $1}'); p=$(echo "$ms" | awk '{print $2}')
    c=$(myq -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_rows_updated','Innodb_rows_inserted','Innodb_rows_deleted')" 2>/dev/null | awk '{printf "%s,",$2}' | sed 's/,$//')
    b=$(docker exec -i "$MY_C" mysql -uroot -pbench -N -B -e 'SHOW BINARY LOGS' 2>/dev/null | awk '{s+=$2} END {print s}')
    echo "$f:$p|$c|binlogbytes=$b"
  fi
}

disk_guard() {
  local free
  while :; do
    free=$(df --output=avail -BG / | tail -1 | tr -dc 0-9)
    [ "${free:-0}" -ge 25 ] && return 0
    echo "DISK_GUARD free=${free}G paused $(date -u +%H:%M:%S)"
    if [ "$ROUTE" = my ]; then my_binlog_purge_keep 6; fi
    sleep 30
  done
}

my_binlog_purge_keep() { # keep the last N binlog files; purge everything older
  local keep=${1:-6} f
  f=$(docker exec -i "$MY_C" mysql -uroot -pbench -N -B -e 'SHOW BINARY LOGS' 2>/dev/null | awk '{print $1}' | tail -"$keep" | head -1)
  [ -n "$f" ] && docker exec -i "$MY_C" mysql -uroot -pbench -e "PURGE BINARY LOGS TO '$f'" 2>/dev/null && echo "PURGED_BINLOGS_TO $f"
  return 0
}

gen_round() { # A|B — per table: 1000 txs of 1000 changes (700U+150I+150D / 400U+300I+300D)
  local round=$1 NU NI USTART IBASE i j ua ub sa sb off
  local SQL=/tmp/heavy30-round.sql NTX=${NTX:-1000}
  if [ "$round" = A ]; then NU=700; NI=150; USTART=1;      IBASE=${IBASE_A:-2000000}
  else                       NU=400; NI=300; USTART=300001; IBASE=${IBASE_B:-3000000}; fi
  for i in $(seq -w 1 $N); do
    : > "$SQL"
    for j in $(seq 0 $((NTX-1))); do
      ua=$((USTART + j*NU)); ub=$((ua + NU - 1))
      sa=$((1 + j*NI));      sb=$((sa + NI - 1))
      off=$((IBASE + j*NI))
      if [ "$ROUTE" = pg ]; then
        printf 'BEGIN; UPDATE public.prof_pg_t%s SET regular_int=regular_int+1 WHERE id BETWEEN %s AND %s; INSERT INTO public.prof_pg_t%s SELECT id+%s, small_str, medium_str, large_str, tiny_int, regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text FROM public.prof_pg_t%s WHERE id BETWEEN %s AND %s; DELETE FROM public.prof_pg_t%s WHERE id BETWEEN %s AND %s; COMMIT;\n' "$i" "$ua" "$ub" "$i" "$IBASE" "$i" "$sa" "$sb" "$i" "$((off+1))" "$((off+NI))" >> "$SQL"
      else
        printf 'BEGIN; UPDATE prof_my_t%s SET regular_int=regular_int+1 WHERE id BETWEEN %s AND %s; INSERT INTO prof_my_t%s SELECT id+%s, small_str, medium_str, large_str, tiny_int, regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, CAST(json_val AS CHAR), extra_text FROM prof_my_t%s WHERE id BETWEEN %s AND %s; DELETE FROM prof_my_t%s WHERE id BETWEEN %s AND %s; COMMIT;\n' "$i" "$ua" "$ub" "$i" "$IBASE" "$i" "$sa" "$sb" "$i" "$((off+1))" "$((off+NI))" >> "$SQL"
      fi
    done
    if [ "$ROUTE" = pg ]; then
      psql -q < "$SQL" >/dev/null
    else
      docker exec -i "$MY_C" mysql -uroot -pbench "$MY_DB" < "$SQL" 2>/dev/null
    fi
    echo "ROUND $round t$i done $(date -u +%H:%M:%S)"
    if ! docker inspect "prof-heavy-$TAG" >/dev/null 2>&1; then
      echo "DRAIN_DIED after round $round table $i — generation aborted"
      return 1
    fi
    disk_guard
  done
  return 0
}

docker rm -f "prof-heavy-$TAG" >/dev/null 2>&1
echo "== drain up: $SP | slots=${SLOTS:-default} | budget=${BUDGET}s =="
docker run -d --name "prof-heavy-$TAG" --network=host --cpus=$CPUS --memory=256m --memory-swap=256m \
  -v "$SP:/py:ro" -e PYTHONPATH=/py \
  -e "APITAP_SRC=$URL" -e "APITAP_DST=$CH_DST" \
  -e "APITAP_TABLE=$TBL" -e "BUDGET_S=$BUDGET" -e "ZERO_STOP=99" -e "CPUS=$CPUS" \
  -e "APITAP_SLOTS=$SLOTS" \
  -e "APITAP_TAG=$TAG" -v "$HERE:/job:ro" python:3.13-slim sh /job/leg.sh >/dev/null
for _ in $(seq 1 180); do docker inspect "prof-heavy-$TAG" >/dev/null 2>&1 && break; sleep 1; done
sleep 12

WF="$LOGS/$TAG.witness"
: > "$WF"
W0=$(witness); T0=$(date +%s); echo "W0 $W0" >> "$WF"
echo "== round A (1000 changes/tx; 700U+150I+150D) =="
if gen_round A; then
  W1=$(witness); T1=$(date +%s); echo "W1 $W1" >> "$WF"
  echo "== round B (1000 changes/tx; 400U+300I+300D) =="
  gen_round B || true
  W2=$(witness); T2=$(date +%s); echo "W2 $W2" >> "$WF"
else
  W1=$(witness); T1=$(date +%s); echo "W1 $W1" >> "$WF"
  W2="$W1"; T2=$T1; echo "W2 $W2" >> "$WF"
fi

echo "== waiting for the drain (budget ${BUDGET}s) =="
for _ in $(seq 1 $((BUDGET/5 + 240))); do
  [ "$(docker inspect -f '{{.State.Running}}' "prof-heavy-$TAG" 2>/dev/null)" = "true" ] || break
  sleep 5
done
docker logs "prof-heavy-$TAG" > "$LOGS/$TAG.drain.log" 2>&1
STATE=$(docker inspect -f '{{.State.ExitCode}} oom={{.State.OOMKilled}}' "prof-heavy-$TAG" 2>/dev/null || echo "gone")
docker rm -f "prof-heavy-$TAG" >/dev/null 2>&1

echo "== validate =="
CH_C="$CH_CT" PG_C="$PG_C" MY_C="$MY_C" bash "$HERE/validate30.sh" "$ROUTE" "$TVAL" | tail -3

{
  echo "TAG $TAG route=$ROUTE slots=${SLOTS:-default} cpus=$CPUS"
  echo "W0 $W0"; echo "W1 $W1"; echo "W2 $W2"
  echo "GEN_A_S $((T1-T0)) GEN_B_S $((T2-T1))"
  echo "DRAIN_STATE $STATE"
  grep -E "^APITAP_VERSION|^APITAP_SO_MD5|^LEG_TAG" "$LOGS/$TAG.drain.log" | head -3
  grep -E "^DRAIN " "$LOGS/$TAG.drain.log" | tail -5
  grep -E "^LEG_DONE|^MEMPEAK|^MEMEVENTS|^CPUPressure|^EXITCODE|^RAISED" "$LOGS/$TAG.drain.log"
} | tee "$LOGS/$TAG.summary"
echo "HEAVY30_DONE $TAG"
