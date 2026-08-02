#!/usr/bin/env bash
set -euo pipefail
# Drain-speed evidence harness: fresh baseline of the 650K-event windows
# (giant-single-tx vs chunked-tx shapes), the server's spill counters
# around each drain, and a perf profile of the WALSENDER backend while it
# decodes. Optional: WORKMEM=2GB applies logical_decoding_work_mem
# server-wide (bench container) before the giant run — the spill lever.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
PY=~/ice-run/bin/python
T=cdc_speed
SRC_URL="${SRC_URL:-postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src}"
LOG(){ echo; echo "== $*"; }

if [ -n "${WORKMEM:-}" ]; then
  LOG "setting logical_decoding_work_mem=$WORKMEM (server-wide, bench box)"
  docker exec apitap-bench-pg-src psql -U postgres -Atc \
    "ALTER SYSTEM SET logical_decoding_work_mem='$WORKMEM'" >/dev/null
  docker exec apitap-bench-pg-src psql -U postgres -Atc "SELECT pg_reload_conf()" >/dev/null
fi
docker exec apitap-bench-pg-src psql -U postgres -Atc "SHOW logical_decoding_work_mem"

reset_rig() {
  $PSRC "DROP TABLE IF EXISTS $T" >/dev/null
  $PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null 2>&1 || true
  for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
    $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
  for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
    $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
  $PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
  $PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series(1,100000) g" >/dev/null
  $PY -c "
import apitap
r = apitap.transfer('$SRC_URL',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
print(f'  bootstrap {r.rows:,}')"
}

spills() {
  $PSRC "SELECT coalesce(sum(spill_txns),0)||' txns / '||coalesce(sum(spill_count),0)||' spills / '||pg_size_pretty(coalesce(sum(spill_bytes),0)) FROM pg_stat_replication_slots"
}

drain_timed() { # $1 label, $2 perf-seconds (0 = no perf)
  local label=$1 perfsec=$2
  if [ "$perfsec" != "0" ]; then
    ( for i in $(seq 1 40); do
        WPID=$(pgrep -f "walsender postgres.*apitap_" | head -1 || true)
        [ -n "$WPID" ] && { sudo perf record -q -o /tmp/drain.perf -g -p "$WPID" -- sleep "$perfsec" 2>/dev/null || true; break; }
        sleep 0.2
      done ) &
    PERF_WAITER=$!
  fi
  APITAP_DEBUG=1 $PY -c "
import time, apitap
t0=time.time()
r = apitap.transfer('$SRC_URL',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
print(f'  $label: {r.rows:,} events in {time.time()-t0:.1f}s')"
  if [ "$perfsec" != "0" ]; then
    wait $PERF_WAITER 2>/dev/null || true
    sudo perf report -i /tmp/drain.perf --stdio 2>/dev/null | grep -vE "^#|^$" | head -15 || echo "  (no perf sample)"
  fi
}

LOG "SHAPE 1: giant single tx (500K ins one tx + 100K upd + 50K del)"
reset_rig
$PSRC "SELECT pg_stat_reset_replication_slot(slot_name) FROM pg_replication_slots" >/dev/null 2>&1 || true
$PSRC "INSERT INTO $T SELECT g, 'bulk'||g, g FROM generate_series(200000, 699999) g" >/dev/null
$PSRC "UPDATE $T SET v='rw', n=n+1 WHERE id BETWEEN 200000 AND 299999" >/dev/null
$PSRC "DELETE FROM $T WHERE id BETWEEN 600000 AND 649999" >/dev/null
echo "  spills before: $(spills)"
drain_timed "giant-tx drain" "${PERFSEC:-8}"
echo "  spills after:  $(spills)"

LOG "SHAPE 2: chunked (25x20K ins + 5x20K upd + 5x10K del)"
reset_rig
$PSRC "SELECT pg_stat_reset_replication_slot(slot_name) FROM pg_replication_slots" >/dev/null 2>&1 || true
for i in $(seq 0 24); do a=$((200000+i*20000)); $PSRC "INSERT INTO $T SELECT g, 'bulk'||g, g FROM generate_series($a, $((a+19999))) g" >/dev/null; done
for i in $(seq 0 4); do a=$((200000+i*20000)); $PSRC "UPDATE $T SET v='rw', n=n+1 WHERE id BETWEEN $a AND $((a+19999))" >/dev/null; done
for i in $(seq 0 4); do a=$((600000+i*10000)); $PSRC "DELETE FROM $T WHERE id BETWEEN $a AND $((a+9999))" >/dev/null; done
echo "  spills before: $(spills)"
drain_timed "chunked drain" 0
echo "  spills after:  $(spills)"

LOG "cleanup"
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
