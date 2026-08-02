#!/usr/bin/env bash
set -euo pipefail
# Post-speed-campaign validation at the 10M scale (campaign rule: ≤10M rows).
# (1) 10M full-load parity: replace vs log_based bootstrap, ch + pg.
# (2) CDC at scale: an 8M-row table, both slots created, then ONE shared
#     2.5M-event window (1M-row SINGLE transaction — the streaming stress —
#     + 1M chunked updates + 0.5M chunked deletes), drained to ch then pg,
#     both row-verified.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
CH="docker exec apitap-bench-ch clickhouse-client -u default --password bench -q"
PY=~/ice-run/bin/python
T=cdc10m
LOG(){ echo; echo "== $*"; }

SRC="postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DPG="postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
DCH="clickhouse://default:bench@127.0.0.1:8124/default"

LOG "PART 1: 10M full-load parity (bench_data_10m_cap)"
for pair in "ch:$DCH" "pg:$DPG"; do
  name=${pair%%:*}; url=${pair#*:}
  $PDST "DROP TABLE IF EXISTS fl10; DROP TABLE IF EXISTS fl10cdc; DELETE FROM _apitap_state WHERE dest_table LIKE 'fl10%'" >/dev/null 2>&1 || true
  $CH "DROP TABLE IF EXISTS fl10; DROP TABLE IF EXISTS fl10cdc; DELETE FROM \`_apitap_state\` WHERE dest_table LIKE 'fl10%'" 2>/dev/null || true
  $PY -c "
import time, apitap
t0=time.time(); r=apitap.transfer('$SRC','$url',table='bench_data_10m_cap',dest_table='fl10')
print(f'  [$name] replace   10M: {time.time()-t0:5.1f}s rows={r.rows:,}')
t0=time.time(); r=apitap.transfer('$SRC','$url',table='bench_data_10m_cap',dest_table='fl10cdc',mode='log_based')
print(f'  [$name] bootstrap 10M: {time.time()-t0:5.1f}s rows={r.rows:,}')"
  $PDST "DROP TABLE IF EXISTS fl10; DROP TABLE IF EXISTS fl10cdc; DELETE FROM _apitap_state WHERE dest_table LIKE 'fl10%'" >/dev/null 2>&1 || true
  $CH "DROP TABLE IF EXISTS fl10; DROP TABLE IF EXISTS fl10cdc; DELETE FROM \`_apitap_state\` WHERE dest_table LIKE 'fl10%'" 2>/dev/null || true
  for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
    $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
done

LOG "PART 2: CDC at scale — seed 8M"
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table LIKE '$T%'" >/dev/null 2>&1 || true
$CH "DROP TABLE IF EXISTS ${T}ch; DELETE FROM \`_apitap_state\` WHERE dest_table LIKE '$T%'" 2>/dev/null || true
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
$PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
for i in $(seq 0 7); do a=$((i*1000000+1)); $PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series($a, $((a+999999))) g" >/dev/null; done
echo "  seeded: $($PSRC "SELECT count(*) FROM $T")"

LOG "bootstrap both dests (both slots exist BEFORE the window)"
$PY -c "
import time, apitap
t0=time.time(); r=apitap.transfer('$SRC','$DCH',table='$T',dest_table='${T}ch',mode='log_based')
print(f'  ch bootstrap 8M: {time.time()-t0:5.1f}s rows={r.rows:,}')
t0=time.time(); r=apitap.transfer('$SRC','$DPG',table='$T',mode='log_based')
print(f'  pg bootstrap 8M: {time.time()-t0:5.1f}s rows={r.rows:,}')"

LOG "generate the 2.5M-event window (1M-row SINGLE tx + 1M upd + 0.5M del)"
$PSRC "INSERT INTO $T SELECT g, 'big'||g, g FROM generate_series(20000001, 21000000) g" >/dev/null
for i in $(seq 0 9); do a=$((i*100000+1)); $PSRC "UPDATE $T SET n=n+1 WHERE id BETWEEN $a AND $((a+99999))" >/dev/null; done
for i in $(seq 0 4); do a=$((6000000+i*100000+1)); $PSRC "DELETE FROM $T WHERE id BETWEEN $a AND $((a+99999))" >/dev/null; done
SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "  source truth: $SRC_SUM"

LOG "drain to CLICKHOUSE (primary, timed)"
APITAP_DEBUG=1 $PY -c "
import time, apitap
t0=time.time(); r=apitap.transfer('$SRC','$DCH',table='$T',dest_table='${T}ch',mode='log_based')
print(f'  ch drain: {r.rows:,} events in {time.time()-t0:.1f}s')" 2>&1 | grep -vE "^\[log_based\] window|^\[log_based\] applied" || true
B=$($CH "SELECT concat(toString(count()),'|',toString(sum(id)),'|',toString(sum(n))) FROM ${T}ch")
echo "  ch dest: $B $([ "$B" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "drain to POSTGRES (timed)"
APITAP_DEBUG=1 $PY -c "
import time, apitap
t0=time.time(); r=apitap.transfer('$SRC','$DPG',table='$T',mode='log_based')
print(f'  pg drain: {r.rows:,} events in {time.time()-t0:.1f}s')" 2>&1 | grep -vE "^\[log_based\] window|^\[log_based\] applied" || true
C=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "  pg dest: $C $([ "$C" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "cleanup (standing rule)"
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table LIKE '$T%'" >/dev/null
$CH "DROP TABLE IF EXISTS ${T}ch; DELETE FROM \`_apitap_state\` WHERE dest_table LIKE '$T%'"
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
