#!/usr/bin/env bash
set -euo pipefail
# The ape-dts race at scale: 8M-row table, ONE shared 2.5M-event window
# (1M-row SINGLE transaction + 1M chunked updates + 0.5M chunked deletes),
# both slots created BEFORE the window, both timed until the destination
# matches the source exactly. Same recipe as bench-cdc-showdown.sh, 4x the
# events, ≤10M rows throughout.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
PY=~/ice-run/bin/python
T=cdc_bench
LOG(){ echo; echo "== $*"; }

LOG "reset + seed 8M"
docker rm -f apedts-run >/dev/null 2>&1 || true
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null 2>&1 || true
docker exec apitap-bench-pg-dst psql -U postgres -Atc "DROP DATABASE IF EXISTS apedts_dst WITH (FORCE)" >/dev/null
docker exec apitap-bench-pg-dst psql -U postgres -Atc "CREATE DATABASE apedts_dst" >/dev/null
$PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
for i in $(seq 0 7); do a=$((i*1000000+1)); $PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series($a, $((a+999999))) g" >/dev/null; done

LOG "apitap bootstrap (slot + pinned full load)"
$PY -c "
import time, apitap
t0=time.time()
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
print(f'  {r.rows:,} rows in {time.time()-t0:.1f}s')"

LOG "ape-dts: seed dest + create its slot"
$PY -c "
import apitap
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apedts_dst',
                    table='$T', mode='replace')
print(f'  seeded {r.rows:,} rows')"
docker exec apitap-bench-pg-dst psql -U postgres -d apedts_dst -Atc \
  "ALTER TABLE $T ADD PRIMARY KEY (id)" >/dev/null
mkdir -p ~/apedts && cat > ~/apedts/task_config.ini <<CFG
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src
slot_name=apedts_bench

[filter]
do_dbs=
do_events=insert,update,delete
do_tbs=public.$T
do_structures=

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:bench@127.0.0.1:5545/apedts_dst
batch_size=200

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
CFG
docker run -d --name apedts-run --network=host -v ~/apedts:/task apecloud/ape-dts:latest /task/task_config.ini >/dev/null
sleep 12
docker stop apedts-run >/dev/null; docker rm -f apedts-run >/dev/null 2>&1

LOG "generate the 2.5M-event window (identical for both)"
$PSRC "INSERT INTO $T SELECT g, 'big'||g, g FROM generate_series(20000001, 21000000) g" >/dev/null
for i in $(seq 0 9); do a=$((i*100000+1)); $PSRC "UPDATE $T SET n=n+1 WHERE id BETWEEN $a AND $((a+99999))" >/dev/null; done
for i in $(seq 0 4); do a=$((6000000+i*100000+1)); $PSRC "DELETE FROM $T WHERE id BETWEEN $a AND $((a+99999))" >/dev/null; done
SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "  source truth: $SRC_SUM"

LOG "apitap catch-up (timed)"
t0=$(date +%s)
$PY -c "
import apitap
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
print(f'  events={r.rows:,}')"
AP_T=$(( $(date +%s) - t0 ))
A=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "APITAP CATCH-UP: ${AP_T}s — $([ "$A" = "$SRC_SUM" ] && echo MATCH || echo "MISMATCH ($A)")"

LOG "ape-dts catch-up (poll until equal, kill at 1200s)"
docker run -d --name apedts-run --network=host -v ~/apedts:/task apecloud/ape-dts:latest /task/task_config.ini >/dev/null
T0=$(date +%s)
CAUGHT=""
for i in $(seq 1 1200); do
  B=$(docker exec apitap-bench-pg-dst psql -U postgres -d apedts_dst -Atc "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T" 2>/dev/null)
  if [ "$B" = "$SRC_SUM" ]; then CAUGHT=$(( $(date +%s) - T0 )); break; fi
  if ! docker ps -q -f name=apedts-run | grep -q .; then echo "  ape-dts container DIED"; break; fi
  sleep 1
done
docker stop apedts-run >/dev/null 2>&1 || true; docker rm -f apedts-run >/dev/null 2>&1 || true
if [ -n "$CAUGHT" ]; then
  echo "APE-DTS CATCH-UP: ${CAUGHT}s — MATCH"
else
  echo "APE-DTS CATCH-UP: not caught up in 1200s (dest: ${B:-?})"
fi

LOG "cleanup (standing rule)"
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null
docker exec apitap-bench-pg-dst psql -U postgres -Atc "DROP DATABASE IF EXISTS apedts_dst WITH (FORCE)" >/dev/null
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
