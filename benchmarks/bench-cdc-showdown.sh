#!/bin/bash
# CDC catch-up showdown: apitap mode="log_based" vs ape-dts CDC (rdb_merge).
# Both tools: slot created BEFORE an identical 650K-event window, then timed
# from start until the destination equals the source (their own methodology).
set -u
PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -Atc"
LOG(){ echo "[$(date +%H:%M:%S)] $*"; }

T=cdc_race
LOG "── setup: fresh $T (100K seed), two dests"
$PSRC "DROP TABLE IF EXISTS $T CASCADE" >/dev/null
$PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
$PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series(1,100000) g" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%' OR slot_name LIKE 'apedts%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1; done
$PDST "DROP DATABASE IF EXISTS apedts_dst" >/dev/null 2>&1
$PDST "CREATE DATABASE apedts_dst" >/dev/null
docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc \
  "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null 2>&1

LOG "── apitap bootstrap (slot + pinned full load)"
~/ice-run/bin/python - <<EOF
import apitap, time
t0=time.time()
r = apitap.transfer("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src",
                    "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst",
                    table="$T", mode="log_based")
print(f"apitap bootstrap: {r.rows:,} rows in {time.time()-t0:.1f}s")
EOF

LOG "── ape-dts: seed dest + create its slot (brief cdc start, then stop)"
~/ice-run/bin/python - <<EOF
import apitap
r = apitap.transfer("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src",
                    "postgres://postgres:bench@127.0.0.1:5545/apedts_dst",
                    table="$T", mode="replace")
print(f"apedts dest seeded: {r.rows:,} rows")
EOF
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
docker rm -f apedts-run >/dev/null 2>&1
docker run -d --name apedts-run --network=host -v ~/apedts:/task apecloud/ape-dts:latest /task/task_config.ini >/dev/null
sleep 12
docker stop apedts-run >/dev/null; docker rm -f apedts-run >/dev/null 2>&1
$PSRC "SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots"

LOG "── generate the 650K-event window (identical for both)"
$PSRC "INSERT INTO $T SELECT g, 'bulk'||g, g FROM generate_series(200000, 699999) g" >/dev/null
$PSRC "UPDATE $T SET v='rw', n=n+1 WHERE id BETWEEN 200000 AND 299999" >/dev/null
$PSRC "DELETE FROM $T WHERE id BETWEEN 600000 AND 649999" >/dev/null
SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
LOG "source now: $SRC_SUM"

LOG "── apitap log_based catch-up"
APITAP_DEBUG=1 ~/ice-run/bin/python - <<EOF
import apitap, time
t0=time.time()
r = apitap.transfer("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src",
                    "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst",
                    table="$T", mode="log_based")
print(f"APITAP CATCH-UP: {r.rows:,} events in {time.time()-t0:.1f}s")
EOF
A=$(docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
LOG "apitap dest: $A vs src: $SRC_SUM → $([ "$A" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "── ape-dts catch-up (poll until equal, kill at 900s)"
docker run -d --name apedts-run --network=host -v ~/apedts:/task apecloud/ape-dts:latest /task/task_config.ini >/dev/null
T0=$(date +%s)
CAUGHT=""
for i in $(seq 1 900); do
  B=$(docker exec apitap-bench-pg-dst psql -U postgres -d apedts_dst -Atc "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T" 2>/dev/null)
  if [ "$B" = "$SRC_SUM" ]; then CAUGHT=$(( $(date +%s) - T0 )); break; fi
  sleep 1
done
docker stop apedts-run >/dev/null 2>&1; docker rm -f apedts-run >/dev/null 2>&1
if [ -n "$CAUGHT" ]; then
  LOG "APE-DTS CATCH-UP: caught up in ${CAUGHT}s → MATCH"
else
  LOG "APE-DTS CATCH-UP: NOT caught up after 900s (dest: $B vs src: $SRC_SUM)"
fi
$PSRC "SELECT pg_drop_replication_slot('apedts_bench')" >/dev/null 2>&1
echo "CDC SHOWDOWN COMPLETE"
