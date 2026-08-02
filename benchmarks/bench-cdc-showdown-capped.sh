#!/usr/bin/env bash
set -euo pipefail
# The CDC race, three-way, every catch-up CAPPED at 0.5 cpu / 256 MB:
# apitap mode=log_based vs ape-dts (rdb_merge) vs pipelinewise
# (tap-postgres LOG_BASED → target-postgres). Setup (bootstraps, window
# generation) runs uncapped — only the timed sections are constrained.
# Window identical to bench-cdc-showdown.sh: 500K inserts (ONE transaction —
# apitap buffers a tx whole, the daemons stream; the honest structural
# difference) + 100K updates + 50K deletes.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
T=cdc_bench
CAP="--cpus=0.5 --memory=256m --memory-swap=256m"
LOG(){ echo; echo "== $*"; }

LOG "reset"
docker rm -f apedts-run >/dev/null 2>&1 || true
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null 2>&1 || true
docker exec apitap-bench-pg-dst psql -U postgres -Atc \
  "DROP DATABASE IF EXISTS apedts_dst WITH (FORCE)" >/dev/null
docker exec apitap-bench-pg-dst psql -U postgres -Atc \
  "CREATE DATABASE apedts_dst" >/dev/null
$PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
$PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series(1,100000) g" >/dev/null

LOG "apitap bootstrap (uncapped — not the timed part)"
~/ice-run/bin/python -c "
import apitap
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
print(f'  {r.rows:,} rows')"

LOG "ape-dts: seed dest + create its slot (brief cdc start, then stop)"
~/ice-run/bin/python -c "
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

LOG "pipelinewise: bootstrap (initial sync + slot, uncapped)"
PW="docker run --rm --network host -v $HOME/pw-bench:/w pw-race"
mkdir -p ~/pw-bench
cat > ~/pw-bench/tap.json <<'EOF'
{"host":"127.0.0.1","port":5544,"user":"postgres","password":"bench",
 "dbname":"apitap_bench_src","default_replication_method":"LOG_BASED",
 "logical_poll_total_seconds":10,"break_at_end_lsn":true}
EOF
cat > ~/pw-bench/target.json <<'EOF'
{"host":"127.0.0.1","port":5545,"user":"postgres","password":"bench",
 "dbname":"apitap_bench_dst","default_target_schema":"pw",
 "batch_size_rows":100000,"hard_delete":true}
EOF
$PDST "DROP SCHEMA IF EXISTS pw CASCADE; CREATE SCHEMA pw" >/dev/null
$PW /opt/tap/bin/tap-postgres --config /w/tap.json --discover > ~/pw-bench/catalog_raw.json
python3 - <<'PY'
import json, os
p = os.path.expanduser('~/pw-bench')
c = json.load(open(p + '/catalog_raw.json'))
streams = []
for s in c['streams']:
    if s['table_name'] != 'cdc_bench':
        continue
    for m in s['metadata']:
        if m['breadcrumb'] == []:
            m['metadata']['selected'] = True
            m['metadata']['replication-method'] = 'LOG_BASED'
    streams.append(s)
assert streams, 'cdc_bench not discovered'
json.dump({'streams': streams}, open(p + '/catalog.json', 'w'))
PY
$PW bash -c "/opt/tap/bin/tap-postgres --config /w/tap.json --properties /w/catalog.json \
  | /opt/target/bin/target-postgres --config /w/target.json > /w/out1.jsonl"
tail -1 ~/pw-bench/out1.jsonl > ~/pw-bench/state.json
echo "  pipelinewise seeded: $($PDST "SELECT count(*) FROM pw.cdc_bench")"

LOG "generate the 650K-event window (identical for all three, ~20K-event txs)"
# Realistic transaction sizes: the 256 MB tier's contract is normal txs —
# a SINGLE 500K-row tx measures 307 MB peak (buffered whole, v1 protocol)
# and needs the 512 MB tier; measured separately.
for i in $(seq 0 24); do
  a=$((200000 + i*20000)); b=$((a + 19999))
  $PSRC "INSERT INTO $T SELECT g, 'bulk'||g, g FROM generate_series($a, $b) g" >/dev/null
done
for i in $(seq 0 4); do
  a=$((200000 + i*20000)); b=$((a + 19999))
  $PSRC "UPDATE $T SET v='rw', n=n+1 WHERE id BETWEEN $a AND $b" >/dev/null
done
for i in $(seq 0 4); do
  a=$((600000 + i*10000)); b=$((a + 9999))
  $PSRC "DELETE FROM $T WHERE id BETWEEN $a AND $b" >/dev/null
done
SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "source now: $SRC_SUM"

LOG "apitap catch-up CAPPED 0.5 cpu / 256 MB"
docker run --rm --network host $CAP -v ~/cap-venv:/venv -e APITAP_DEBUG=1 \
  python:3.11-slim /venv/bin/python -c "
import time, apitap
t0=time.time()
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
peak=int(open('/sys/fs/cgroup/memory.peak').read())
print(f'APITAP CAPPED CATCH-UP: {r.rows:,} events in {time.time()-t0:.1f}s peak={peak/1048576:.0f}MB')"
A=$(docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "apitap dest: $A → $([ "$A" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "ape-dts catch-up CAPPED 0.5 cpu / 256 MB (poll until equal, kill at 900s)"
docker run -d --name apedts-run --network=host $CAP -v ~/apedts:/task apecloud/ape-dts:latest /task/task_config.ini >/dev/null
T0=$(date +%s)
CAUGHT=""
for i in $(seq 1 900); do
  B=$(docker exec apitap-bench-pg-dst psql -U postgres -d apedts_dst -Atc "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T" 2>/dev/null)
  if [ "$B" = "$SRC_SUM" ]; then CAUGHT=$(( $(date +%s) - T0 )); break; fi
  if ! docker ps -q -f name=apedts-run | grep -q .; then echo "  ape-dts container DIED (OOM?)"; break; fi
  sleep 1
done
PEAK=$(docker inspect apedts-run --format '{{.State.OOMKilled}}' 2>/dev/null || echo "?")
docker stop apedts-run >/dev/null 2>&1 || true; docker rm -f apedts-run >/dev/null 2>&1 || true
if [ -n "$CAUGHT" ]; then
  echo "APE-DTS CAPPED CATCH-UP: ${CAUGHT}s → MATCH (oom-killed=$PEAK)"
else
  echo "APE-DTS CAPPED CATCH-UP: not caught up (dest: ${B:-?} vs src: $SRC_SUM, oom-killed=$PEAK)"
fi

LOG "pipelinewise catch-up CAPPED 0.5 cpu / 256 MB"
T0=$(date +%s)
docker run --rm --network host $CAP -v $HOME/pw-bench:/w pw-race bash -c \
  "/opt/tap/bin/tap-postgres --config /w/tap.json --properties /w/catalog.json --state /w/state.json \
   | /opt/target/bin/target-postgres --config /w/target.json > /w/out2.jsonl" \
  && PW_RC=0 || PW_RC=$?
PW_T=$(( $(date +%s) - T0 ))
PW_SUM=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM pw.cdc_bench" 2>/dev/null || echo "?")
echo "PIPELINEWISE CAPPED CATCH-UP: ${PW_T}s rc=$PW_RC — dst $PW_SUM $([ "$PW_SUM" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "cleanup (standing rule)"
$PDST "DROP SCHEMA IF EXISTS pw CASCADE" >/dev/null
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null
docker exec apitap-bench-pg-dst psql -U postgres -Atc "DROP DATABASE IF EXISTS apedts_dst WITH (FORCE)" >/dev/null
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
