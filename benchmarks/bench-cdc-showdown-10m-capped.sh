#!/usr/bin/env bash
set -euo pipefail
# THE arena: 0.5 cpu / 256 MB — apitap's home turf — at scale. 8M-row
# table, ONE shared 2.5M-event window in normal-sized transactions (the
# small-tier workload contract; a 1M-row single tx needs ~630 MB and is
# out of contract here), both slots created before the window, both
# catch-ups CAPPED, both row-verified.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
PY=~/ice-run/bin/python
T=cdc_bench
CAP="--cpus=0.5 --memory=256m --memory-swap=256m"
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

LOG "apitap bootstrap (uncapped setup)"
$PY -c "
import apitap
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='$T', mode='log_based')
print(f'  {r.rows:,} rows')"

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

LOG "generate the 2.5M-event window (normal-sized txs, identical for both)"
for i in $(seq 0 9); do a=$((20000001+i*100000)); $PSRC "INSERT INTO $T SELECT g, 'big'||g, g FROM generate_series($a, $((a+99999))) g" >/dev/null; done
for i in $(seq 0 9); do a=$((i*100000+1)); $PSRC "UPDATE $T SET n=n+1 WHERE id BETWEEN $a AND $((a+99999))" >/dev/null; done
for i in $(seq 0 4); do a=$((6000000+i*100000+1)); $PSRC "DELETE FROM $T WHERE id BETWEEN $a AND $((a+99999))" >/dev/null; done
SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "  source truth: $SRC_SUM"

LOG "apitap catch-up CAPPED 0.5 cpu / 256 MB"
docker run --rm --network host $CAP -v ~/apitap-lib/benchmarks/wheels:/w python:3.11-slim sh -c "
pip install -q /w/apitap-0.19.0-*.whl 2>/dev/null
python - <<PYEOF
import time, apitap
t0=time.time()
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='cdc_bench', mode='log_based')
peak=int(open('/sys/fs/cgroup/memory.peak').read())
cpu=int([l for l in open('/sys/fs/cgroup/cpu.stat') if l.startswith('usage_usec')][0].split()[1])
wall=time.time()-t0
print(f'APITAP CAPPED: {r.rows:,} events in {wall:.1f}s peak={peak/1048576:.0f}MB avg_cores={cpu/1e6/wall:.2f}')
PYEOF"
A=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "  apitap dest: $([ "$A" = "$SRC_SUM" ] && echo MATCH || echo "MISMATCH ($A)")"

LOG "ape-dts catch-up CAPPED 0.5 cpu / 256 MB (poll until equal, 1800s cap)"
docker run -d --name apedts-run --network=host $CAP -v ~/apedts:/task apecloud/ape-dts:latest /task/task_config.ini >/dev/null
T0=$(date +%s)
CAUGHT=""
for i in $(seq 1 1800); do
  B=$(docker exec apitap-bench-pg-dst psql -U postgres -d apedts_dst -Atc "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T" 2>/dev/null)
  if [ "$B" = "$SRC_SUM" ]; then CAUGHT=$(( $(date +%s) - T0 )); break; fi
  if ! docker ps -q -f name=apedts-run | grep -q .; then
    OOM=$(docker inspect apedts-run --format '{{.State.OOMKilled}}' 2>/dev/null || echo '?')
    echo "  ape-dts container DIED (oom=$OOM) after $(( $(date +%s) - T0 ))s"; break
  fi
  sleep 1
done
docker stop apedts-run >/dev/null 2>&1 || true; docker rm -f apedts-run >/dev/null 2>&1 || true
if [ -n "$CAUGHT" ]; then
  echo "APE-DTS CAPPED: ${CAUGHT}s — MATCH"
else
  echo "APE-DTS CAPPED: not caught up (dest: ${B:-?} vs $SRC_SUM)"
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
