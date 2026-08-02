#!/usr/bin/env bash
set -euo pipefail
# ape-dts FULL LOAD (extract_type=snapshot), measured on the same 10M-row
# table apitap's numbers come from (bench_data_10m_cap), pg→pg, two tiers:
# uncapped and 0.5 cpu / 256 MB. Dest structure pre-created (their contract).
# apitap references, same table: pg→pg 21.7 s uncapped / 62.4 s capped;
# pg→ch 10.8 s uncapped / 23.0 s capped.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PAD="docker exec apitap-bench-pg-dst psql -U postgres -d apedts_dst -Atc"
T=bench_data_10m_cap
LOG(){ echo; echo "== $*"; }

LOG "prep: fresh apedts_dst + table structure from the source"
docker rm -f apedts-run >/dev/null 2>&1 || true
docker exec apitap-bench-pg-dst psql -U postgres -Atc "DROP DATABASE IF EXISTS apedts_dst WITH (FORCE)" >/dev/null
docker exec apitap-bench-pg-dst psql -U postgres -Atc "CREATE DATABASE apedts_dst" >/dev/null
docker exec apitap-bench-pg-src pg_dump -U postgres -d apitap_bench_src -s -t $T \
  | docker exec -i apitap-bench-pg-dst psql -U postgres -d apedts_dst -q
echo "  dest structure: $($PAD "SELECT count(*) FROM information_schema.columns WHERE table_name='$T'") columns"

mkdir -p ~/apedts && cat > ~/apedts/snap_config.ini <<CFG
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src

[filter]
do_dbs=
do_events=insert
do_tbs=public.$T
do_structures=

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:bench@127.0.0.1:5545/apedts_dst
batch_size=200

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
CFG

run_snapshot() { # $1 label, $2 extra docker flags
  local label=$1 flags=$2
  $PAD "TRUNCATE $T" >/dev/null
  docker run -d --name apedts-run --network=host $flags -v ~/apedts:/task apecloud/ape-dts:latest /task/snap_config.ini >/dev/null
  local T0=$(date +%s) DONE="" N=0
  for i in $(seq 1 1800); do
    N=$($PAD "SELECT count(*) FROM $T" 2>/dev/null || echo 0)
    if [ "$N" = "10000000" ]; then DONE=$(( $(date +%s) - T0 )); break; fi
    if ! docker ps -q -f name=apedts-run | grep -q .; then
      OOM=$(docker inspect apedts-run --format '{{.State.OOMKilled}}' 2>/dev/null || echo '?')
      echo "  [$label] container EXITED (oom=$OOM) at $(( $(date +%s) - T0 ))s with $N rows"
      break
    fi
    sleep 2
  done
  docker stop apedts-run >/dev/null 2>&1 || true; docker rm -f apedts-run >/dev/null 2>&1 || true
  if [ -n "$DONE" ]; then
    echo "  [$label] APE-DTS FULL LOAD 10M: ${DONE}s"
  else
    echo "  [$label] incomplete: $N rows"
  fi
}

LOG "run 1: uncapped"
run_snapshot "uncapped" ""

LOG "run 2: capped 0.5 cpu / 256 MB"
run_snapshot "0.5cpu/256MB" "--cpus=0.5 --memory=256m --memory-swap=256m"

LOG "cleanup"
docker exec apitap-bench-pg-dst psql -U postgres -Atc "DROP DATABASE IF EXISTS apedts_dst WITH (FORCE)" >/dev/null
echo done
