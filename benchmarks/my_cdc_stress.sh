#!/bin/bash
# CDC stress, MySQL edition: binlog -> ClickHouse, 10 tables, CDC side inside
# 0.5 CPU / 256 MB. Same shape as the Postgres run so the numbers compare.
exec > >(tee -a /home/ubuntu/my-cdc-stress.log) 2>&1
ts() { date +"[%H:%M:%S]"; }
set -e
die() { echo "$(ts) STEP FAILED — aborting"; exit 1; }
CAP="--cpus=0.5 --memory=256m --memory-swap=256m"
V="-v /home/ubuntu/read-venv:/venv"
CH="http://127.0.0.1:8124/?user=default&password=bench"

capped() { docker run --rm --network host $CAP $V -v "/home/ubuntu/$1:/s.py:ro" python:3.11-slim /venv/bin/python /s.py "$2"; }

echo ""
echo "$(ts) ============ CDC STRESS (MySQL binlog): my -> ch, 10 tables ============"
docker run --rm --network host -v /home/ubuntu/apitap-lib/benchmarks/wheels:/w:ro $V python:3.11-slim \
  sh -c "/venv/bin/pip install -q --force-reinstall --no-deps /w/apitap-0.26.0-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"

echo "$(ts) [0/5] clean slate + seed 10 x 1M rows"
for i in $(seq -w 1 10); do curl -s "$CH" --data "DROP TABLE IF EXISTS my_cdc_t$i" >/dev/null; done
curl -s "$CH" --data "ALTER TABLE _apitap_state DELETE WHERE 1=1 SETTINGS mutations_sync=1" >/dev/null 2>&1 || true
docker exec apitap-bench-my mysql -uroot -pbench -e "RESET MASTER" 2>/dev/null || true
~/ice-run/bin/python ~/my_cdc_seed.py 1000000 || die

echo "$(ts) [1/5] BOOTSTRAP — full load 10M rows + binlog coordinate (capped)"
capped my_cdc_drain.py "bootstrap" || die

for r in 1 2 3; do
  echo "$(ts) [$((r+1))/5] ROUND $r — write 10M changes, then drain (capped)"
  ~/ice-run/bin/python ~/my_cdc_burst.py 1000000 || die
  capped my_cdc_drain.py "round $r  " || die
done

echo "$(ts) [5/5] verify mysql vs clickhouse, per table"
~/ice-run/bin/python ~/my_cdc_verify.py
echo "$(ts) MY CDC STRESS DONE"
