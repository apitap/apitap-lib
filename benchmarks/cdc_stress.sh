#!/bin/bash
# CDC stress: Postgres -> ClickHouse, 10 tables, everything CDC-side inside
# 0.5 CPU / 256 MB. Seed 10M, then 3 bursts of 10M changes each.
exec > >(tee -a /home/ubuntu/cdc-stress.log) 2>&1
ts() { date +"[%H:%M:%S]"; }
set -e
die() { echo "$(ts) STEP FAILED — aborting"; exit 1; }
CAP="--cpus=0.5 --memory=256m --memory-swap=256m"
V="-v /home/ubuntu/read-venv:/venv"
WHEEL=/home/ubuntu/apitap-lib/benchmarks/wheels/apitap-0.26.0-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

capped() { # $1=script $2=label
  docker run --rm --network host $CAP $V -v "/home/ubuntu/$1:/s.py:ro" python:3.11-slim \
    /venv/bin/python /s.py "$2"
}

echo ""
echo "$(ts) ================= CDC STRESS: pg -> ch, 10 tables ================="
docker run --rm --network host -v /home/ubuntu/apitap-lib/benchmarks/wheels:/w:ro $V \
  python:3.11-slim sh -c "/venv/bin/pip install -q --force-reinstall --no-deps /w/$(basename $WHEEL)"

echo "$(ts) [0/5] clean slate + seed 10 x 1M rows"
docker exec -i apitap-bench-pg-src psql -U postgres -d apitap_bench_src -q <<'SQL'
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;
SQL
for i in $(seq -w 1 10); do
  curl -s "http://127.0.0.1:8124/?user=default&password=bench" --data "DROP TABLE IF EXISTS cdc_t$i" >/dev/null
done
curl -s "http://127.0.0.1:8124/?user=default&password=bench" --data "ALTER TABLE _apitap_state DELETE WHERE 1=1 SETTINGS mutations_sync=1" >/dev/null 2>&1 || true
~/ice-run/bin/python ~/cdc_stress_seed.py 1000000 || die

echo "$(ts) [1/5] BOOTSTRAP — full load 10M rows + slot coordinate (capped)"
capped cdc_drain.py "bootstrap" || die

for r in 1 2 3; do
  echo "$(ts) [$((r+1))/5] ROUND $r — write 10M changes, then drain (capped)"
  ~/ice-run/bin/python ~/cdc_burst.py 1000000 || die
  capped cdc_drain.py "round $r  " || die
done

echo "$(ts) [5/5] verify pg vs ch, per table"
~/ice-run/bin/python ~/cdc_verify.py
echo "$(ts) CDC STRESS DONE"
