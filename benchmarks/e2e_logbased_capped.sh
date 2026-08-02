#!/usr/bin/env bash
set -euo pipefail
# CDC on the smallest tier: bootstrap AND a multi-million-event drain inside
# a 0.5 cpu / 44 MB container. The windowed drain (budget = cgroup/8,
# floor 4 MiB) must hold the cap; APITAP_DEBUG shows the window count.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
T=cdc_cap
WHEELS=~/apitap-lib/benchmarks/wheels
CAP="--memory=44m --memory-swap=44m --cpus=0.5"

echo "== prep venv (uncapped — pip itself doesn't fit 44 MB)"
sudo rm -rf ~/cap-venv && mkdir -p ~/cap-venv
docker run --rm --network host -v "$WHEELS":/w -v ~/cap-venv:/venv python:3.11-slim \
  sh -c "python -m venv /venv && /venv/bin/pip install -q /w/apitap-0.17.0-*.whl"

echo "== reset + seed 1M"
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null 2>&1 || true
$PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
$PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series(1,1000000) g" >/dev/null

echo "== bootstrap INSIDE the cap (replace path, proven tier)"
docker run --rm --network host $CAP -v ~/cap-venv:/venv -v ~/apitap-lib/benchmarks:/b \
  -e APITAP_DEBUG=1 python:3.11-slim /venv/bin/python -c "
import apitap
r = apitap.transfer('postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                    'postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst',
                    table='cdc_cap', mode='log_based')
print(f'bootstrap: rows={r.rows:,}')"

echo "== generate ~2.2M-event backlog in ~20K-event transactions"
# A transaction ALWAYS buffers whole (pgoutput v1 ships it only after its
# commit) — the 44 MB tier's workload contract is many normal-sized
# transactions, so the backlog uses ~20K-event ones; the budget's job is
# packing MANY of them per window without breaching the cap.
for i in $(seq 0 49); do
  a=$((1000001 + i*20000)); b=$((a + 19999))
  $PSRC "INSERT INTO $T SELECT g, 'w'||g, g FROM generate_series($a, $b) g" >/dev/null
done
for i in $(seq 0 54); do
  a=$((i*40000)); b=$((a + 39999))
  $PSRC "UPDATE $T SET n = n + 1 WHERE id % 2 = 0 AND id BETWEEN $a AND $b" >/dev/null
done
for i in $(seq 0 10); do
  a=$((i*200000)); b=$((a + 199999))
  $PSRC "DELETE FROM $T WHERE id % 20 = 0 AND id BETWEEN $a AND $b" >/dev/null
done

echo "== drain INSIDE the cap (timed, windowed)"
docker run --rm --network host $CAP -v ~/cap-venv:/venv -v ~/apitap-lib/benchmarks:/b \
  -e APITAP_DEBUG=1 python:3.11-slim /venv/bin/python /b/capped_drain.py

echo "== verify"
A=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
B=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "src $A / dst $B"; [ "$A" = "$B" ] && echo MATCH || { echo MISMATCH; exit 1; }

echo "== cleanup"
$PDST "DROP TABLE IF EXISTS $T; DELETE FROM _apitap_state WHERE dest_table='$T'" >/dev/null
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
echo done
