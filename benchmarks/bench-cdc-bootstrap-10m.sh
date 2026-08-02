#!/usr/bin/env bash
set -euo pipefail
# Full-load honesty check at 10M rows, per CDC destination: plain replace vs
# the log_based BOOTSTRAP (replace pinned to the slot snapshot + identity +
# state) — the bootstrap should cost replace + noise, nothing more.
# Standing rule: dest tables dropped after, seeds kept.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
PY=~/ice-run/bin/python
T=bench_data_10m_cap

echo "== seed sanity (10M + PK — log_based needs the identity)"
n=$($PSRC "SELECT count(*) FROM $T")
[ "$n" = "10000000" ] || { echo "seed is $n rows"; exit 1; }
$PSRC "ALTER TABLE $T ADD PRIMARY KEY (id)" >/dev/null 2>&1 || true

echo "== reset slots/pubs/state"
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done

$PY - <<'PY'
import time, apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "bench_data_10m_cap"
DESTS = [
    ("pg",  "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"),
    ("ch",  "clickhouse://default:bench@127.0.0.1:8124/default"),
    ("my",  "mysql://root:bench@127.0.0.1:3307/bench"),
    ("ice", "iceberg://127.0.0.1:8181/cdc_e2e?endpoint=http://127.0.0.1:9100"
            "&access_key_id=bench&secret_access_key=benchbench"),
]

for name, dst in DESTS:
    t0 = time.time()
    r = apitap.transfer(SRC, dst, table=T, dest_table="boot10m", mode="replace")
    print(f"[{name}] replace   10M: {time.time()-t0:6.1f}s rows={r.rows:,}", flush=True)
    t0 = time.time()
    r = apitap.transfer(SRC, dst, table=T, dest_table="boot10m_cdc", mode="log_based")
    print(f"[{name}] bootstrap 10M: {time.time()-t0:6.1f}s rows={r.rows:,}", flush=True)
PY

echo "== cleanup (dest dropped, slots/pubs/state cleared, seed kept)"
$PDST "DROP TABLE IF EXISTS boot10m; DROP TABLE IF EXISTS boot10m_cdc; \
       DELETE FROM _apitap_state WHERE dest_table LIKE 'boot10m%'" >/dev/null
docker exec apitap-bench-ch clickhouse-client -u default --password bench -q \
  "DROP TABLE IF EXISTS boot10m; DROP TABLE IF EXISTS boot10m_cdc; \
   DELETE FROM \`_apitap_state\` WHERE dest_table LIKE 'boot10m%'" >/dev/null
docker exec apitap-bench-my mysql -uroot -pbench bench -e \
  "DROP TABLE IF EXISTS boot10m; DROP TABLE IF EXISTS boot10m_cdc; \
   DELETE FROM _apitap_state WHERE dest_table LIKE 'boot10m%'" 2>/dev/null
for t in boot10m boot10m_cdc; do
  curl -s -X DELETE "http://127.0.0.1:8181/v1/namespaces/cdc_e2e/tables/$t?purgeRequested=true" >/dev/null || true
done
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
