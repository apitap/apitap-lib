#!/usr/bin/env bash
set -euo pipefail
# The multi-table-per-slot wager, measured: Postgres decodes a slot's WAL
# once PER SLOT, so three single-table pipelines pay the decode three times
# — the group pays it once. Identical 3-table workload (~660K events), setup
# A = three single-table slots timed back-to-back, setup B = one group slot,
# both verified row-exact.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
PY=~/ice-run/bin/python
TBLS="cdc_p1 cdc_p2 cdc_p3"
LOG(){ echo; echo "== $*"; }

reset_all() {
  for t in $TBLS; do
    $PSRC "DROP TABLE IF EXISTS $t CASCADE" >/dev/null
    $PDST "DROP TABLE IF EXISTS $t" >/dev/null 2>&1 || true
  done
  $PDST "DELETE FROM _apitap_state WHERE dest_table LIKE 'cdc_p%'" >/dev/null 2>&1 || true
  for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'"); do
    $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
  for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
    $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
  for t in $TBLS; do
    $PSRC "CREATE TABLE $t (id int PRIMARY KEY, v text, n bigint)" >/dev/null
    $PSRC "INSERT INTO $t SELECT g, 'v'||g, g*7 FROM generate_series(1,200000) g" >/dev/null
  done
}

gen_window() {
  # Per table: 150K inserts + 50K updates + 20K deletes = 220K events, 660K total.
  for t in $TBLS; do
    $PSRC "INSERT INTO $t SELECT g, 'w'||g, g FROM generate_series(300000,449999) g" >/dev/null
    $PSRC "UPDATE $t SET n = n+1 WHERE id BETWEEN 1 AND 50000" >/dev/null
    $PSRC "DELETE FROM $t WHERE id BETWEEN 150000 AND 169999" >/dev/null
  done
}

verify() {
  for t in $TBLS; do
    A=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $t")
    B=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $t")
    [ "$A" = "$B" ] || { echo "MISMATCH on $t: src $A dst $B"; exit 1; }
  done
  echo "verified: all 3 MATCH"
}

LOG "SETUP A: three single-table slots"
reset_all
$PY - <<'PY'
import apitap
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
for t in ["cdc_p1", "cdc_p2", "cdc_p3"]:
    r = apitap.transfer(SRC, DST, table=t, mode="log_based")
    print(f"  bootstrap {t}: {r.rows:,}")
PY
gen_window
LOG "A: three sequential catch-ups (timed as one)"
t0=$(date +%s.%N)
APITAP_DEBUG=1 $PY - <<'PY'
import time, apitap
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
for t in ["cdc_p1", "cdc_p2", "cdc_p3"]:
    t1 = time.time()
    r = apitap.transfer(SRC, DST, table=t, mode="log_based")
    print(f"  {t}: {r.rows:,} events in {time.time()-t1:.1f}s")
PY
A_T=$(echo "$(date +%s.%N) - $t0" | bc)
verify
echo "SETUP A TOTAL: ${A_T}s"

LOG "SETUP B: one group slot"
reset_all
$PY - <<'PY'
import apitap
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
r = apitap.transfer(SRC, DST, tables=["cdc_p1", "cdc_p2", "cdc_p3"], mode="log_based")
print(f"  group bootstrap: {r.rows:,}")
PY
gen_window
LOG "B: one group catch-up (timed)"
t0=$(date +%s.%N)
APITAP_DEBUG=1 $PY - <<'PY'
import time, apitap
SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
t1 = time.time()
r = apitap.transfer(SRC, DST, tables=["cdc_p1", "cdc_p2", "cdc_p3"], mode="log_based")
print(f"  group: {r.rows:,} events in {time.time()-t1:.1f}s "
      f"({[(t.table, t.rows) for t in r.tables]})")
PY
B_T=$(echo "$(date +%s.%N) - $t0" | bc)
verify
echo "SETUP B TOTAL: ${B_T}s"

LOG "RESULT: three slots ${A_T}s vs one group ${B_T}s"

LOG "cleanup (standing rule)"
for t in $TBLS; do
  $PSRC "DROP TABLE IF EXISTS $t CASCADE" >/dev/null
  $PDST "DROP TABLE IF EXISTS $t" >/dev/null
done
$PDST "DELETE FROM _apitap_state WHERE dest_table LIKE 'cdc_p%'" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
