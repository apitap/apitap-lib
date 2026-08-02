#!/usr/bin/env bash
set -euo pipefail
# ingestr CDC (v1.1+, postgres+cdc:// batch mode — their equivalent of
# mode="log_based") raced on the standard recipe: slot+publication created
# BEFORE an identical 650K-event window, catch-up timed until the
# destination matches the source. Deletes may be SOFT (_cdc_deleted) —
# verification filters them. Legs: uncapped, then 0.5 cpu / 256 MB.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
T=cdc_ing
LOG(){ echo; echo "== $*"; }

LOG "ensure the ingestr image"
if ! docker image inspect ingestr-race >/dev/null 2>&1; then
  docker build -q -t ingestr-race - <<'DOCKEREOF'
FROM python:3.11-slim
RUN pip install --no-cache-dir -q ingestr
DOCKEREOF
fi
docker run --rm ingestr-race ingestr --version || true

reset_all() {
  for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
    $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
  for p in $($PSRC "SELECT pubname FROM pg_publication"); do
    $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
  $PSRC "DROP TABLE IF EXISTS $T" >/dev/null
  $PDST "DROP TABLE IF EXISTS $T" >/dev/null 2>&1 || true
  $PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
  $PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series(1,100000) g" >/dev/null
  $PSRC "CREATE PUBLICATION ing_pub FOR TABLE ONLY $T" >/dev/null
  $PSRC "SELECT pg_create_logical_replication_slot('ing_slot','pgoutput')" >/dev/null
}

ING_SRC="postgres+cdc://postgres:bench@127.0.0.1:5544/apitap_bench_src?publication=ing_pub&slot=ing_slot"
ING_DST="postgresql://postgres:bench@127.0.0.1:5545/apitap_bench_dst"

run_leg() { # $1 label, $2 extra docker flags
  local label=$1 flags=$2
  reset_all
  LOG "[$label] ingestr bootstrap run (their own first pass, uncapped setup)"
  local t0=$(date +%s)
  docker run --rm --network host $flags ingestr-race ingestr ingest \
    --source-uri "$ING_SRC" --source-table "public.$T" \
    --dest-uri "$ING_DST" --dest-table "public.$T" --yes >/tmp/ing1.log 2>&1 \
    || { echo "  bootstrap run FAILED:"; tail -5 /tmp/ing1.log; return 1; }
  echo "  bootstrap: $(( $(date +%s) - t0 ))s, dest rows: $($PDST "SELECT count(*) FROM $T" 2>/dev/null || echo '?')"

  LOG "[$label] generate the 650K-event window"
  $PSRC "INSERT INTO $T SELECT g, 'bulk'||g, g FROM generate_series(200000, 699999) g" >/dev/null
  $PSRC "UPDATE $T SET v='rw', n=n+1 WHERE id BETWEEN 200000 AND 299999" >/dev/null
  $PSRC "DELETE FROM $T WHERE id BETWEEN 600000 AND 649999" >/dev/null
  local SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
  echo "  source truth: $SRC_SUM"

  LOG "[$label] ingestr catch-up (timed, batch mode exits when caught up)"
  t0=$(date +%s)
  docker run --rm --network host $flags ingestr-race ingestr ingest \
    --source-uri "$ING_SRC" --source-table "public.$T" \
    --dest-uri "$ING_DST" --dest-table "public.$T" --yes >/tmp/ing2.log 2>&1 \
    || { echo "  catch-up FAILED:"; tail -5 /tmp/ing2.log; return 1; }
  local ING_T=$(( $(date +%s) - t0 ))
  local HAS_SOFT=$($PDST "SELECT count(*) FROM information_schema.columns WHERE table_name='$T' AND column_name='_cdc_deleted'")
  local B
  if [ "$HAS_SOFT" != "0" ]; then
    B=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T WHERE coalesce(_cdc_deleted,false) = false")
  else
    B=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
  fi
  echo "INGESTR [$label] CATCH-UP: ${ING_T}s — dst $B $([ "$B" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"
}

run_leg "uncapped" ""
run_leg "0.5cpu/256MB" "--cpus=0.5 --memory=256m --memory-swap=256m"

LOG "cleanup (standing rule)"
$PDST "DROP TABLE IF EXISTS $T" >/dev/null
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
for p in $($PSRC "SELECT pubname FROM pg_publication"); do
  $PSRC "DROP PUBLICATION $p" >/dev/null 2>&1 || true; done
echo done
