#!/usr/bin/env bash
set -euo pipefail
# pipelinewise (tap-postgres LOG_BASED → target-postgres) vs apitap
# mode="log_based", same shape as bench-cdc-showdown.sh: seed 100K rows,
# slot created BEFORE an identical 650K-event window (500K inserts +
# 100K updates + 50K deletes), then each tool timed until the destination
# matches the source exactly.
#
# pipelinewise's LOG_BASED decodes via the wal2json plugin (not pgoutput) —
# the ensure step below builds it inside the source container if missing.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
PDST="docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc"
T=cdc_pw
LOG(){ echo; echo "== $*"; }

LOG "ensure wal2json in the source container"
if ! $PSRC "SELECT 1 FROM pg_create_logical_replication_slot('w2j_probe','wal2json')" >/dev/null 2>&1; then
  docker exec -u root apitap-bench-pg-src sh -c '
    apk add --no-cache build-base git postgresql-dev >/dev/null 2>&1 || apk add --no-cache build-base git >/dev/null;
    rm -rf /tmp/wal2json && git clone -q --depth 1 https://github.com/eulerto/wal2json /tmp/wal2json &&
    cd /tmp/wal2json && make USE_PGXS=1 >/dev/null && make USE_PGXS=1 install >/dev/null && echo wal2json-installed'
  $PSRC "SELECT 1 FROM pg_create_logical_replication_slot('w2j_probe','wal2json')" >/dev/null
fi
$PSRC "SELECT pg_drop_replication_slot('w2j_probe')" >/dev/null 2>&1 || true

LOG "ensure tap/target venvs (own venvs — singer connectors pin conflicting deps)"
[ -x ~/pw-tap/bin/tap-postgres ] || (python3 -m venv ~/pw-tap && ~/pw-tap/bin/pip install -q pipelinewise-tap-postgres)
[ -x ~/pw-target/bin/target-postgres ] || (python3 -m venv ~/pw-target && ~/pw-target/bin/pip install -q pipelinewise-target-postgres)

mkdir -p ~/pw-bench && cd ~/pw-bench
cat > tap.json <<'EOF'
{"host":"127.0.0.1","port":5544,"user":"postgres","password":"bench",
 "dbname":"apitap_bench_src","default_replication_method":"LOG_BASED",
 "logical_poll_total_seconds":10,"break_at_end_lsn":true}
EOF
cat > target.json <<'EOF'
{"host":"127.0.0.1","port":5545,"user":"postgres","password":"bench",
 "dbname":"apitap_bench_dst","default_target_schema":"public",
 "batch_size_rows":100000,"hard_delete":true}
EOF

LOG "reset"
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'pipelinewise%' OR slot_name LIKE 'apitap_%'"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
$PDST "DROP TABLE IF EXISTS $T; DROP TABLE IF EXISTS ${T}_apitap; DELETE FROM _apitap_state WHERE dest_table='${T}_apitap'" >/dev/null 2>&1 || true
$PSRC "CREATE TABLE $T (id int PRIMARY KEY, v text, n bigint)" >/dev/null
$PSRC "INSERT INTO $T SELECT g, 'v'||g, g*7 FROM generate_series(1,100000) g" >/dev/null

LOG "discover + select the stream (LOG_BASED)"
~/pw-tap/bin/tap-postgres --config tap.json --discover > catalog_raw.json
python3 - <<'PY'
import json
c = json.load(open('catalog_raw.json'))
streams = []
for s in c['streams']:
    if s['table_name'] != 'cdc_pw':
        continue
    for m in s['metadata']:
        if m['breadcrumb'] == []:
            m['metadata']['selected'] = True
            m['metadata']['replication-method'] = 'LOG_BASED'
    streams.append(s)
assert streams, 'cdc_pw not discovered'
json.dump({'streams': streams}, open('catalog.json', 'w'))
PY

LOG "pipelinewise bootstrap (initial sync + slot)"
t0=$(date +%s)
~/pw-tap/bin/tap-postgres --config tap.json --properties catalog.json \
  | ~/pw-target/bin/target-postgres --config target.json > out1.jsonl
tail -1 out1.jsonl > state.json
echo "pipelinewise bootstrap: $(( $(date +%s) - t0 ))s"
$PDST "SELECT count(*), sum(id), sum(n) FROM $T"

LOG "apitap bootstrap (slot + pinned full load, dest ${T}_apitap)"
t0=$(date +%s)
~/ice-run/bin/python - <<'PY'
import apitap
r = apitap.transfer("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src",
                    "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst",
                    table="cdc_pw", dest_table="cdc_pw_apitap", mode="log_based")
print(f"rows={r.rows:,}")
PY
echo "apitap bootstrap: $(( $(date +%s) - t0 ))s"

LOG "generate the 650K-event window (both slots retain it)"
$PSRC "INSERT INTO $T SELECT g, 'bulk'||g, g FROM generate_series(200000, 699999) g" >/dev/null
$PSRC "UPDATE $T SET v='rw', n=n+1 WHERE id BETWEEN 200000 AND 299999" >/dev/null
$PSRC "DELETE FROM $T WHERE id BETWEEN 600000 AND 649999" >/dev/null
SRC_SUM=$($PSRC "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "source truth: $SRC_SUM"

LOG "pipelinewise CDC catch-up (timed)"
t0=$(date +%s)
~/pw-tap/bin/tap-postgres --config tap.json --properties catalog.json --state state.json \
  | ~/pw-target/bin/target-postgres --config target.json > out2.jsonl
PW_T=$(( $(date +%s) - t0 ))
PW_SUM=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM $T")
echo "pipelinewise catch-up: ${PW_T}s — dst $PW_SUM $([ "$PW_SUM" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "apitap CDC catch-up (timed)"
t0=$(date +%s)
APITAP_DEBUG=1 ~/ice-run/bin/python - <<'PY'
import apitap
r = apitap.transfer("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src",
                    "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst",
                    table="cdc_pw", dest_table="cdc_pw_apitap", mode="log_based")
print(f"events={r.rows:,}")
PY
AP_T=$(( $(date +%s) - t0 ))
AP_SUM=$($PDST "SELECT count(*)||'|'||sum(id)||'|'||sum(n) FROM cdc_pw_apitap")
echo "apitap catch-up: ${AP_T}s — dst $AP_SUM $([ "$AP_SUM" = "$SRC_SUM" ] && echo MATCH || echo MISMATCH)"

LOG "cleanup (standing rule: dest dropped, slots gone)"
$PDST "DROP TABLE IF EXISTS $T; DROP TABLE IF EXISTS ${T}_apitap; DELETE FROM _apitap_state WHERE dest_table='${T}_apitap'" >/dev/null
$PSRC "DROP TABLE IF EXISTS $T" >/dev/null
for s in $($PSRC "SELECT slot_name FROM pg_replication_slots"); do
  $PSRC "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true; done
echo done
