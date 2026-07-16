#!/usr/bin/env bash
# Round A harness — Postgres source. Runs ON bench-ingest.
#   SRC_IP=10.x DST_IP=10.y ./bench_a.sh bench_data_1m bench_data_10m
#
# Routes PG->PG and PG->ClickHouse, each with apitap / ingestr / dlt. dlt is run
# with both backends (default SQLAlchemy + pyarrow). Every run is wrapped in
# `/usr/bin/time -v` and hard-capped at 5 minutes; a run only counts when the
# destination checksum matches the source.
set -uo pipefail
SRC_IP="${SRC_IP:?}"; DST_IP="${DST_IP:?}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SRCPG="postgresql://postgres:bench@${SRC_IP}:5432/bench"
DSTPG="postgresql://postgres:bench@${DST_IP}:5432/bench"
CAP=300
export PGPASSWORD=bench
psrc(){ psql -h $SRC_IP -U postgres -d bench -tAc "$1" 2>/dev/null; }
pdst(){ psql -h $DST_IP -U postgres -d bench -tAc "$1" 2>/dev/null; }
chd(){ curl -s "http://default:bench@${DST_IP}:8123/" --data-binary "$1"; }
CKPG="count(*)||'|'||sum(id::numeric)||'|'||sum(big_int::numeric)||'|'||sum(length(medium_str))"
CKCH="toString(count())||'|'||toString(sum(toInt128(id)))||'|'||toString(sum(toInt128(big_int)))||'|'||toString(sum(length(medium_str)))"

timeit(){ printf '%s\n' "$1" >/tmp/cmd.sh
  /usr/bin/time -v timeout -s KILL $CAP bash /tmp/cmd.sh >/tmp/r.out 2>/tmp/t.txt; local rc=$?
  local w=$(grep Elapsed /tmp/t.txt|grep -oE '[0-9:.]+$'); local c=$(grep 'Percent of CPU' /tmp/t.txt|grep -oE '[0-9]+%')
  local m=$(( $(grep 'Maximum resident' /tmp/t.txt|grep -oE '[0-9]+$')/1024 )); echo "${w:-?}|${c:-?}|${m}|$rc"; }
report(){ printf "RESULT|%-16s|%s|wall=%s|cpu=%s|mem=%sMB|rc=%s|%s\n" "$1" "$2" "$3" "$4" "$5" "$6" "$7"; }

run_pg(){ local tool=$1 t=$2 lbl cmd
  case $tool in
    apitap)  lbl="apitap PG->PG";       pdst "DROP TABLE IF EXISTS public.$t CASCADE" >/dev/null 2>&1
             cmd="~/va_apitap/bin/python -c \"import apitap; apitap.transfer('$SRCPG','$DSTPG',table='public.$t',dest_table='public.$t',mode='replace',cursor='id')\"";;
    ingestr) lbl="ingestr PG->PG";      pdst "DROP TABLE IF EXISTS public.$t CASCADE" >/dev/null 2>&1
             cmd="~/va_ingestr/bin/ingestr ingest --source-uri '$SRCPG' --source-table 'public.$t' --dest-uri '$DSTPG' --dest-table 'public.$t' --yes";;
    dlt)     lbl="dlt PG->PG";          pdst "DROP SCHEMA IF EXISTS dltbench CASCADE" >/dev/null 2>&1
             cmd="~/va_dlt/bin/python $HERE/dlt_run.py '$SRCPG' pg '$DSTPG' dltbench $t '' public";;
    dltarr)  lbl="dlt-arrow PG->PG";    pdst "DROP SCHEMA IF EXISTS dltbench CASCADE" >/dev/null 2>&1
             cmd="~/va_dlt/bin/python $HERE/dlt_run.py '$SRCPG' pg '$DSTPG' dltbench $t pyarrow public";;
  esac
  IFS='|' read w c m rc <<<"$(timeit "$cmd")"
  local src=$(psrc "SELECT $CKPG FROM public.$t") dst
  case $tool in dlt|dltarr) dst=$(pdst "SELECT $CKPG FROM dltbench.$t");; *) dst=$(pdst "SELECT $CKPG FROM public.$t");; esac
  local ck=MISMATCH; [ -n "$src" ] && [ "$src" = "$dst" ] && ck=MATCH; [ "$rc" = 137 ] && ck="CAPPED>5min"
  report "$lbl" "$t" "$w" "$c" "$m" "$rc" "$ck"
}

run_ch(){ local tool=$1 t=$2 lbl cmd cktbl
  case $tool in
    apitap)  lbl="apitap PG->CH";    chd "DROP TABLE IF EXISTS default.$t" >/dev/null; cktbl="default.$t"
             cmd="~/va_apitap/bin/python -c \"import apitap; apitap.transfer('$SRCPG','clickhouse://default:bench@${DST_IP}:8123/default',table='public.$t',dest_table='$t',mode='replace',cursor='id')\"";;
    ingestr) lbl="ingestr PG->CH";   chd "DROP TABLE IF EXISTS default.$t" >/dev/null; cktbl="default.$t"
             cmd="~/va_ingestr/bin/ingestr ingest --source-uri '$SRCPG' --source-table 'public.$t' --dest-uri 'clickhouse://default:bench@${DST_IP}:9000/default?http_port=8123&secure=0' --dest-table 'default.$t' --yes";;
    dltarr)  lbl="dlt-arrow PG->CH"; for x in $(chd "SELECT name FROM system.tables WHERE database='default' AND startsWith(name,'dltbench___')"); do chd "DROP TABLE IF EXISTS default.$x" >/dev/null; done; cktbl="default.dltbench___$t"
             cmd="~/va_dlt/bin/python $HERE/dlt_run.py '$SRCPG' ch 'clickhouse://default:bench@${DST_IP}:9000/default' dltbench $t pyarrow public";;
  esac
  IFS='|' read w c m rc <<<"$(timeit "$cmd")"
  local src=$(psrc "SELECT $CKPG FROM public.$t") dst=$(chd "SELECT $CKCH FROM $cktbl")
  local ck=MISMATCH; [ -n "$src" ] && [ "$src" = "$dst" ] && ck=MATCH; [ "$rc" = 137 ] && ck="CAPPED>5min"
  report "$lbl" "$t" "$w" "$c" "$m" "$rc" "$ck"
}

for t in "$@"; do
  echo "########## PG -> PG ($t) ##########"
  for tool in apitap ingestr dltarr dlt; do run_pg $tool $t; done
  echo "########## PG -> ClickHouse ($t) ##########"
  for tool in apitap ingestr dltarr; do run_ch $tool $t; done
done
echo "===== DONE ROUND A ($*) ====="
