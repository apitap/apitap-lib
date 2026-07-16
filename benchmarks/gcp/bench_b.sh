#!/usr/bin/env bash
# Round B harness — MySQL source. Runs ON bench-ingest.
#   SRC_IP=10.x DST_IP=10.y ./bench_b.sh bench_my_1m bench_my_10m
#
# Routes MySQL->MySQL (apitap, ingestr) and MySQL->ClickHouse (apitap, ingestr,
# dlt-pyarrow). dlt has no MySQL destination, so MySQL->MySQL has no dlt row.
# Same `/usr/bin/time -v`, 5-minute hard cap, and checksum gate as Round A.
set -uo pipefail
SRC_IP="${SRC_IP:?}"; DST_IP="${DST_IP:?}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="mysql://root:bench@${SRC_IP}:3306/bench"
DMY="mysql://root:bench@${DST_IP}:3306/bench"
DCH_A="clickhouse://default:bench@${DST_IP}:8123/default"
CAP=300
mys(){ mysql -h $SRC_IP -uroot -pbench -N -e "$1" 2>/dev/null; }
myd(){ mysql -h $DST_IP -uroot -pbench -N -e "$1" 2>/dev/null; }
chd(){ curl -s "http://default:bench@${DST_IP}:8123/" --data-binary "$1"; }
CKMY="CONCAT(count(*),'|',sum(id),'|',sum(big_int),'|',sum(length(medium_str)))"
CKCH="toString(count())||'|'||toString(sum(toInt128(id)))||'|'||toString(sum(toInt128(big_int)))||'|'||toString(sum(length(medium_str)))"

timeit(){ printf '%s\n' "$1" >/tmp/cmd.sh
  /usr/bin/time -v timeout -s KILL $CAP bash /tmp/cmd.sh >/tmp/r.out 2>/tmp/t.txt; local rc=$?
  local w=$(grep Elapsed /tmp/t.txt|grep -oE '[0-9:.]+$'); local c=$(grep 'Percent of CPU' /tmp/t.txt|grep -oE '[0-9]+%')
  local m=$(( $(grep 'Maximum resident' /tmp/t.txt|grep -oE '[0-9]+$')/1024 )); echo "${w:-?}|${c:-?}|${m}|$rc"; }
report(){ printf "RESULT|%-16s|%s|wall=%s|cpu=%s|mem=%sMB|rc=%s|%s\n" "$1" "$2" "$3" "$4" "$5" "$6" "$7"; }

run_my(){ local tool=$1 t=$2 lbl cmd
  myd "DROP TABLE IF EXISTS bench.$t" >/dev/null 2>&1
  if [ "$tool" = apitap ]; then lbl="apitap MY->MY"
    cmd="~/va_apitap/bin/python -c \"import apitap; apitap.transfer('$SRC','$DMY',table='$t',dest_table='$t',mode='replace',cursor='id')\""
  else lbl="ingestr MY->MY"
    cmd="~/va_ingestr/bin/ingestr ingest --source-uri '$SRC' --source-table 'bench.$t' --dest-uri '$DMY' --dest-table 'bench.$t' --yes"
  fi
  IFS='|' read w c m rc <<<"$(timeit "$cmd")"
  local src=$(mys "SELECT $CKMY FROM bench.$t") dst=$(myd "SELECT $CKMY FROM bench.$t")
  local ck=MISMATCH; [ -n "$src" ] && [ "$src" = "$dst" ] && ck=MATCH; [ "$rc" = 137 ] && ck="CAPPED>5min"
  report "$lbl" "$t" "$w" "$c" "$m" "$rc" "$ck"
}

run_ch(){ local tool=$1 t=$2 lbl cmd cktbl
  if [ "$tool" = apitap ]; then lbl="apitap MY->CH"; chd "DROP TABLE IF EXISTS default.$t" >/dev/null; cktbl="default.$t"
    cmd="~/va_apitap/bin/python -c \"import apitap; apitap.transfer('$SRC','$DCH_A',table='$t',dest_table='$t',mode='replace',cursor='id')\""
  elif [ "$tool" = ingestr ]; then lbl="ingestr MY->CH"; chd "DROP TABLE IF EXISTS default.$t" >/dev/null; cktbl="default.$t"
    cmd="~/va_ingestr/bin/ingestr ingest --source-uri '$SRC' --source-table 'bench.$t' --dest-uri 'clickhouse://default:bench@${DST_IP}:9000/default?http_port=8123&secure=0' --dest-table 'default.$t' --yes"
  else lbl="dlt-arrow MY->CH"; cktbl="default.dltbench___$t"
    for x in $(chd "SELECT name FROM system.tables WHERE database='default' AND startsWith(name,'dltbench___')"); do chd "DROP TABLE IF EXISTS default.$x" >/dev/null; done
    cmd="~/va_dlt/bin/python $HERE/dlt_run.py '$SRC' ch 'clickhouse://default:bench@${DST_IP}:9000/default' dltbench $t pyarrow bench"
  fi
  IFS='|' read w c m rc <<<"$(timeit "$cmd")"
  local src=$(mys "SELECT $CKMY FROM bench.$t") dst=$(chd "SELECT $CKCH FROM $cktbl")
  local ck=MISMATCH; [ -n "$src" ] && [ "$src" = "$dst" ] && ck=MATCH; [ "$rc" = 137 ] && ck="CAPPED>5min"
  report "$lbl" "$t" "$w" "$c" "$m" "$rc" "$ck"
}

for t in "$@"; do
  echo "########## MySQL -> MySQL ($t) ##########"
  run_my apitap $t; run_my ingestr $t
  echo "NOTE: dlt has no MySQL destination — no dlt row for MySQL->MySQL"
  echo "########## MySQL -> ClickHouse ($t) ##########"
  run_ch apitap $t; run_ch ingestr $t; run_ch dlt $t
done
echo "===== DONE ROUND B ($*) ====="
