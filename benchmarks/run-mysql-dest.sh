#!/bin/bash
# PG->MySQL showdown: apitap vs ingestr vs dlt(sqlalchemy). Same host, same
# loopback DBs, each tool in an identical uncapped python:3.12-slim container.
set -u
d(){ sudo docker "$@"; }
PSQL(){ d exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -tAc "$1"; }
MSQL(){ d exec apitap-bench-mysql-dst mysql -uroot -pbench --default-character-set=utf8mb4 -N -e "$1" 2>/dev/null; }
PS='postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src'
MYD_APITAP='mysql://root:bench@127.0.0.1:3308/bench'
MYD_SQLA='mysql+pymysql://root:bench@127.0.0.1:3308/bench'

echo "=== 0. seed ==="
d exec apitap-bench-pg-src psql -U postgres -qc "CREATE DATABASE apitap_bench_src" 2>/dev/null
d exec apitap-bench-mysql-dst mysql -uroot -pbench -e "CREATE DATABASE IF NOT EXISTS bench; SET GLOBAL group_concat_max_len=64000000" 2>/dev/null
cd ~/apitap-build/benchmarks
for N in 1m:1000000 10m:10000000; do
  T=bench_data_${N%%:*}; R=${N##*:}
  if [ "$(PSQL "SELECT count(*) FROM $T" 2>/dev/null)" != "$R" ]; then
    sed "s/BENCH_TABLE_PLACEHOLDER/$T/g; s/BENCH_ROWS_PLACEHOLDER/$R/g" seed.sql > /tmp/s.sql
    d cp /tmp/s.sql apitap-bench-pg-src:/tmp/s.sql
    d exec apitap-bench-pg-src bash -c "psql -U postgres -d apitap_bench_src -q -f /tmp/s.sql" >/dev/null 2>&1
  fi
  echo "  $T: $(PSQL "SELECT count(*) FROM $T")"
done

echo "=== 1. build tool images ==="
cd "$(dirname "$0")"
cp wheels/apitap-*.whl . 2>/dev/null || pip download apitap --no-deps -d . >/dev/null
printf 'FROM python:3.12-slim\nCOPY apitap-*.whl /tmp/\nRUN pip install -q --no-cache-dir /tmp/*.whl\n' | d build -q -t shoot-apitap -f- . >/dev/null && echo "  apitap img ok"
printf 'FROM python:3.12-slim\nRUN pip install -q --no-cache-dir ingestr\n' | d build -q -t shoot-ingestr -f- . >/dev/null && echo "  ingestr img ok: $(d run --rm shoot-ingestr ingestr --version 2>/dev/null | tail -1)"
printf 'FROM python:3.12-slim\nRUN pip install -q --no-cache-dir "dlt[sql_database]" pymysql psycopg2-binary pyarrow pandas\nCOPY dlt-mysql.py /dlt_my.py\n' | d build -q -t shoot-dlt -f- . >/dev/null && echo "  dlt img ok: $(d run --rm shoot-dlt python -c "import dlt; print(dlt.__version__)")"

run_apitap(){ # table dest
  d run --rm --network=host shoot-apitap python -c "
import apitap, time
t0=time.time()
r = apitap.transfer('$PS', '$MYD_APITAP', table='public.$1', dest_table='$2')
print(f'  apitap $1: {r.rows:,} rows {time.time()-t0:.1f}s')"
}
run_ingestr(){ # table dest
  d run --rm --network=host shoot-ingestr sh -c "
python - <<'PYEOF'
import subprocess, time
t0=time.time()
p = subprocess.run(['ingestr','ingest','--source-uri','$PS',
  '--source-table','public.$1','--dest-uri','$MYD_SQLA',
  '--dest-table','bench.$2','--yes'], capture_output=True, text=True)
dt=time.time()-t0
tail = (p.stdout+p.stderr).strip().splitlines()[-1] if (p.stdout+p.stderr).strip() else ''
print(f'  ingestr $1: rc={p.returncode} {dt:.1f}s  [{tail[:80]}]')
PYEOF"
}

echo "=== 2. apitap ==="
run_apitap bench_data_1m apitap_1m
run_apitap bench_data_10m apitap_10m

echo "=== 3. ingestr (run-2 = warm) ==="
run_ingestr bench_data_1m ing_1m
run_ingestr bench_data_1m ing_1m
run_ingestr bench_data_10m ing_10m

echo "=== 4. dlt -> sqlalchemy(mysql) 1M (timeout 45m each) ==="
timeout 2700 sudo docker run --rm --network=host shoot-dlt sh -c "
python - <<'PYEOF'
import subprocess, time
t0=time.time()
p = subprocess.run(['python','/dlt_my.py','$PS','$MYD_SQLA','dltset','bench_data_1m'], capture_output=True, text=True)
dt=time.time()-t0
tail=(p.stdout+p.stderr).strip().splitlines()[-1] if (p.stdout+p.stderr).strip() else ''
print(f'  dlt-default 1m: rc={p.returncode} {dt:.1f}s  [{tail[:110]}]')
PYEOF" || echo "  dlt-default 1m: TIMEOUT/FAIL rc=$?"
timeout 2700 sudo docker run --rm --network=host shoot-dlt sh -c "
python - <<'PYEOF'
import subprocess, time
t0=time.time()
p = subprocess.run(['python','/dlt_my.py','$PS','$MYD_SQLA','dltpa','bench_data_1m','pyarrow'], capture_output=True, text=True)
dt=time.time()-t0
tail=(p.stdout+p.stderr).strip().splitlines()[-1] if (p.stdout+p.stderr).strip() else ''
print(f'  dlt-pyarrow 1m: rc={p.returncode} {dt:.1f}s  [{tail[:110]}]')
PYEOF" || echo "  dlt-pyarrow 1m: TIMEOUT/FAIL rc=$?"

echo "=== 5. validation ==="
PG1=$(PSQL "SELECT count(*)||'|'||sum(id)||'|'||md5(string_agg(small_str,'' ORDER BY id)) FROM bench_data_1m")
PG10=$(PSQL "SELECT count(*)||'|'||sum(id) FROM bench_data_10m")
echo "  pg 1m : $PG1"
echo "  pg 10m: $PG10"
vt(){ # label db.table full?
  if [ "${3:-}" = full ]; then
    echo "  $1: $(MSQL "SELECT CONCAT(COUNT(*),'|',SUM(id),'|',MD5(GROUP_CONCAT(small_str ORDER BY id SEPARATOR ''))) FROM $2")"
  else
    echo "  $1: $(MSQL "SELECT CONCAT(COUNT(*),'|',SUM(id)) FROM $2")"
  fi
}
vt apitap-1m bench.apitap_1m full
vt apitap-10m bench.apitap_10m
vt ingestr-1m bench.ing_1m full
vt ingestr-10m bench.ing_10m
echo "  dlt tables: $(MSQL "SELECT GROUP_CONCAT(CONCAT(table_schema,'.',table_name)) FROM information_schema.tables WHERE table_name LIKE '%bench_data_1m%'")"
for t in $(MSQL "SELECT CONCAT(table_schema,'.',table_name) FROM information_schema.tables WHERE table_name LIKE '%bench_data_1m%'"); do
  vt "dlt:$t" "$t" full
done
echo DONE_SHOOTOUT
