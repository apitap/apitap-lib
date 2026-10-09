#!/usr/bin/env bash
# The heavy 30-table shape: 30 tables x 1,000,000 rows, 15 columns, large_str
# carrying ~1KB per row (900 + id%200 chars). A clone of the campaign seed
# (bench_data_1m / bench_my_1m) with ONLY large_str widened heavy; the campaign
# seed itself is never touched. Seeding never enters the binlog (MySQL:
# sql_log_bin=0; PG: no slot exists until the drain bootstraps).
#   heavy30-seed.sh <pg|my> [n_tables]
set -uo pipefail
ROUTE="${1:?pg|my}"
N="${2:-30}"
PG_C=${PG_C:-apitap-bench-pg-src}; PG_DB=${PG_DB:-apitap_bench_src}
MY_C=${MY_C:-apitap-bench-my}; MY_DB=${MY_DB:-bench}
T0=$(date +%s)
el() { echo "$(( $(date +%s) - T0 ))s"; }

if [ "$ROUTE" = pg ]; then
  psql() { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -v ON_ERROR_STOP=1 "$@"; }
  echo "== template public.prof2_pg_tpl (15 cols, large_str ~1KB) =="
  psql -q -c "DROP TABLE IF EXISTS public.prof2_pg_tpl;"
  psql -q -c "CREATE TABLE public.prof2_pg_tpl (LIKE public.bench_data_1m INCLUDING ALL); ALTER TABLE public.prof2_pg_tpl ALTER COLUMN large_str TYPE text;"
  psql -q -c "INSERT INTO public.prof2_pg_tpl SELECT id, small_str, medium_str, left(repeat(md5(id::text)||md5((id+1)::text),20), 900+(id%200)), tiny_int, regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text FROM public.bench_data_1m;"
  psql -tAc "SELECT 'TPL rows='||count(*)||' avg_large='||round(avg(length(large_str)))||' size='||pg_size_pretty(pg_total_relation_size('public.prof2_pg_tpl')) FROM public.prof2_pg_tpl;"
  for i in $(seq -w 1 "$N"); do
    psql -q -c "DROP TABLE IF EXISTS public.prof_pg_t$i; CREATE TABLE public.prof_pg_t$i (LIKE public.prof2_pg_tpl INCLUDING ALL); INSERT INTO public.prof_pg_t$i SELECT * FROM public.prof2_pg_tpl;"
    echo "PG_T$i seeded $(el)"
  done
  psql -tAc "SELECT 'TOTAL rows='||sum(n_live_tup)||' bytes='||pg_size_pretty(sum(pg_total_relation_size(relid))) FROM pg_stat_user_tables WHERE relname LIKE 'prof_pg_t%';"
  psql -q -c CHECKPOINT
else
  my() { docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" "$@"; }
  echo "== template bench.prof2_my_tpl (15 cols, large_str ~1KB, LONGTEXT json) =="
  docker exec -i "$MY_C" mysql -uroot -pbench "$MY_DB" -e "SET SESSION sql_log_bin=0; DROP TABLE IF EXISTS prof2_my_tpl; CREATE TABLE prof2_my_tpl (LIKE bench_my_1m); ALTER TABLE prof2_my_tpl MODIFY json_val LONGTEXT, MODIFY large_str MEDIUMTEXT; INSERT INTO prof2_my_tpl SELECT id, small_str, medium_str, LEFT(REPEAT(CONCAT(MD5(id), MD5(id+1)),20), 900 + (id % 200)), tiny_int, regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, CAST(json_val AS CHAR), extra_text FROM bench_my_1m;" 2>/dev/null
  my -e "SELECT CONCAT('TPL rows=', COUNT(*), ' avg_large=', ROUND(AVG(CHAR_LENGTH(large_str)))) FROM prof2_my_tpl;" 2>/dev/null
  for i in $(seq -w 1 "$N"); do
    docker exec -i "$MY_C" mysql -uroot -pbench "$MY_DB" -e "SET SESSION sql_log_bin=0; DROP TABLE IF EXISTS prof_my_t$i; CREATE TABLE prof_my_t$i (LIKE prof2_my_tpl); INSERT INTO prof_my_t$i SELECT * FROM prof2_my_tpl;" 2>/dev/null
    echo "MY_T$i seeded $(el)"
  done
  my -e "SELECT CONCAT('TOTAL tables=', COUNT(*)) FROM information_schema.tables WHERE table_schema='$MY_DB' AND table_name LIKE 'prof_my_t%';" 2>/dev/null
fi
echo "SEED_DONE route=$ROUTE $(el)"
