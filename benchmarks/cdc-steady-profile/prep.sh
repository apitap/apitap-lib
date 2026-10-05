#!/usr/bin/env bash
# Create this campaign's OWN seed tables on the two named bench sources.
#   pg: public.prof_pg_m  — a structural+data clone of public.bench_data_1m (15 cols, 1M rows)
#   my: bench.prof_my_m   — a structural clone of bench.bench_my_1m, json_val as LONGTEXT
#                          (apitap's MySQL CDC lane refuses a native json column)
# The seeds are recreated from scratch on every call; nothing pre-existing is touched.
set -euo pipefail

PG_C=${PG_C:-apitap-bench-pg-src}
PG_DB=${PG_DB:-apitap_bench_src}
MY_C=${MY_C:-apitap-bench-my}
MY_DB=${MY_DB:-bench}

echo "== pg ${PG_C}.${PG_DB}: public.prof_pg_m =="
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -v ON_ERROR_STOP=1 -c \
  "DROP TABLE IF EXISTS public.prof_pg_m;"
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -v ON_ERROR_STOP=1 -c \
  "CREATE TABLE public.prof_pg_m (LIKE public.bench_data_1m INCLUDING ALL);"
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO public.prof_pg_m SELECT * FROM public.bench_data_1m;"
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "SELECT 'PG_ROWS '||count(*)||' '||pg_size_pretty(pg_total_relation_size('public.prof_pg_m')) FROM public.prof_pg_m;"

echo "== mysql ${MY_C}.${MY_DB}: bench.prof_my_m =="
docker exec "$MY_C" mysql -uroot -pbench -e \
  "DROP TABLE IF EXISTS ${MY_DB}.prof_my_m;
   CREATE TABLE ${MY_DB}.prof_my_m (LIKE ${MY_DB}.bench_my_1m);
   ALTER TABLE ${MY_DB}.prof_my_m MODIFY json_val LONGTEXT;
   SET SESSION sql_log_bin=0;
   INSERT INTO ${MY_DB}.prof_my_m (id, small_str, medium_str, large_str, tiny_int,
       regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val,
       ts_tz_val, json_val, extra_text)
     SELECT id, small_str, medium_str, large_str, tiny_int, regular_int, big_int,
       float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val,
       CAST(json_val AS CHAR), extra_text FROM ${MY_DB}.bench_my_1m;" 2>/dev/null
docker exec "$MY_C" mysql -uroot -pbench -N -B -e \
  "SELECT CONCAT('MY_ROWS ',COUNT(*)) FROM ${MY_DB}.prof_my_m;" 2>/dev/null
echo "PREP_DONE"
