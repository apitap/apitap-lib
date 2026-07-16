#!/usr/bin/env bash
# Start the stock database containers and seed the benchmark tables.
# Run ON each DB VM (copy this repo's seed*.sql next to it first).
#
# On bench-source:   ROLE=source FAMILY=pg    ./setup-dbs.sh   # Round A source
#                    ROLE=source FAMILY=mysql ./setup-dbs.sh   # Round B source
# On bench-dest:     ROLE=dest   ./setup-dbs.sh                # PG + MySQL + CH
#
# No DB tuning: every container runs its image defaults. The one non-default is
# MySQL's local_infile=1 — the standard bulk-load switch every tool needs to
# LOAD DATA into MySQL; it is off by default in mysql:8.
set -euo pipefail
ROLE="${ROLE:?set ROLE=source|dest}"
FAMILY="${FAMILY:-pg}"        # source only: pg | mysql
PW=bench
d(){ sudo docker "$@"; }

seed_pg(){   # container port
  for _ in $(seq 60); do d exec "$1" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 2; done
  d exec -i "$1" psql -U postgres -c "CREATE DATABASE bench" 2>/dev/null || true
  for spec in bench_data_1m:1000000 bench_data_10m:10000000; do
    t=${spec%%:*}; n=${spec##*:}
    sed "s/BENCH_TABLE_PLACEHOLDER/$t/g; s/BENCH_ROWS_PLACEHOLDER/$n/g" seed.sql \
      | d exec -i "$1" psql -U postgres -d bench >/dev/null
    echo "seeded public.$t = $(d exec "$1" psql -U postgres -d bench -tAc "SELECT count(*) FROM $t")"
  done
}
seed_mysql(){  # container
  for _ in $(seq 90); do d exec "$1" mysqladmin ping -uroot -p$PW >/dev/null 2>&1 && break; sleep 2; done
  d exec "$1" mysql -uroot -p$PW -e "CREATE DATABASE IF NOT EXISTS bench" 2>/dev/null || true
  for spec in bench_my_1m:1000000 bench_my_10m:10000000; do
    t=${spec%%:*}; n=${spec##*:}
    sed "s/BENCH_TABLE_PLACEHOLDER/$t/g; s/BENCH_ROWS_PLACEHOLDER/$n/g" seed_mysql.sql \
      | d exec -i "$1" mysql -uroot -p$PW bench
    echo "seeded bench.$t = $(d exec "$1" mysql -uroot -p$PW -N -e "SELECT count(*) FROM bench.$t")"
  done
}

if [ "$ROLE" = source ]; then
  if [ "$FAMILY" = pg ]; then
    d rm -fv pgsrc >/dev/null 2>&1 || true
    d run -d --name pgsrc -p 5432:5432 -e POSTGRES_PASSWORD=$PW postgres:16 >/dev/null
    seed_pg pgsrc
  else
    d rm -fv mysrc >/dev/null 2>&1 || true
    d run -d --name mysrc -p 3306:3306 -e MYSQL_ROOT_PASSWORD=$PW mysql:8.0 \
      --default-authentication-plugin=mysql_native_password --local-infile=1 >/dev/null
    seed_mysql mysrc
  fi
else   # dest: PG + MySQL + ClickHouse, all stock (MySQL with local_infile=1)
  d rm -fv pgdst mydst chdst >/dev/null 2>&1 || true
  d run -d --name pgdst -p 5432:5432 -e POSTGRES_PASSWORD=$PW postgres:16 >/dev/null
  d run -d --name mydst -p 3306:3306 -e MYSQL_ROOT_PASSWORD=$PW mysql:8.0 \
    --default-authentication-plugin=mysql_native_password --local-infile=1 >/dev/null
  d run -d --name chdst -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_PASSWORD=$PW \
    clickhouse/clickhouse-server:24.8 >/dev/null
  for _ in $(seq 90); do d exec mydst mysqladmin ping -uroot -p$PW >/dev/null 2>&1 && break; sleep 2; done
  d exec mydst mysql -uroot -p$PW -e "CREATE DATABASE IF NOT EXISTS bench; SET GLOBAL local_infile=1;" 2>/dev/null || true
  echo "dest ready: pg 5432, mysql 3306 (local_infile=1), clickhouse 8123/9000"
fi
