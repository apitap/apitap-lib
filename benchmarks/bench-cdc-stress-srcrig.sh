#!/usr/bin/env bash
# Restart ONLY this campaign's PostgreSQL source with the stress rig's settings,
# keeping its volume — the thirty seeded tables (1,036,000 rows each) live in
# there and are never rebuilt. The destination is left alone.
#
# Every setting changed from the previous campaign's rig is printed, with the
# reason, so the report can disclose them rather than inherit them silently.
set -uo pipefail
PG_C=${PG_C:-apitap-bench-cdc-pg}
PG_DB=${PG_DB:-bench}
PORT=5548

docker rm -f "$PG_C" >/dev/null 2>&1
docker run -d --name "$PG_C" \
    -p 127.0.0.1:${PORT}:5432 \
    -e POSTGRES_PASSWORD=bench -e POSTGRES_DB="$PG_DB" \
    -v apitap-bench-cdc-pg-data:/var/lib/postgresql/data \
    postgres:16-alpine \
        -c wal_level=logical \
        -c max_replication_slots=10 \
        -c max_wal_senders=10 \
        -c max_slot_wal_keep_size=12GB \
        -c max_wal_size=2GB \
        -c min_wal_size=512MB \
        -c shared_buffers=1GB \
        -c max_connections=200 \
        -c logical_decoding_work_mem=256MB \
        -c checkpoint_timeout=5min \
        -c max_worker_processes=8 \
        -c synchronous_commit=off \
        -c full_page_writes=on \
    >/dev/null
until docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "SELECT 1" >/dev/null 2>&1; do sleep 1; done
# the `host replication` trust line the official image omits; the previous
# campaign's walshadow leg needed it and it costs nothing to keep
docker exec -i "$PG_C" bash -c "printf '%s\n' \
    'local   all             all                                     trust' \
    'host    all             all             all                     trust' \
    'local   replication     all                                     trust' \
    'host    replication     all             all                     trust' \
    > /var/lib/postgresql/data/pg_hba.conf"
docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "SELECT pg_reload_conf()" >/dev/null
# wal_compression is deliberately NOT on the command line: a server command-line
# setting overrides postgresql.auto.conf FOREVER, so leaving `wal_compression=on`
# there would make `ALTER SYSTEM SET wal_compression = 'lz4'` a silent no-op and
# every compression comparison would silently measure pglz. It is set through
# auto.conf below, which a sighup re-read honours.
docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc \
  "ALTER SYSTEM SET wal_compression = 'pglz'" >/dev/null
sleep 1.5

echo "== the stress rig's source settings, as the server reports them =="
docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc \
  "SELECT '  ' || name || ' = ' || setting || coalesce(unit,'') FROM pg_settings
   WHERE name IN ('server_version','wal_level','max_slot_wal_keep_size','max_wal_size',
     'min_wal_size','shared_buffers','max_connections','logical_decoding_work_mem',
     'checkpoint_timeout','synchronous_commit','wal_compression','full_page_writes')
   ORDER BY name"
echo "== the seeds survived the restart =="
docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc \
  "SELECT '  ' || count(*) || ' tables, ' || pg_size_pretty(sum(pg_total_relation_size(quote_ident(table_name))))
   FROM information_schema.tables WHERE table_schema='public'"
docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc \
  "SELECT '  rows: ' || sum(n) || ' over ' || count(*) || ' tables, min=' || min(n) || ' max=' || max(n)
   FROM (SELECT (xpath('/row/c/text()', query_to_xml(
       format('SELECT count(*) c FROM public.%I', table_name), false,true,'')))[1]::text::bigint AS n
     FROM information_schema.tables WHERE table_schema='public') x"
echo "== disk =="
df -h / | tail -1 | sed 's/^/  /'
sudo du -sh /var/lib/docker/volumes/apitap-bench-cdc-pg-data/_data/pg_wal 2>/dev/null | sed 's/^/  pg_wal /'