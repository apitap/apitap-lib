#!/usr/bin/env bash
# Drop everything the wal-tcp-cost mission created: the destination table, its
# _apitap_state/_apitap_cdc_pending rows, and the apitap-created slot and
# publication for public.prof_pg_m -> waltcp_pg_m. Keeps the shared seed.
set -uo pipefail
PG_C=${PG_C:-apitap-bench-pg-src}
PG_DB=${PG_DB:-apitap_bench_src}
CH_C=${CH_C:-apitap-bench-ch}
DEST_TABLE=${DEST_TABLE:-waltcp_pg_m}
TABLE=${TABLE:-prof_pg_m}

echo "== clickhouse =="
docker exec "$CH_C" clickhouse-client --query \
  "DROP TABLE IF EXISTS default.$DEST_TABLE"
docker exec "$CH_C" clickhouse-client --query \
  "ALTER TABLE default._apitap_state DELETE WHERE dest_table='$DEST_TABLE'"
docker exec "$CH_C" clickhouse-client --query \
  "ALTER TABLE default._apitap_cdc_pending DELETE WHERE dest_table='$DEST_TABLE'"

echo "== postgres slot/publication (only the one publishing $TABLE) =="
PUBS=$(docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "select p.pubname from pg_publication p join pg_publication_tables pt on pt.pubname=p.pubname where pt.tablename='$TABLE'")
for pub in $PUBS; do
  slot="${pub%_pub}"
  echo "dropping publication $pub and slot $slot"
  docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c \
    "SELECT pg_drop_replication_slot('$slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$slot' AND NOT active);"
  docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c "DROP PUBLICATION IF EXISTS \"$pub\";"
done
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "select count(*) from pg_replication_slots where slot_name like 'apitap_%'; select count(*) from pg_publication where pubname like 'apitap_%'"
echo "CLEANUP_DONE"
