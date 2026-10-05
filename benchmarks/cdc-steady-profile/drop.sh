#!/usr/bin/env bash
# Drop this campaign's destination tables and their state rows, and the
# replication slots/publications a previous prof_* drain left on the pg source.
# ONLY objects whose name contains 'prof_' are touched.
set -uo pipefail

PG_C=${PG_C:-apitap-bench-pg-src}
PG_DB=${PG_DB:-apitap_bench_src}
CH_C=${CH_C:-apitap-bench-ch}
MY_C=${MY_C:-apitap-bench-my}
MY_DB=${MY_DB:-bench}

echo "== destinations =="
docker exec "$CH_C" clickhouse-client --password bench -q \
  "DROP TABLE IF EXISTS default.prof_pg_m SYNC;
   DROP TABLE IF EXISTS default.prof_my_m SYNC;
   DROP TABLE IF EXISTS default.\`prof_pg_m__apitap_cdc_pending\` SYNC;" 2>/dev/null
# state rows for our tables, on CH
docker exec "$CH_C" clickhouse-client --password bench -q \
  "ALTER TABLE default._apitap_state DELETE WHERE dest_table LIKE 'prof_%' SETTINGS mutations_sync=1" 2>/dev/null
# per-run key tables are tokenized; sweep loose ones by prefix
for t in $(docker exec "$CH_C" clickhouse-client --password bench -q \
    "SELECT name FROM system.tables WHERE database='default' AND (name LIKE 'prof_%__apitap%' OR name LIKE 'prof_%') FORMAT TSV" 2>/dev/null); do
  case "$t" in
    prof_pg_m|prof_my_m) ;; # dropped above
    _*) ;;
    *) docker exec "$CH_C" clickhouse-client --password bench -q "DROP TABLE IF EXISTS default.\`$t\` SYNC" 2>/dev/null ;;
  esac
done

echo "== pg slots/publications for prof_ =="
# Publications: only those whose tables ALL belong to this campaign (prof_*).
# A publication that also covers a foreign table is never touched.
OURPUBS=$(docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "SELECT pubname FROM pg_publication p WHERE EXISTS (SELECT 1 FROM pg_publication_tables t WHERE t.pubname=p.pubname AND t.tablename LIKE 'prof_%') AND NOT EXISTS (SELECT 1 FROM pg_publication_tables t WHERE t.pubname=p.pubname AND t.tablename NOT LIKE 'prof_%')")
for p in $OURPUBS; do
  # slot name = publication name without the trailing _pub
  s="${p%_pub}"
  act=$(docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc "SELECT active FROM pg_replication_slots WHERE slot_name='$s'")
  if [ "$act" = "f" ]; then
    docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c "SELECT pg_drop_replication_slot('$s')" >/dev/null && echo "dropped slot $s"
  elif [ -n "$act" ]; then
    echo "slot $s ACTIVE (left alone)"
  fi
  docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c "DROP PUBLICATION \"$p\"" >/dev/null && echo "dropped publication $p"
done
echo "DROP_DONE"
