#!/usr/bin/env bash
# Dump ClickHouse system.query_log for this campaign's destination work since
# a marker, classified into the per-window statement classes. Usage:
#   querylog.sh <minutes_back> <out.tsv>
set -uo pipefail
CH_C=${CH_C:-apitap-bench-ch}
MIN=${1:-60}
OUT=${2:-/home/ubuntu/bench-cdc-steady/logs/querylog.tsv}
docker exec "$CH_C" clickhouse-client --password bench -q "SYSTEM FLUSH LOGS"
docker exec "$CH_C" clickhouse-client --password bench -q "
SELECT event_time_microseconds, query_duration_ms, read_rows, written_rows, memory_usage,
       replaceRegexpAll(replaceRegexpAll(query, '\\\\s+', ' '), '([0-9a-f]{8,}|[0-9]{4,})', 'N') AS q
FROM system.query_log
WHERE event_time > now() - INTERVAL $MIN MINUTE AND type = 'QueryFinish'
  AND (query LIKE '%prof_pg_m%' OR query LIKE '%prof_my_m%' OR query LIKE '%_apitap_state%'
       OR query LIKE '%_apitap_lease%' OR query LIKE '%_apitap_cdc_pending%')
ORDER BY event_time_microseconds FORMAT TSV" > "$OUT"
echo "wrote $OUT ($(wc -l < "$OUT") rows)"
