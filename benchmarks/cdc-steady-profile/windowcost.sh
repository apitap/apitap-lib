#!/usr/bin/env bash
# Per-window statement census from ClickHouse's own query_log: every statement
# the drain issued to the destination inside [start, end), classified, with the
# SERVER-side duration and rows. This is what prices the window setup/close
# steps (key table, DELETE, state write, lease probe) against the per-window
# destination INSERT, measured on the engine that ran them.
#
#   windowcost.sh <start 'YYYY-MM-DD HH:MM:SS'> <end 'YYYY-MM-DD HH:MM:SS'> [out.tsv]
set -uo pipefail
CH_C=${CH_C:-apitap-bench-ch}
START="${1:?start}"
END="${2:?end}"
OUT="${3:-/home/ubuntu/bench-cdc-steady/logs/windowcost.tsv}"

docker exec "$CH_C" clickhouse-client --password bench -q "SYSTEM FLUSH LOGS"
docker exec -i "$CH_C" clickhouse-client --password bench > "$OUT" <<SQL
SELECT class, count() AS n,
       round(sum(query_duration_ms) / 1000.0, 2) AS srv_s,
       round(avg(query_duration_ms), 2) AS avg_ms,
       round(quantile(0.5)(query_duration_ms), 2) AS p50_ms,
       round(max(query_duration_ms), 1) AS max_ms,
       sum(read_rows) AS read_rows, sum(written_rows) AS written_rows
FROM (
    SELECT multiIf(
        position(q, '_apitap_state') > 0 AND startsWith(q, 'insert'), 'state_write',
        startsWith(q, 'insert into') AND position(q, '__apitap_cdc_del') > 0, 'key_insert',
        startsWith(q, 'insert into') AND position(q, 'input(') > 0, 'dest_insert',
        position(q, '_apitap_lease') > 0 AND startsWith(q, 'insert'), 'lease_write',
        startsWith(q, 'delete from'), 'dest_delete',
        position(q, '_apitap_lease') > 0, 'lease_probe',
        position(q, '_apitap_state') > 0, 'state_read',
        position(q, '_apitap_cdc_pending') > 0, 'pending',
        position(q, '__apitap_cdc_del') > 0 AND startsWith(q, 'truncate'), 'key_truncate',
        position(q, '__apitap') > 0, 'staging_other',
        startsWith(q, 'create') OR startsWith(q, 'drop') OR startsWith(q, 'alter'), 'ddl_setup',
        'other') AS class,
        query_duration_ms, read_rows, written_rows
    FROM (
        SELECT lower(query) AS q, query_duration_ms, read_rows, written_rows, query
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND event_time >= toDateTime('$START') AND event_time < toDateTime('$END')
          AND (query LIKE '%prof_%' OR query LIKE '%_apitap%')
    )
)
GROUP BY class ORDER BY srv_s DESC
FORMAT TabSeparatedWithNames
SQL
echo "wrote $OUT ($(wc -l < "$OUT") lines)"
cat "$OUT"
