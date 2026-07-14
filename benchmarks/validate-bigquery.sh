#!/usr/bin/env bash
# 9-aggregate checksum: Postgres truth vs a BigQuery table. A benchmark time
# only counts when every aggregate matches. Usage:
#   BQ_PROJECT=… BQ_DATASET=… BQ_SA=/path/key.json \
#     ./validate-bigquery.sh public.bench_data_10m bench_apitap
#
# Notes from the recorded runs:
# - bool_val: apitap lands INT64 0/1 (the text lane casts), ingestr/dlt land
#   BOOL — CAST(… AS INT64) makes the count comparable across all three.
# - BigQuery doesn't print trailing zeros on NUMERIC (5220496050 vs
#   5220496050.0000) — compare values, not strings, on the decimal column.
# - _dlt_id/_dlt_load_id columns (ingestr/dlt) are ignored: only the 15
#   source columns are aggregated.
set -uo pipefail
PGT="$1"; BQT="$2"
BQ_PROJECT="${BQ_PROJECT:?}"; BQ_DATASET="${BQ_DATASET:?}"; BQ_SA="${BQ_SA:?}"

echo "-- PG $PGT"
docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -tAc \
  "SELECT count(*)||'|'||sum(id)||'|'||sum(big_int::numeric)||'|'||round(sum(decimal_val),4)
     ||'|'||sum(length(medium_str))||'|'||sum(tiny_int::bigint)
     ||'|'||count(*) FILTER (WHERE bool_val)
     ||'|'||max(ts_tz_val AT TIME ZONE 'UTC')||'|'||max(date_val) FROM $PGT"

echo "-- BQ $BQT"
docker run --rm -v "$BQ_SA":/sa/key.json:ro python:3.12-slim bash -c "
pip install -q google-auth requests 2>/dev/null
python - <<'PYEOF'
import google.auth.transport.requests, google.oauth2.service_account, requests
creds = google.oauth2.service_account.Credentials.from_service_account_file(
    '/sa/key.json', scopes=['https://www.googleapis.com/auth/bigquery'])
creds.refresh(google.auth.transport.requests.Request())
sql = '''SELECT CAST(COUNT(*) AS STRING), CAST(SUM(id) AS STRING),
    CAST(SUM(CAST(big_int AS BIGNUMERIC)) AS STRING),
    CAST(ROUND(SUM(decimal_val), 4) AS STRING),
    CAST(SUM(LENGTH(medium_str)) AS STRING), CAST(SUM(tiny_int) AS STRING),
    CAST(COUNTIF(CAST(bool_val AS INT64) = 1) AS STRING),
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', MAX(ts_tz_val)),
    CAST(MAX(date_val) AS STRING)
    FROM \`$BQ_PROJECT.$BQ_DATASET.$BQT\`'''
r = requests.post('https://bigquery.googleapis.com/bigquery/v2/projects/$BQ_PROJECT/queries',
    headers={'Authorization': f'Bearer {creds.token}'},
    json={'query': sql, 'useLegacySql': False, 'timeoutMs': 60000})
d = r.json()
if 'rows' not in d:
    print('ERROR:', str(d)[:400]); raise SystemExit(1)
print('|'.join((c['v'] or '') for c in d['rows'][0]['f']))
PYEOF"
