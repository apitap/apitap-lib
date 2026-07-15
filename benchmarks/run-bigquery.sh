#!/usr/bin/env bash
# PG → BigQuery head-to-head: apitap vs ingestr vs dlt, each in its own
# resource-capped container, checksum-validated (see validate-bigquery.sh),
# every destination table dropped afterwards so a billed project isn't left
# holding storage. This is the script behind the numbers in README.md
# ("Postgres → BigQuery" and the tiny-box section).
#
#   BQ_PROJECT=my-project BQ_DATASET=bench BQ_SA=/path/sa.json ./run-bigquery.sh
#
# Knobs (env): ROWS=10000000  CAP_CPUS=2  CAP_MEM=2g  TOOLS="apitap ingestr dlt"
#   dlt's backend: DLT_BACKEND= (empty = default row-by-row) or pyarrow.
# Needs: docker, the bench Postgres from run-server.sh (PG_DOCKER=1) or your
# own via PG_URL, and an apitap wheel image (built here if missing).
#
# The BigQuery service account needs BigQuery Data Editor + Job User. The
# whole apitap path is load/copy jobs + SELECT only — it runs on sandbox
# (no-billing) projects too; validation queries are billed where billing is on.
set -uo pipefail
cd "$(dirname "$0")"
REPO="$(cd .. && pwd)"

BQ_PROJECT="${BQ_PROJECT:?set BQ_PROJECT}"
BQ_DATASET="${BQ_DATASET:?set BQ_DATASET}"
BQ_SA="${BQ_SA:?set BQ_SA=/abs/path/service-account.json}"
ROWS="${ROWS:-10000000}"
CAP_CPUS="${CAP_CPUS:-2}"
CAP_MEM="${CAP_MEM:-2g}"
TOOLS="${TOOLS:-apitap ingestr dlt}"
DLT_BACKEND="${DLT_BACKEND:-}"
INGESTR_VERSION="${INGESTR_VERSION:-1.0.75}"
PG_URL="${PG_URL:-postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src}"

if (( ROWS >= 1000000 )); then SUFFIX="$((ROWS / 1000000))m"; else SUFFIX="$((ROWS / 1000))k"; fi
TABLE="bench_data_${SUFFIX}"
BQ_URL="bigquery://${BQ_PROJECT}/${BQ_DATASET}?credentials=/sa/key.json"

pgs() { docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -tAc "$1"; }

bq_drop() { # table…
    docker run --rm -v "$BQ_SA":/sa/key.json:ro python:3.12-slim bash -c "
pip install -q google-auth requests 2>/dev/null
python - <<PYEOF
import google.auth.transport.requests, google.oauth2.service_account, requests
creds = google.oauth2.service_account.Credentials.from_service_account_file(
    '/sa/key.json', scopes=['https://www.googleapis.com/auth/bigquery'])
creds.refresh(google.auth.transport.requests.Request())
for t in '$*'.split():
    r = requests.delete('https://bigquery.googleapis.com/bigquery/v2/projects/$BQ_PROJECT/datasets/$BQ_DATASET/tables/'+t,
        headers={'Authorization': f'Bearer {creds.token}'})
    print(f'  drop {t}: {r.status_code}')
PYEOF"
}

validate() { # dest_table
    BQ_PROJECT="$BQ_PROJECT" BQ_DATASET="$BQ_DATASET" BQ_SA="$BQ_SA" \
        ./validate-bigquery.sh "public.${TABLE}" "$1"
}

echo "==> images"
if ! docker image inspect apitap-bench-apitap:local >/dev/null 2>&1; then
    ls "$REPO"/benchmarks/wheels/*.whl >/dev/null 2>&1 || {
        echo "no wheel in benchmarks/wheels — build one first (see pgo-build.sh)"; exit 1; }
    printf 'FROM python:3.12-slim\nCOPY *.whl /tmp/\nRUN pip install --no-cache-dir /tmp/*.whl\n' \
        | docker build -q -t apitap-bench-apitap:local -f- "$REPO/benchmarks/wheels" >/dev/null
fi
printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir ingestr==%s\n' "$INGESTR_VERSION" \
    | docker build -q -t apitap-bench-ingestr - >/dev/null
printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir "dlt[bigquery]==1.*" sqlalchemy psycopg2-binary pyarrow pandas\n' \
    | docker build -q -t apitap-bench-dlt - >/dev/null
echo "==> source rows: $(pgs "SELECT count(*) FROM public.${TABLE}")"

for tool in $TOOLS; do
case "$tool" in
apitap)
    echo "==> apitap (${CAP_CPUS} cpu / ${CAP_MEM})"
    docker run --rm --network=host --cpus="$CAP_CPUS" --memory="$CAP_MEM" \
        -v "$BQ_SA":/sa/key.json:ro apitap-bench-apitap:local python -c "
import apitap, time
t0 = time.time()
r = apitap.transfer('$PG_URL', '$BQ_URL',
                    table='public.${TABLE}', dest_table='bench_apitap',
                    mode='replace', cursor='id')
print(f'  apitap: {r.rows} rows in {time.time()-t0:.1f}s over {r.parallel} pipes')" \
        || echo "  apitap FAILED rc=$? (137 = OOM-killed)"
    validate bench_apitap
    bq_drop bench_apitap bench_apitap__apitap_staging
    ;;
ingestr)
    echo "==> ingestr ${INGESTR_VERSION} (${CAP_CPUS} cpu / ${CAP_MEM})"
    docker run --rm --network=host --cpus="$CAP_CPUS" --memory="$CAP_MEM" \
        -v "$BQ_SA":/sa/key.json:ro apitap-bench-ingestr bash -c "
t0=\$(date +%s)
ingestr ingest --source-uri '$PG_URL' --source-table 'public.${TABLE}' \
  --dest-uri 'bigquery://${BQ_PROJECT}?credentials_path=/sa/key.json' \
  --dest-table '${BQ_DATASET}.bench_ing' --full-refresh --progress log
rc=\$?; echo \"  ingestr: \$(( \$(date +%s) - t0 ))s (exit \$rc; 137 = OOM-killed)\"" 2>&1 | tail -4
    validate bench_ing
    bq_drop bench_ing
    # ingestr stages through its own dataset — clean it or it lingers billed.
    echo "  note: check/drop the '_bruin_staging' dataset it creates"
    ;;
dlt)
    echo "==> dlt (backend='${DLT_BACKEND:-default}', ${CAP_CPUS} cpu / ${CAP_MEM})"
    docker run --rm -i --network=host --cpus="$CAP_CPUS" --memory="$CAP_MEM" \
        -v "$BQ_SA":/sa/key.json:ro -e GOOGLE_APPLICATION_CREDENTIALS=/sa/key.json \
        apitap-bench-dlt python -u - <<PYEOF 2>&1 | tail -6 || echo "  dlt FAILED rc=$? (137 = OOM-killed)"
import dlt, time
from dlt.sources.sql_database import sql_database
kw = dict(schema="public", table_names=["${TABLE}"])
if "${DLT_BACKEND}":
    kw["backend"] = "${DLT_BACKEND}"
src = sql_database("$PG_URL", **kw)
pipe = dlt.pipeline(pipeline_name="bq_bench", destination="bigquery",
                    dataset_name="${BQ_DATASET}")
t0 = time.time()
pipe.run(src, write_disposition="replace", table_name="bench_dlt")
print(f"  dlt: {time.time()-t0:.1f}s")
PYEOF
    validate bench_dlt
    bq_drop bench_dlt _dlt_loads _dlt_pipeline_state _dlt_version
    ;;
esac
done
echo "==> done"
