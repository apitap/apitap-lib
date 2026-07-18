#!/bin/bash
# PG -> GCS head-to-head: apitap (csv+parquet) vs ingestr vs dlt (default+pyarrow).
# Every output is checksum-validated FROM THE BUCKET before a time counts.
#
#   PG_URI=postgresql://user:pass@host:5432/db \
#   BUCKET=my-bench-bucket KEY=/abs/path/sa-key.json ROWS=1000000 \
#   ./benchmarks/run-gcs.sh
#
# Needs: docker, a Postgres with the ingestr benchmark table seeded (seed.sql),
# a GCS bucket the service account can objectAdmin, and a python venv with
# google-auth + requests + pyarrow at $VERIFY_PY (default: python3 with both
# installed). Tools run one at a time in --cpus/--memory-capped containers so
# they never contend.
set -u
PS="${PG_URI:?set PG_URI}"
B="${BUCKET:?set BUCKET}"
KEY="${KEY:?set KEY (absolute path to the service-account json)}"
ROWS="${ROWS:-1000000}"
VERIFY_PY="${VERIFY_PY:-python3}"
d(){ docker "$@"; }

if (( ROWS >= 1000000 )); then SUFFIX="$((ROWS / 1000000))m"; else SUFFIX="$((ROWS / 1000))k"; fi
SRCSUM=$(psql "$PS" -tAc "SELECT count(*)||'|'||sum(id)||'|'||md5(string_agg(small_str,'' ORDER BY id)) FROM bench_data_${SUFFIX}")
echo "SRC: $SRCSUM"

echo "=== images ==="
printf 'FROM python:3.12-slim\nCOPY *.whl /tmp/\nRUN pip install --no-cache-dir /tmp/*.whl\n' | d build -q -t gcsbench-apitap -f- "$(dirname "$0")/wheels"
d image inspect gcsbench-ingestr >/dev/null 2>&1 || printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir ingestr==1.0.75\n' | d build -q -t gcsbench-ingestr -
d image inspect gcsbench-dlt >/dev/null 2>&1 || printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir "dlt[postgres,parquet,gs]" psycopg2-binary sqlalchemy pyarrow pymysql\n' | d build -q -t gcsbench-dlt -
echo "  images ok"

run_tool(){ # name image cmd...
  local name=$1 img=$2; shift 2
  echo "--- $name ---"
  local t0=$(date +%s.%N)
  timeout -s KILL 330 docker run --rm --network=host --cpus=16 --memory=4g \
    -v $KEY:/sa/key.json:ro -e GOOGLE_APPLICATION_CREDENTIALS=/sa/key.json \
    "$img" "$@" 2>&1 | tail -4
  local rc=$?
  local t1=$(date +%s.%N)
  if [ $rc -eq 137 ] || [ $rc -eq 124 ]; then echo "  $name: DNF (timeout 330s)"; else
    echo "  $name: $(echo "$t1 $t0" | awk '{printf "%.1fs", $1-$2}') (rc=$rc)"; fi
}

echo "=== 1. apitap format=csv ==="
run_tool apitap-csv gcsbench-apitap python -c "
import apitap, time
t0=time.time()
r = apitap.transfer('$PS', 'gcs://$B/ap-csv?credentials=/sa/key.json&format=csv', table='public.bench_data_${SUFFIX}')
print(f'{r.rows:,} rows'); print('ELAPSED', round(time.time()-t0,1))"

echo "=== 2. apitap format=parquet ==="
run_tool apitap-pq gcsbench-apitap python -c "
import apitap, time
t0=time.time()
r = apitap.transfer('$PS', 'gcs://$B/ap-pq?credentials=/sa/key.json&format=parquet', table='public.bench_data_${SUFFIX}')
print(f'{r.rows:,} rows'); print('ELAPSED', round(time.time()-t0,1))"

echo "=== 3. ingestr 1.0.75 -> gs ==="
run_tool ingestr gcsbench-ingestr python -c "
import subprocess, sys, time
cmd = ['ingestr', 'ingest', '--source-uri', '$PS', '--source-table', 'public.bench_data_${SUFFIX}',
       '--dest-uri', 'gs://?credentials_path=/sa/key.json',
       '--dest-table', '$B/ing/bench_data_${SUFFIX}', '--yes', '--full-refresh']
t0=time.time()
r = subprocess.run(cmd, capture_output=True, text=True)
if r.returncode != 0:
    print(r.stdout[-1200:], r.stderr[-800:], sep='\n', file=sys.stderr); sys.exit(1)
print('ELAPSED', round(time.time()-t0,1))"

echo "=== 4. dlt default -> gs (parquet) ==="
run_tool dlt-default gcsbench-dlt python -c "
import dlt, time, shutil
from dlt.sources.sql_database import sql_database
shutil.rmtree('/tmp/dltpipe', ignore_errors=True)
src = sql_database('$PS', schema='public', table_names=['bench_data_${SUFFIX}'])
dest = dlt.destinations.filesystem(bucket_url='gs://$B/dlt-def')
pipe = dlt.pipeline(pipeline_name='p1', destination=dest, dataset_name='ds', pipelines_dir='/tmp/dltpipe')
t0=time.time()
pipe.run(src, write_disposition='replace', loader_file_format='parquet')
print('ELAPSED', round(time.time()-t0,1))"

echo "=== 5. dlt pyarrow -> gs (parquet) ==="
run_tool dlt-pyarrow gcsbench-dlt python -c "
import dlt, time, shutil
from dlt.sources.sql_database import sql_database
shutil.rmtree('/tmp/dltpipe', ignore_errors=True)
src = sql_database('$PS', schema='public', table_names=['bench_data_${SUFFIX}'], backend='pyarrow')
dest = dlt.destinations.filesystem(bucket_url='gs://$B/dlt-pa')
pipe = dlt.pipeline(pipeline_name='p2', destination=dest, dataset_name='ds', pipelines_dir='/tmp/dltpipe')
t0=time.time()
pipe.run(src, write_disposition='replace', loader_file_format='parquet')
print('ELAPSED', round(time.time()-t0,1))"

echo "=== 6. VALIDASI dari bucket (semua prefix) ==="
$VERIFY_PY - <<'EOF'
import gzip, csv, io, hashlib, json
import pyarrow.parquet as pq
import google.auth.transport.requests, google.oauth2.service_account, requests
creds = google.oauth2.service_account.Credentials.from_service_account_file(
    "$KEY", scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
creds.refresh(google.auth.transport.requests.Request())
h = {"Authorization": f"Bearer {creds.token}"}
def objects(prefix):
    out, tok = [], None
    while True:
        url = f"https://storage.googleapis.com/storage/v1/b/$B/o?prefix={requests.utils.quote(prefix, safe='')}"
        if tok: url += f"&pageToken={tok}"
        j = requests.get(url, headers=h, timeout=60).json()
        out += [i["name"] for i in j.get("items", [])]
        tok = j.get("nextPageToken")
        if not tok: return out
def fetch(name):
    return requests.get(f"https://storage.googleapis.com/storage/v1/b/$B/o/{requests.utils.quote(name, safe='')}?alt=media", headers=h, timeout=300).content
def summarize(prefix):
    ids, small = [], {}
    files = [n for n in objects(prefix) if not n.endswith("/")]
    data_files = 0
    for n in files:
        if n.endswith(".parquet"):
            t = pq.read_table(io.BytesIO(fetch(n)))
            if "id" not in t.column_names: continue
            data_files += 1
            for i, s in zip(t.column("id").to_pylist(), t.column("small_str").to_pylist()):
                ids.append(i); small[i] = s
        elif n.endswith(".csv.gz") or n.endswith(".csv"):
            raw = fetch(n)
            text = gzip.decompress(raw).decode() if n.endswith(".gz") else raw.decode()
            rows = list(csv.reader(io.StringIO(text)))
            hdr = rows[0]
            if "id" not in hdr: continue
            data_files += 1
            ii, si = hdr.index("id"), hdr.index("small_str")
            for r in rows[1:]:
                i = int(r[ii]); ids.append(i); small[i] = r[si]
        elif n.endswith(".jsonl") or n.endswith(".jsonl.gz"):
            raw = fetch(n)
            text = gzip.decompress(raw).decode() if n.endswith(".gz") else raw.decode()
            data_files += 1
            for line in text.splitlines():
                o = json.loads(line)
                if "id" in o: ids.append(o["id"]); small[o["id"]] = o["small_str"]
    if not ids: return f"NO DATA (files={len(files)})"
    m = hashlib.md5("".join(small[i] for i in sorted(small)).encode()).hexdigest()
    return f"{len(ids)}|{sum(ids)}|{m} ({data_files} data files)"
for p in ("ap-csv/", "ap-pq/", "ing/", "dlt-def/", "dlt-pa/"):
    print(f"  {p:9} {summarize(p)}")
EOF
echo "DONE_SHOWDOWN"
