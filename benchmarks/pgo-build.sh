#!/usr/bin/env bash
# PGO wheel build — the recipe measured at −12% wall on the CPU-bound tier
# (0.5 vCPU / 256 MB, mysql→clickhouse 10M: 44.8s → 39.5s) and neutral at 16c
# where the databases are the wall. Release wheels should ship PGO-built.
#
# Three phases, all inside the maturin manylinux container so the LLVM that
# instruments is the LLVM that merges:
#   1. instrumented build (-Cprofile-generate)
#   2. TRAINING: run every route the wheel supports — profiles are per-branch,
#      and routes missing from training can REGRESS (pydantic-core measured
#      -14% on untrained paths). Uses the bench containers from run-server.sh
#      at 1M rows per route.
#   3. merge + optimized rebuild (-Cprofile-use)
#
# Usage: benchmarks/pgo-build.sh   (from the repo root, bench containers up)
set -euo pipefail
cd "$(dirname "$0")/.."
REPO="$PWD"

PS='postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src'
PD='postgresql://postgres:bench@127.0.0.1:5545/apitap_bench_dst'
CH='clickhouse://default:bench@127.0.0.1:8124/default'
MY='mysql://root:bench@127.0.0.1:3307/bench'
MYD='mysql://root:bench@127.0.0.1:3308/bench'   # second MySQL for the MySQL->MySQL sink path

rm -rf pgo-data merged.profdata && mkdir -p pgo-data && chmod 777 pgo-data

echo "== 1/3 instrumented build =="
rm -f benchmarks/wheels/*.whl 2>/dev/null || true
docker run --rm -v "$REPO":/io -v apitap-bench-cargo:/root/.cargo/registry \
    -e RUSTFLAGS="-Cprofile-generate=/pgodata" \
    ghcr.io/pyo3/maturin build --release -m py-apitap/Cargo.toml -o benchmarks/wheels
printf 'FROM python:3.12-slim\nCOPY *.whl /tmp/\nRUN pip install --no-cache-dir /tmp/*.whl pyarrow\n' \
    | docker build -q -t apitap-pgo:inst -f- benchmarks/wheels

echo "== 2/3 training (all routes, 1M) =="
train() {
    docker run --rm --network=host \
        -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
        apitap-pgo:inst python -c "
import apitap
apitap.transfer('$1', '$2', table='$3')"
}
# Object-store lanes (0.16.0): the parquet encoder + SigV4 client + Iceberg
# commit path are distinct hot branches — untrained they can regress.
S3T="s3://apitap-bench/pgotrain?format=parquet&endpoint=http://127.0.0.1:9100&access_key_id=bench&secret_access_key=benchbench"
ICET="iceberg://127.0.0.1:8181/pgotrain?endpoint=http://127.0.0.1:9100&access_key_id=bench&secret_access_key=benchbench"
for _ in 1 2; do
    train "$PS" "$PD" public.bench_data_1m
    train "$PS" "$CH" public.bench_data_1m
    train "$MY" "$CH" bench_my_1m
    train "$MY" "$PD" bench_my_1m
    train "$MY" "$MYD" bench_my_1m
    train "$PS" "$MYD" public.bench_data_1m   # pg->mysql (0.13.0): pgmytsv transcoder
    train "$PS" "$S3T" public.bench_data_1m   # s3 multipart + parquet encode
    train "$PS" "$ICET" public.bench_data_1m  # iceberg: same lane + REST commit
done
# log_based CDC (0.17.0): pgoutput decode + collapse + walsender are new hot
# branches. Bootstrap + one drained window against the logical-wal source.
docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc \
    "DROP TABLE IF EXISTS pgo_cdc; CREATE TABLE pgo_cdc(id int primary key, v text); \
     INSERT INTO pgo_cdc SELECT g, 'v'||g FROM generate_series(1,200000) g" >/dev/null
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap
apitap.transfer('$PS', '$PD', table='pgo_cdc', mode='log_based')"
docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc \
    "INSERT INTO pgo_cdc SELECT g, 'w'||g FROM generate_series(200001,400000) g; \
     UPDATE pgo_cdc SET v='u' WHERE id <= 50000; DELETE FROM pgo_cdc WHERE id BETWEEN 60000 AND 70000" >/dev/null
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap
apitap.transfer('$PS', '$PD', table='pgo_cdc', mode='log_based')"
docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc \
    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'; \
     DROP TABLE pgo_cdc" >/dev/null 2>&1 || true
docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc \
    "DROP TABLE IF EXISTS pgo_cdc; DELETE FROM _apitap_state WHERE dest_table='pgo_cdc'" >/dev/null 2>&1 || true

# GCS (both formats) needs live GCP creds: set GCS_TRAIN_URL to the parquet
# URL (gcs://bucket/prefix?format=parquet&credentials=/abs/key.json) and
# GCS_TRAIN_SA to the key path; skipped otherwise.
if [ -n "${GCS_TRAIN_URL:-}" ] && [ -n "${GCS_TRAIN_SA:-}" ]; then
    train_gcs() {
        docker run --rm --network=host \
            -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
            -v "$GCS_TRAIN_SA":/sa/key.json:ro \
            apitap-pgo:inst python -c "
import apitap
apitap.transfer('$PS', '$1', table='public.bench_data_1m', dest_table='pgo_train_gcs')"
    }
    train_gcs "$GCS_TRAIN_URL"
    train_gcs "${GCS_TRAIN_URL/format=parquet/format=csv}"
fi
# BigQuery route (both lanes — untrained branches can regress): needs a live
# project. Set BQ_TRAIN_URL='bigquery://proj/ds?credentials=/sa/key.json' and
# BQ_TRAIN_SA=/abs/path/key.json to enable; skipped otherwise.
if [ -n "${BQ_TRAIN_URL:-}" ] && [ -n "${BQ_TRAIN_SA:-}" ]; then
    for par in 8 2; do  # 8 = parquet lane, 2 = CSV lane
        docker run --rm --network=host \
            -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
            -v "$BQ_TRAIN_SA":/sa/key.json:ro \
            apitap-pgo:inst python -c "
import apitap
apitap.transfer('$PS', '$BQ_TRAIN_URL', table='public.bench_data_1m',
                dest_table='pgo_train_bq', parallel=$par)"
    done
fi

# read() -> Arrow (0.21.0): the arrowcol builders, raw COPY plane and the
# capsule stream are new hot branches — train BOTH consumption shapes
# (streaming pull and the materialize fast path).
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap, pyarrow as pa
rdr = pa.RecordBatchReader.from_stream(apitap.read('$PS', table='public.bench_data_1m'))
assert sum(b.num_rows for b in rdr) == 1_000_000"
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap
assert apitap.read('$PS', table='public.bench_data_1m').to_arrow().num_rows == 1_000_000"
# 0.22.0: the projection-pushdown path (columns= narrows the SELECT and the
# decode) is its own hot shape — train it too.
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap, pyarrow as pa
rdr = pa.RecordBatchReader.from_stream(
    apitap.read('$PS', table='public.bench_data_1m', columns=['big_int', 'regular_int']))
assert sum(b.num_rows for b in rdr) == 1_000_000"
# 0.24.0: the mysql read lanes — the RAW wire plane (mywire packet walk +
# append_cell) and the sqlx direct-Arrow fallback are both hot shapes.
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap, pyarrow as pa
rdr = pa.RecordBatchReader.from_stream(apitap.read('$MY', table='bench_my_1m'))
assert sum(b.num_rows for b in rdr) == 1_000_000"
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    -e APITAP_MY_RAW=0 apitap-pgo:inst python -c "
import apitap, pyarrow as pa
rdr = pa.RecordBatchReader.from_stream(
    apitap.read('$MY', table='bench_my_1m', columns=['big_int', 'regular_int']))
assert sum(b.num_rows for b in rdr) == 1_000_000"

# 0.25.0: the mysql binlog CDC lanes (mywire dump, mybinlog decode,
# collapse+apply) — one small bootstrap+mutate+drain cycle trains them.
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import subprocess, apitap
m = lambda q: subprocess.run(['mysql','-h','127.0.0.1','-P','3307','-uroot','-pbench','-N','-e',q], check=True)
" 2>/dev/null || true
docker exec apitap-bench-my mysql -uroot -pbench -N -e "DROP TABLE IF EXISTS bench.pgo_cdc; CREATE TABLE bench.pgo_cdc (id INT PRIMARY KEY, v VARCHAR(40), d DECIMAL(10,2)); INSERT INTO bench.pgo_cdc SELECT id, small_str, decimal_val FROM bench.bench_my_1m LIMIT 50000;"
docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc "DROP TABLE IF EXISTS pgo_cdc; DO \$\$ BEGIN IF to_regclass('_apitap_state') IS NOT NULL THEN DELETE FROM _apitap_state WHERE dest_table='pgo_cdc'; END IF; END \$\$;" >/dev/null
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap
apitap.transfer('$MY', 'postgresql://postgres:bench@127.0.0.1:5545/apitap_bench_dst', table='pgo_cdc', mode='log_based')"
docker exec apitap-bench-my mysql -uroot -pbench -N -e "UPDATE bench.pgo_cdc SET d = d + 1 WHERE id <= 20000; DELETE FROM bench.pgo_cdc WHERE id > 45000;"
docker run --rm --network=host \
    -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
    apitap-pgo:inst python -c "
import apitap
apitap.transfer('$MY', 'postgresql://postgres:bench@127.0.0.1:5545/apitap_bench_dst', table='pgo_cdc', mode='log_based')"
docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc "DROP TABLE IF EXISTS pgo_cdc; DELETE FROM _apitap_state WHERE dest_table='pgo_cdc';" >/dev/null

echo "== 3/3 merge + optimized build =="
docker run --rm -v "$REPO":/io --entrypoint /bin/bash ghcr.io/pyo3/maturin -c \
    'rustup component add llvm-tools-preview >/dev/null 2>&1; \
     $(find /root/.rustup -name llvm-profdata | head -1) merge -o /io/merged.profdata /io/pgo-data/*.profraw'
find crates py-apitap vendor -name '*.rs' -exec touch {} +
rm -f benchmarks/wheels/*.whl 2>/dev/null || true
docker run --rm -v "$REPO":/io -v apitap-bench-cargo:/root/.cargo/registry \
    -e RUSTFLAGS="-Cprofile-use=/io/merged.profdata" \
    ghcr.io/pyo3/maturin build --release -m py-apitap/Cargo.toml -o benchmarks/wheels
echo "PGO wheel: $(ls benchmarks/wheels/*.whl)"
