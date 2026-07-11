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

rm -rf pgo-data merged.profdata && mkdir -p pgo-data && chmod 777 pgo-data

echo "== 1/3 instrumented build =="
rm -f benchmarks/wheels/*.whl
docker run --rm -v "$REPO":/io -v apitap-bench-cargo:/root/.cargo/registry \
    -e RUSTFLAGS="-Cprofile-generate=/pgodata" \
    ghcr.io/pyo3/maturin build --release -m py-apitap/Cargo.toml -o benchmarks/wheels
printf 'FROM python:3.12-slim\nCOPY *.whl /tmp/\nRUN pip install --no-cache-dir /tmp/*.whl\n' \
    | docker build -q -t apitap-pgo:inst -f- benchmarks/wheels

echo "== 2/3 training (all routes, 1M) =="
train() {
    docker run --rm --network=host \
        -v "$REPO/pgo-data":/pgodata -e LLVM_PROFILE_FILE=/pgodata/apitap-%m-%p.profraw \
        apitap-pgo:inst python -c "
import apitap
apitap.transfer('$1', '$2', table='$3')"
}
for _ in 1 2; do
    train "$PS" "$PD" public.bench_data_1m
    train "$PS" "$CH" public.bench_data_1m
    train "$MY" "$CH" bench_my_1m
    train "$MY" "$PD" bench_my_1m
done

echo "== 3/3 merge + optimized build =="
docker run --rm -v "$REPO":/io --entrypoint /bin/bash ghcr.io/pyo3/maturin -c \
    'rustup component add llvm-tools-preview >/dev/null 2>&1; \
     $(find /root/.rustup -name llvm-profdata | head -1) merge -o /io/merged.profdata /io/pgo-data/*.profraw'
find crates py-apitap vendor -name '*.rs' -exec touch {} +
rm -f benchmarks/wheels/*.whl
docker run --rm -v "$REPO":/io -v apitap-bench-cargo:/root/.cargo/registry \
    -e RUSTFLAGS="-Cprofile-use=/io/merged.profdata" \
    ghcr.io/pyo3/maturin build --release -m py-apitap/Cargo.toml -o benchmarks/wheels
echo "PGO wheel: $(ls benchmarks/wheels/*.whl)"
