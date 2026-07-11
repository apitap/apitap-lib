#!/usr/bin/env bash
# Head-to-head PG→PG benchmark on a Linux server: apitap vs ingestr, each inside its own
# resource-capped docker container (CAP_CPUS / CAP_MEM), against YOUR Postgres (.env).
#
#   cp benchmarks/.env.example benchmarks/.env   # fill in host/port/user/password
#   ./benchmarks/run-server.sh
#
# Needs on the server: docker, psql (postgresql-client), and this repo checked out.
# The table schema + generators are ingestr's own benchmark (see seed.sql), and both
# results are checksum-validated against the source before a time counts.
set -euo pipefail
cd "$(dirname "$0")"
REPO="$(cd .. && pwd)"

# .env is OPTIONAL: by default (PG_DOCKER=1) the script runs its own throwaway bench
# Postgres containers and no credentials are needed. Set PG_DOCKER=0 + PG* vars in .env
# to benchmark against an existing server instead.
#
# .env supplies DEFAULTS only — a variable already set in the environment WINS, so
# `ROWS=10000000 ./run-server.sh` beats a ROWS line in .env (sourcing it wholesale
# silently overrode inline vars; that bit us).
if [[ -f .env ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]] || continue
        k="${BASH_REMATCH[1]}"; v="${BASH_REMATCH[2]}"
        [[ -n "${!k+x}" ]] || export "$k=$v"
    done < .env
fi
PG_DOCKER="${PG_DOCKER:-1}"
SRC_DB="${SRC_DB:-apitap_bench_src}"; DST_DB="${DST_DB:-apitap_bench_dst}"
ROWS="${ROWS:-1000000}"; WITH_INGESTR="${WITH_INGESTR:-1}"
CAP_CPUS="${CAP_CPUS:-2}"; CAP_MEM="${CAP_MEM:-2g}"
# Destination engine: postgres (default) or clickhouse.
DEST="${DEST:-postgres}"

INGESTR_VERSION="1.0.75"
APITAP_IMG="apitap-bench-apitap:local"
INGESTR_IMG="apitap-bench-ingestr:${INGESTR_VERSION}"
SRC_CONTAINER="apitap-bench-pg-src"
DST_CONTAINER="apitap-bench-pg-dst"
CH_CONTAINER="apitap-bench-ch"
# 8123/9000 may be taken by other ClickHouses on the host — bench uses its own ports.
CH_HTTP_PORT="${CH_HTTP_PORT:-8124}"; CH_NATIVE_PORT="${CH_NATIVE_PORT:-9124}"

if (( ROWS >= 1000000 )); then SUFFIX="$((ROWS / 1000000))m"; else SUFFIX="$((ROWS / 1000))k"; fi
TABLE="bench_data_${SUFFIX}"

start_pg() { # name port — loopback ONLY: never expose a password-'bench' postgres publicly.
    if docker inspect "$1" >/dev/null 2>&1; then
        # POSTGRES_PASSWORD is baked at initdb — if an existing bench container was
        # created with a different password (e.g. an old .env), TCP auth fails even
        # though docker-exec seeding works. Recreate it (bench data is throwaway).
        local have
        have="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$1" \
            | sed -n 's/^POSTGRES_PASSWORD=//p')"
        if [[ "$have" != "$PGPASSWORD" ]]; then
            echo "==> $1 exists with a different password — recreating"
            docker rm -f "$1" >/dev/null
        fi
    fi
    if ! docker inspect "$1" >/dev/null 2>&1; then
        echo "==> starting $1 (postgres:16-alpine, 127.0.0.1:$2)"
        docker run -d --name "$1" -e POSTGRES_PASSWORD="$PGPASSWORD" \
            -p "127.0.0.1:$2:5432" --shm-size=1g postgres:16-alpine >/dev/null
    fi
    # postgres images boot twice (initdb temp server, then the real one) — wait for a
    # real query, not pg_isready.
    for _ in $(seq 60); do
        docker exec "$1" psql -U postgres -qc "SELECT 1" >/dev/null 2>&1 && break
        sleep 1
    done
}

start_ch() {
    # An older bench CH may lack the native-port mapping (dlt needs it) — recreate.
    if docker inspect "$CH_CONTAINER" >/dev/null 2>&1 \
        && [[ -z "$(docker port "$CH_CONTAINER" 9000 2>/dev/null)" ]]; then
        echo "==> $CH_CONTAINER exists without a native-port mapping — recreating"
        docker rm -f "$CH_CONTAINER" >/dev/null
    fi
    if ! docker inspect "$CH_CONTAINER" >/dev/null 2>&1; then
        echo "==> starting $CH_CONTAINER (clickhouse-server:24.8, 127.0.0.1:${CH_HTTP_PORT}/http ${CH_NATIVE_PORT}/native)"
        docker run -d --name "$CH_CONTAINER" -e CLICKHOUSE_PASSWORD=bench \
            -p "127.0.0.1:${CH_HTTP_PORT}:8123" -p "127.0.0.1:${CH_NATIVE_PORT}:9000" \
            --ulimit nofile=262144:262144 clickhouse/clickhouse-server:24.8 >/dev/null
    fi
    for _ in $(seq 60); do
        docker exec "$CH_CONTAINER" clickhouse-client --password bench -q "SELECT 1" >/dev/null 2>&1 && break
        sleep 1
    done
}
chq() { docker exec "$CH_CONTAINER" clickhouse-client --password bench -q "$1"; }

if [[ "$PG_DOCKER" == "1" ]]; then
    # SOURCE and DESTINATION are two SEPARATE postgres instances (own WAL, buffers,
    # checkpoints) so the read side and the write side don't share a server.
    PGHOST="127.0.0.1"; PGUSER="postgres"; PGPASSWORD="${PGPASSWORD:-bench}"
    SRC_PORT="${SRC_PORT:-5544}"; DST_PORT="${DST_PORT:-5545}"
    start_pg "$SRC_CONTAINER" "$SRC_PORT"
    [[ "$DEST" == "postgres" ]] && start_pg "$DST_CONTAINER" "$DST_PORT"
    sleep 1
    # psql runs INSIDE the containers → no postgresql-client needed on the host.
    qs()  { docker exec -i "$SRC_CONTAINER" psql -U postgres -d "$1" -tAq -c "$2"; }
    qsf() { docker exec -i "$SRC_CONTAINER" psql -U postgres -d "$1" -tAq < "$2"; }
    qd()  { docker exec -i "$DST_CONTAINER" psql -U postgres -d "$1" -tAq -c "$2"; }
    SRC_URI="postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${SRC_PORT}/${SRC_DB}"
    DST_URI="postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${DST_PORT}/${DST_DB}"
else
    # External mode: one server, two databases (whatever the .env points at).
    : "${PGHOST:?set in benchmarks/.env}" "${PGPORT:?}" "${PGUSER:?}" "${PGPASSWORD:?}"
    export PGPASSWORD
    qs()  { psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$1" -tAq -c "$2"; }
    qsf() { psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$1" -tAq -f "$2"; }
    qd()  { psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$1" -tAq -c "$2"; }
    SRC_URI="postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${SRC_DB}"
    DST_URI="postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${DST_DB}"
fi

if [[ "$DEST" == "clickhouse" ]]; then
    start_ch
    DST_URI="clickhouse://default:bench@127.0.0.1:${CH_HTTP_PORT}/default"
    # dlt's clickhouse destination speaks native TCP + HTTP for staging.
    INGESTR_DST_URI="clickhouse://default:bench@localhost:${CH_NATIVE_PORT}?http_port=${CH_HTTP_PORT}&secure=0"
else
    INGESTR_DST_URI="$DST_URI"
fi

# Canonical checksum — one string producible from BOTH engines (decimal trim_scale'd,
# timestamps formatted, tstz normalized to UTC; float sums excluded — their text form
# is not comparable across engines). 16 per-column aggregates.
VALIDATE_PG="SELECT count(*) || '|' || sum(id) || '|' || sum(tiny_int) || '|' || sum(regular_int) || '|' ||
 sum(big_int) || '|' || md5(string_agg(small_str, '' ORDER BY id)) || '|' ||
 md5(string_agg(medium_str, '' ORDER BY id)) || '|' || sum(length(large_str)) || '|' ||
 sum(length(extra_text)) || '|' || trim_scale(sum(decimal_val)) || '|' ||
 count(*) FILTER (WHERE bool_val) || '|' || min(date_val) || '|' || max(date_val) || '|' ||
 to_char(min(ts_val), 'YYYY-MM-DD HH24:MI:SS') || '|' || to_char(max(ts_val), 'YYYY-MM-DD HH24:MI:SS') || '|' ||
 to_char(max(ts_tz_val) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || '|' ||
 md5(string_agg(json_val->>'key', '' ORDER BY id)) FROM public.TBL"
# NB: sum(big_int) at 10M rows exceeds Int64 (5.0e19 > 9.2e18) — ClickHouse's sum()
# wraps silently while Postgres promotes to numeric, so the CH side sums in Int128.
VALIDATE_CH="SELECT concat(toString(count()), '|', toString(sum(id)), '|', toString(sum(tiny_int)), '|',
 toString(sum(regular_int)), '|', toString(sum(toInt128(big_int))), '|',
 lower(hex(MD5(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((id, small_str)))))))), '|',
 lower(hex(MD5(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((id, medium_str)))))))), '|',
 toString(sum(length(large_str))), '|', toString(sum(length(extra_text))), '|',
 toString(sum(decimal_val)), '|', toString(countIf(bool_val = 1)), '|',
 toString(min(date_val)), '|', toString(max(date_val)), '|',
 formatDateTime(min(ts_val), '%Y-%m-%d %H:%i:%S'), '|', formatDateTime(max(ts_val), '%Y-%m-%d %H:%i:%S'), '|',
 formatDateTime(max(ts_tz_val), '%Y-%m-%d %H:%i:%S'), '|',
 lower(hex(MD5(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((id, JSONExtractString(json_val, 'key')))))))))
 ) FROM TBL"

# Checksum of a destination table, per engine.
val_dest() { # table
    if [[ "$DEST" == "clickhouse" ]]; then chq "${VALIDATE_CH//TBL/\`$1\`}"; else qd "$DST_DB" "${VALIDATE_PG//TBL/$1}"; fi
}

echo "==> databases"
qs postgres "SELECT 1 FROM pg_database WHERE datname = '$SRC_DB'" | grep -q 1 \
    || qs postgres "CREATE DATABASE \"$SRC_DB\"" >/dev/null
if [[ "$DEST" == "postgres" ]]; then
    qd postgres "SELECT 1 FROM pg_database WHERE datname = '$DST_DB'" | grep -q 1 \
        || qd postgres "CREATE DATABASE \"$DST_DB\"" >/dev/null
fi

echo "==> seeding public.${TABLE} (${ROWS} rows) in ${SRC_DB}…"
if [[ "$(qs "$SRC_DB" "SELECT count(*) FROM public.${TABLE}" 2>/dev/null || echo 0)" == "$ROWS" ]]; then
    echo "    already seeded"
else
    sed "s/BENCH_TABLE_PLACEHOLDER/${TABLE}/g; s/BENCH_ROWS_PLACEHOLDER/${ROWS}/g" seed.sql > /tmp/apitap_seed.sql
    time qsf "$SRC_DB" /tmp/apitap_seed.sql >/dev/null
fi
SRC_SUM="$(qs "$SRC_DB" "${VALIDATE_PG//TBL/$TABLE}")"

# ---- one-time images -----------------------------------------------------------------
if ! docker image inspect "$APITAP_IMG" >/dev/null 2>&1; then
    echo "==> building the apitap manylinux wheel (one-time)…"
    docker run --rm -v "$REPO":/io -v apitap-bench-cargo:/root/.cargo/registry \
        ghcr.io/pyo3/maturin build --release -m py-apitap/Cargo.toml -o benchmarks/wheels
    echo "==> building the apitap runner image…"
    printf 'FROM python:3.12-slim\nCOPY *.whl /tmp/\nRUN pip install --no-cache-dir /tmp/*.whl\n' \
        | docker build -t "$APITAP_IMG" -f- "$REPO/benchmarks/wheels"
fi
if [[ "$WITH_INGESTR" == "1" ]] && ! docker image inspect "$INGESTR_IMG" >/dev/null 2>&1; then
    echo "==> building the ingestr ${INGESTR_VERSION} image (one-time)…"
    printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir ingestr==%s\n' "$INGESTR_VERSION" \
        | docker build -t "$INGESTR_IMG" -
fi

# ---- capped runs (timed INSIDE the container so startup doesn't skew either tool) -----
echo "==> apitap (capped ${CAP_CPUS} cpu / ${CAP_MEM})"
APITAP_T="$(docker run --rm --network=host --cpus="$CAP_CPUS" --memory="$CAP_MEM" "$APITAP_IMG" \
    python -c "
import apitap, time
t0 = time.time()
r = apitap.transfer('$SRC_URI', '$DST_URI', table='public.${TABLE}')
print(f'    {r.rows:,} rows over {r.parallel} pipes', flush=True)
print('ELAPSED', time.time() - t0)
" | tee /dev/stderr | awk '/^ELAPSED/{print $2}')"
APITAP_OK="MISMATCH"
APITAP_SUM="$(val_dest "$TABLE")"
if [[ "$APITAP_SUM" == "$SRC_SUM" ]]; then APITAP_OK="match"; else
    echo "    src:    $SRC_SUM"
    echo "    apitap: $APITAP_SUM"
fi

INGESTR_T=""; INGESTR_OK=""
if [[ "$WITH_INGESTR" == "1" ]]; then
    echo "==> ingestr ${INGESTR_VERSION} (capped ${CAP_CPUS} cpu / ${CAP_MEM})"
    if [[ "$DEST" == "clickhouse" ]]; then INGESTR_DST_TABLE="default.${TABLE}_ingestr"; else INGESTR_DST_TABLE="public.${TABLE}_ingestr"; fi
    INGESTR_T="$(docker run --rm --network=host --cpus="$CAP_CPUS" --memory="$CAP_MEM" "$INGESTR_IMG" \
        python -c "
import subprocess, sys, time
cmd = ['ingestr', 'ingest', '--source-uri', '$SRC_URI', '--source-table', 'public.${TABLE}',
       '--dest-uri', '$INGESTR_DST_URI', '--dest-table', '$INGESTR_DST_TABLE', '--yes', '--full-refresh']
t0 = time.time()
r = subprocess.run(cmd, capture_output=True, text=True)
if r.returncode != 0:
    print(r.stdout[-2000:], r.stderr[-2000:], sep='\n', file=sys.stderr); sys.exit(1)
print('ELAPSED', time.time() - t0)
" | tee /dev/stderr | awk '/^ELAPSED/{print $2}')"
    INGESTR_OK="MISMATCH"
    if [[ "$DEST" == "clickhouse" ]]; then
        # dlt may prefix/rename its destination table — find what it actually created.
        ING_TBL="$(chq "SELECT name FROM system.tables WHERE database='default' AND name ILIKE '%${TABLE}_ingestr%' ORDER BY length(name) LIMIT 1")"
        echo "    ingestr table: ${ING_TBL:-<none>}"
        ING_SUM=""; [[ -n "$ING_TBL" ]] && ING_SUM="$(val_dest "$ING_TBL")"
    else
        ING_SUM="$(val_dest "${TABLE}_ingestr")"
    fi
    if [[ -n "$ING_SUM" && "$ING_SUM" == "$SRC_SUM" ]]; then INGESTR_OK="match"; else
        echo "    src:     $SRC_SUM"
        echo "    ingestr: $ING_SUM"
    fi
fi

# ---- report ----------------------------------------------------------------------------
echo
echo "${ROWS} rows · PG → ${DEST} · ingestr benchmark schema (15 cols) · each tool capped at ${CAP_CPUS} cpu / ${CAP_MEM}"
printf '%-10s %10s %12s\n' tool time validated
printf '%-10s %9.1fs %12s\n' apitap "$APITAP_T" "$APITAP_OK"
if [[ -n "$INGESTR_T" ]]; then
    printf '%-10s %9.1fs %12s\n' ingestr "$INGESTR_T" "$INGESTR_OK"
    awk -v a="$APITAP_T" -v i="$INGESTR_T" 'BEGIN{printf "\napitap is %.1f× faster\n", i/a}'
fi
[[ "$APITAP_OK" == "match" && ( -z "$INGESTR_T" || "$INGESTR_OK" == "match" ) ]] || { echo "VALIDATION FAILED" >&2; exit 1; }
