#!/usr/bin/env bash
set -euo pipefail
# A/B the bulk modes (replace) across the release wheel vs the branch wheel —
# the CDC work must not move them. Routes cover every touched sink/source:
# pg→pg 10M (overlap COPY loader), pg→ch 10M (RowBinary lane),
# my→pg 1M (mysql source), pg→my 1M (mysql LOAD DATA sink).
# Data cap per the campaign rule: ≤10M rows.

PSRC="docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -Atc"
REL_WHEEL=~/apitap-src/benchmarks/wheels/apitap-0.17.0-*.whl
BR_WHEEL=~/apitap-lib/benchmarks/wheels/apitap-0.17.0-*.whl
PY=~/ice-run/bin/python

echo "== ensure the 10M seed (bench_data_10m is 11.8M — cap at 10M)"
if [ "$($PSRC "SELECT count(*) FROM bench_data_10m_cap" 2>/dev/null || echo 0)" != "10000000" ]; then
  $PSRC "DROP TABLE IF EXISTS bench_data_10m_cap" >/dev/null
  $PSRC "CREATE TABLE bench_data_10m_cap AS SELECT * FROM bench_data_10m LIMIT 10000000" >/dev/null
  $PSRC "ALTER TABLE bench_data_10m_cap ADD PRIMARY KEY (id)" >/dev/null 2>&1 || true
fi

run_matrix() {
  label=$1
  $PY - <<PYEOF
import time, apitap
SRC_PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST_PG = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
DST_CH = "clickhouse://default:bench@127.0.0.1:8124/default"
SRC_MY = "mysql://root:bench@127.0.0.1:3307/bench?ssl-mode=disabled"
DST_MY = "mysql://root:bench@127.0.0.1:3308/bench"
routes = [
    ("pg->pg 10M", SRC_PG, DST_PG, "bench_data_10m_cap"),
    ("pg->ch 10M", SRC_PG, DST_CH, "bench_data_10m_cap"),
    ("my->pg  1M", SRC_MY, DST_PG, "bench_my_1m"),
    ("pg->my  1M", SRC_PG, DST_MY, "bench_data_1m"),
]
for name, s, d, t in routes:
    t0 = time.time()
    r = apitap.transfer(s, d, table=t, dest_table="reg_" + t)
    print(f"[$label] {name}: {time.time()-t0:6.1f}s  rows={r.rows:,}")
PYEOF
}

echo "== release wheel"
~/ice-run/bin/pip install -q --force-reinstall $REL_WHEEL
run_matrix release
echo "== branch wheel"
~/ice-run/bin/pip install -q --force-reinstall $BR_WHEEL
run_matrix branch

echo "== cleanup (dest tables dropped, seeds kept)"
docker exec apitap-bench-pg-dst psql -U postgres -d apitap_bench_dst -Atc \
  "DROP TABLE IF EXISTS reg_bench_data_10m_cap; DROP TABLE IF EXISTS reg_bench_my_1m" >/dev/null
docker exec apitap-bench-ch clickhouse-client -u default --password bench \
  -q "DROP TABLE IF EXISTS reg_bench_data_10m_cap" >/dev/null
docker exec apitap-bench-my-dst mysql -uroot -pbench bench -e \
  "DROP TABLE IF EXISTS reg_bench_data_1m" 2>/dev/null
echo done
