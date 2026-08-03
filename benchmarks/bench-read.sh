#!/usr/bin/env bash
set -euo pipefail
# apitap.read() raced: connectorx (the Rust->Arrow incumbent) and
# pandas.read_sql (the default everyone suffers), same box, same tables.
# Legs: (1) 10M-row to_polars, uncapped; (2) 1M to_polars at 0.5cpu/256MB;
# (3) the frugality demo — STREAMING aggregation over 10M rows at
# 0.5cpu/256MB, where materializing readers cannot play at all.

PY=~/ice-run/bin/python
CAP="--cpus=0.5 --memory=256m --memory-swap=256m"
URI="postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
LOG(){ echo; echo "== $*"; }

LOG "prep the capped venv (uncapped — pip does not fit small tiers)"
if [ ! -x ~/read-venv/bin/python ]; then
  docker run --rm --network host -v ~/apitap-lib/benchmarks/wheels:/w -v ~/read-venv:/venv python:3.11-slim     sh -c "python -m venv /venv && /venv/bin/pip install -q /w/apitap-0.20.0-*.whl polars connectorx pyarrow"
fi

LOG "LEG 1: 10M-row read -> polars DataFrame (uncapped)"
$PY - <<'PY'
import time, apitap, polars as pl, connectorx as cx
URI = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"

t0 = time.time()
df = apitap.read(URI, table="bench_data_10m_cap").to_polars()
print(f"  apitap.read().to_polars(): {time.time()-t0:6.1f}s rows={df.height:,}")
del df

t0 = time.time()
df = cx.read_sql(URI.replace("postgres://","postgresql://"),
                 "SELECT * FROM bench_data_10m_cap", return_type="polars")
print(f"  connectorx  -> polars    : {time.time()-t0:6.1f}s rows={df.height:,}")
del df

import pandas as pd, sqlalchemy
t0 = time.time()
eng = sqlalchemy.create_engine(URI.replace("postgres://","postgresql+psycopg2://"))
pdf = pd.read_sql("SELECT * FROM bench_data_10m_cap", eng)
print(f"  pandas.read_sql          : {time.time()-t0:6.1f}s rows={len(pdf):,}")
del pdf
PY

LOG "LEG 2: 1M-row to_polars at 0.5 cpu / 256 MB (with cgroup peak)"
for tool in apitap connectorx; do
  docker run --rm --network host $CAP -v ~/read-venv:/venv python:3.11-slim sh -c "
/venv/bin/python - <<PYEOF
import time
t0=time.time()
if '$tool' == 'apitap':
    import apitap
    df = apitap.read('$URI', table='bench_data_1m').to_polars()
else:
    import connectorx as cx
    df = cx.read_sql('postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                     'SELECT * FROM bench_data_1m', return_type='polars')
peak=int(open('/sys/fs/cgroup/memory.peak').read())
print(f'  [$tool] 1M to_polars: {time.time()-t0:5.1f}s rows={df.height:,} peak={peak/1048576:.0f}MB')
PYEOF" || echo "  [$tool] FAILED/OOM at 256MB"
done

LOG "LEG 3: STREAMING sum over 10M rows at 0.5 cpu / 256 MB"
docker run --rm --network host $CAP -v ~/read-venv:/venv python:3.11-slim sh -c "
/venv/bin/python - <<PYEOF
import time, apitap, pyarrow as pa, pyarrow.compute as pc
t0=time.time(); total=0; rows=0
reader = pa.RecordBatchReader.from_stream(apitap.read('$URI', table='bench_data_10m_cap'))
for batch in reader:
    rows += batch.num_rows
    total += pc.sum(batch.column('id')).as_py()
peak=int(open('/sys/fs/cgroup/memory.peak').read())
print(f'  [apitap streaming] 10M sum: {time.time()-t0:5.1f}s rows={rows:,} sum={total} peak={peak/1048576:.0f}MB')
PYEOF"
docker run --rm --network host $CAP -v ~/read-venv:/venv python:3.11-slim sh -c "
/venv/bin/python - <<PYEOF
import time, connectorx as cx
t0=time.time()
df = cx.read_sql('postgresql://postgres:bench@127.0.0.1:5544/apitap_bench_src',
                 'SELECT * FROM bench_data_10m_cap', return_type='polars')
print(f'  [connectorx] 10M at 256MB: {time.time()-t0:5.1f}s rows={df.height:,}')
PYEOF" || echo "  [connectorx] 10M at 256MB: OOM-KILLED (materializes the table — cannot stream)"
echo done