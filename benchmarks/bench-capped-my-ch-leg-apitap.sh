#!/usr/bin/env bash
# The apitap leg of the capped MySQL->ClickHouse head-to-head: all 10 tables,
# ONE process, inside ONE container capped at 0.5 CPU / 256 MB.
#
#   apitap.transfer(src, dst, tables=[...]) — the multi-table path, which shares
#   ONE pipe budget across every table (so peak stays at the single-table
#   ceiling). No knobs: no parallel=, no chunk_bytes, no env tuning — apitap
#   sizes itself to the cgroup limit it finds.
#
# The venv's site-packages is mounted READ-ONLY and put on PYTHONPATH, exactly
# like benchmarks/e2e_parquet_capped.py does, so the PyPI wheel is what runs and
# nothing is built here.
set -u

SRC='mysql://root:bench@127.0.0.1:3307/bench'
DST='clickhouse://default:bench@127.0.0.1:8124/default'
TABLES="cmp_my_t01 cmp_my_t02 cmp_my_t03 cmp_my_t04 cmp_my_t05 cmp_my_t06 cmp_my_t07 cmp_my_t08 cmp_my_t09 cmp_my_t10"

python - "$SRC" "$DST" "$TABLES" <<'PY'
import sys, time
src, dst, tables = sys.argv[1], sys.argv[2], sys.argv[3].split()

t_imp = time.time()
import apitap
imp_s = time.time() - t_imp
print(f"APITAP_VERSION {apitap.__version__}", flush=True)

t0 = time.time()
try:
    r = apitap.transfer(src, dst, tables=tables)
except Exception as exc:                                  # noqa: BLE001
    print(f"RAISED {type(exc).__name__}: {exc}", flush=True)
    for t in getattr(getattr(exc, "report", None), "tables", ()) or ():
        print(f"  table {t.table}: rows={t.rows} pipes={t.parallel} err={t.error}", flush=True)
    raise SystemExit(9)
el = time.time() - t0
print(f"ROWS {r.rows}", flush=True)
print(f"PIPE_BUDGET {r.parallel}", flush=True)
for t in r.tables:
    print(f"  table {t.table}: rows={t.rows} pipes={t.parallel} ms={t.elapsed_ms}", flush=True)
print(f"IMPORT_S {imp_s:.3f}", flush=True)
print(f"ELAPSED {el:.3f}", flush=True)
PY
rc=$?
# The kernel's own peak for THIS container — read before the cgroup goes away.
echo "MEMPEAK=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
exit $rc