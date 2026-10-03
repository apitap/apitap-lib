#!/bin/sh
# The ingestr leg of the capped MySQL->ClickHouse head-to-head: all 10 tables,
# inside ONE container capped at 0.5 CPU / 256 MB.
#
# ingestr's documented batch model is one --source-table per invocation (a
# multi-table run is a CDC-source feature), so 10 concurrent ingestr processes,
# one per table, is ingestr's own native concurrency for many tables. Every
# other setting is left at its default — no apitap-side tuning is applied to it,
# and none of its defaults are changed either (page-size 25000, batch-size 512,
# extract-parallelism 5).
#
# The binary mounted at /usr/local/bin/ingestr is the exact executable the pip
# package downloads and execs (~/.cache/ingestr/bin/v1.1.61/Linux_x86_64).
set -u

SRC='mysql://root:bench@127.0.0.1:3307/bench'
DST='clickhouse://default:bench@127.0.0.1:9124?http_port=8124'
INGESTR_VERSION='1.1.61'

mkdir -p /out
echo "INGESTR_VERSION $INGESTR_VERSION" >&2
ingestr --version 2>&1 | head -1

start_ns=$(date +%s%N)
pids=""
for t in 01 02 03 04 05 06 07 08 09 10; do
    ingestr ingest \
        --source-uri "$SRC" \
        --source-table "cmp_my_t$t" \
        --dest-uri "$DST" \
        --dest-table "cmp_my_t$t" \
        --yes --full-refresh --progress log \
        > "/out/ingestr_cmp_my_t$t.log" 2>&1 &
    pids="$pids $! cmp_my_t$t"
done

rc_all=0
set -- $pids
while [ "$#" -gt 0 ]; do
    pid=$1
    name=$2
    shift 2
    wait "$pid"
    rc=$?
    echo "PROC $name rc=$rc"
    if [ "$rc" -ne 0 ]; then
        rc_all=1
    fi
done
end_ns=$(date +%s%N)

echo "PROC_ALL_RC $rc_all"
echo "ELAPSED_MS $(( (end_ns - start_ns) / 1000000 ))"
# The kernel's own peak for THIS container (all 10 processes in one cgroup).
echo "MEMPEAK=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
exit $rc_all