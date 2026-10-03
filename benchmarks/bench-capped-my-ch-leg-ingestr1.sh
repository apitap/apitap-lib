#!/bin/sh
# The ingestr leg, SEQUENTIAL arm: all 10 tables, inside ONE container capped at
# 0.5 CPU / 256 MB, but ONE ingestr process at a time.
#
# Why this arm exists: ingestr's documented batch model is one --source-table per
# invocation, so the concurrent arm is 10 processes at once — which is ingestr's
# own native concurrency for many tables. When that arm cannot fit in the cage,
# the fair question is what ingestr's BEST configuration is at this tier, so this
# arm gives it the same work with the least concurrency its model allows.
# Every setting is still at ingestr's own default (page-size 25000, batch-size
# 512, extract-parallelism 5).
set -u

SRC='mysql://root:bench@127.0.0.1:3307/bench'
DST='clickhouse://default:bench@127.0.0.1:9124?http_port=8124'
INGESTR_VERSION='1.1.61'

mkdir -p /out
echo "INGESTR_VERSION $INGESTR_VERSION"
ingestr --version 2>&1 | head -1

start_ns=$(date +%s%N)
rc_all=0
for t in 01 02 03 04 05 06 07 08 09 10; do
    ingestr ingest \
        --source-uri "$SRC" \
        --source-table "cmp_my_t$t" \
        --dest-uri "$DST" \
        --dest-table "cmp_my_t$t" \
        --yes --full-refresh --progress log \
        > "/out/ingestr1_cmp_my_t$t.log" 2>&1
    rc=$?
    echo "PROC cmp_my_t$t rc=$rc"
    [ "$rc" -ne 0 ] && rc_all=1
done
end_ns=$(date +%s%N)

echo "PROC_ALL_RC $rc_all"
echo "ELAPSED_MS $(( (end_ns - start_ns) / 1000000 ))"
# The kernel's own peak for THIS container.
echo "MEMPEAK=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
exit $rc_all