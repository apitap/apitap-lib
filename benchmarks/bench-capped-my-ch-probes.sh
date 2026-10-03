#!/bin/sh
# Probes that run INSIDE the capped container, beside the legs:
#
#   startup            what the tool costs in the cage before it moves a row
#   ceiling ROWS       how many rows ONE ingestr process can land at this cap
#
# Both print the kernel's own memory.peak, so the cage cost of the tool itself
# can be read next to the cage cost of the transfer.
set -u
case "${1:-}" in
startup)
    case "${2:-}" in
    apitap)  # what apitap's leg pays before transfer(): importing the wheel
        python -c "import apitap; print('apitap', apitap.__version__)" ;;
    ingestr) # what ingestr's leg pays before its first query: loading the 254 MB binary
        ingestr --version 2>&1 | head -1 ;;
    *) echo "usage: probes.sh startup apitap|ingestr" >&2; exit 2 ;;
    esac
    ;;
ceiling)
    LIMIT=$2
    DEST_TABLE=cmp_probe_${LIMIT}
    # ingestr's own --sql-limit: this is a measurement of ingestr's ceiling at
    # this tier, not part of the head-to-head. What lands is validated against
    # the matching MySQL subset (WHERE id <= N) by the caller.
    echo "== ingestr, 1 process, --sql-limit $LIMIT, capped 0.5cpu/256MB"
    start_ns=$(date +%s%N)
    ingestr ingest \
        --source-uri "mysql://root:bench@127.0.0.1:3307/bench" \
        --source-table cmp_my_t01 \
        --dest-uri "clickhouse://default:bench@127.0.0.1:9124?http_port=8124" \
        --dest-table "$DEST_TABLE" \
        --sql-limit "$LIMIT" \
        --yes --full-refresh --progress log 2>&1 | tail -14
    end_ns=$(date +%s%N)
    echo "PROBE_LIMIT $LIMIT ELAPSED_MS $(( (end_ns - start_ns) / 1000000 ))"
    ;;
*) sed -n 2,10p "$0"; exit 2 ;;
esac
echo "MEMPEAK=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
# anon vs file-backed: a mapped binary's pages are charged to this cgroup, and
# saying how much of the peak is file-backed is part of the number.
awk '/^anon |^file /{printf "MEMSTAT %s %s\n", $1, $2}' /sys/fs/cgroup/memory.stat 2>/dev/null