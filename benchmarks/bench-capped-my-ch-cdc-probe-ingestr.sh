#!/usr/bin/env bash
# Where does ingestr 1.1.61's MySQL CDC path actually END?
#
# The capped arm found, in about a second per attempt:
#
#   mysql+cdc -> ClickHouse   "destination scheme \"clickhouse\" cannot safely run
#                              managed CDC: destination-managed state with fencing,
#                              pruning, and truncation is not supported"
#
# That is a statement about the DESTINATION, not about our source, our rig, or the
# tool's MySQL connector — and the difference matters, because a reader is entitled
# to know which of the three it is. So this probe runs the SAME source URI (the same
# mysql+cdc scheme, the same database, the same table) against several destinations,
# uncapped, one at a time, and prints each outcome verbatim.
#
# A destination that lands rows proves the source path is healthy and the refusal is
# destination-specific. A destination that refuses too would mean the path is broken
# here. Either way the answer is measured rather than asserted.
#
#   bash bench-capped-my-ch-cdc-probe-ingestr.sh [TABLE] [WORKDIR]
#
# Files land under WORKDIR and nowhere else; the shared ClickHouse destination is
# never written to by this script.
set -uo pipefail

TABLE=${1:-cdc_ctrl}
WORK=${2:-$HOME/bench-cdc-my-probe}
ING_BIN=${ING_BIN:-/home/ubuntu/.cache/ingestr/bin/v1.1.61/Linux_x86_64/ingestr}
SRC_URI=${SRC_URI:-"mysql+cdc://root:bench@127.0.0.1:3313/bench?server_id=18888&dest_schema=default"}
mkdir -p "$WORK"

# The probe owns its own tiny table, so the ClickHouse refusal can be shown to be
# independent of the campaign's schema: three columns, two rows, no longtext, no
# JSON, nothing of ours. It lives in whatever database the source URI names.
SRC_DB=$(printf '%s' "$SRC_URI" | sed 's|.*/||; s|?.*||')
SRC_PORT=$(printf '%s' "$SRC_URI" | sed 's|.*@[^:]*:||; s|/.*||')
docker exec apitap-bench-cdc-my mysql -uroot -pbench -e "
    CREATE DATABASE IF NOT EXISTS \`$SRC_DB\`;
    DROP TABLE IF EXISTS \`$SRC_DB\`.probe_mini;
    CREATE TABLE \`$SRC_DB\`.probe_mini (
        id INT NOT NULL PRIMARY KEY, a VARCHAR(20) DEFAULT NULL, b INT DEFAULT NULL
    ) ENGINE=InnoDB;
    INSERT INTO \`$SRC_DB\`.probe_mini VALUES (1,'x',7),(2,'y',8);" 2>/dev/null

echo "ingestr      : $("$ING_BIN" --version 2>&1 | head -1)"
echo "source       : ${SRC_URI%%\?*}   (mysql+cdc, this campaign's own source)"
echo "source-db    : $SRC_DB  (probe_mini: 3 columns, 2 rows, created by this probe)"
echo "workdir      : $WORK"

# name:dest-uri pairs. ClickHouse first: it is the destination the campaign's other
# arm uses, so its refusal is the one the report has to explain. duckdb and sqlite
# are ingestr's own documented CDC destinations (its MySQL CDC tutorial targets
# DuckDB), so they are the control for "is the SOURCE path healthy". mysql is in the
# list for one specific reason: the campaign's validator already has a MySQL-side
# digest definition, so a landing there is the only rival landing the SAME checksum
# function can read — which is what makes "the rival validates" a statement about the
# transfer rather than about a rule one tool satisfies and the other cannot.
DESTS='clickhouse|clickhouse://default:bench@127.0.0.1:9128?http_port=8128
duckdb|duckdb:///out/probe.duckdb
sqlite|sqlite:///out/probe.sqlite
mysql|mysql://root:bench@127.0.0.1:3313/cdcdst'

while IFS='|' read -r name uri; do
    [ -n "$name" ] || continue
    # Only THIS destination's artefact is cleared. An earlier version removed both
    # database files at the top of every iteration, so the duckdb landing was gone
    # from the listing by the time the probe printed its own verdict — the evidence
    # for one destination deleted by the next.
    case "$name" in
        duckdb) rm -f "$WORK/probe.duckdb" "$WORK/probe.duckdb.ingestr-cdc.lock" ;;
        sqlite) rm -f "$WORK/probe.sqlite" "$WORK/probe.sqlite.ingestr-cdc.lock" ;;
    esac
    rm -f "$WORK/run-$name.log" "$WORK/run-$name.rc"
    echo "=================================================================="
    echo "== $name"
    echo "   dest-uri: $uri"
    # The run's own log goes to a file and its exit status is captured directly, so
    # neither depends on a pipe (a pipe hands back the STATUS OF `tail`, which is
    # always 0 and would report every destination as a clean run).
    docker run --rm --network=host --cpus=0.5 \
        -e "P_URI=$SRC_URI" -e "P_TABLE=$TABLE" -e "P_DEST=$uri" \
        -v "$ING_BIN:/usr/local/bin/ingestr:ro" -v "$WORK:/out" \
        python:3.13-slim \
        sh -c "ingestr ingest --source-uri \"\$P_URI\" --source-table \"\$P_TABLE\" \
                 --dest-uri \"\$P_DEST\" --dest-table \"\$P_TABLE\" --yes \
                 > /out/run-$name.log 2>&1; echo \$? > /out/run-$name.rc"
    rc=$(cat "$WORK/run-$name.rc" 2>/dev/null || echo "no-rc")
    echo "   container exit: $rc"
    echo "   --- ingestr's own output (last 12 lines, verbatim) ---"
    tail -12 "$WORK/run-$name.log" 2>/dev/null | sed 's/^/   /'
    if [ "$name" = duckdb ] || [ "$name" = sqlite ]; then
        if [ -f "$WORK/probe.$name" ]; then
            echo "   LANDED: probe.$name ($(stat -c %s "$WORK/probe.$name") bytes)"
        else
            echo "   LANDED: nothing — no database file was written"
        fi
    fi
    echo
done <<EOF
$DESTS
EOF
echo "== probe workdir =="
ls -la "$WORK" | sed 's/^/  /'