#!/usr/bin/env bash
# The control for the capped PostgreSQL -> ClickHouse campaign.
#
# A validator that has only ever agreed is untested. Before any timed leg, on a
# 1000-row control table, this proves three things — and it is expected to be
# GREEN on all three, or the campaign does not start:
#
#   1. AGREEMENT     apitap's landing digest equals the source digest, exactly.
#   2. SENSITIVITY    changing ONE value in ONE row changes the digest, and
#                     restoring it brings the digest back. A digest that does not
#                     move is a digest that proves nothing.
#   3. THE RIVAL     walshadow's landing passes the SAME validator, so a MATCH in
#                     the campaign is a statement about the transfer and not about
#                     a rule one tool satisfies and the other cannot.
#
# It runs both tools in the capped cage, against the same harness the campaign
# uses, with its own WORK directory so it cannot pollute the campaign's results.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-pg-ch-0.57.sh"
export WORK=${WORK:-$HOME/bench-capped-pg-ctrl}
export TABLES_OVERRIDE=cmp_ctrl
export EXPECT_ROWS=1000
export LEG_TIMEOUT=${LEG_TIMEOUT:-900}
CTRL=cmp_ctrl
mkdir -p "$WORK/logs"

echo "== 0. the control table (1000 rows, the campaign's schema) =="
docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -q \
    -v rows=1000 -v tbl=$CTRL < "$HERE/bench-capped-pg-ch-schema.sql"
src_before=$("$H" agg src $CTRL)
echo "SOURCE   $src_before"
[[ -n "$src_before" ]] || { echo "CONTROL: source aggregate empty"; exit 1; }

echo
echo "== 1. AGREEMENT: apitap's landing equals the source =="
bash "$H" drop >/dev/null
bash "$H" leg apitap control >/dev/null
got=$("$H" agg dst $CTRL)
echo "APITAP   $got"
if [[ "$got" == "$src_before" ]]; then echo "  GREEN agreement"; else
    echo "  *** RED agreement"; echo "src: $src_before"; echo "dst: $got"
    bash "$HERE/bench-capped-pg-ch-validator.sh" cols src $CTRL > "$WORK/cols-src.txt"
    bash "$HERE/bench-capped-pg-ch-validator.sh" cols dst $CTRL > "$WORK/cols-dst.txt"
    diff -u "$WORK/cols-src.txt" "$WORK/cols-dst.txt" | head -40
    exit 1
fi

echo
echo "== 2. SENSITIVITY: one value in one row must move the digest =="
tampered=$(docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -Atc \
    "UPDATE $CTRL SET extra_text = 'tampered-by-control' WHERE id = 777 RETURNING id")
src_tampered=$("$H" agg src $CTRL)
echo "TAMPERED $src_tampered  (row: $tampered)"
if [[ "$src_tampered" != "$src_before" ]]; then echo "  GREEN sensitivity"; else
    echo "  *** RED: the digest did not move"; exit 1
fi
docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -Atc \
    "UPDATE $CTRL SET extra_text = 'extra-' || substr(md5((777*3)::text),1,60) WHERE id = 777" >/dev/null
src_restored=$("$H" agg src $CTRL)
echo "RESTORED $src_restored"
if [[ "$src_restored" == "$src_before" ]]; then echo "  GREEN reversible"; else
    echo "  *** RED: the restore did not come back"; exit 1
fi

echo
echo "== 3. THE RIVAL: walshadow's landing passes the same validator =="
# Uncapped on purpose. This leg exists to prove that walshadow's landing can be
# READ by the same validator, not to measure it: whether walshadow fits in 256 MB
# is the campaign's question, and a control that fails on a memory ceiling would
# report "the validator is untested" when what it means is "the rival does not
# fit". The apitap leg above stays capped because apitap does fit.
bash "$H" drop >/dev/null
CAP="--cpus=0.5" LEG_TIMEOUT=1800 bash "$H" leg wsdefault control >/dev/null
got_ws=$("$H" agg dst $CTRL)
echo "WALSHADOW $got_ws"
if [[ "$got_ws" == "$src_before" ]]; then echo "  GREEN rival validates"; else
    echo "  *** RED: the rival does not validate under the same rules"
    bash "$HERE/bench-capped-pg-ch-validator.sh" cols src $CTRL > "$WORK/cols-src.txt"
    bash "$HERE/bench-capped-pg-ch-validator.sh" cols dst $CTRL > "$WORK/cols-dst.txt"
    diff -u "$WORK/cols-src.txt" "$WORK/cols-dst.txt" | head -40
    exit 1
fi

echo
echo "== the landed schema, as each tool built it =="
docker exec -i apitap-bench-ch clickhouse-client --password bench -q \
    "SELECT name, type FROM system.columns WHERE database='default' AND table='$CTRL' ORDER BY position FORMAT TSV"
docker exec -i apitap-bench-ch clickhouse-client --password bench -q \
    "SELECT engine, create_table_query FROM system.tables WHERE database='default' AND table='$CTRL' FORMAT TSVRaw"

echo
echo "== cleanup: the destination tables go, the control table goes =="
bash "$H" drop >/dev/null
docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -Atc "DROP TABLE IF EXISTS $CTRL" >/dev/null
bash "$H" wsreset >/dev/null
echo "CONTROL GREEN x3"