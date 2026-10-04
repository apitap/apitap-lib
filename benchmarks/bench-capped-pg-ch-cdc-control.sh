#!/usr/bin/env bash
# The control for the capped PostgreSQL -> ClickHouse CDC campaign.
#
# A validator that has only ever agreed is untested, and a CDC lane that has never
# run end to end on this rig is untested too. Before any timed leg, on a 1000-row
# control table with the campaign's own schema, this proves four things — and it is
# expected to be GREEN on all four or the campaign does not start:
#
#   1. AGREEMENT   apitap's CDC landing digest equals the source digest, exactly.
#   2. SENSITIVITY changing ONE value in ONE row moves the digest, and restoring
#                  it brings the digest back.
#   3. THE RIVAL   walshadow's landing passes the SAME validator, so a MATCH later
#                  is a statement about the transfer and not about a rule one tool
#                  satisfies and the other cannot. Uncapped on purpose: this leg
#                  exists to prove the rival's landing can be READ by the same
#                  validator, and a control that failed on a memory ceiling would
#                  report "the validator is untested" when what it means is "the
#                  rival does not fit".
#   4. THE CDC LANE ITSELF: insert, update and delete on the source, drain again,
#                  and the landing must follow — and an empty drain must report 0
#                  changes. This is the leg that would catch a checksum that only
#                  ever proved the bootstrap.
#
# It drives this campaign's own harness, with its own WORK directory so it cannot
# pollute the campaign's results.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-pg-ch-cdc-0.57.sh"
export WORK=${WORK:-$HOME/bench-cdc-ctrl}
export TABLES_OVERRIDE=cdc_ctrl
export EXPECT_ROWS=1000
CTRL=cdc_ctrl
mkdir -p "$WORK/logs"
PG_C=apitap-bench-cdc-pg
CH_C=apitap-bench-cdc-ch
P() { docker exec -i "$PG_C" psql -U postgres -d bench -Atc "$1"; }
C() { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1"; }
dst_agg() { bash "$H" agg dst "$CTRL"; }
src_agg() { bash "$H" agg src "$CTRL"; }

echo "== 0. the control table (1000 rows, the campaign's schema, one apitap slot) =="
docker exec -i "$PG_C" psql -U postgres -d bench -q -v rows=1000 -v tbl=$CTRL \
    < "$HERE/bench-capped-pg-ch-schema.sql"
src_before=$(src_agg)
echo "SOURCE   $src_before"
[[ -n "$src_before" ]] || { echo "CONTROL: source aggregate empty"; exit 1; }

echo
echo "== 1. AGREEMENT: apitap's CDC landing equals the source =="
bash "$H" drop >/dev/null
bash "$H" slotdrop >/dev/null
bash "$H" leg ctrl-agree catchup 0 >/dev/null
got=$(dst_agg)
echo "APITAP   $got"
echo "  the landing apitap built, before anyone else touches it:"
C "SELECT concat('    engine=', engine) FROM system.tables WHERE database='default' AND table='$CTRL' FORMAT TSVRaw"
C "SELECT concat('    ', name, ' ', type) FROM system.columns WHERE database='default' AND table='$CTRL' ORDER BY position FORMAT TSV"
C "SELECT concat('    state: cursor=', cursor_col, ' mode=', mode, ' watermark=', watermark, ' last_rows=', last_rows)
    FROM _apitap_state FINAL WHERE dest_table='$CTRL' FORMAT TSVRaw"
if [[ "$got" == "$src_before" ]]; then echo "  GREEN agreement"; else
    echo "  *** RED agreement"; echo "src: $src_before"; echo "dst: $got"
    bash "$HERE/bench-capped-pg-ch-validator.sh" cols src $CTRL > "$WORK/cols-src.txt"
    bash "$HERE/bench-capped-pg-ch-validator.sh" cols dst $CTRL > "$WORK/cols-dst.txt"
    diff -u "$WORK/cols-src.txt" "$WORK/cols-dst.txt" | head -40
    exit 1
fi

echo
echo "== 2. SENSITIVITY: one value in one row must move the digest =="
P "UPDATE $CTRL SET extra_text = 'tampered-by-control' WHERE id = 777 RETURNING id" >/dev/null
src_tampered=$(src_agg)
echo "TAMPERED $src_tampered"
if [[ "$src_tampered" != "$src_before" ]]; then echo "  GREEN sensitivity"; else
    echo "  *** RED: the digest did not move"; exit 1
fi
P "UPDATE $CTRL SET extra_text = 'extra-' || substr(md5((777*3)::text),1,60) WHERE id = 777" >/dev/null
src_restored=$(src_agg)
echo "RESTORED $src_restored"
if [[ "$src_restored" == "$src_before" ]]; then echo "  GREEN reversible"; else
    echo "  *** RED: the restore did not come back"; exit 1
fi

echo
echo "== 3. THE CDC LANE: insert + update + delete, drained again =="
P "INSERT INTO $CTRL (id, small_str, medium_str, large_str, tiny_int, regular_int, big_int,
      float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text)
    SELECT g, substr(md5(g::text),1,8),
           CASE WHEN g % 97 = 0 THEN NULL ELSE substr(md5((g*7)::text),1,40) END,
           substr(md5((g*13)::text),1,200), (g % 100)::smallint, g, g::bigint*1000000,
           (g % 10000)::double precision / 8,
           CASE WHEN g % 89 = 0 THEN NULL ELSE ((g % 1000000)::numeric(18,4) / 100) END,
           (g % 2 = 0), date '2024-01-01' + (g % 900),
           timestamp '2024-01-01 00:00:00' + (g % 1000000) * interval '1 microsecond',
           timestamptz '2024-01-01 00:00:00+00' + (g % 1000000) * interval '1 microsecond',
           CASE WHEN g % 83 = 0 THEN NULL
                ELSE ('{\"k\":' || g || ',\"s\":\"' || substr(md5(g::text),1,16) || '\"}')::json END,
           CASE WHEN g % 79 = 0 THEN NULL ELSE 'extra-' || substr(md5((g*3)::text),1,60) END
    FROM generate_series(2000001, 2000050) AS g" >/dev/null
P "UPDATE $CTRL SET regular_int = regular_int + 1 WHERE id BETWEEN 1 AND 40" >/dev/null
P "DELETE FROM $CTRL WHERE id BETWEEN 101 AND 110" >/dev/null
src_after=$(src_agg)
echo "SOURCE   $src_after   (expected rows 1000 + 50 - 10 = 1040)"
bash "$H" leg ctrl-cdc drain 1 >/dev/null
got2=$(dst_agg)
echo "APITAP   $got2"
if [[ "$got2" == "$src_after" ]]; then echo "  GREEN the change stream follows"; else
    echo "  *** RED: the landing did not follow the changes"
    echo "src: $src_after"; echo "dst: $got2"; exit 1
fi
empty=$(grep -o 'IDEMPOTENT second_drain_rows=[0-9]*' "$WORK/logs/apitap-bench-cdc-ctrl-cdc.log" | tail -1)
echo "  the empty drain reported: ${empty:-<none>}"
[[ "$empty" == "IDEMPOTENT second_drain_rows=0" ]] \
    && echo "  GREEN an empty drain is a no-op" \
    || { echo "  *** RED: ${empty:-the empty drain reported nothing}"; exit 1; }

echo
echo "== 4. THE RIVAL: walshadow's landing passes the same validator (uncapped) =="
# EXPECT_ROWS is what the rival leg polls against, and by now the control table
# holds 1040 rows, not the 1000 it started with. Left at 1000 the poll declares
# "caught up" the moment total_rows crosses 1000, SIGTERMs the daemon mid-COPY and
# reports a short landing — which is exactly what this control did on its first run
# (1024 of 1040 rows, verdict RED). The number the leg must reach is the SOURCE's
# current count.
export EXPECT_ROWS
EXPECT_ROWS=$(P "SELECT count(*) FROM $CTRL")
echo "  the rival leg will poll for $EXPECT_ROWS rows, the source's current count"
bash "$H" drop >/dev/null
bash "$H" slotdrop >/dev/null
CAP="--cpus=0.5" LEG_TIMEOUT=1800 bash "$H" wsleg ctrl no >/dev/null
got_ws=$(dst_agg)
echo "WALSHADOW $got_ws"
if [[ -n "$got_ws" && "$got_ws" == "$src_after" ]]; then echo "  GREEN the rival validates"; else
    echo "  *** RED: the rival does not validate under the same rules (or landed nothing)"
    echo "src: $src_after"; echo "dst: ${got_ws:-<nothing landed>}"
    grep -E 'docker_state|stopped_by|wall_container|ROWS' "$WORK/results.txt" | tail -6
    exit 1
fi
# The landing can be right while the daemon is not: the receipt is the verdict on
# the daemon, the digest is the verdict on the data, and both are reported.
echo "  the leg's own receipt:"
grep -A6 '^LEG walshadow' "$WORK/results.txt" | sed 's/^/    /'
echo "  the landing's shape, as walshadow built it:"
C "SELECT engine FROM system.tables WHERE database='default' AND table='$CTRL' FORMAT TSVRaw"
C "SELECT name, type FROM system.columns WHERE database='default' AND table='$CTRL'
    AND name LIKE '\\_%' ORDER BY position FORMAT TSV"

echo
echo "== cleanup: the destination tables go, the control table goes =="
bash "$H" drop >/dev/null
bash "$H" slotdrop >/dev/null
P "DROP TABLE IF EXISTS $CTRL" >/dev/null
echo "CONTROL GREEN x4"