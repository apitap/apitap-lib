#!/usr/bin/env bash
# The control for the capped MySQL -> ClickHouse CDC campaign.
#
# A validator that has only ever agreed is untested, and a CDC lane that has never
# run end to end on this rig is untested too. Before any timed leg, on a 1000-row
# control table cloned from the campaign's own seed, this proves four things — and
# it is expected to be GREEN on all five or the campaign does not start:
#
#   1. AGREEMENT   apitap's CDC landing digest equals the source digest, exactly.
#   2. SENSITIVITY changing ONE value in ONE row moves the digest, and restoring
#                  it brings the digest back.
#   3. THE CDC LANE ITSELF: insert, update and delete on the source, drain again,
#                  and the landing must follow — and an empty drain must report 0
#                  changes. This is the leg that would catch a checksum that only
#                  ever proved the bootstrap.
#   4. THE RIVAL   ingestr's CDC landing passes the SAME validator, so a MATCH later
#                  is a statement about the transfer and not about a rule one tool
#                  satisfies and the other cannot. Uncapped on purpose: this leg
#                  exists to prove the rival's landing can be READ by the same
#                  validator, and a control that failed on a memory ceiling would
#                  report "the validator is untested" when what it means is "the
#                  rival does not fit".
#
# It drives this campaign's own harness, with its own WORK directory so it cannot
# pollute the campaign's results.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-my-ch-cdc-0.57.sh"
export WORK=${WORK:-$HOME/bench-cdc-my-ctrl}
export TABLES_OVERRIDE=cdc_ctrl
export EXPECT_ROWS=1000
CTRL=cdc_ctrl
mkdir -p "$WORK/logs"
P() { docker exec -i apitap-bench-cdc-my mysql -uroot -pbench -N -B bench -e "$1" 2>/dev/null; }
C() { docker exec -i apitap-bench-cdc-ch clickhouse-client --password bench -q "$1" 2>/dev/null; }
src_agg() { bash "$H" agg src "$CTRL"; }
dst_agg() { bash "$H" agg dst "$CTRL"; }
# This campaign's own object, and the only state cleanup it needs. The harness's
# cmd_drop clears rows whose dest_table starts with cdc_my_t; the control table is
# named cdc_ctrl, so its watermark is dropped here — otherwise the next control leg
# would meet its own stale watermark with a fresh destination and a drain would
# apply nothing.
clear_state() { C "DELETE FROM _apitap_state WHERE dest_table = '$CTRL'" || true; }
reset_dest() { bash "$H" drop >/dev/null; clear_state; }
COLS="id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text"
# json_val is read as CAST(... AS CHAR) because the campaign's schema stores the
# document in a longtext column (apitap's MySQL CDC lane refuses a native JSON column
# — see the schema file's header for the verbatim refusal). bench_my_1m still has the
# native type, so the control casts on the way in and lands byte-identical text.
SEL="id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, CAST(json_val AS CHAR), extra_text"

echo "== 0. the control table (1000 rows, the campaign's OWN schema, loaded from the bulk arm's seed) =="
P "DROP TABLE IF EXISTS $CTRL" >/dev/null
# The shape comes from the campaign's schema FILE with the table name swapped, not
# from `LIKE bench_my_1m`: the bulk arm's table still carries the native JSON column
# the CDC lane refuses, so LIKE would build a control the engine declines to run and
# the leg would fail for a reason that has nothing to do with what it tests.
docker exec -i apitap-bench-cdc-my mysql -uroot -pbench bench 2>/dev/null \
    < <(sed 's/`cdc_my_t01`/`cdc_ctrl`/g' "$HERE/bench-capped-my-ch-cdc-schema.sql")
P "INSERT INTO $CTRL ($COLS) SELECT $SEL FROM bench_my_1m WHERE id BETWEEN 1 AND 1000" >/dev/null
# The proof that the longtext column holds the bulk arm's bytes: the SAME aggregate,
# over the SAME 1000 ids, on a table whose column is native JSON and on ours, whose is
# longtext. That is the claim the schema file's header makes, so it is checked.
#
# Compared through the MAIN aggregate's third field rather than through `cols`: CRC32
# is summed per row, so the 1000-row control and the 1,000,000-row seed can never
# agree, and `cols` has no WHERE to narrow the seed to the control's id range.
ctrl_json=$(src_agg | cut -d'|' -f3)
seed_json=$(MY_C=apitap-bench-cdc-my bash "$HERE/bench-capped-my-ch-validator.sh" \
               src bench_my_1m "id BETWEEN 1 AND 1000" | cut -d'|' -f3)
echo "  json_crc over the SAME 1000 ids — control (longtext) vs bulk seed (native JSON):"
echo "    control $ctrl_json"
echo "    seed    $seed_json"
if [[ -n "$ctrl_json" && "$ctrl_json" == "$seed_json" ]]; then
    echo "    GREEN the document's bytes are unchanged by storing it as longtext"
else
    echo "    *** RED: the longtext column does NOT hold the seed's bytes"
    exit 1
fi
src_before=$(src_agg)
echo "SOURCE   $src_before"
[[ -n "$src_before" ]] || { echo "*** RED: the source aggregate is empty"; exit 1; }

echo
echo "== 1. AGREEMENT: apitap's CDC landing equals the source =="
reset_dest
bash "$H" leg ctrl-agree catchup 0 >/dev/null
got=$(dst_agg)
echo "APITAP   $got"
echo "  the landing apitap built, before anyone else touches it:"
C "SELECT concat('    engine=', engine, ' sorting=', sorting_key)
    FROM system.tables WHERE database='default' AND table='$CTRL' FORMAT TSVRaw"
C "SELECT concat('    ', name, ' ', type) FROM system.columns
    WHERE database='default' AND table='$CTRL' ORDER BY position FORMAT TSV"
C "SELECT concat('    state: cursor=', cursor_col, ' mode=', mode, ' watermark=', watermark,
                ' last_rows=', last_rows)
    FROM _apitap_state FINAL WHERE dest_table='$CTRL' FORMAT TSVRaw"
echo "  the binlog coordinate the group minted:"
bash "$H" binlog | sed 's/^/    /'
if [[ "$got" == "$src_before" ]]; then echo "  GREEN agreement"; else
    echo "  *** RED agreement"; echo "src: $src_before"; echo "dst: $got"
    MY_C=apitap-bench-cdc-my bash "$HERE/bench-capped-my-ch-validator.sh" cols src $CTRL > "$WORK/cols-src.txt"
    CH_C=apitap-bench-cdc-ch bash "$HERE/bench-capped-my-ch-validator.sh" cols dst $CTRL > "$WORK/cols-dst.txt"
    diff -u "$WORK/cols-src.txt" "$WORK/cols-dst.txt" | head -40
    exit 1
fi

echo
echo "== 2. SENSITIVITY: one value in one row must move the digest =="
P "UPDATE $CTRL SET extra_text = 'tampered-by-control' WHERE id = 777" >/dev/null
src_tampered=$(src_agg)
echo "TAMPERED $src_tampered"
if [[ "$src_tampered" != "$src_before" ]]; then echo "  GREEN sensitivity"; else
    echo "  *** RED: the digest did not move"; exit 1
fi
P "UPDATE $CTRL SET extra_text = (SELECT extra_text FROM bench_my_1m WHERE id = 777)
    WHERE id = 777" >/dev/null
src_restored=$(src_agg)
echo "RESTORED $src_restored"
if [[ "$src_restored" == "$src_before" ]]; then echo "  GREEN reversible"; else
    echo "  *** RED: the restore did not come back"; exit 1
fi

echo
echo "== 3. THE CDC LANE: insert + update + delete, drained again =="
P "INSERT INTO $CTRL ($COLS)
     SELECT s.id + 2000000, s.small_str, s.medium_str, s.large_str, s.tiny_int,
            s.id + 2000000, s.big_int, s.float_val, s.decimal_val, s.bool_val,
            s.date_val, s.ts_val, s.ts_tz_val, s.json_val, s.extra_text
       FROM $CTRL s WHERE s.id BETWEEN 1 AND 50" >/dev/null
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
echo "== 4. THE RIVAL: ingestr's CDC landing, read by the SAME validator =="
# WHERE the rival can land at all is a product boundary, measured by
# benchmarks/bench-capped-my-ch-cdc-probe-ingestr.sh and reported as the rival's
# arm: ingestr 1.1.61 refuses ClickHouse as a managed-CDC destination ("destination
# scheme \"clickhouse\" cannot safely run managed CDC: destination-managed state with
# fencing, pruning, and truncation is not supported"), and refuses sqlite for the
# same class of reason. So a control that demanded a ClickHouse landing would be
# demanding something the tool does not do — and the leg that proves "the rival's
# landing is readable by this campaign's validator" has to run where the rival runs.
#
# It lands in THIS campaign's own MySQL container, in its own schema, and it is read
# by the campaign's MySQL-side digest definition: literally the same aggregate
# expression that reads the source. If it matches, a MATCH in the timed arms is a
# statement about the transfer and not about a rule one tool satisfies and the
# other cannot.
ING_DB=cdcdst
ING_PORT=${ING_PORT:-3313}
P "CREATE DATABASE IF NOT EXISTS $ING_DB" >/dev/null
P "DROP TABLE IF EXISTS $ING_DB.$CTRL" >/dev/null
ING_DEST_URI="mysql://root:bench@127.0.0.1:${ING_PORT}/${ING_DB}" \
CAP="--cpus=0.5" LEG_TIMEOUT=1800 bash "$H" ingleg ctrl-my >/dev/null
got_ing=$(MY_C=apitap-bench-cdc-my MY_DB="$ING_DB" \
          bash "$HERE/bench-capped-my-ch-validator.sh" src "$CTRL")
echo "INGESTR  $got_ing"
if [[ -n "$got_ing" && "$got_ing" == "$src_after" ]]; then echo "  GREEN the rival validates"; else
    echo "  *** RED: the rival does not validate under the same rules (or landed nothing)"
    echo "src: $src_after"; echo "dst: ${got_ing:-<nothing landed>}"
    grep -A12 '^LEG ingestr' "$WORK/results.txt" | tail -14
    exit 1
fi
# The landing can be right while the run is not: the receipt is the verdict on the
# process, the digest is the verdict on the data, and both are reported.
echo "  the leg's own receipt:"
grep -A12 '^LEG ingestr' "$WORK/results.txt" | tail -14 | sed 's/^/    /'
echo "  the landing's shape, as ingestr built it (its own CDC metadata columns are"
echo "  extra and are NOT read by the aggregate, which names the fifteen data columns):"
P "SELECT CONCAT('    ', GROUP_CONCAT(column_name ORDER BY ordinal_position))
   FROM information_schema.columns
   WHERE table_schema='$ING_DB' AND table_name='$CTRL'" | tr ',' '\n' | sed 's/^/    /'

echo
echo "== 4b. the campaign's ACTUAL destination: what ingestr does with ClickHouse =="
# Recorded here as well as in the report, because it is the single fact that decides
# the rival's arm and it should be re-readable without re-running the timed legs.
reset_dest
ING_DEST_URI="clickhouse://default:bench@127.0.0.1:9128?http_port=8128" \
CAP="--cpus=0.5" LEG_TIMEOUT=300 bash "$H" ingleg ctrl-ch >/dev/null
echo "  the refusal, verbatim from the leg's own log:"
grep -iE '^Error:|cannot safely run' "$WORK/logs/apitap-bench-cdc-ing-ctrl-ch.log" \
    | sed 's/^/    /'
echo "  the same refusal against a 3-column, 2-row table (so it is not about our"
echo "  schema): $(bash "$HERE/bench-capped-my-ch-cdc-probe-ingestr.sh" probe_mini 2>/dev/null \
    | grep -m1 'cannot safely run' | sed 's/^ *//')"

echo
echo "== 5. THE TYPE GATE: a native JSON column is refused, with the engine's own words =="
# This is a precondition of the campaign, not a footnote. apitap 0.57.0's MySQL CDC
# lane refuses a table whose JSON column is a NATIVE `json`, and the seed schema
# therefore stores the document in a longtext column instead. The refusal is a
# receipt here rather than a memory: without this leg the report would be quoting an
# error that no longer existed anywhere in the campaign's own logs, because every
# later run of the control overwrites the failing leg's log file.
P "DROP TABLE IF EXISTS ctrl_json" >/dev/null
P "CREATE TABLE ctrl_json LIKE bench_my_1m" >/dev/null
P "INSERT INTO ctrl_json SELECT * FROM bench_my_1m WHERE id <= 100" >/dev/null
echo "  $(P "SELECT CONCAT('the table under the gate declares json_val as: ', data_type)
              FROM information_schema.columns
              WHERE table_schema='bench' AND table_name='ctrl_json'
                AND column_name='json_val'")"
# In the `bench` database, because the harness's leg builds its own source URL against
# it — a control table in a side schema is "table not found" there, and the leg would
# print something that reads like a different failure entirely.
C "DROP TABLE IF EXISTS default.ctrl_json" >/dev/null
( export WORK="$WORK" TABLES_OVERRIDE=ctrl_json EXPECT_ROWS=100
  bash "$HERE/bench-capped-my-ch-cdc-0.57.sh" leg ctrl-json catchup 0 ) >/dev/null 2>&1
refusal=$(grep -h 'RAISED' "$WORK/logs/apitap-bench-cdc-ctrl-json.log" 2>/dev/null | head -1)
echo "  the refusal, verbatim from that leg's own log:"
printf '%s\n' "${refusal:-<the leg produced no RAISED line>}" | fold -s -w 96 | sed 's/^/    /'
if printf '%s' "$refusal" | grep -q "binary JSON encoding"; then
    echo "  GREEN the CDC lane refuses a native JSON column by name, and says why"
else
    echo "  *** RED: the native-JSON refusal did not reproduce — the campaign's schema"
    echo "     note about json_val being longtext would then be unexplained."
    P "DROP TABLE IF EXISTS ctrl_json" >/dev/null
    exit 1
fi
P "DROP TABLE IF EXISTS ctrl_json" >/dev/null

echo
echo "== cleanup: the destination tables go, the control table goes =="
reset_dest
P "DROP TABLE IF EXISTS $CTRL" >/dev/null
P "DROP TABLE IF EXISTS ${ING_DB}.${CTRL}" 2>/dev/null || true
echo "CONTROL GREEN x5"