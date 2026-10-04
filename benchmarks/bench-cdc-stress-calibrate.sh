#!/usr/bin/env bash
# Calibration for the capped-tier CDC STRESS test: the numbers that decide the
# whole design, measured on the rig rather than assumed.
#
#   1. WAL BYTES PER CHANGE, per statement kind, for the campaign's own row
#      shape — because "90,000,000 changes" is a STORAGE question before it is a
#      throughput question, and a box with 41 GB free cannot hold an unbounded WAL.
#   2. WAL bytes per change on COLD pages vs HOT pages — an UPDATE that needs a
#      full page image costs 8 KB, the same UPDATE on a page still resident in
#      shared_buffers costs ~300 B, and the difference decides the design.
#   3. WRITER THROUGHPUT, the same 1000-change transaction with 1 / 8 / 15 / 30
#      concurrent psql sessions — the offered rate is whatever the source can
#      actually make, and that has to be measured, never assumed.
#
# Everything runs on a SCRATCH table (cdc_cal, a clone of cdc_pg_t01) which is
# dropped at the end. The campaign's thirty seeded tables are never written to by
# this script, so their counts and digests stay exactly as recorded.
#
#   bash bench-cdc-stress-calibrate.sh            # all of it
#   bash bench-cdc-stress-calibrate.sh wal        # only 1 and 2
#   bash bench-cdc-stress-calibrate.sh tput       # only 3
set -uo pipefail

export PG_C=${PG_C:-apitap-bench-cdc-pg}
export PG_DB=${PG_DB:-bench}
CAL=cdc_cal
# The seed's own generator expressions (bench-capped-pg-ch-schema.sql), so a row
# this test inserts is indistinguishable from a seeded one and a checksum later
# compares like with like.
VALS="SELECT g, substr(md5(g::text),1,8),
         CASE WHEN g % 97 = 0 THEN NULL ELSE substr(md5((g*7)::text),1,40) END,
         substr(md5((g*13)::text),1,200), (g % 100)::smallint, g, g::bigint*1000000,
         (g % 10000)::double precision / 8,
         CASE WHEN g % 89 = 0 THEN NULL ELSE ((g % 1000000)::numeric(18,4)/100) END,
         (g % 2 = 0), date '2024-01-01' + (g % 900),
         timestamp '2024-01-01 00:00:00' + (g % 1000000) * interval '1 microsecond',
         timestamptz '2024-01-01 00:00:00+00' + (g % 1000000) * interval '1 microsecond',
         CASE WHEN g % 83 = 0 THEN NULL ELSE ('{\"k\":' || g || ',\"s\":\"' || substr(md5(g::text),1,16) || '\"}')::json END,
         CASE WHEN g % 79 = 0 THEN NULL ELSE 'extra-' || substr(md5((g*3)::text),1,60) END
  FROM generate_series(__A__, __B__) AS g"
# Substituted, never printf'd: the VALUES text carries '% 97', '% 89' and JSON
# braces, and both printf and str.format() read those as their own syntax — a
# measure() of 0 bytes and a syntax error at the same time.
insstmt() {
    local v="$VALS"
    v="${v//__A__/$1}"; v="${v//__B__/$2}"
    printf 'INSERT INTO %s (%s) %s;' "$CAL" "$COLS" "$v"
}
COLS="id, small_str, medium_str, large_str, tiny_int, regular_int, big_int,
    float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text"

pgq() { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "$1"; }

setup_cal() {
    echo "== the scratch table: a clone of cdc_pg_t01, 1,036,000 rows =="
    pgq "DROP TABLE IF EXISTS public.$CAL" >/dev/null
    pgq "CREATE TABLE public.$CAL (LIKE public.cdc_pg_t01 INCLUDING ALL)" >/dev/null
    pgq "INSERT INTO public.$CAL SELECT * FROM public.cdc_pg_t01" >/dev/null
    pgq "SELECT '  ' || count(*) || ' rows, ' || pg_size_pretty(pg_total_relation_size('public.$CAL'))
         FROM public.$CAL"
}

# Bytes of WAL one statement cost, from the LSN bracket around it, and the same
# figure per changed row. $1 = the statement(s), $2 = the changed-row count,
# $3 = a label.
#
# pg_wal_lsn_diff does the subtraction SERVER-side. Doing it in awk does not work
# and silently reports 0: an LSN's low half is HEX ("2/D90199A8"), and awk's
# string-to-number conversion of that is 0. The first run of this script read
# "0 B / 800 changes" for a statement that had plainly written WAL, and the only
# reason it looked like a data verdict was that zero divides without complaint.
measure() {
    local before after got
    before=$(pgq "SELECT pg_current_wal_lsn()")
    printf '%s\n' "$1" | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q -f - >/dev/null
    after=$(pgq "SELECT pg_current_wal_lsn()")
    got=$(pgq "SELECT pg_wal_lsn_diff('$after'::pg_lsn, '$before'::pg_lsn)")
    awk -v d="${got:-0}" -v n="$2" -v l="$3" 'BEGIN{
        printf "   %-34s %12d B  / %7d changes = %8.1f B/change\n", l, d, n, d/n }'
}

set_comp() {
    if [[ "$1" == off ]]; then
        pgq "ALTER SYSTEM RESET wal_compression" >/dev/null
    else
        pgq "ALTER SYSTEM SET wal_compression = '$1'" >/dev/null
    fi
    pgq "SELECT pg_reload_conf()" >/dev/null
    sleep 1.5
}

cmd_wal() {
    setup_cal
    local i ins del band
    echo
    echo "== source settings in force for this measurement =="
    pgq "SELECT '   ' || name || ' = ' || setting || coalesce(unit,'') FROM pg_settings
          WHERE name IN ('wal_level','wal_compression','full_page_writes','max_wal_size',
                         'min_wal_size','shared_buffers','checkpoint_timeout',
                         'synchronous_commit','max_slot_wal_keep_size') ORDER BY name"
    echo
    echo "== 1 + 2. WAL bytes per change, by compression setting and page temperature =="
    echo "   COLD = a band no transaction in this test has touched (full page images)."
    echo "   HOT  = the same band, still resident in shared_buffers (no FPI)."
    for mode in off pglz lz4 zstd; do
        set_comp "$mode"
        local got; got=$(pgq "SHOW wal_compression")
        [[ "$got" == "$mode" ]] || { echo "   wal_compression=$mode: NOT AVAILABLE (server reports '$got')"; continue; }
        ins=6000000; del=6100000; band=500000
        printf '   --- wal_compression = %s\n' "$mode"
        # 800 UPDATEs over a never-touched 800-row band (cold pages)
        measure "SET synchronous_commit=off;
UPDATE $CAL SET regular_int = regular_int + 1 WHERE id BETWEEN $band AND $((band+799));" \
                800 "UPDATE x800  COLD band"
        # the SAME statement again, the pages still hot
        measure "SET synchronous_commit=off;
UPDATE $CAL SET regular_int = regular_int + 1 WHERE id BETWEEN $band AND $((band+799));" \
                800 "UPDATE x800  HOT  band"
        # 100 INSERTs of fresh ids (never-touched pages -> FPI)
        measure "SET synchronous_commit=off; $(insstmt $ins $((ins+99)))" \
                100 "INSERT x100 COLD (new pages)"
        ins=$((ins+100))
        # 100 more INSERTs into the next 100 never-touched pages
        measure "SET synchronous_commit=off; $(insstmt $ins $((ins+99)))" \
                100 "INSERT x100 COLD (new pages)"
        ins=$((ins+100))
        # 100 DELETEs of rows inserted a moment ago (their pages are hot)
        measure "SET synchronous_commit=off;
DELETE FROM $CAL WHERE id BETWEEN $((ins-100)) AND $((ins-1));" \
                100 "DELETE x100 (recent inserts)"
        # 800 UPDATEs of a fresh 800-row band, twice: the first pays the FPI, the
        # second does not. The pair is the honest per-change cost of a cold band.
        band=$((band+100000))
        measure "SET synchronous_commit=off;
UPDATE $CAL SET regular_int = regular_int + 1 WHERE id BETWEEN $band AND $((band+799));
UPDATE $CAL SET regular_int = regular_int + 1 WHERE id BETWEEN $band AND $((band+799));" \
                1600 "UPDATE x800 x2, same COLD band"
        # 20 repeats of the hot statement, so the fixed cost of the transaction
        # does not show up as per-change cost
        local s=""
        for ((i = 0; i < 20; i++)); do
            s+="UPDATE $CAL SET regular_int = regular_int + 1 WHERE id BETWEEN $band AND $((band+799));"$'\n'
        done
        measure "SET synchronous_commit=off;
$s" 16000 "UPDATE x800 x20, same HOT band"
        ins=$((ins+2000))
    done
    set_comp off
    echo "   restored wal_compression = $(pgq 'SHOW wal_compression')"
    echo
    echo "== what a 90,000,000-change stream costs in WAL, from the HOT figures above =="
    echo "   (80% UPDATE / 10% INSERT / 10% DELETE, the mix the stress writer will use)"
    pgq "DROP TABLE IF EXISTS public.$CAL" >/dev/null
}

# One session's script: $1 = number of transactions, $2 = this session's id.
# Each transaction is EXACTLY 1000 changes — 800 UPDATEs of a FIXED 800-row band,
# 100 INSERTs of fresh ids, and 100 DELETEs of the PREVIOUS transaction's inserts,
# so the deletes always hit real rows and the row count never moves.
tput_script() {
    local ntx=$1 s=$2 band=$((100 + s * 1000)) base=$((6000000 + s * 400000)) i
    local ins prev=""
    for ((i = 0; i < ntx; i++)); do
        ins=$((base + i * 100))
        printf 'BEGIN;\nUPDATE %s SET regular_int = regular_int + 1 WHERE id BETWEEN %d AND %d;\n' \
               "$CAL" "$band" "$((band+799))"
        printf 'INSERT INTO %s (%s) %s;\n' "$CAL" "$COLS" "$(insstmt "$ins" "$((ins+99))")"
        [[ -n "$prev" ]] && printf 'DELETE FROM %s WHERE id BETWEEN %d AND %d;\n' "$CAL" "$prev" "$((prev+99))"
        printf 'COMMIT;\n'
        prev=$ins
    done
}

tput_round() {
    local sess=$1 ntx=$2 s
    local before after t0 t1 p pids=() rc=0
    before=$(pgq "SELECT pg_current_wal_lsn()")
    t0=$(date +%s.%N)
    for ((s = 0; s < sess; s++)); do
        # Each session gets its OWN script file so the sessions do not replay
        # each other's statements.
        tput_script "$ntx" "$s" > "/tmp/cal-tput-$s.sql"
        docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q -f - < "/tmp/cal-tput-$s.sql" >/dev/null 2>&1 &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || rc=1; done
    t1=$(date +%s.%N)
    after=$(pgq "SELECT pg_current_wal_lsn()")
    local d; d=$(pgq "SELECT pg_wal_lsn_diff('$after'::pg_lsn, '$before'::pg_lsn)")
    awk -v d="$d" -v t0="$t0" -v t1="$t1" -v s="$sess" -v n="$ntx" 'BEGIN{
        w = t1 - t0; ch = s * n * 1000;
        printf "   %2d sessions x %3d txns: %6.2f s  %9.0f changes/s  %8.0f/s/session  WAL %6.2f GB  %5.0f B/change\n",
               s, n, w, ch/w, ch/w/s, d/1073741824, d/ch }'
}

cmd_tput() {
    setup_cal
    pgq "ALTER SYSTEM SET wal_compression = 'lz4'" >/dev/null
    pgq "SELECT pg_reload_conf()" >/dev/null; sleep 1.5
    echo
    echo "== 3. writer throughput: the same 1000-change transaction, N concurrent psql sessions =="
    pgq "SELECT '   wal_compression=' || current_setting('wal_compression')
          || ' synchronous_commit=' || current_setting('synchronous_commit')
          || ' shared_buffers=' || current_setting('shared_buffers')"
    local n
    for n in 1 8 15 30; do tput_round "$n" 40; done
    echo "   each session owns its own fixed 800-row update band and its own id ranges,"
    echo "   so the sessions never collide — this is exactly the stress writer's shape"
    echo "   host: loadavg $(cut -d' ' -f1-3 /proc/loadavg)  MemAvailable=$(awk '/MemAvailable/{print $2}' /proc/meminfo) kB"
    set_comp off
}

cleanup() {
    set_comp off
    pgq "DROP TABLE IF EXISTS public.$CAL" >/dev/null
    echo
    echo "== the scratch table is gone; the campaign's thirty tables are untouched =="
    pgq "SELECT '  ' || count(*) || ' tables, ' || pg_size_pretty(sum(pg_total_relation_size(quote_ident(table_name))))
         FROM information_schema.tables WHERE table_schema='public'"
    pgq "SELECT '  rows: ' || sum(n) FROM (SELECT (xpath('/row/c/text()', query_to_xml(
            format('SELECT count(*) c FROM public.%I', table_name), false,true,'')))[1]::text::bigint AS n
          FROM information_schema.tables WHERE table_schema='public') x"
}

case "${1:-all}" in
    wal)  cmd_wal;  cleanup ;;
    tput) cmd_tput; cleanup ;;
    all)  cmd_wal; cmd_tput; cleanup ;;
    *) echo "usage: $0 [all|wal|tput]" >&2; exit 2 ;;
esac