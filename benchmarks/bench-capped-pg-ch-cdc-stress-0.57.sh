#!/usr/bin/env bash
# Capped-tier CDC STRESS: can a 0.5 CPU / 256 MB drain keep up with
# 1,000,000 CHANGED ROWS PER TABLE PER MINUTE across 30 tables in ONE CDC group?
#
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh start        # reset dest + graveyard, run the control
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh bootstrap    # establish the group over 30 tables, verify
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh calm         # WINDOW C: the correctness window
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh stress       # WINDOW S: the requested rate
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh prefix       # the applied-prefix check
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh wsleg        # walshadow 0.1.2, one leg
#   bash bench-capped-pg-ch-cdc-stress-0.57.sh leaks | state | disk
#
# The rig is this campaign's own: apitap-bench-cdc-pg (postgres:16-alpine,
# wal_level=logical, 5548) and apitap-bench-cdc-ch (clickhouse-server:24.8,
# 8128/9128). Nothing that existed before this campaign is touched, and the
# thirty seeded tables are never dropped.
set -uo pipefail

export PG_C=${PG_C:-apitap-bench-cdc-pg}
export CH_C=${CH_C:-apitap-bench-cdc-ch}
export PG_DB=${PG_DB:-bench}
PG_PORT=5548; CH_HTTP=8128; CH_NATIVE=9128
PG_URL="postgres://postgres:bench@127.0.0.1:${PG_PORT}/${PG_DB}"
CH_URL="clickhouse://default:bench@127.0.0.1:${CH_HTTP}/default"

CAP=${CAP:-"--cpus=0.5 --memory=256m --memory-swap=256m"}
SP_APITAP=/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages
IMG=python:3.13-slim
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/bench-cdc-stress}
NT=30
TABLES=(); for ((i = 1; i <= NT; i++)); do TABLES+=("$(printf 'cdc_pg_t%02d' "$i")"); done
mkdir -p "$WORK/logs" "$WORK/ledger" "$WORK/out"

# ── window parameters ─────────────────────────────────────────────────────────
# WINDOW C ("calm"): paced so the drain CAN converge inside the catch-up budget,
# which is what buys an exact checksum verdict on a change stream.
CALM_TXN=${CALM_TXN:-133}         # transactions per table
CALM_TICK=${CALM_TICK:-1.35}      # seconds between a session's transactions
CALM_BAND=${CALM_BAND:-952801}    # window C's own 800-row update band
# WINDOW S ("stress"): the rate the owner asked about — 1,000,000 changes per
# table per minute = 500,000 changes/s across 30 tables = 1,800 transactions
# per table in 180 s. Full tilt, unpaced; how long it actually runs is decided
# by PostgreSQL's own max_slot_wal_keep_size, not by this script.
STRESS_TXN=${STRESS_TXN:-1800}
STRESS_TICK=${STRESS_TICK:-0}
STRESS_BAND=${STRESS_BAND:-957601}  # window S's own band, above every band this
                                # campaign has already written to, so its ledger is the
                                # only one that describes it
SESSIONS=${SESSIONS:-15}          # measured ceiling: 15 psql sessions ≈ 315,000 changes/s
DRAIN_EVERY=${DRAIN_EVERY:-60}
CATCHUP_BUDGET=${CATCHUP_BUDGET:-2700}   # seconds, window C's convergence budget
STRESS_BUDGET=${STRESS_BUDGET:-1500}    # seconds, window S's total budget
SLICE=${SLICE:-30}                    # seconds, one drain container's wall budget
SAMPLE_EVERY=${SAMPLE_EVERY:-5}
# The two reasons the sampler stops the writer, and both are the box's, not the
# writer's: PostgreSQL's own max_slot_wal_keep_size invalidating the slot, and
# free disk. Neither is a pgrep of the writer's own name — the pids come from the
# file run_writer wrote.
WAL_FLOOR_GB=${WAL_FLOOR_GB:-20}         # absolute GB free; the box sits at 38

now() { date +%s.%N; }
pgq() { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "$1"; }
chq() { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1"; }
inlist() { printf "'%s'," "${TABLES[@]}"; }
host_state() {
    { printf '%s loadavg=%s memavail_kb=%s cached_kb=%s disk_avail=%s\n' "$1" \
        "$(cut -d' ' -f1-3 /proc/loadavg)" \
        "$(awk '/MemAvailable/{print $2}' /proc/meminfo)" \
        "$(awk '/^Cached:/{print $2}' /proc/meminfo)" \
        "$(df --output=avail -BG / | tail -1 | tr -d ' ')"
    } >> "$WORK/host-state.log"; tail -1 "$WORK/host-state.log"
}
# disk free in GB, as an integer, for the safety valve
disk_free_gb() { df --output=avail -BG / | tail -1 | tr -dc '0-9'; }

# ── the validator, the previous campaign's definitions, verbatim ───────────────
source "$HERE/bench-capped-pg-ch-validator.sh"
join_digest() {
    awk -v sep="$1" '{
        i = index($0, sep)
        if (i == 0) { print $0 "\t"; next }
        print substr($0, 1, i - 1) "\t" substr($0, i + length(sep))
    }'
}
src_digest_batch() {
    local t out=""
    for t in "${TABLES[@]}"; do
        [[ -n "$out" ]] && out+=" UNION ALL "
        out+="SELECT '$t' AS t, * FROM (${PG_AGG//@TBL@/public.\"$t\"}) AS q"
    done
    docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "$out" | join_digest '|'
}
ch_digest_batch() {
    local t out=""
    for t in "${TABLES[@]}"; do
        [[ -n "$out" ]] && out+=" UNION ALL "
        out+="SELECT '$t' AS t, * FROM (${CH_AGG//@TBL@/\`default\`.\`$t\`$(ch_final "$t")}) AS q"
    done
    docker exec -i "$CH_C" clickhouse-client --password bench -q "$out" | join_digest "$(printf '\t')"
}
watermarks() {
    chq "SELECT concat('lsn ', min(watermark), ' .. ', max(watermark),
                       ' distinct=', uniqExact(watermark), ' tables=', count())
          FROM (SELECT dest_table, argMax(watermark, synced_at) AS watermark
                FROM _apitap_state FINAL
                WHERE startsWith(dest_table, 'cdc_pg_t') GROUP BY dest_table)"
}
slot_info() {
    pgq "SELECT coalesce(string_agg(slot_name || ' active=' || active || ' ' ||
                 coalesce(wal_status,'-') || ' retained=' ||
                 pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), ', '), '(none)')
          FROM pg_replication_slots"
}
ch_rows_total() {
    chq "SELECT toString(coalesce(sum(total_rows),0)) FROM system.tables
          WHERE database='default' AND name IN ($(inlist))"
}

# ── destination hygiene: an EMPTY destination at the start, dropped at the end ──
# SYNC matters: ClickHouse keeps a dropped table's data on disk for eight minutes
# without it, and the previous campaign measured 42 GB -> 9.5 GB free across
# three consecutive 8 GB landings.
cmd_drop() {
    chq "SELECT concat('DROP TABLE IF EXISTS ', database, '.', name, ' SYNC;')
          FROM system.tables
          WHERE name IN ($(inlist)) OR startsWith(name, 'cdc_ctrl')
             OR position(name, '__apitap') > 0 OR position(name, '__ws') > 0
          FORMAT TSVRaw" \
        | docker exec -i "$CH_C" clickhouse-client --password bench --multiquery 2>/dev/null
    chq "DELETE FROM _apitap_state
          WHERE startsWith(dest_table, 'cdc_pg_t') OR startsWith(dest_table, 'cdc_ctrl')" 2>/dev/null
    echo -n "  leftover destination tables: "
    chq "SELECT count() FROM system.tables
          WHERE name IN ($(inlist)) OR startsWith(name, 'cdc_ctrl')
             OR position(name, '__apitap') > 0 OR position(name, '__ws') > 0"
}
# Only THIS campaign's own apitap_* names: a blanket "drop every apitap_* slot"
# is a grenade on a shared rig (benchmarks/cdc-test-traps.md #9).
cmd_slotdrop() {
    local s
    for s in $(pgq "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap\_%'"); do
        pgq "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true
    done
    for s in $(pgq "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap\_%'"); do
        pgq "DROP PUBLICATION IF EXISTS $s" >/dev/null 2>&1 || true
    done
    echo "  slots left: $(pgq "SELECT coalesce(string_agg(slot_name, ','), '(none)') FROM pg_replication_slots")"
}

# ── one apitap leg: ONE process, ONE group over 30 tables, ONE slot ───────────
# No knobs: no parallel=, no chunk_bytes, no slots=, no APITAP_* env. The wheel
# is bind-mounted read-only and put on PYTHONPATH, so the PyPI wheel is what runs
# and nothing is built. mode=catchup is the bootstrap, mode=drain everything the
# slot has accumulated since the last drain.
cmd_leg() {
    local tag=$1 mode=${2:-catchup} res=${3:-$WORK/results.txt}
    # Two `local` statements, not one: bash expands every word of a `local`
    # before assigning any of them, so `local name=x log=$WORK/logs/${name}.log`
    # reads `name` UNBOUND — which under `set -u` killed the first bootstrap.
    local name="apitap-bench-cdc-stress-$tag"
    local log="$WORK/logs/${name}.log"
    local t0 t1 wall state mempeak rc
    docker rm -f "$name" >/dev/null 2>&1
    : > "$log"; host_state "before $name"
    t0=$(now)
    docker run --name "$name" --network=host $CAP \
        -v "$SP_APITAP:/py:ro" -e PYTHONPATH=/py -v "$HERE:/job:ro" \
        -e "APITAP_TABLES=${TABLES[*]}" -e "APITAP_SRC=$PG_URL" -e "APITAP_DST=$CH_URL" \
        -e "APITAP_MODE=$mode" -e "APITAP_RUN_TWICE=0" \
        "$IMG" sh /job/bench-capped-pg-ch-cdc-leg-apitap.sh >"$log" 2>&1
    rc=$?; t1=$(now)
    state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    mempeak=$(grep -o 'MEMPEAK=[0-9]*' "$log" | tail -1 | cut -d= -f2)
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
    docker rm -f "$name" >/dev/null 2>&1
    host_state "after  $name"
    {
        printf 'LEG apitap tag=%s mode=%s\n' "$tag" "$mode"
        printf '  stopped_by %s\n' "$([[ $rc -eq 0 ]] && echo completed || echo "exit_${rc}")"
        printf '  wall_container_s %s\n' "$wall"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' \
            "$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')"
        grep -E '^(MEMPEAK|MEMSTAT|MEMEVENTS|DRAIN|ELAPSED|IMPORT_S|APITAP_|FDCOUNT|FD_PEAK|FD_END|RAISED|  table |EXITCODE|  per_table)' \
            "$log" | sed 's/^/  /'
    } | tee -a "$res"
    return 0
}

# ── verification: per table COUNT + an order-independent checksum ─────────────
# 30 aggregates in ONE query per engine, so each server spreads the scan over its
# own cores instead of paying 30 sequential round trips.
cmd_verify() {
    local fresh=${1:-} srcfile="$WORK/src.txt" chfile="$WORK/dst.txt"
    local t ok=0 bad=0 s d body=""
    local okc badc
    [[ -n "$fresh" ]] && { src_digest_batch | sort > "$srcfile"; } || \
        src_digest_batch | sort > "$srcfile"
    local nsrc ndig
    nsrc=$(wc -l < "$srcfile")
    ndig=$(awk -F'\t' 'length($2) == 0 { n++ } END { print n+0 }' "$srcfile")
    if [[ "$nsrc" != "$NT" || "$ndig" != 0 ]]; then
        echo "VERIFY_BROKEN the source side is malformed: $nsrc lines (want $NT), $ndig with an empty digest"
        head -2 "$srcfile" | cat -A | sed 's/^/    /'; return 2
    fi
    ch_digest_batch 2>/dev/null | sort > "$chfile"
    if (( $(wc -l < "$chfile") < NT )); then
        for t in "${TABLES[@]}"; do
            grep -q "^$t	" "$chfile" && continue
            d=$(chsum "$t"); [[ -n "$d" ]] && printf '%s\t%s\n' "$t" "$d" >> "$chfile"
        done
        sort -o "$chfile" "$chfile"
    fi
    while IFS=$'\t' read -r t d; do
        s=$(awk -F'\t' -v k="$t" '$1==k{print $2}' "$srcfile")
        if [[ -n "$d" && "$s" == "$d" ]]; then
            body+="$(printf 'VERIFY %s rows=%s MATCH %s' "$t" "${d%%|*}" "$d")"$'\n'; ok=$((ok+1))
        else
            body+="$(printf 'VERIFY %s MISMATCH\n  src=%s\n  dst=%s' "$t" "$s" "${d:-<absent>}")"$'\n'; bad=$((bad+1))
        fi
    done < "$chfile"
    printf '%s' "$body" | tee -a "$WORK/verify.log"
    echo "VERIFY_SUMMARY checked=$((ok+bad)) of $NT match=$ok mismatch=$bad"
    if (( ok + bad < NT )); then
        echo "VERIFY_INCOMPLETE $(( NT - ok - bad )) of $NT tables are not in the destination at all — THIS IS NOT A PASS"
        while IFS=$'\t' read -r t _; do
            grep -q "^$t	" "$chfile" || echo "VERIFY_ABSENT $t"
        done < "$srcfile"
        return 1
    fi
    chq "SELECT concat('TOTAL_ROWS ', sum(total_rows)) FROM system.tables
          WHERE database='default' AND name IN ($(inlist))"
    [[ $bad -eq 0 ]]
}

# ── the sampler: the backlog curve, at a fixed cadence ────────────────────────
# One CSV row every SAMPLE_EVERY seconds. The deliverable of this campaign is
# this curve: how many bytes of WAL the slot is holding, how the group's
# watermark is moving, how much has landed in the destination, what the capped
# container's cgroup is holding, and how much disk is left. Started and stopped
# by ARTIFACT (the file), never by pgrep of its own name.
cmd_sampler() {
    local csv=$1 stopfile=$2 name=${3:-apitap-bench-cdc-stress-drain}
    printf 'epoch,t_rel,slot_retained_bytes,slot_wal_status,slot_active,wm_min,wm_max,wm_distinct,ch_rows_total,mem_current_bytes,mem_peak_bytes,disk_free_gb,pg_wal_lsn\n' > "$csv"
    local t0; t0=$(now)
    while [[ ! -e "$stopfile" ]]; do
        local cg id slot wm rows memc memp lsn dfree rel
        slot=$(pgq "SELECT coalesce(string_agg(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::text ||
                        '|' || coalesce(wal_status,'-') || '|' || active, ','), '|none|false')
                FROM pg_replication_slots WHERE slot_name LIKE 'apitap\_%'" 2>/dev/null)
        IFS='|' read -r _ret _ws _act <<< "${slot:-|||none|false}"
        _ret=$(cut -d, -f1 <<< "${_ret:-0}"); _ws=$(cut -d, -f2 <<< "${_ws:--}"); _act=$(cut -d, -f3 <<< "${_act:-false}")
        wm=$(watermarks 2>/dev/null)
        rows=$(ch_rows_total 2>/dev/null)
        local dn="$name"
        [[ -n "$dn" ]] || dn=$(docker ps --filter 'name=apitap-bench-cdc-stress-drain' \
                             --format '{{.Names}}' | grep -v '^$' | head -1)
        id=$(docker inspect -f '{{.Id}}' "$dn" 2>/dev/null || true)
        memc=0; memp=0
        if [[ -n "$id" ]]; then
            cg="/sys/fs/cgroup/${id}"
            memc=$(cat "$cg/memory.current" 2>/dev/null || echo 0)
            memp=$(cat "$cg/memory.peak" 2>/dev/null || echo 0)
        fi
        lsn=$(pgq "SELECT pg_current_wal_lsn()" 2>/dev/null)
        dfree=$(disk_free_gb)
        rel=$(awk -v a="$t0" -v b="$(now)" 'BEGIN{printf "%.1f", b-a}')
        # the watermark line is `lsn A .. B distinct=N tables=M`
        local wmn wmx wmd
        wmn=$(sed -n 's/.*lsn \([0-9]*\) .*/\1/p' <<< "$wm"); wmn=${wmn:-0}
        wmx=$(sed -n 's/.*\.\. \([0-9]*\) distinct.*/\1/p' <<< "$wm"); wmx=${wmx:-0}
        wmd=$(sed -n 's/.*distinct=\([0-9]*\).*/\1/p' <<< "$wm"); wmd=${wmd:-0}
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$(date -u +%H:%M:%S)" "$rel" "${_ret:-0}" "${_ws:--}" "${_act:-false}" \
            "$wmn" "$wmx" "$wmd" "${rows:-0}" "$memc" "$memp" "$dfree" "${lsn:-}" >> "$csv"
        # The safety valve, acted on rather than described. PostgreSQL refusing to
        # keep the slot is the finding; letting the writer keep producing after it
        # would add nothing to it and would fill a box that is already at 89%.
        if [[ -s "$WORK/ledger/${4:-none}.pids" && ( "$_ws" == "lost" || "$dfree" -lt "${WAL_FLOOR_GB:-20}" ) ]]; then
            printf '%s,WRITER_STOPPED slot_wal_status=%s disk_free_gb=%s\n' \
                "$(date -u +%H:%M:%S)" "$_ws" "$dfree" >> "$csv"
            echo "$WORK/ledger/${4:-none}.STOPPED" > "$WORK/ledger/${4:-none}.stopped"
            while read -r p; do kill "$p" 2>/dev/null; done < "$WORK/ledger/${4:-none}.pids"
            pgq "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
                  WHERE application_name LIKE 'apitap-stress-${4}-%'" >/dev/null 2>&1
        fi
        # The drain container being gone is NOT a reason to stop the writer: between
        # slices there is deliberately no drain container, and a reader that treated
        # the gap as a death stopped the writer 12 s into the first run and cost the
        # whole sequence. The writer's own clock is the authority.
        if ! docker ps --format '{{.Names}}' | grep -q "^$dn$" 2>/dev/null && [[ -n "$dn" ]]; then
            printf '%s,DRAIN_SLOT_EMPTY container=%s\n' "$(date -u +%H:%M:%S)" "${dn:-none}" >> "$csv"
        fi
        sleep "$SAMPLE_EVERY"
    done
}

# ── the writer ────────────────────────────────────────────────────────────────
# $1 = window label, $2 = transactions per table, $3 = tick seconds.
# SESSIONS psql sessions over disjoint table sets, each session's statement file
# generated on the host and piped into one psql session in the source container.
# Progress is an artifact (the LEDGER / WINDOW_PROGRESS / WRITER_DONE lines in
# each session's own log), never a pgrep of the pipeline.
# WRITER_BG=1 starts the sessions and returns; the caller drives the drains and
# calls writer_wait. WINDOW S needs that, and the reason is a fact about the
# product that cost this campaign a run: `apitap.transfer(mode="log_based")` is a
# BOUNDED read — it consumes the WAL that exists when it starts and returns. It
# is not a follower. Starting the drain first and generating afterwards therefore
# yields a drain that reports 0 changes and exits in under six seconds, with the
# whole change stream stranded in the slot.
run_writer() {
    local label=$1 ntxn=$2 tick=$3 band=${4:-950001}
    local per=$(( NT / SESSIONS )) s t ts i
    local tbls
    rm -f "$WORK/ledger/${label}"-s*.log
    local pids=() t0 t1
    t0=$(now)
    for ((s = 0; s < SESSIONS; s++)); do
        ts=""
        for ((i = s * per + 1; i <= (s + 1) * per; i++)); do ts+="${i},"; done
        python3 "$HERE/bench-capped-pg-ch-cdc-stress-writer.py" "$s" "${ts%,}" "$ntxn" "$tick" "$band" \
            > "$WORK/out/${label}-s${s}.sql"
        # The application_name is how the safety valve stops the writer. Killing
        # the pipeline's PID does not: `docker exec -i ... psql` dies, the pipe
        # closes, and the psql INSIDE the container keeps committing — which is how
        # one run recorded 5,114 ledger lines for 7,375 committed transactions and
        # left the reconstruction unable to describe its own band.
        { echo "SET application_name = 'apitap-stress-${label}-s${s}';"
          cat "$WORK/out/${label}-s${s}.sql"
          echo "\\echo"
        } | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q -f - \
            > "$WORK/ledger/${label}-s${s}.log" 2>&1 &
        pids+=($!)
    done
    # Confirm by ARTIFACT immediately: the writer's own progress lines. A pgrep of
    # the pipeline would match the check itself (benchmarks/cdc-test-traps.md).
    sleep 5
    echo "  writer: $SESSIONS sessions x $ntxn transactions x $per tables = \
$(( ntxn * per * SESSIONS * 1000 )) changes offered"
    echo "  writer progress lines after 5 s: $(cat "$WORK/ledger/${label}"-s*.log 2>/dev/null | grep -c WINDOW_PROGRESS)"
    printf '%s\n' "${pids[@]}" > "$WORK/ledger/${label}.pids"
    WRITER_T0=$t0
    if [[ -n "${WRITER_BG:-}" ]]; then
        echo "  writer is running in the background; drains drive while it writes"
        return 0
    fi
    writer_wait "$label"
    local done_ch done_tx
    done_tx=$(cat "$WORK/ledger/${label}"-s*.log | grep -c '^LEDGER ' || true)
    done_ch=$(cat "$WORK/ledger/${label}"-s*.log | grep '^WRITER_DONE' | head -1)
    echo "  writer finished in $(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}') s"
    echo "  committed transactions (ledger lines): $done_tx  -> $(( done_tx * 1000 )) changes"
    grep -h '^WRITER_DONE' "$WORK/ledger/${label}"-s*.log | sed 's/^/  /' | head -3
    printf 'WRITER label=%s sessions=%s ntxn=%s tick=%s band_base=%s wall_s=%s committed_txns=%s changes=%s\n' \
        "$label" "$SESSIONS" "$ntxn" "$tick" "$band" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')" "$done_tx" "$(( done_tx * 1000 ))" \
        | tee -a "$WORK/results.txt"
}

# The backgrounded writer's wall time and achieved rate, once it is finished.
writer_wait() {
    local label=$1 p
    for p in $(cat "$WORK/ledger/${label}.pids"); do wait "$p" 2>/dev/null; done
    local t1; t1=$(now)
    local done_tx done_ch
    done_tx=$(cat "$WORK/ledger/${label}"-s*.log 2>/dev/null | grep -c '^LEDGER ' || true)
    done_ch=$(( done_tx * 1000 ))
    echo "  writer finished in $(awk -v a="$WRITER_T0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}') s"
    echo "  committed transactions (ledger lines): $done_tx  -> $done_ch changes"
    grep -h '^WRITER_DONE' "$WORK/ledger/${label}"-s*.log 2>/dev/null | sort -u | head -2 | sed 's/^/  /'
    printf 'WRITER label=%s committed_txns=%s changes=%s wall_s=%s changes_per_s=%s changes_per_min_per_table=%s\n' \
        "$label" "$done_tx" "$done_ch" \
        "$(awk -v a="$WRITER_T0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')" \
        "$(awk -v c="$done_ch" -v a="$WRITER_T0" -v b="$t1" 'BEGIN{printf "%.0f", c/(b-a)}')" \
        "$(awk -v c="$done_ch" -v a="$WRITER_T0" -v b="$t1" 'BEGIN{printf "%.0f", c*60/(b-a)/30}')" \
        | tee -a "$WORK/results.txt"
    WRITER_CHANGES=$done_ch
}

# ── one BOUNDED drain slice: a container, a wall budget, then a graceful stop ──
# SIGTERM is apitap's own graceful-stop path (APITAP_GRACEFUL_STOP is on unless
# set to 0), and the applied windows and the group's watermark are durable across
# it. That claim is MEASURED here, not asserted: the slice's own DRAIN/RAISED
# lines, the monotonic watermark in the curve, and WINDOW C's convergence on two
# drains are the evidence.
cmd_drain_slice() {
    local n=$1 slice=$2 res=$3
    local name="apitap-bench-cdc-stress-drain$n"
    local log="$WORK/logs/${name}.log" t0 t1 wall state mempeak stopped rc=0
    docker rm -f "$name" >/dev/null 2>&1; : > "$log"
    t0=$(date +%s.%N)
    docker run -d --name "$name" --network=host $CAP \
        -v "$SP_APITAP:/py:ro" -e PYTHONPATH=/py -v "$HERE:/job:ro" \
        -e "APITAP_TABLES=${TABLES[*]}" -e "APITAP_SRC=$PG_URL" -e "APITAP_DST=$CH_URL" \
        -e "APITAP_MODE=drain" -e "APITAP_RUN_TWICE=0" \
        "$IMG" sh /job/bench-capped-pg-ch-cdc-leg-apitap.sh >/dev/null 2>&1
    sleep "$slice"
    if docker ps --format '{{.Names}}' | grep -qx "$name"; then
        stopped="slice_wall_reached_sigterm"
        docker stop -t 180 "$name" >/dev/null 2>&1 || rc=1
    else
        stopped="returned_on_its_own"
    fi
    t1=$(date +%s.%N)
    docker logs "$name" > "$log" 2>&1 || true
    state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    mempeak=$(cat "/sys/fs/cgroup/$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null)/memory.peak" 2>/dev/null || echo 0)
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
    local applied
    applied=$(grep -h '^DRAIN ' "$log" 2>/dev/null | tail -1 | sed -n 's/.*rows=\([0-9]*\).*/\1/p')
    local raised; raised=$(grep -c '^RAISED' "$log" 2>/dev/null || echo 0)
    {
        printf 'LEG apitap tag=slice%s mode=drain\n' "$n"
        printf '  stopped_by %s\n' "$stopped"
        printf '  wall_container_s %s (slice budget %ss)\n' "$wall" "$slice"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' "$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')"
        printf '  changes_applied %s\n' "${applied:-<the drain raised before reporting>}"
        printf '  raised %s\n' "$raised"
        grep -E '^(APITAP_|MEMPEAK|MEMEVENTS|MEMSTAT|DRAIN|ELAPSED|RAISED|FD_PEAK|FD_END|  table |EXITCODE)' "$log" | grep -v per_table_rows | sed 's/^/  /'
    } | tee -a "$res"
    SLICE_APPLIED=${applied:-0}
    SLICE_RAISED=$raised
    docker rm -f "$name" >/dev/null 2>&1
    return 0
}

# k_at(W) per table: the number of this window's transactions of table t whose
# commit LSN is <= W. Straight out of the ledger, which is why the ledger exists.
# $1 = window label, $2 = watermark LSN. Prints `table k` lines.
k_at() {
    python3 - "$WORK/ledger" "$1" "$2" <<'PY'
import os, re, sys
ledger_dir, label, w = sys.argv[1], sys.argv[2], int(sys.argv[3])
pat = re.compile(r"^LEDGER\s+s\d+\s+(cdc_pg_t\d+)\s+(\d+)\s+([0-9A-F]+/[0-9A-F]+)\s*$")
def lsn_int(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)
best = {}
for name in sorted(os.listdir(ledger_dir)):
    if not name.startswith(label + "-s"):
        continue
    for line in open(os.path.join(ledger_dir, name), errors="replace"):
        m = pat.match(line.strip())
        if not m:
            continue
        t, k, l = m.group(1), int(m.group(2)), lsn_int(m.group(3))
        if lsn_int(l) <= w and k > best.get(t, 0):
            best[t] = k
for t in sorted(best):
    print(f"{t} {best[t]}")
PY
}

# ── start: an EMPTY destination, the graveyard block, and the control ─────────
cmd_start() {
    echo "== the state this campaign found, recorded before anything is touched =="
    echo "   the previous campaign's five-minute window ran ONTO these tables, so"
    echo "   they are NOT at 1,000,000 rows each and no assumption is made about it:"
    pgq "SELECT concat('   ', count(*), ' tables, ', pg_size_pretty(sum(pg_total_relation_size(quote_ident(table_name)))),
                 ' total, every table at ', min(n), '..', max(n), ' rows')
          FROM (SELECT (xpath('/row/c/text()', query_to_xml(
                    format('SELECT count(*) c FROM public.%I', table_name), false,true,'')))[1]::text::bigint AS n,
                       pg_total_relation_size(quote_ident(table_name)) AS b
                FROM information_schema.tables WHERE table_schema='public') x"
    pgq "SELECT concat('   id range over t01: ', min(id), '..', max(id),
                       '   (the previous window inserted at 2,000,000 and deleted 900,001..909,000)')
          FROM public.cdc_pg_t01"
    pgq "SELECT concat('   all tables: min(id)=', min(lo), '..', max(hi))
          FROM (SELECT table_name,
                       (xpath('/row/m/text()', query_to_xml(
                          format('SELECT min(id) m, max(id) FROM public.%I', table_name),
                          false,true,'')))[1]::text::bigint AS lo
                FROM information_schema.tables WHERE table_schema='public') x"
    echo "== the destination, emptied =="
    cmd_drop
    cmd_slotdrop
    echo "== the graveyard block: 100 rows per table, this campaign's own, =="
    echo "   inserted BEFORE the CDC group exists so a window's first transaction"
    echo "   has real rows to delete and the window is row-neutral from txn 0 =="
    local t
    for t in "${TABLES[@]}"; do
        # `${t##*_}` leaves "t01", and `10#t01` is "value too great for base" —
        # which printed thirty times and left every table at its old row count,
        # reading exactly like a seed that had not taken the graveyard.
        python3 "$HERE/bench-capped-pg-ch-cdc-stress-writer.py" 0 "${t##*t}" 0 0 950001 graveyard \
            | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q >/dev/null
    done
    pgq "SELECT concat('   ', count(*), ' tables at ', min(n), ' rows (was 1,036,000)')
          FROM (SELECT (xpath('/row/c/text()', query_to_xml(
                format('SELECT count(*) c FROM public.%I', table_name), false,true,'')))[1]::text::bigint AS n
              FROM information_schema.tables WHERE table_schema='public') x"
    echo "   the graveyard block lands at ids 40,100,000.. on t01 — inside the checksum,"
    echo "   so a digest that ever disagrees names them rather than hiding in a delta"
    pgq "SELECT concat('   t01 graveyard rows: ', count(*), ' at ids ', min(id), '..', max(id))
          FROM public.cdc_pg_t01 WHERE id >= 40000000"
    pgq "SELECT '   ' || pg_size_pretty(sum(pg_total_relation_size(quote_ident(table_name))))
          FROM information_schema.tables WHERE table_schema='public'"
    echo "   (those rows are inside the checksum, so a mismatch localises them)"
    echo "== the control: GREEN x4 or the campaign does not start =="
    WORK="$HOME/bench-cdc-ctrl-stress" bash "$HERE/bench-capped-pg-ch-cdc-control.sh" \
        2>&1 | tee "$WORK/logs/control.log" | grep -E "GREEN|RED|CONTROL|SOURCE|APITAP |WALSHADOW|==|empty" | head -60
    echo "== after the control =="
    # the control's walshadow leg leaves its own physical slot behind; it is that
    # leg's artifact and no daemon is running, so it goes before the campaign's
    # own slot is established
    pgq "SELECT pg_drop_replication_slot('walshadow')" >/dev/null 2>&1 || true
    pgq "SELECT coalesce(string_agg(slot_name, ','), '(none)') FROM pg_replication_slots" | sed 's/^/   slots: /'
    pgq "SELECT coalesce(string_agg(pubname, ','), '(none)') FROM pg_publication" | sed 's/^/   publications: /'
    cmd_drop >/dev/null; cmd_slotdrop >/dev/null
    echo "== the prefix reconstruction's CONTROL, before any window has run =="
    echo "   the source state now, versus the same rows rebuilt as 'k transactions'"
    echo "   committed' — with k=0 they must be the identical digest =="
    : > "$WORK/k0.txt"
    for t in "${TABLES[@]}"; do
        python3 "$HERE/bench-cdc-stress-prefix.py" --control "$t" \
            | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -At -f - >> "$WORK/k0.txt"
    done
    src_digest_batch | sort > "$WORK/k0-plain.txt"
    sort -o "$WORK/k0.txt" "$WORK/k0.txt"
    local okc=0 badc=0 s d
    while IFS=$'\t' read -r t d; do
        s=$(awk -F'\t' -v k="$t" '$1==k{print $2}' "$WORK/k0-plain.txt")
        [[ "$s" == "$d" ]] && okc=$((okc+1)) || { badc=$((badc+1)); echo "   CONTROL MISMATCH $t"; }
    done < "$WORK/k0.txt"
    echo "   RECONSTRUCTION_CONTROL checked=$((okc+badc)) of $NT match=$okc mismatch=$badc"
    [[ $badc -eq 0 ]] || { echo "   RECONSTRUCTION CONTROL FAILED — the campaign stops here"; return 1; }
    host_state "after start"
}

# ── bootstrap: establish the group over 30 tables in ONE slot, then verify ────
cmd_bootstrap() {
    echo "== EMPTY destination, pre-seeded source: ONE transfer over ONE group =="
    cmd_drop >/dev/null; cmd_slotdrop >/dev/null
    cmd_leg bootstrap catchup "$WORK/results.txt"
    echo
    echo "== the bootstrap's landing, checksum-verified BEFORE any change stream =="
    cmd_verify || { echo "BOOTSTRAP ABORTED: the landing does not match the source"; return 1; }
    echo "  watermark: $(watermarks)"
    echo "  slot:      $(slot_info)"
    host_state "after bootstrap"
    df -h / | tail -1 | sed 's/^/  disk /'
}

# ── WINDOW C: the correctness window ─────────────────────────────────────────
# Paced so the drain converges inside the budget, which is what buys an exact
# checksum verdict on a real change stream rather than on the bootstrap alone.
cmd_calm() {
    local i res="$WORK/calm-results.txt" win0 win1
    : > "$res"; : > "$WORK/calm.log"
    echo "== WINDOW C: ${CALM_TXN} transactions per table x 1000 changes, ${CALM_TICK}s apart =="
    # each session runs CALM_TXN iterations spaced CALM_TICK apart, so the wall is
    # the same for one session as for fifteen — dividing by SESSIONS here printed
    # a window 15x shorter than the one that ran.
    # CALM_TICK is a float (1.35 s), so the arithmetic is done in awk; $(( ... ))
    # dies on "1.35" with "invalid arithmetic operator", which reads like a typo in
    # the window definition and is actually just bash.
    awk -v n="$(( CALM_TXN * 1000 * NT ))" -v t="$CALM_TXN" -v k="$CALM_TICK" 'BEGIN{
        printf "   offered: %d changes over %.1fs = %.0f changes/s = %.0f changes/minute
",
               n, t * k, n / (t * k), n * 60 / (t * k)}' 
    echo "   the drain runs on a ${DRAIN_EVERY}s cadence DURING the window =="
    win0=$(now)
    run_writer calm "$CALM_TXN" "$CALM_TICK" "$CALM_BAND" 2>&1 | tee -a "$WORK/calm.log"
    local d1 d2
    d1=$(ch_rows_total)
    echo "  watermark after generation: $(watermarks)"
    # drain to convergence, on the cadence, with the budget as the deadline
    local deadline=$(( $(date +%s) + CATCHUP_BUDGET )) d=0
    while (( $(date +%s) < deadline )); do
        d=$((d + 1))
        cmd_leg "calm-drain$d" drain "$res" 2>&1 | tee -a "$WORK/calm.log"
        local ch; ch=$(cat "$WORK/ledger/calm"-s*.log | grep -c '^LEDGER ' || true)
        {
            echo "  drain $d: watermark $(watermarks)   slot $(slot_info)   ch_rows $(ch_rows_total) (was $d1)"
        } | tee -a "$WORK/calm.log"
        # The raw container log's line has no indent; the indented one is the
        # results file, and grepping the wrong file reports "applied 0" for every
        # drain and declares convergence after the first one.
        local applied
        applied=$(grep -h '^DRAIN ' "$WORK/logs/apitap-bench-cdc-stress-calm-drain${d}.log" 2>/dev/null | tail -1 | sed -n 's/.*rows=\([0-9]*\).*/\1/p')
        echo "  (drain $d applied ${applied:-<no DRAIN line>} changes)"
        # converged when a drain applies nothing: the slot has no WAL left
        [[ "${applied:-1}" == "0" ]] && { echo "  CONVERGED after drain $d (applied 0 changes)"; break; }
        sleep "$DRAIN_EVERY"
    done
    win1=$(now)
    echo "== WINDOW C total wall $(awk -v a="$win0" -v b="$win1" 'BEGIN{printf "%.1f", b-a}')s =="
    echo "== and the destination against the source, FRESH checksums =="
    cmd_verify 2>&1 | tee -a "$WORK/calm.log"
    echo "  watermark: $(watermarks)"
    echo "  slot:      $(slot_info)"
    host_state "after calm"
    df -h / | tail -1 | sed 's/^/  disk /'
}

# ── WINDOW S: the rate the owner asked about ─────────────────────────────────
# Full tilt on the source, the drain capped at 0.5 CPU / 256 MB, and the
# sampler running throughout. How long it lasts is decided by PostgreSQL's own
# max_slot_wal_keep_size: past it the slot is invalidated and the drain's next
# read is refused. That refusal, verbatim, is the ceiling behaviour.
cmd_stress() {
    local res="$WORK/stress-results.txt"
    local stopfile="$WORK/out/sample.stop" csv="$WORK/stress-backlog.csv"
    local n=0 pid deadline
    : > "$res"; rm -f "$stopfile" "$WORK/ledger/stress.stopped"
    echo "== WINDOW S: the requested rate =="
    echo "   target 1,000,000 changes per table per minute = $(( 1000000 * NT / 60 )) changes/s"
    awk -v n="$STRESS_TXN" -v nt="$NT" \
        'BEGIN { printf "   = %d transactions per table x 1000 changes x %d tables = %d changes offered unpaced; mix 800 UPDATE + 100 INSERT + 100 DELETE\n", n, nt, n*1000*nt }'
    echo "   the source is UNBOUNDED; only the drain is capped at $CAP"
    echo "   the drain runs as a sequence of ${SLICE}s slices, each its own container,"
    echo "   because apitap's CDC transfer is a bounded read-until-end-of-WAL and not a"
    echo "   follower: starting one before the writer produces anything returns 0 in 6 s."
    # THE GUARD. WINDOW S runs on the group WINDOW C established, so it inherits
    # C's replication slot; a `slotdrop` between the two leaves the destination
    # holding a watermark whose slot no longer exists, the drain container exits
    # in two seconds, and every change after that is un-drainable. That happened
    # once — the writer was stopped 13 s in having produced 4,140,000 changes and
    # the whole sequence had to be re-bootstrapped — so the state is asserted.
    local nslot nwm
    nslot=$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'apitap\_%'")
    nwm=$(chq "SELECT count() FROM _apitap_state FINAL WHERE startsWith(dest_table, 'cdc_pg_t')")
    if [[ "${nslot:-0}" -lt 1 && "${nwm:-0}" -gt 0 ]]; then
        echo "STRESS ABORTED: the destination holds $nwm table watermarks and there is no"
        echo "  apitap replication slot. The drain would exit in seconds and every change"
        echo "  after that point would be un-drainable. Run 'bootstrap' (or 'calm') first."
        return 1
    fi
    echo "  preconditions: apitap slots=$nslot, destination watermark rows=$nwm"
    host_state "before stress"
    echo "  disk free before: $(disk_free_gb) GB   slot: $(slot_info)"
    local wlsn0 wlsn1
    STRESS_T0=$(date +%s)
    wlsn0=$(pgq "SELECT pg_current_wal_lsn()")

    cmd_sampler "$csv" "$stopfile" "" stress & pid=$!
    sleep 1
    WRITER_BG=1 run_writer stress "$STRESS_TXN" "$STRESS_TICK" "$STRESS_BAND" 2>&1 | tee -a "$WORK/stress.log"

    deadline=$(( $(date +%s) + STRESS_BUDGET ))
    local applied_total=0 writer_running=1 why=""
    while (( $(date +%s) < deadline )); do
        n=$((n + 1))
        # NOT in a pipeline: `cmd_drain_slice … | tee` runs it in a subshell, its
        # SLICE_APPLIED/SLICE_RAISED globals are lost, and the running total prints
        # "applied ?" for every slice while the log beside it holds the number.
        { cmd_drain_slice "$n" "$SLICE" "$res"; } 2>&1 | tee -a "$WORK/stress.log"
        applied_total=$(( applied_total + ${SLICE_APPLIED:-0} ))
        {
            printf '  after slice %s (t+%ss): applied %s, total %s, watermark %s, slot %s, disk %s GB\n' \
                "$n" "$(awk -v a="$STRESS_T0" -v b="$(date +%s)" 'BEGIN{printf "%.0f", b-a}')" \
                "${SLICE_APPLIED:-?}" "$applied_total" "$(watermarks)" "$(slot_info)" "$(disk_free_gb)"
        } | tee -a "$WORK/stress.log"
        local ws; ws=$(pgq "SELECT coalesce(string_agg(wal_status,' '),'-') FROM pg_replication_slots WHERE slot_name LIKE 'apitap\_%'")
        # Is the writer still running? From the pid file it wrote — never a pgrep of
        # the pipeline, which matches the check itself. The old test here compared
        # against a container list that is deliberately EMPTY between slices, so it
        # declared the writer finished after the first slice and then dereferenced
        # `writer_alive`, which no longer existed.
        local p
        writer_running=0
        for p in $(cat "$WORK/ledger/stress.pids" 2>/dev/null); do
            kill -0 "$p" 2>/dev/null && writer_running=1
        done
        # "lost" is an invalidated slot; "-" is no apitap slot at all, which is what
        # apitap leaves behind after it has already reported the invalidation and
        # dropped the slot. Both are the same ceiling.
        if [[ "$ws" == "lost" || "$ws" == "-" ]]; then
            echo "  POSTGRESQL INVALIDATED THE SLOT at slice $n — the ceiling, reached from"
            echo "  the SOURCE side rather than from the cage. wal_status='$ws'."
            why="postgres_invalidated_the_slot"; break
        fi
        if (( writer_running == 0 )) && [[ "${SLICE_RAISED:-1}" == "0" ]] \
           && [[ "${SLICE_APPLIED:-1}" == "0" ]]; then
            why="converged_a_slice_found_nothing_to_do"; break
        fi
    done
    # NOT in a pipeline: `writer_wait … | tee` runs it in a subshell, its globals
    # are discarded on return, and STRESS_TOTAL then printed changes_offered 0
    # beside a ledger holding 54,000 committed transactions.
    { writer_wait stress; } 2>&1 | tee -a "$WORK/stress.log"
    local wbytes wtxns
    wbytes=$(pgq "SELECT pg_wal_lsn_diff('$wlsn1'::pg_lsn, '$wlsn0'::pg_lsn)")
    wtxns=$(cat "$WORK/ledger/stress"-s*.log 2>/dev/null | grep -c '^LEDGER ' || true)
    printf 'GENERATION_WAL lsn %s -> %s  bytes %s  committed_txns %s  b_per_change %s\n' \
        "$wlsn0" "$wlsn1" "${wbytes:-?}" "$wtxns" \
        "$(awk -v b="${wbytes:-0}" -v t="${wtxns:-1}" 'BEGIN{printf "%.1f", b/(t*1000)}')" \
        | tee -a "$res"
    printf 'STRESS_TOTAL changes_offered %s changes_applied %s slices %s\n' \
        "$(( ${WRITER_CHANGES:-0} ))" "$applied_total" "$n" | tee -a "$res"
    touch "$stopfile"; wait $pid 2>/dev/null

    echo "== every slice's own log, in full: this is where the ceiling speaks =="
    for ((i = 1; i <= n; i++)); do
        echo "--- slice $i ---"
        cat "$WORK/logs/apitap-bench-cdc-stress-drain${i}.log" 2>/dev/null | sed 's/^/  /'
    done
    echo "  watermark: $(watermarks)"
    echo "  slot:      $(slot_info)"
    echo "  ch rows:   $(ch_rows_total)"
    [[ -e "$WORK/ledger/stress.stopped" ]] && echo "  the sampler stopped the writer: $(cat "$WORK/ledger/stress.stopped")"
    host_state "after stress"
    df -h / | tail -1 | sed 's/^/  disk /'
    echo "  backlog curve: $csv ($(wc -l < "$csv") lines)"
}

# ── the applied-prefix check ─────────────────────────────────────────────────
# What landed is "the source as of the drain's watermark", and because the
# writer's shape is fixed that is a closed form: the ledger says how many
# transactions of each table had committed at the watermark, every band row has
# exactly that many increments to undo, and the rows to put back or take out are
# the writer's own generator expressions. The CONTROL is k=0, which must
# reproduce the digest WINDOW C's verification already signed off.
cmd_prefix() {
    local w
    w=$(chq "SELECT toString(min(watermark)) FROM _apitap_state FINAL
             WHERE startsWith(dest_table, 'cdc_pg_t')")
    [[ -n "$w" && "$w" != "0" ]] || { echo "PREFIX: no watermark in the destination"; return 1; }
    echo "== the drain's final watermark: $w =="
    # What the drain says it applied, per table, summed over every drain slice's
    # own `per_table_rows` line — apitap's number, not ours.
    local applied_all="" n
    for ((n = 1; n <= 40; n++)); do
        [[ -s "$WORK/logs/apitap-bench-cdc-stress-drain${n}.log" ]] || continue
        applied_all+=" $(grep -h '^  per_table_rows ' "$WORK/logs/apitap-bench-cdc-stress-drain${n}.log" 2>/dev/null | tail -1)"
    done
    local t
    : > "$WORK/prefix-applied.txt"
    for t in "${TABLES[@]}"; do
        local a
        a=$(printf '%s\n' $applied_all | tr ' ' '\n' | sed -n "s/.*\b$t=\([0-9]*\).*/\1/p" \
            | awk '{s+=$1} END{print s+0}')
        echo "$t $a" >> "$WORK/prefix-applied.txt"
    done
    echo "== what the drain says it applied, per table (apitap's own per-table counter) =="
    sed 's/^/  applied_changes /' "$WORK/prefix-applied.txt"
    echo "== what the source wrote, per table (the writer's committed ledger lines) =="
    : > "$WORK/prefix-k.txt"
    for t in "${TABLES[@]}"; do
        echo "$t $(cat "$WORK/ledger/stress"-s*.log 2>/dev/null | grep -c " $t " || true)" \
            >> "$WORK/prefix-k.txt"
    done
    head -3 "$WORK/prefix-k.txt" | sed 's/^/  ledger_txns /'
    echo "   ... $(wc -l < "$WORK/prefix-k.txt") tables, $(awk '{s+=$2} END{print s}' "$WORK/prefix-k.txt") transactions"
    echo "== the source AS OF that watermark, per table, recomputed from scratch =="
    : > "$WORK/prefix-src.txt"
    local kat kt
    for t in "${TABLES[@]}"; do
        kat=$(( $(awk -v k="$t" '$1==k{print $2}' "$WORK/prefix-applied.txt") / 1000 ))
        # k_now is the TOTAL the source wrote, not the count at the watermark:
        # `k_at …` is a lookup at the watermark and using it for both ends makes
        # undo zero and every table mismatch.
        kt=$(cat "$WORK/ledger/stress"-s*.log 2>/dev/null | grep -c " $t " || true)
        python3 "$HERE/bench-cdc-stress-prefix.py" "$t" "$kat" "$kt" "$STRESS_BAND" \
            | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -At -f - >> "$WORK/prefix-src.txt"
    done
    sort -o "$WORK/prefix-src.txt" "$WORK/prefix-src.txt"
    ch_digest_batch 2>/dev/null | sort > "$WORK/prefix-dst.txt"
    local ok=0 bad=0
    while IFS=$'\t' read -r t d; do
        local s; s=$(awk -F'\t' -v k="$t" '$1==k{print $2}' "$WORK/prefix-src.txt")
        if [[ -n "$d" && "$s" == "$d" ]]; then
            printf 'PREFIX_MATCH %s rows=%s %s\n' "$t" "${d%%|*}" "$d"; ok=$((ok+1))
        else
            bad=$((bad+1))
            printf 'PREFIX_MISMATCH %s\n  src@watermark=%s\n  destination  =%s\n' "$t" "$s" "${d:-<absent>}"
        fi
    done < "$WORK/prefix-dst.txt"
    echo "PREFIX_SUMMARY checked=$((ok+bad)) of $NT match=$ok mismatch=$bad"

    echo "== THE CONTROL: the same reconstruction with NO correction (undo=0) =="
    echo "   must reproduce the PLAIN SOURCE digest, exactly — which tests the row text,"
    echo "   the band arithmetic and the region split, with the drain not involved =="
    : > "$WORK/prefix-plain.txt"
    src_digest_batch | sort > "$WORK/prefix-plain.txt"
    : > "$WORK/prefix-k0-out.txt"
    # The band base has to reach the control too: it defaults to the window-S
    # band hardcoded in the module, and a control that corrects a DIFFERENT
    # 800-row band than the one under test mismatches on all thirty tables for a
    # reason that has nothing to do with the drain.
    python3 "$HERE/bench-cdc-stress-prefix.py" --control "$STRESS_BAND" "${TABLES[@]}" \
        | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -At -f - >> "$WORK/prefix-k0-out.txt"
    sort -o "$WORK/prefix-k0-out.txt" "$WORK/prefix-k0-out.txt"
    okc=0; badc=0
    # The reconstruction (uncorrected) against the PLAIN SOURCE — both files, not
    # the destination. The destination is at k=6, so comparing an uncorrected
    # reconstruction against it is guaranteed to mismatch and says nothing about
    # whether the reconstruction is right.
    while IFS=$'\t' read -r t d; do
        local s; s=$(awk -F'\t' -v k="$t" '$1==k{print $2}' "$WORK/prefix-plain.txt")
        if [[ "$s" == "$d" ]]; then okc=$((okc+1)); else
            badc=$((badc+1)); printf 'CONTROL_MISMATCH %s\n  reconstruction, uncorrected =%s\n  plain source digest        =%s\n' "$t" "$d" "$s"
        fi
    done < "$WORK/prefix-k0-out.txt"
    echo "CONTROL_SUMMARY checked=$((okc+badc)) of $NT match=$okc mismatch=$badc"
    [[ $badc -eq 0 ]] || { echo "CONTROL FAILED — the prefix reconstruction is not trustworthy, and neither is PREFIX_SUMMARY"; return 2; }
}

# ── walshadow: ONE leg, same rig, same cap, same start state ──────────────────
cmd_wsleg() {
    local WS_IMG=apitap-bench-ws16:0.1.2
    local WS_STATE=${WS_STATE:-$HOME/bench-cdc-ws}
    local WS_CONF=${WS_CONF:-$HOME/bench-cdc-ws-conf}
    local name="apitap-bench-cdc-stress-ws"
    local log="$WORK/logs/${name}.log"
    docker image inspect "$WS_IMG" >/dev/null 2>&1 || { echo "  $WS_IMG is not on this box; skipping"; return 0; }
    echo "== walshadow 0.1.2, ONE leg, the same source/destination/cap =="
    echo "   empty destination, no apitap slot, the same 30 tables, the same cap"
    cmd_drop >/dev/null; cmd_slotdrop >/dev/null
    sudo rm -rf "$WS_STATE" "$WS_CONF"
    mkdir -p "$WS_STATE/shadow-data" "$WS_STATE/out" "$WS_STATE/spill" "$WS_CONF/ch-config.d"
    sudo chown -R 999:999 "$WS_STATE" "$WS_CONF"
    pgq "SELECT pg_drop_replication_slot('walshadow')" >/dev/null 2>&1 || true
    pgq "SELECT pg_create_physical_replication_slot('walshadow')" >/dev/null
    { echo '[source]'; echo 'host = "127.0.0.1"'; echo "port = $PG_PORT"
      echo 'user = "postgres"'; echo 'password = "bench"'; echo "dbname = \"$PG_DB\""
      echo 'sslmode = "disable"'; echo 'slot = "walshadow"'; echo
      echo '[ch]'; echo 'host = "127.0.0.1"'; echo "port = $CH_NATIVE"
      echo 'database = "default"'; echo 'user = "default"'; echo 'password = "bench"'; echo
      echo '[stream]'; echo 'replicate_all = false'
      for t in "${TABLES[@]}"; do echo; echo "[table.public.$t]"
           echo 'replicate = true'; echo 'initial_load = "copy"'; done
    } > /tmp/ws.conf
    sudo install -m 0644 -o 999 -g 999 /tmp/ws.conf "$WS_CONF/ch-config.toml"
    echo "  config: $(grep -c '^\[table' "$WS_CONF/ch-config.toml") table blocks"
    : > "$log"; host_state "before $name"
    docker run --name "$name" --network=host $CAP \
        -e "WALSHADOW_PG_URL=$PG_URL" \
        -e "WALSHADOW_CH_URL=clickhouse://default:bench@127.0.0.1:${CH_NATIVE}/default" \
        -e WALSHADOW_SHADOW_PORT=5442 -e WALSHADOW_WALSENDER_BIND=127.0.0.1:5433 \
        -v "$WS_STATE:/var/lib/walshadow" -v "$WS_CONF:/etc/walshadow" \
        "$WS_IMG" >"$log" 2>&1 &
    local dpid=$! t0 t1=0
    sleep 0.3
    t0=$(date -u -d "$(docker inspect -f '{{.State.StartedAt}}' "$name" 2>/dev/null)" +%s 2>/dev/null || echo 0)
    # The host-sampled peak: a container the kernel OOM-killed takes its cgroup
    # with it, and memory.peak cannot be read afterwards.
    ( local m=0 cg="" id
      for _ in $(seq 300); do
          id=$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null) || true
          if [[ -n "$id" ]]; then cg="/sys/fs/cgroup/${id}"
              [[ -r "$cg" ]] && break; fi; sleep 0.1; done
      while [[ -n "$cg" && -r "$cg" ]]; do
          local v; v=$(cat "$cg" 2>/dev/null) || break
          (( v > m )) && { m=$v; echo "$m" > "$WORK/logs/${name}.peak"; }
          sleep 0.05; done ) &
    local wpid=$!
    local waited=0 rows=0 cg
    while (( waited < 180 )); do
        if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
            t1=$(now); echo "  container exited after ${waited}s"; break; fi
        rows=$(ch_rows_total)
        printf '  +%3ds ch_rows=%s\n' "$waited" "$rows"
        sleep 5; waited=$((waited + 5))
    done
    cg="/sys/fs/cgroup/$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null)"
    local mempeak=0
    [[ -d "$cg" ]] && mempeak=$(cat "$cg/memory.peak" 2>/dev/null || echo 0)
    local state; state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    { printf 'LEG walshadow tag=stress default-config\n'
      printf '  stopped_by %s\n' "$([[ -n "$state" ]] && echo container_exited || echo still_running)"
      printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
      printf '  cgroup_memory_peak_mb %s\n' "$(awk -v v="$mempeak" 'BEGIN{printf "%.1f", v/1048576}')"
      printf '  host_sampled_peak_mb %s\n' "$(awk -v v="$(cat "$WORK/logs/${name}.peak" 2>/dev/null || echo 0)" 'BEGIN{printf "%.1f", v/1048576}')"
      printf '  rows_landed %s of 31080000\n' "$rows"
      printf '  last_log_lines:\n'; tail -8 "$log" | sed 's/^/    /'
    } | tee -a "$WORK/results.txt"
    kill $wpid 2>/dev/null
    docker rm -f "$name" >/dev/null 2>&1
    wait $dpid 2>/dev/null
    pgq "SELECT pg_drop_replication_slot('walshadow')" >/dev/null 2>&1 || true
    host_state "after  $name"
}

cmd_leaks() {
    echo "== descriptors, per leg, measured from inside each container =="
    grep -hE '^FD_PEAK|^FD_END|^FDCOUNT' "$WORK"/logs/apitap-bench-cdc-stress-*.log 2>/dev/null | sed 's/^/  /'
    echo "== replication slots and the WAL they retain =="
    pgq "SELECT concat('  ', slot_name, ' type=', slot_type, ' active=', active,
                      ' wal_status=', coalesce(wal_status,'-'), ' retained=',
                      pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)))
          FROM pg_replication_slots ORDER BY slot_name"
    pgq "SELECT concat('  publications: ', coalesce(string_agg(pubname, ','), '(none)')) FROM pg_publication"
    pgq "SELECT concat('  total WAL held by slots: ',
                      pg_size_pretty(coalesce(sum(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)),0)))
          FROM pg_replication_slots"
    pgq "SELECT concat('  spilly transactions: ', coalesce(sum(spill_txns),0), ' txns / ',
                      coalesce(sum(spill_count),0), ' spills / ', pg_size_pretty(coalesce(sum(spill_bytes),0)))
          FROM pg_stat_replication_slots"
    echo "== _apitap_lease: a leaked lease is a table a peer cannot claim =="
    chq "SELECT concat('  rows=', count()) FROM _apitap_lease" 2>/dev/null || echo "  (none)"
    chq "SELECT concat('  UNCOLLECTED AND STILL LIVE = ', count())
          FROM (SELECT dest_key, token, argMax(collected, seq) AS c, argMax(expires_at, seq) AS e
                FROM _apitap_lease GROUP BY dest_key, token)
          WHERE c = 0 AND e > now64(6)" 2>/dev/null || true
    echo "== staging / marker leftovers =="
    chq "SELECT concat('  ', name, ' engine=', engine) FROM system.tables
          WHERE database='default' AND (position(name, '__apitap') > 0 OR position(name, '__ws') > 0)"
    chq "SELECT concat('  _apitap_cdc_pending rows=', count()) FROM _apitap_cdc_pending" 2>/dev/null \
        || echo "  (no _apitap_cdc_pending table)"
    echo "== the group's own state rows =="
    chq "SELECT concat('  rows=', count(), ' mode=', arrayStringConcat(groupUniqArray(mode)),
                      ' cursor=', arrayStringConcat(groupUniqArray(cursor_col)),
                      ' distinct_watermarks=', uniqExact(watermark),
                      ' watermark=', min(watermark), '..', max(watermark))
          FROM _apitap_state FINAL WHERE startsWith(dest_table, 'cdc_pg_t')"
}

cmd_state() {
    echo "== this campaign's containers =="
    docker ps --filter "name=apitap-bench-cdc" --format '  {{.Names}}  {{.Image}}  {{.Status}}'
    echo "== this campaign's volumes =="
    for v in apitap-bench-cdc-pg-data apitap-bench-cdc-ch-data; do
        echo "  $v $(sudo du -sh /var/lib/docker/volumes/$v/_data 2>/dev/null | cut -f1)"
    done
    sudo du -sh /var/lib/docker/volumes/apitap-bench-cdc-pg-data/_data/pg_wal 2>/dev/null | sed 's/^/  pg_wal /'
    echo "== the seed, KEPT =="
    pgq "SELECT concat('  ', count(*), ' tables, ', pg_size_pretty(sum(pg_total_relation_size(quote_ident(table_name)))))
          FROM information_schema.tables WHERE table_schema='public'"
    echo "== destination leftovers =="
    chq "SELECT concat('  cdc_pg_t* tables: ', count()) FROM system.tables
          WHERE database='default' AND startsWith(name, 'cdc_pg_t')"
    echo "== replication slots =="
    pgq "SELECT concat('  ', coalesce(string_agg(slot_name || ' active=' || active, ', '), '(none)'))
          FROM pg_replication_slots"
    echo "== disk =="
    df -h / | tail -1 | sed 's/^/  /'
}

case "${1:-}" in
    start)     cmd_start ;;
    bootstrap) cmd_bootstrap ;;
    calm)      cmd_calm ;;
    stress)    cmd_stress ;;
    prefix)    cmd_prefix ;;
    wsleg)     cmd_wsleg ;;
    drop)      cmd_drop ;;
    slotdrop)  cmd_slotdrop ;;
    verify)    shift; cmd_verify "$@" ;;
    leaks)     cmd_leaks ;;
    state)     cmd_state ;;
    wm)        watermarks ;;
    leg)       shift; cmd_leg "$@" ;;
    disk)      df -h / | tail -1 ;;
    *) sed -n '2,16p' "$0"; exit 2 ;;
esac