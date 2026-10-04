#!/usr/bin/env bash
# Capped PostgreSQL -> ClickHouse head-to-head: apitap 0.57.0 (PyPI) vs
# walshadow 0.1.2 (ClickHouse/walshadow), at the capped tier — 10 tables x
# 1,000,000 rows x 15 columns, all 10 synced in ONE job, each tool inside ONE
# container capped at --cpus=0.5 --memory=256m --memory-swap=256m.
#
#   ./bench-capped-pg-ch-0.57.sh seed       # 10 identical tables (kept between runs)
#   ./bench-capped-pg-ch-0.57.sh srcsum     # source aggregates (cached per table)
#   ./bench-capped-pg-ch-0.57.sh slot       # the physical slot walshadow needs
#   ./bench-capped-pg-ch-0.57.sh slotdrop
#   ./bench-capped-pg-ch-0.57.sh leg ARM ROUND
#   ./bench-capped-pg-ch-0.57.sh verify     # per-table rows + checksum vs the source
#   ./bench-capped-pg-ch-0.57.sh drop       # destination tables (the SEEDS stay)
#   ./bench-capped-pg-ch-0.57.sh wsreset    # wipe walshadow state (shadow, WAL, spill)
#   ./bench-capped-pg-ch-0.57.sh startup    # what each tool pays in the cage
#   ./bench-capped-pg-ch-0.57.sh run        # 3 interleaved rounds of every arm
#   ./bench-capped-pg-ch-0.57.sh results    # the per-round table
#   ./bench-capped-pg-ch-0.57.sh wsceiling 512m 1g 2g uncapped
#   ./bench-capped-pg-ch-0.57.sh state      # dest clean? seeds intact? disk?
#
# ARM is apitap | wsdefault | wstuned. The two walshadow arms differ only in the
# knobs walshadow itself documents for a small box; wsdefault is what upstream's
# entrypoint ships. Both are measured because "walshadow failed" is only an
# honest statement if it also failed with its own best settings.
set -uo pipefail

PG_C=apitap-bench-ws-pg       # source, 127.0.0.1:5547, db bench, postgres/bench
CH_C=apitap-bench-ch         # destination, 127.0.0.1:8124 http / 9124 native
PG_DB=bench
PG_URL='postgres://postgres:bench@127.0.0.1:5547/bench'
CH_URL='clickhouse://default:bench@127.0.0.1:9124/default'
# The capped tier. Inheritable, so a wrapper (or the cap ladder) can move the
# ceiling without editing this file — and so nothing can move it by accident.
CAP=${CAP:-"--cpus=0.5 --memory=256m --memory-swap=256m"}
APITAP_VERSION=0.57.0
APITAP_SO_MD5=41e9f7c252d3e1eb5403b70f87bf5435
WALSHADOW_VERSION=0.1.2
SP_APITAP=/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages
IMG=python:3.13-slim
WS_IMG=apitap-bench-ws:${WALSHADOW_VERSION}
WS_STATE=${WS_STATE:-$HOME/bench-capped-pg-ws}       # shadow data dir, WAL, spill
WS_CONF=${WS_CONF:-$HOME/bench-capped-pg-ws-conf}    # ch-config.toml + ctl fragments
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/bench-capped-pg}
SEED_ROWS=${SEED_ROWS:-1000000}
TABLES=(cmp_pg_t01 cmp_pg_t02 cmp_pg_t03 cmp_pg_t04 cmp_pg_t05 cmp_pg_t06
        cmp_pg_t07 cmp_pg_t08 cmp_pg_t09 cmp_pg_t10)
# The control leg drives this same harness over one 1000-row table, so the table
# list and the row target are overridable rather than duplicated.
if [[ -n "${TABLES_OVERRIDE:-}" ]]; then read -r -a TABLES <<< "$TABLES_OVERRIDE"; fi
EXPECT_ROWS=${EXPECT_ROWS:-$(( ${SEED_ROWS:-1000000} * ${#TABLES[@]} ))}
RESULTS=${RESULTS:-$WORK/results.txt}
mkdir -p "$WORK/out" "$WORK/logs"

now() { date +%s.%N; }

# ── the seed ────────────────────────────────────────────────────────────────────
# cmp_pg_t01 is generated once from `id` alone, then the other nine are clones of
# it, so all ten are byte-identical and any difference can only come from the
# transfer. The seeds are NEVER dropped: every run reuses them.
cmd_seed() {
    local t0 t
    t0=$(now)
    pgq "SELECT CASE WHEN (SELECT count(*) FROM public.cmp_pg_t01) = $SEED_ROWS
                     THEN 'already' ELSE 'rebuild' END" | grep -q already || {
        echo "SEED: generating cmp_pg_t01 with $SEED_ROWS rows"
        docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q \
            -v rows="$SEED_ROWS" -v tbl=cmp_pg_t01 < "$HERE/bench-capped-pg-ch-schema.sql" \
            2>&1 | grep -Ei 'error|fatal' && { echo "SEED FAILED"; return 1; }
    }
    # Self-healing: a clone that exists but holds the wrong number of rows (a
    # half-finished earlier seed, say) is dropped and rebuilt rather than kept,
    # because "the table is there" is not the same claim as "the table is right".
    for t in "${TABLES[@]:1}"; do
        if [[ "$(pgq "SELECT count(*) FROM information_schema.tables
                      WHERE table_schema='public' AND table_name='$t'")" == "1" ]]; then
            [[ "$(pgq "SELECT count(*) FROM public.\"$t\"")" == "$SEED_ROWS" ]] && continue
            echo "SEED: $t exists with the wrong row count, rebuilding"
            pgq "DROP TABLE public.\"$t\"" >/dev/null
        fi
        echo "SEED: cloning -> $t"
        pgq "CREATE TABLE public.\"$t\" (LIKE public.cmp_pg_t01 INCLUDING ALL);
             INSERT INTO public.\"$t\" SELECT * FROM public.cmp_pg_t01;" >/dev/null
    done
    printf 'SEED_SECONDS %.1f\n' "$(echo "$(now) - $t0" | bc -l)"
    for t in "${TABLES[@]}"; do
        pgq "SELECT concat('$t cols=',
              (SELECT count(*) FROM information_schema.columns
                WHERE table_schema='public' AND table_name='$t'),
              ' rows=', count(*), ' bytes=', pg_total_relation_size(quote_ident('$t')))
              FROM public.\"$t\""
    done
}

# ── the physical replication slot walshadow needs ───────────────────────────────
# walshadow reserves WAL through a physical slot; without one it would only be
# bounded by wal_keep_size. This is the source slot named in its config.
cmd_slot() {
    local n
    n=$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name='walshadow'")
    [[ "$n" == "0" ]] && pgq "SELECT pg_create_physical_replication_slot('walshadow')" >/dev/null
    pgq "SELECT concat('slot=', slot_name, ' type=', slot_type, ' active=', active,
                      ' restart_lsn=', restart_lsn) FROM pg_replication_slots"
}
# PostgreSQL 18 REMOVED pg_drop_physical_replication_slot() in favour of the
# unified pg_drop_replication_slot(); calling the old name raises "function does
# not exist", which a quiet wrapper swallows — so this resolves the name once and
# says which one it used.
cmd_slotdrop() {
    local fn
    fn=$(pgq "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_proc
                              WHERE proname = 'pg_drop_physical_replication_slot')
                     THEN 'pg_drop_physical_replication_slot' ELSE 'pg_drop_replication_slot' END")
    echo "slotdrop_via $fn"
    pgq "SELECT $fn(slot_name::text) FROM pg_replication_slots
         WHERE slot_name='walshadow' AND NOT active" || true
    pgq "SELECT count(*) || ' slots left' FROM pg_replication_slots"
}

# ── the validator ───────────────────────────────────────────────────────────────
source "$HERE/bench-capped-pg-ch-validator.sh"
pgq() { pgq_ "$1"; }
chq() { chq_ "$1"; }

# Cached per table, keyed on the validator text, so the source scan is paid once
# for the whole campaign (the seed never changes under it).
cmd_srcsum() {
    local t key f
    for t in "${TABLES[@]}"; do
        key=$(printf '%s' "$PG_AGG" | md5sum | cut -c1-8)
        f="$WORK/srcsum-$t-$key"
        if [[ -s "$f" ]]; then echo "$t cached $(cat "$f")"; continue; fi
        printf '%s' "$(srcsum "$t")" > "$f"
        echo "$t computed $(cat "$f")"
    done
}
srcsum_of() {
    local key; key=$(printf '%s' "$PG_AGG" | md5sum | cut -c1-8)
    cat "$WORK/srcsum-$1-$key"
}

# ── destination hygiene ─────────────────────────────────────────────────────────
# Every run ends here. walshadow's ReplacingMergeTree tables, its TOAST mirror
# tables and any staging table it leaves behind all match the prefix.
cmd_drop() {
    chq "SELECT concat('DROP TABLE IF EXISTS ', database, '.', name, ';')
         FROM system.tables
         WHERE name LIKE '%cmp_pg_t%' OR name LIKE '%cmp_ctrl%'
            OR name LIKE '\_\_wsstg%' OR name LIKE '\_\_wspending%'
            OR name LIKE '\_\_ws\_%' FORMAT TSVRaw" \
      | docker exec -i "$CH_C" clickhouse-client --password bench --multiquery 2>/dev/null
    chq "SELECT count() FROM system.tables
         WHERE name LIKE '%cmp_pg_t%' OR name LIKE '%cmp_ctrl%'
            OR name LIKE '\_\_wsstg%' OR name LIKE '\_\_wspending%'
            OR name LIKE '\_\_ws\_%'"
}

# ── walshadow state ─────────────────────────────────────────────────────────────
# A fresh catch-up needs a fresh shadow: the shadow data dir, the filtered WAL
# (--out-dir), the transaction spill and the manifest are all "how far it got",
# and reusing them would measure a resume instead of a first landing.
cmd_wsreset() {
    docker rm -f $(docker ps -aq --filter name='^cmp-ws') >/dev/null 2>&1
    # sudo: the previous leg left these owned by the container's uid 999, so a
    # plain rm -rf fails with "Permission denied" and the leg then RESUMES the
    # old shadow instead of catching up from nothing — which would have made the
    # control green on a stale landing and every walshadow round a resume.
    sudo rm -rf "$WS_STATE" "$WS_CONF"
    mkdir -p "$WS_STATE/shadow-data" "$WS_STATE/out" "$WS_STATE/spill" "$WS_CONF/ch-config.d"
    # The container runs as the image's postgres (uid 999), and the shadow
    # PostgreSQL refuses to start on a data dir it does not own at 0700 — so the
    # bind mounts are owned by 999 before every leg. (Ubuntu's own user is 1000,
    # which is why the daemon's first act would otherwise be a chmod failure.)
    sudo chown -R 999:999 "$WS_STATE" "$WS_CONF"
    cmd_slotdrop >/dev/null
    cmd_slot >/dev/null
    echo "WS_RESET state=$WS_STATE conf=$WS_CONF"
}

# walshadow's own declaration of what it replicates: the ten tables, initial load
# through COPY, nothing else in the database. Discrete keys only — `url` is a CLI
# and environment form, not a documented [source] key, and an unknown key in a
# non-[table] section is silently ignored, which would look like "it connected
# and replicated nothing". Written with sudo install because the config
# directory belongs to the container's uid.
ws_config() {
    local mode=${1:-copy} tuned=${2:-no} t tmp
    tmp=$(mktemp)
    {
        echo '[source]'
        echo 'host = "127.0.0.1"'
        echo 'port = 5547'
        echo 'user = "postgres"'
        echo 'password = "bench"'
        echo 'dbname = "bench"'
        echo 'sslmode = "disable"'
        echo 'slot = "walshadow"'
        echo
        echo '[ch]'
        echo 'host = "127.0.0.1"'
        echo 'port = 9124'
        echo 'database = "default"'
        echo 'user = "default"'
        echo 'password = "bench"'
        # The sealed-insert byte budget. 256 MiB is its default, which is the
        # whole cgroup limit on this rig.
        [[ "$tuned" == tuned ]] && echo 'byte_budget = 33554432'
        echo
        if [[ "$tuned" == tuned ]]; then
            # walshadow's own configuration.md, "Memory budget": the default is
            # one half of the cgroup limit WITH A 512 MiB MINIMUM, a floor a
            # 256 MB cage cannot satisfy. Total reserved memory is
            # decoder_pool_size * value_reserve and must fit within half of
            # resident_payload_max.
            echo '[memory]'
            echo 'resident_payload_max = 67108864'
            echo 'value_reserve = 8388608'
            echo
        fi
        echo '[stream]'
        echo 'replicate_all = false'
        for t in "${TABLES[@]}"; do
            echo
            echo "[table.public.$t]"
            echo 'replicate = true'
            echo "initial_load = \"$mode\""
        done
    } > "$tmp"
    sudo install -m 0644 -o 999 -g 999 "$tmp" "$WS_CONF/ch-config.toml"
    rm -f "$tmp"
    cat "$WS_CONF/ch-config.toml"
}

# ── verification ────────────────────────────────────────────────────────────────
cmd_verify() {
    local t src got rows ok=0 bad=0
    for t in "${TABLES[@]}"; do
        src=$(srcsum_of "$t")
        got=$(chsum "$t")
        rows=$(chrows "$t")
        if [[ "$src" == "$got" && -n "$got" ]]; then
            printf 'VERIFY %s rows=%s MATCH %s\n' "$t" "${rows:-?}" "$got" | tee -a "$WORK/verify.log"
            ok=$((ok + 1))
        else
            printf 'VERIFY %s rows=%s MISMATCH\n  src=%s\n  dst=%s\n' \
                "$t" "${rows:-?}" "$src" "${got:-<no table / no row>}" | tee -a "$WORK/verify.log"
            bad=$((bad + 1))
        fi
    done
    printf 'VERIFY_SUMMARY match=%d mismatch=%d\n' "$ok" "$bad"
    chq "SELECT concat('TOTAL_ROWS ', sum(total_rows)) FROM system.tables
         WHERE database='default' AND name IN ('$(printf "%s','" "${TABLES[@]}")')"
    [[ $bad -eq 0 ]]
}

# ── host state and the container's own peak ─────────────────────────────────────
host_state() {
    local tag=$1
    { printf '%s loadavg=%s memavail_kb=%s cached_kb=%s disk_avail=%s\n' "$tag" \
        "$(cut -d' ' -f1-3 /proc/loadavg)" \
        "$(awk '/MemAvailable/{print $2}' /proc/meminfo)" \
        "$(awk '/^Cached:/{print $2}' /proc/meminfo)" \
        "$(df --output=avail -BG / | tail -1 | tr -d ' ')"
    } >> "$WORK/host-state.log"
    tail -1 "$WORK/host-state.log"
}

# The max of memory.current, sampled from the HOST, so a peak survives an OOM
# kill: a container that is OOM-killed takes its cgroup with it, and memory.peak
# (a cgroup-v2 file that does not exist on this host's cgroup-v1 driver once the
# container is gone) cannot be read afterwards. 20 Hz, because a leg that dies in
# under a second is otherwise reported at whatever the sampler happened to catch.
# For a leg that is still running this is a LOWER BOUND on memory.peak; the
# kernel's own memory.peak is read from the cgroup before the container is
# stopped, and that is the number the result reports.
host_peak_watch() {
    local name=$1 out=$2 id="" cg="" v m=0
    : > "$out"
    for _ in $(seq 300); do
        id=$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null) || true
        if [[ -n "$id" ]]; then
            cg="/sys/fs/cgroup/system.slice/docker-${id}.scope/memory.current"
            [[ -r "$cg" ]] && break
        fi
        sleep 0.1
    done
    [[ -n "$cg" && -r "$cg" ]] || return 0
    while :; do
        v=$(cat "$cg" 2>/dev/null) || break
        if (( v > m )); then m=$v; echo "$m" > "$out"; fi
        sleep 0.05
    done
}

# ── one timed leg ───────────────────────────────────────────────────────────────
# Arms:
#   apitap     one process, one transfer call, tables=[...10...]
#   wsdefault  walshadow with upstream's shipped entrypoint flags
#   wstuned    the same daemon with the knobs walshadow documents for a small
#              box (see the report); nothing else differs
#
# A walshadow leg is a CATCH-UP, not a one-shot: the daemon is started against an
# empty destination and the leg ends when all 10,000,000 rows are READABLE in
# ClickHouse — first by a cheap poll on system.tables, then by the authoritative
# per-table count() with FINAL, whose time is the reported number.
cmd_leg() {
    local arm=$1 round=$2
    local name="cmp-${arm}-r${round}"
    local log="$WORK/logs/${name}.log"
    local peakfile="$WORK/logs/${name}.peak"
    local poll="$WORK/logs/${name}.poll"
    local deadline=${LEG_TIMEOUT:-5400}
    local t0 t1="" wall rc state stopped mempeak cg events
    t1=""; dockerpid=0; state=""
    docker rm -f "$name" >/dev/null 2>&1
    : > "$log"
    : > "$poll"
    host_state "before $name"

    local -a envs=()
    case "$arm" in
    apitap)
        t0=$(now)
        docker run --name "$name" --network=host $CAP \
            -v "$SP_APITAP:/py:ro" -e PYTHONPATH=/py \
            -e "APITAP_TABLES=${TABLES[*]}" \
            -v "$HERE:/job:ro" \
            "$IMG" sh /job/bench-capped-pg-ch-leg-apitap.sh >"$log" 2>&1
        rc=$?
        t1=$(now)
        ;;
    wsdefault|wstuned)
        cmd_wsreset >/dev/null
        if [[ "$arm" == wstuned ]]; then ws_config copy tuned >/dev/null
        else ws_config copy >/dev/null; fi
        # The knobs walshadow's own documentation gives for a memory-capped box.
        # wsdefault gets none of them, i.e. exactly upstream's entrypoint.
        envs=(-e "WALSHADOW_PG_URL=$PG_URL" -e "WALSHADOW_CH_URL=$CH_URL"
              -e WALSHADOW_SHADOW_PORT=5442 -e WALSHADOW_WALSENDER_BIND=127.0.0.1:5433)
        if [[ "$arm" == wstuned ]]; then
            envs+=(-e WALSHADOW_XACT_BUFFER_MAX=33554432      # upstream injects 1 GiB
                   -e WALSHADOW_INSERTER_POOL=2
                   -e WALSHADOW_DECODER_POOL=2)
        fi
        t0=$(now)
        # NOT -d: run it in the background with its output attached, so the
        # daemon's own log lands in $log whatever happens to it, and so the exit
        # code comes back from docker rather than from a detached container.
        docker run --name "$name" --network=host $CAP \
            "${envs[@]}" \
            -v "$WS_STATE:/var/lib/walshadow" -v "$WS_CONF:/etc/walshadow" \
            "$WS_IMG" >"$log" 2>&1 &
        dockerpid=$!
        # t0 becomes the container's own start, not docker(1)'s round trip
        sleep 0.3
        t0=$(docker inspect -f '{{.State.StartedAt}}' "$name" 2>/dev/null || echo "$t0")
        local host_peak_watch_pid t_poll t_verify waited=0 rows=0 fin_at=""
        host_peak_watch "$name" "$peakfile" &
        host_peak_watch_pid=$!
        stopped="completed"
        while :; do
            if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
                # it is gone: record WHEN, because its lifetime is the only
                # duration this leg has, and read docker's verdict before the
                # cleanup below destroys it
                t1=$(now)
                stopped="container_exited"
                state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
                fin_at=$(docker inspect -f '{{.State.FinishedAt}}' "$name" 2>/dev/null)
                break
            fi
            rows=$(chq "SELECT toString(coalesce(sum(total_rows),0)) FROM system.tables
                        WHERE database='default' AND name IN ('$(printf "%s','" "${TABLES[@]}")')")
            rows=${rows:-0}
            printf '%s rows=%s\n' "$(now)" "$rows" >> "$poll"
            sleep 2
            waited=$((waited + 2))
            if (( rows >= EXPECT_ROWS )); then
                t_poll=$(now)
                # authoritative: every table's real row count, with FINAL
                local union="" all_rows t
                for t in "${TABLES[@]}"; do
                    union+="${union:+ UNION ALL }SELECT count() AS c FROM \`default\`.\`$t\` FINAL"
                done
                all_rows=$(chq "SELECT toString(sum(c)) FROM ( $union )")
                t_verify=$(now)
                if [[ "${all_rows:-0}" -ge $EXPECT_ROWS ]]; then
                    t1=$(date -u -d "@$t_verify" +%s.%N 2>/dev/null || echo "$t_verify")
                    stopped="caught_up"
                    printf 'ROWS %s\nT_POLL %s\nT_VERIFY %s\n' "$all_rows" "$t_poll" "$t_verify" >> "$log"
                    break
                fi
            fi
            (( waited < deadline )) || { stopped="deadline_${deadline}s"; break; }
        done
        # evidence from the container's own cgroup BEFORE it goes away
        cg="/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null).scope"
        if [[ -d "$cg" ]]; then
            mempeak=$(cat "$cg/memory.peak" 2>/dev/null || echo 0)
            events=$(grep -E '^(oom|oom_kill|high|max) ' "$cg/memory.events" 2>/dev/null | tr '\n' ' ')
            { printf 'MEMPEAK_BYTES %s\nMEMEVENTS %s\nMEMSTAT %s\n' "$mempeak" "$events" \
                "$(grep -E '^(anon|file|shmem) ' "$cg/memory.stat" 2>/dev/null | tr '\n' ' ')"; } >> "$log"
        fi
        # walshadow's own account of itself, taken while it is alive
        docker exec "$name" walshadow-stream ctl status >>"$log" 2>&1 || true
        # docker's own verdict, read BEFORE anything removes the container
        state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
        if [[ "$stopped" == caught_up ]]; then
            # SIGTERM: walshadow drains its pipeline and writes a final checkpoint
            docker stop -t 60 "$name" >>"$log" 2>&1 || true
            wait $dockerpid 2>/dev/null; rc=$?
            state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
        else
            docker rm -f "$name" >/dev/null 2>&1
            wait $dockerpid 2>/dev/null; rc=$?
        fi
        kill $host_peak_watch_pid 2>/dev/null
        ;;
    *) echo "unknown arm $arm" >&2; return 2 ;;
    esac

    # apitap reports the kernel's peak from inside the cage, before its cgroup
    # is gone; a walshadow leg has it appended above from the host.
    [[ "$arm" == apitap ]] && mempeak=$(grep -o 'MEMPEAK=[0-9]*' "$log" | tail -1 | cut -d= -f2)
    # Do NOT clobber a state already read while the container still existed:
    # docker rm -f destroys it, and "(container already gone)" is exactly the
    # answer that hides an OOM kill.
    if [[ -z "$state" ]]; then
        state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
        [[ -n "$state" ]] || state="(container already gone)"
    fi
    local mempeak_mb hostpeak_mb wall_s wall_note=""
    mempeak_mb=$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')
    hostpeak_mb=$(awk -v v="$(cat "$peakfile" 2>/dev/null || echo 0)" 'BEGIN{printf "%.1f", v/1048576}')
    if [[ "$arm" == apitap ]]; then
        wall_s=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
    elif [[ -n "$t1" ]]; then
        # container StartedAt -> the moment the destination was verifiably full
        wall_s=$(awk -v a="$(date -u -d "$t0" +%s.%N)" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
    else
        # No landing time exists: the container's OWN lifetime is the only
        # duration this leg has, and stopped_by says which one it is. The exact
        # value is the container's own StartedAt -> FinishedAt, so the poll
        # interval cannot flatter or penalise it.
        wall_s=$(awk -v a="$(date -u -d "$t0" +%s.%N 2>/dev/null)" \
                    -v b="$(date -u -d "${fin_at:-}" +%s.%N 2>/dev/null || echo 0)" \
                    'BEGIN{printf "%.1f", (b>0? b-a : 0)}')
        wall_note="(container lifetime: it never reached full landing)"
    fi
    docker rm -f "$name" >/dev/null 2>&1
    host_state "after  $name"

    {
        printf 'LEG %s round=%s\n' "$arm" "$round"
        printf '  stopped_by %s\n' "${stopped:-completed}"
        printf '  wall_container_s %s %s\n' "$wall_s" "$wall_note"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' "$mempeak_mb"
        printf '  host_sampled_peak_mb %s\n' "$hostpeak_mb"
        grep -E '^(MEMPEAK_BYTES|MEMEVENTS|MEMSTAT|ROWS|T_POLL|T_VERIFY|APITAP_VERSION|ELAPSED|IMPORT_S|PIPE_BUDGET|RAISED)' "$log" \
            | sed 's/^/  /'
        printf '  poll_last %s\n' "$(tail -1 "$poll" 2>/dev/null)"
        printf '  log %s\n' "$log"
    } | tee -a "$RESULTS"
    return 0
}

# The arms, in the order they run inside every round. Interleaved A B A B, so a
# block of one tool cannot be confounded with host drift on a shared box.
ARMS=(apitap wsdefault wstuned)

cmd_run() {
    local rounds=${ROUNDS:-3}
    cmd_seed
    cmd_srcsum
    : > "$RESULTS"
    : > "$WORK/verify.log"
    : > "$WORK/host-state.log"
    local r arm
    for ((r = 1; r <= rounds; r++)); do
        for arm in "${ARMS[@]}"; do
            echo "=== round $r · $arm  ($(date -u +%H:%M:%S))"
            cmd_drop >/dev/null
            cmd_leg "$arm" "$r"
            cmd_verify
            cmd_drop >/dev/null
            cmd_slotdrop >/dev/null
        done
    done
    cmd_results
}

# What each tool pays inside the cage before it moves a single row.
cmd_startup() {
    RESULTS=$WORK/results-startup.txt
    : > "$RESULTS"
    local name
    for spec in "apitap-import:/job/bench-capped-pg-ch-probes.sh import apitap" \
                "walshadow-import:import ws"; do
        name="cmp-${spec%%:*}"
        docker rm -f "$name" >/dev/null 2>&1
        if [[ "$name" == *apitap* ]]; then
            docker run --name "$name" --network=host $CAP \
                -v "$SP_APITAP:/py:ro" -e PYTHONPATH=/py -v "$HERE:/job:ro" \
                "$IMG" sh ${spec#*:} >"$WORK/logs/${name}.log" 2>&1
        else
            # the floor of loading walshadow's 166 MB binary, with no state, no
            # source and no destination — the exact analogue of `import apitap`
            docker run --name "$name" --network=host $CAP -v "$HERE:/job:ro" \
                "$WS_IMG" ${spec#*:} >"$WORK/logs/${name}.log" 2>&1
        fi
        local mempeak state
        mempeak=$(grep -o 'MEMPEAK=[0-9]*' "$WORK/logs/${name}.log" | tail -1 | cut -d= -f2)
        state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
        { printf 'STARTUP %-18s peak_mb=%s state=%s\n' "$name" \
            "$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')" "$state"
          grep -E '^(WALL|MEMPEAK|MEMSTAT|APITAP_VERSION|walshadow-stream)' "$WORK/logs/${name}.log" | sed 's/^/  /'
        } | tee -a "$WORK/results-startup.txt"
        docker rm -f "$name" >/dev/null 2>&1
    done
}

# How far walshadow gets as the cage grows. "walshadow failed at 256 MB" is only
# an honest statement alongside "here is the smallest cage it does finish in", so
# the same leg is re-run with upstream's DEFAULT configuration at 512 MB, 1 GB,
# 2 GB and uncapped, each from a pre-seeded source and an empty destination. The
# default configuration (not wstuned) is used here on purpose: at a bigger cap the
# honest best config is the one walshadow ships, byte_budget 256 MiB and 8
# inserters included.
cmd_wsceiling() {
    # APPEND, never truncate: a ladder whose earlier rungs get overwritten by a
    # later invocation is not a ladder.
    local out="$WORK/results-ceiling.txt"
    printf '===== wsceiling %s  (%s)\n' "$*" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$out"
    local mem
    for mem in "$@"; do
        local cap
        if [[ "$mem" == uncapped ]]; then cap="--cpus=0.5"
        else cap="--cpus=0.5 --memory=$mem --memory-swap=$mem"; fi
        printf 'CEILING cap=%s\n' "$cap" | tee -a "$out"
        CAP="$cap" cmd_leg wsdefault "ceil-${mem}" 2>&1 | tee -a "$out"
        cmd_verify 2>&1 | tail -2 | tee -a "$out"
        cmd_drop >/dev/null
        cmd_slotdrop >/dev/null
    done
    cat "$out"
}

cmd_results() {
    echo
    printf '%-9s %-3s %10s %12s %10s %-22s %s\n' TOOL RND WALL_S PEAK_MB STOPPED DOCKER_ROWS
    python3 - "$WORK/results.txt" <<'PY'
import re, sys, statistics
rounds, tool = {}, None
for line in open(sys.argv[1]):
    line = line.rstrip()
    m = re.match(r"LEG (\S+) round=(\S+)", line)
    if m:
        tool, rnd = m.group(1), m.group(2)
        rounds.setdefault(tool, {})[rnd] = {}
        continue
    m = re.match(r"\s+(\S+) (.*)", line)
    if m and tool:
        rounds[tool][rnd][m.group(1)] = m.group(2)
for tool in sorted(rounds):
    for rnd in sorted(rounds[tool]):
        d = rounds[tool][rnd]
        print(f"{tool:<9} {rnd:<3} {d.get('wall_container_s','?'):>10} "
              f"{d.get('cgroup_memory_peak_mb','?'):>12} {d.get('stopped_by','?'):>10} "
              f"{d.get('docker_state','?'):<22} {d.get('ROWS','-')}")
print()
for tool in sorted(rounds):
    ws = [float(d["wall_container_s"]) for d in rounds[tool].values() if "wall_container_s" in d]
    pk = [float(d["cgroup_memory_peak_mb"]) for d in rounds[tool].values()
          if d.get("cgroup_memory_peak_mb", "0") not in ("?", "0")]
    if ws:
        line = f"MEDIAN {tool} wall_s {statistics.median(ws):.1f}  rounds {[round(w,1) for w in ws]}"
        if pk:
            line += f"  peak_mb {max(pk):.1f}"
        print(line)
PY
    echo
    echo "checksum verdicts per leg: the VERIFY lines above (dest dropped after each)"
}

case "${1:-}" in
seed) cmd_seed ;;
srcsum) cmd_srcsum ;;
slot) cmd_slot ;;
slotdrop) cmd_slotdrop ;;
wsreset) cmd_wsreset ;;
config) shift; ws_config "${1:-copy}" ;;
agg) shift; case "${1:-}" in
          src) srcsum "$2" ;;
          dst) chsum "$2" ;;
          *) echo "usage: agg src|dst TABLE" >&2; exit 2 ;;
      esac ;;
leg) shift; cmd_leg "$@" ;;
startup) cmd_startup ;;
verify) cmd_verify ;;
drop) cmd_drop ;;
run) cmd_run ;;
results) cmd_results ;;
wsceiling) shift; cmd_wsceiling "$@" ;;
state)
    echo "== ClickHouse leftovers from this campaign:"
    chq "SELECT concat('  tables matching the campaign: ', count()) FROM system.tables
         WHERE name LIKE '%cmp_pg_t%' OR name LIKE '%cmp_ctrl%'
            OR name LIKE '\_\_ws%'"
    echo "== PostgreSQL seeds (columns per seed, then exact row counts):"
    pgq "SELECT concat('  seeds: ', count(*), ' tables, columns each ', min(c), '-', max(c))
         FROM (SELECT table_name, count(*) AS c FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name LIKE 'cmp_pg_t%'
               GROUP BY table_name) x"
    for t in "${TABLES[@]}"; do
        pgq "SELECT concat('  $t rows=', count(*)) FROM public.\"$t\""
    done
    echo "== replication slots left on the source:"
    pgq "SELECT concat('  ', slot_name, ' active=', active) FROM pg_replication_slots"
    echo "== walshadow state on disk:"
    du -sh "$WS_STATE" 2>/dev/null | sed 's/^/  /'
    echo "== disk:"
    df -h / | tail -1 | sed 's/^/  /'
    ;;
*) sed -n 2,25p "$0"; exit 2 ;;
esac