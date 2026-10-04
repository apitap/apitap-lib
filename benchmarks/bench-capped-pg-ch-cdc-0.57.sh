#!/usr/bin/env bash
# Capped PostgreSQL -> ClickHouse **CDC**: apitap 0.57.0 (PyPI) vs walshadow
# 0.1.2 (ClickHouse/walshadow) at the capped tier, with a SHORT window.
#
#   30 tables x 1,000,000 rows x 15 columns, ALL 30 in ONE CDC group — one
#   replication slot, one publication, one transfer call — each tool inside ONE
#   container capped at --cpus=0.5 --memory=256m --memory-swap=256m.
#
#   ./bench-capped-pg-ch-cdc-0.57.sh rig       # this campaign's two containers
#   ./bench-capped-pg-ch-cdc-0.57.sh seed      # 30 identical tables (KEPT between legs)
#   ./bench-capped-pg-ch-cdc-0.57.sh srcsum    # source aggregates (cached per table)
#   ./bench-capped-pg-ch-cdc-0.57.sh catchup ARM...   # interleaved cold catch-up legs
#   ./bench-capped-pg-ch-cdc-0.57.sh verify [--fresh]  # rows + checksum, per table
#   ./bench-capped-pg-ch-cdc-0.57.sh drop      # destination tables (the SEEDS stay)
#   ./bench-capped-pg-ch-cdc-0.57.sh window    # the 5-minute window
#   ./bench-capped-pg-ch-cdc-0.57.sh leaks     # the end-of-campaign leak checks
#   ./bench-capped-pg-ch-cdc-0.57.sh state     # containers? disk? seeds? slots?
#   ./bench-capped-pg-ch-cdc-0.57.sh results   # the per-leg tables
#
# ARM is apitap | wsdef | wstuned. The two walshadow arms differ only in the knobs
# walshadow itself documents for a memory-capped box; wsdef is what upstream's
# entrypoint ships. Both are measured because "walshadow failed" is only an
# honest statement if it also failed with its own best settings.
#
# The rig is this campaign's own: apitap-bench-cdc-pg (postgres:16-alpine,
# wal_level=logical) and apitap-bench-cdc-ch (clickhouse-server:24.8), each with
# its own volume, on free host ports, each on the apitap-bench-cdc-* name. No
# container that existed before this campaign is touched, moved or restarted.
set -uo pipefail

export PG_C=${PG_C:-apitap-bench-cdc-pg}
export CH_C=${CH_C:-apitap-bench-cdc-ch}
export PG_DB=${PG_DB:-bench}
PG_PORT=5548
CH_HTTP=8128
CH_NATIVE=9128
PG_URL="postgres://postgres:bench@127.0.0.1:${PG_PORT}/${PG_DB}"
CH_URL="clickhouse://default:bench@127.0.0.1:${CH_HTTP}/default"

# The capped tier. Inheritable, so a wrapper can move the ceiling without editing
# this file — and so nothing can move it by accident.
CAP=${CAP:-"--cpus=0.5 --memory=256m --memory-swap=256m"}
APITAP_SO_MD5=41e9f7c252d3e1eb5403b70f87bf5435
WALSHADOW_VERSION=0.1.2
SP_APITAP=/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages
IMG=python:3.13-slim
WS_IMG=apitap-bench-ws16:${WALSHADOW_VERSION}   # pg16 module: this source is 16
WS_STATE=${WS_STATE:-$HOME/bench-cdc-ws}
WS_CONF=${WS_CONF:-$HOME/bench-cdc-ws-conf}
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/bench-cdc}
SEED_ROWS=${SEED_ROWS:-1000000}
NT=${NT:-30}
TABLES=()
for ((i = 1; i <= NT; i++)); do TABLES+=("$(printf 'cdc_pg_t%02d' "$i")"); done
if [[ -n "${TABLES_OVERRIDE:-}" ]]; then read -r -a TABLES <<< "$TABLES_OVERRIDE"; fi
EXPECT_ROWS=${EXPECT_ROWS:-$(( SEED_ROWS * ${#TABLES[@]} ))}
# The window: TICKS ticks of exactly (INS+UPD+DEL) changes per table, TICK_S
# apart, and a drain every DRAIN_EVERY seconds.
TICKS=${TICKS:-450}
TICK_S=${TICK_S:-0.65}
DRAIN_EVERY=${DRAIN_EVERY:-60}
INS=${INS:-100}
UPD=${UPD:-100}
DEL=${DEL:-20}
RESULTS=${RESULTS:-$WORK/results.txt}
mkdir -p "$WORK/out" "$WORK/logs"

now() { date +%s.%N; }
pgq() { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "$1"; }
chq() { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1"; }
# A quoted, comma-terminated list for IN (...). The LEADING quote is part of the
# output: `printf "%s','"` leaves the first element bare, so `IN ($(inlist))`
# silently becomes IN (a','b',...) — which is a syntax error, not a wrong answer,
# and every query that used it was failing.
inlist() { printf "'%s'," "${TABLES[@]}"; }

# ── the rig ────────────────────────────────────────────────────────────────────
# wal_level=logical and the slot/sender ceilings are command-line settings because
# they need a restart; the pg_hba change below is a reload.
cmd_rig() {
    echo "== this campaign's two isolated containers =="
    # Recreating: the source keeps its volume (the 30-table SEED lives there and is
    # never rebuilt), the destination's volume is replaced so a previous run's
    # orphaned parts cannot ride along. Only ever this campaign's own names.
    docker rm -f "$PG_C" >/dev/null 2>&1
    docker volume rm apitap-bench-cdc-ch-data >/dev/null 2>&1 || true
    docker rm -f "$CH_C" >/dev/null 2>&1
    docker volume create apitap-bench-cdc-pg-data >/dev/null
    docker volume create apitap-bench-cdc-ch-data >/dev/null
    # max_wal_size is 4GB, not the 8GB this rig started with: pg_wal was sitting on
    # 7.8GB of recycled segments, which is slack a box at 88% cannot spare. It is a
    # source-side setting on an UNCAPPED source, and it does not change what the
    # capped container may do.
    docker run -d --name "$PG_C" \
        -p 127.0.0.1:${PG_PORT}:5432 \
        -e POSTGRES_PASSWORD=bench -e POSTGRES_DB="$PG_DB" \
        -v apitap-bench-cdc-pg-data:/var/lib/postgresql/data \
        postgres:16-alpine \
            -c wal_level=logical \
            -c max_replication_slots=10 \
            -c max_wal_senders=10 \
            -c max_slot_wal_keep_size=6GB \
            -c max_wal_size=4GB \
            -c shared_buffers=128MB \
            -c max_connections=80 \
            -c logical_decoding_work_mem=256MB \
            -c checkpoint_timeout=30min \
            -c max_worker_processes=8 >/dev/null
    # The `host replication` trust line the official image omits for non-loopback
    # peers. Without it walshadow's BASE_BACKUP / START_REPLICATION is not an
    # error, it is an infinite retry loop — "source unreachable — waiting for it",
    # every two seconds, forever — and every millisecond of it is spent on auth,
    # not on data.
    until pgq "SELECT 1" >/dev/null 2>&1; do sleep 1; done
    docker exec -i "$PG_C" bash -c "printf '%s\n' \
        'local   all             all                                     trust' \
        'host    all             all             all                     trust' \
        'local   replication     all                                     trust' \
        'host    replication     all             all                     trust' \
        > /var/lib/postgresql/data/pg_hba.conf"
    pgq "SELECT pg_reload_conf()" >/dev/null

    docker run -d --name "$CH_C" \
        -p 127.0.0.1:${CH_HTTP}:8123 -p 127.0.0.1:${CH_NATIVE}:9000 \
        -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=bench -e CLICKHOUSE_DB=default \
        -v apitap-bench-cdc-ch-data:/var/lib/clickhouse \
        clickhouse/clickhouse-server:24.8 >/dev/null
    until chq "SELECT 1" >/dev/null 2>&1; do sleep 1; done
    echo "  $PG_C  postgres:16-alpine             127.0.0.1:${PG_PORT}"
    echo "  $CH_C  clickhouse-server:24.8         127.0.0.1:${CH_HTTP} http / ${CH_NATIVE} native"
    pgq "SELECT concat('  ', name, ' = ', setting, unit) FROM pg_settings
          WHERE name IN ('wal_level','max_replication_slots','max_wal_senders',
          'max_slot_wal_keep_size','max_wal_size','shared_buffers','max_connections',
          'logical_decoding_work_mem','server_version') ORDER BY name"
    chq "SELECT concat('  ClickHouse ', version())"
    df -h / | tail -1 | sed 's/^/  disk /'
}

# ── the seed ───────────────────────────────────────────────────────────────────
# cdc_pg_t01 is generated once from `id` alone with the SAME schema file the bulk
# arm used, then the other 29 are LIKE-clones, so all thirty are byte-identical and
# any difference can only come from the transfer. Never dropped: every leg reuses.
cmd_seed() {
    local t0 t
    t0=$(now)
    echo "SEED: $# tables x $SEED_ROWS rows x 15 columns (the bulk arm's schema, verbatim)"
    pgq "SELECT CASE WHEN (SELECT count(*) FROM public.cdc_pg_t01) = $SEED_ROWS
                     THEN 'already' ELSE 'rebuild' END" | grep -q already || {
        docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q \
            -v rows="$SEED_ROWS" -v tbl=cdc_pg_t01 < "$HERE/bench-capped-pg-ch-schema.sql" \
            2>&1 | grep -Ei 'error|fatal' && { echo "SEED FAILED"; return 1; }
    }
    for t in "${TABLES[@]:1}"; do
        if [[ "$(pgq "SELECT count(*) FROM information_schema.tables
                      WHERE table_schema='public' AND table_name='$t'")" == "1" ]]; then
            [[ "$(pgq "SELECT count(*) FROM public.\"$t\"")" == "$SEED_ROWS" ]] && continue
            echo "SEED: $t exists with the wrong row count, rebuilding"
            pgq "DROP TABLE public.\"$t\"" >/dev/null
        fi
        pgq "CREATE TABLE public.\"$t\" (LIKE public.cdc_pg_t01 INCLUDING ALL);
             INSERT INTO public.\"$t\" SELECT * FROM public.cdc_pg_t01;" >/dev/null
    done
    printf 'SEED_SECONDS %.1f\n' "$(echo "$(now) - $t0" | bc -l)"
    echo "== the seed, verified =="
    pgq "SELECT concat('  ', table_name, ' cols=',
            (SELECT count(*) FROM information_schema.columns
              WHERE table_schema='public' AND table_name=t.table_name),
            ' ', pg_size_pretty(pg_total_relation_size(quote_ident(table_name))))
          FROM information_schema.tables t
          WHERE table_schema='public' AND table_name LIKE 'cdc\_pg\_t%'
          ORDER BY table_name LIMIT 2"
    pgq "SELECT concat('  ... ', COUNT(*), ' tables x ', MIN(c), '-', MAX(c), ' columns, ',
                      pg_size_pretty(SUM(b)), ' total')
          FROM (SELECT table_name, (SELECT count(*) FROM information_schema.columns
                                    WHERE table_schema='public' AND table_name=t.table_name) c,
                       pg_total_relation_size(quote_ident(table_name)) b
                FROM information_schema.tables t
                WHERE table_schema='public' AND table_name LIKE 'cdc\_pg\_t%') x"
    echo "  all thirty are clones of one generated table: srcsum prints the same digest 30x"
}

# ── the validator, shared verbatim with the bulk arm ───────────────────────────
source "$HERE/bench-capped-pg-ch-validator.sh"

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

# Thirty aggregates in ONE query per engine: 30 sequential docker execs would be 30
# sequential md5 scans of a million rows each, and one UNION ALL of 30 arms lets
# each server spread it across its own cores.
#
# Each arm is the SHARED definition with its @TBL@ filled in and the whole SELECT
# used as a DERIVED TABLE, so a batch cannot drift from the single-table validator it
# is built from. The shape took two attempts to get right, and both failures looked
# like "no rows", which is the worst way for a validator to fail:
#
#   * naming the outer column `d` needs `AS q(d)`, which PostgreSQL accepts and
#     ClickHouse rejects outright ("Expected one of: FINAL, SAMPLE, table …": it
#     parses `q(d)` as a table function);
#   * `SELECT * ` from the derived table works in both — so the digest arrives as
#     its eleven SEPARATE columns and `join_digest` puts the `|` back.
# t<TAB>d1|d2|… — the SAME shape the single-table validator returns, so the two
# paths are comparable by construction.
#
# $1 is the separator the PRODUCER puts between its columns, and the two do not
# agree: `psql -A` separates unaligned output with `|`, clickhouse-client's TSV
# with a tab. Splitting psql's output on tabs yields ONE field (the whole row) and
# an empty digest, which reads as a MISMATCH on all thirty tables rather than as an
# error. So the caller states which producer it is.
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

# ── destination hygiene ────────────────────────────────────────────────────────
# Every run ends here. Dropping a destination table does NOT clear its watermark
# — that lives in the destination's _apitap_state — so the state rows go with the
# tables, or the next run meets its own stale watermark with the slot gone and
# dies with "destination has a watermark but slot ... is GONE".
cmd_drop() {
    # The table LIST, not a prefix: the control leg's table (cdc_ctrl) is not one
    # of the campaign's thirty, and a prefix-only drop leaves it behind — which is
    # not cosmetic. walshadow's CREATE TABLE IF NOT EXISTS then keeps the shape
    # apitap left there, a plain MergeTree with no _lsn, and the daemon dies on
    # "No such column _lsn". Measured, on the first control run.
    # SYNC, and it matters here: without it ClickHouse keeps a dropped table's
    # metadata (and its data on disk) for database_atomic_delay_before_drop_table_sec
    # = 480 s so UNDROP can still find it — CH's own description says the delay is
    # IGNORED for a SYNC drop. Measured: three consecutive legs at 8 GB each took
    # the box from 42 GB free to 9.5 GB free; with SYNC the landing goes with the
    # drop. This is drop hygiene on this campaign's own ClickHouse, not the
    # measured path.
    chq "SELECT concat('DROP TABLE IF EXISTS ', database, '.', name, ' SYNC;')
          FROM system.tables
          WHERE name IN ($(inlist)) OR startsWith(name, 'cdc_ctrl')
             OR position(name, '__apitap') > 0 OR position(name, '__ws') > 0
          FORMAT TSVRaw" \
        | docker exec -i "$CH_C" clickhouse-client --password bench --multiquery 2>/dev/null
    chq "DELETE FROM _apitap_state
          WHERE startsWith(dest_table, 'cdc_pg_t') OR startsWith(dest_table, 'cdc_ctrl')" 2>/dev/null
    echo -n "leftover destination tables: "
    chq "SELECT count() FROM system.tables
          WHERE name IN ($(inlist)) OR startsWith(name, 'cdc_ctrl')
             OR position(name, '__apitap') > 0 OR position(name, '__ws') > 0"
}

# The apitap CDC group's own server-side state. Only THIS campaign's own apitap_*
# names are dropped: a blanket "drop every apitap_* slot" is a grenade on a shared
# rig (benchmarks/cdc-test-traps.md #9).
cmd_slotdrop() {
    local s
    for s in $(pgq "SELECT slot_name FROM pg_replication_slots
                     WHERE slot_name LIKE 'apitap\_%'"); do
        pgq "SELECT pg_drop_replication_slot('$s')" >/dev/null 2>&1 || true
    done
    for s in $(pgq "SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap\_%'"); do
        pgq "DROP PUBLICATION IF EXISTS $s" >/dev/null 2>&1 || true
    done
    echo -n "  slots left: "
    pgq "SELECT coalesce(string_agg(slot_name, ','), '(none)') FROM pg_replication_slots"
}

# ── host state, and a peak that survives an OOM kill ────────────────────────────
host_state() {
    { printf '%s loadavg=%s memavail_kb=%s cached_kb=%s disk_avail=%s\n' "$1" \
        "$(cut -d' ' -f1-3 /proc/loadavg)" \
        "$(awk '/MemAvailable/{print $2}' /proc/meminfo)" \
        "$(awk '/^Cached:/{print $2}' /proc/meminfo)" \
        "$(df --output=avail -BG / | tail -1 | tr -d ' ')"
    } >> "$WORK/host-state.log"
    tail -1 "$WORK/host-state.log"
}

# The max of memory.current sampled from the HOST at 20 Hz: a container the kernel
# OOM-killed takes its cgroup with it, and memory.peak cannot be read afterwards.
# For a leg that survives, this is a LOWER BOUND on memory.peak.
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

# ── one timed apitap leg ───────────────────────────────────────────────────────
# MODE=catchup: the destination is EMPTY, so the call is the bootstrap plus the
# group's first drain — the headline, and the comparable shape to the bulk arm.
# MODE=drain:   the group is established; the call lands everything the slot has
#               accumulated since the last drain. twice=1 runs it a SECOND time in
#               the same process: the empty drain's idempotence, and a descriptor
#               count before/after two runs of one process.
cmd_leg_apitap() {
    local tag=$1 mode=${2:-catchup} twice=${3:-0} res=${4:-$RESULTS}
    local name="apitap-bench-cdc-$tag"
    local log="$WORK/logs/${name}.log" t0 t1 wall state mempeak
    docker rm -f "$name" >/dev/null 2>&1
    : > "$log"
    host_state "before $name"
    t0=$(now)
    docker run --name "$name" --network=host $CAP \
        -v "$SP_APITAP:/py:ro" -e PYTHONPATH=/py -v "$HERE:/job:ro" \
        -e "APITAP_TABLES=${TABLES[*]}" -e "APITAP_SRC=$PG_URL" -e "APITAP_DST=$CH_URL" \
        -e "APITAP_MODE=$mode" -e "APITAP_RUN_TWICE=$twice" \
        "$IMG" sh /job/bench-capped-pg-ch-cdc-leg-apitap.sh >"$log" 2>&1
    local rc=$?
    t1=$(now)
    state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    mempeak=$(grep -o 'MEMPEAK=[0-9]*' "$log" | tail -1 | cut -d= -f2)
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
    docker rm -f "$name" >/dev/null 2>&1
    host_state "after  $name"
    {
        printf 'LEG apitap tag=%s mode=%s twice=%s\n' "$tag" "$mode" "$twice"
        printf '  stopped_by %s\n' "$([[ $rc -eq 0 ]] && echo completed || echo "exit_${rc}")"
        printf '  wall_container_s %s\n' "$wall"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' \
            "$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')"
        grep -E '^(MEMPEAK|MEMSTAT|MEMEVENTS|DRAIN|ELAPSED|IMPORT_S|APITAP_|FDCOUNT|FD_PEAK|FD_END|IDEMPOTENT|RAISED|EXITCODE|  per_table)' \
            "$log" | sed 's/^/  /'
        printf '  log %s\n' "$log"
    } | tee -a "$res"
    return 0
}

# ── the walshadow arm ──────────────────────────────────────────────────────────
# walshadow is a continuous CDC daemon, so its arm is a CATCH-UP too: a
# pre-seeded source, an empty destination, and the time until the rows are
# readable. It is not a loader and the number is not dressed up as one.
cmd_leg_ws() {
    local tag=$1 tuned=${2:-no} res=${3:-$RESULTS}
    # The arm is part of the NAME: `ws-$tag` alone gave both walshadow arms of a
    # round the same container name AND the same log file, so the second arm
    # truncated the first one's daemon log. The OOM verdict survived (it is read
    # from each leg's own container before removal) but the evidence did not.
    local name="apitap-bench-cdc-ws-${tag}-$([[ "$tuned" == tuned ]] && echo tuned || echo def)"
    local log="$WORK/logs/${name}.log" peak="$WORK/logs/${name}.peak"
    local poll="$WORK/logs/${name}.poll" deadline=${LEG_TIMEOUT:-5400}
    local t0 t1="" wall rc state stopped mempeak hostpeak waited=0 rows=0 fin_at=""
    docker rm -f "$name" >/dev/null 2>&1
    : > "$log"; : > "$poll"
    host_state "before $name"
    cmd_wsreset >/dev/null
    ws_config "$tuned"
    local -a envs=(-e "WALSHADOW_PG_URL=$PG_URL"
                   -e "WALSHADOW_CH_URL=clickhouse://default:bench@127.0.0.1:${CH_NATIVE}/default"
                   -e WALSHADOW_SHADOW_PORT=5442 -e WALSHADOW_WALSENDER_BIND=127.0.0.1:5433)
    if [[ "$tuned" == tuned ]]; then
        envs+=(-e WALSHADOW_XACT_BUFFER_MAX=33554432 -e WALSHADOW_INSERTER_POOL=2 -e WALSHADOW_DECODER_POOL=2)
    fi
    t0=$(now)
    docker run --name "$name" --network=host $CAP "${envs[@]}" \
        -v "$WS_STATE:/var/lib/walshadow" -v "$WS_CONF:/etc/walshadow" \
        "$WS_IMG" >"$log" 2>&1 &
    local dockerpid=$!
    sleep 0.3
    t0=$(docker inspect -f '{{.State.StartedAt}}' "$name" 2>/dev/null || echo "$t0")
    host_peak_watch "$name" "$peak" &
    local watchpid=$!
    stopped="completed"
    while :; do
        if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
            t1=$(now); stopped="container_exited"
            fin_at=$(docker inspect -f '{{.State.FinishedAt}}' "$name" 2>/dev/null)
            break
        fi
        rows=$(chq "SELECT toString(coalesce(sum(total_rows),0)) FROM system.tables
                    WHERE database='default' AND name IN ($(inlist))")
        rows=${rows:-0}
        printf '%s rows=%s\n' "$(now)" "$rows" >> "$poll"
        sleep 2
        waited=$((waited + 2))
        if (( rows >= EXPECT_ROWS )); then
            local union="" all_rows t t_verify
            for t in "${TABLES[@]}"; do
                union+="${union:+ UNION ALL }SELECT count() AS c FROM \`default\`.\`$t\` FINAL"
            done
            all_rows=$(chq "SELECT toString(sum(c)) FROM ( $union )")
            t_verify=$(now)
            if [[ "${all_rows:-0}" -ge $EXPECT_ROWS ]]; then
                t1="$t_verify"; stopped="caught_up"
                printf 'ROWS %s\nT_VERIFY %s\n' "$all_rows" "$t_verify" >> "$log"
                break
            fi
        fi
        (( waited < deadline )) || { stopped="deadline_${deadline}s"; break; }
    done
    # The cgroup's own accounting, read BEFORE the container goes away.
    local cg="/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null).scope"
    if [[ -d "$cg" ]]; then
        mempeak=$(cat "$cg/memory.peak" 2>/dev/null || echo 0)
        { printf 'MEMPEAK_BYTES %s\nMEMEVENTS %s\nMEMSTAT %s\n' "$mempeak" \
            "$(grep -E '^(oom|oom_kill|high|max) ' "$cg/memory.events" 2>/dev/null | tr '\n' ' ')" \
            "$(grep -E '^(anon|file|shmem) ' "$cg/memory.stat" 2>/dev/null | tr '\n' ' ')"; } >> "$log"
    fi
    docker exec "$name" walshadow-stream ctl status >>"$log" 2>&1 || true
    state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    if [[ "$stopped" == caught_up ]]; then
        docker stop -t 60 "$name" >>"$log" 2>&1 || true
        wait $dockerpid 2>/dev/null; rc=$?
    else
        docker rm -f "$name" >/dev/null 2>&1
        wait $dockerpid 2>/dev/null; rc=$?
    fi
    kill $watchpid 2>/dev/null
    docker rm -f "$name" >/dev/null 2>&1
    host_state "after  $name"
    hostpeak=$(awk -v v="$(cat "$peak" 2>/dev/null || echo 0)" 'BEGIN{printf "%.1f", v/1048576}')
    if [[ -n "$t1" ]]; then
        wall=$(awk -v a="$(date -u -d "$t0" +%s.%N)" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
    else
        wall=$(awk -v a="$(date -u -d "$t0" +%s.%N 2>/dev/null)" \
                    -v b="$(date -u -d "${fin_at:-}" +%s.%N 2>/dev/null || echo 0)" \
                    'BEGIN{printf "%.1f", (b>0? b-a : 0)}')
    fi
    {
        printf 'LEG walshadow tag=%s tuned=%s\n' "$tag" "$tuned"
        printf '  stopped_by %s\n' "$stopped"
        printf '  wall_container_s %s %s\n' "$wall" \
            "$([[ "$stopped" == caught_up ]] || echo "(container lifetime: it never reached full landing)")"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' \
            "$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')"
        printf '  host_sampled_peak_mb %s\n' "$hostpeak"
        grep -E '^(MEMPEAK_BYTES|MEMEVENTS|MEMSTAT|ROWS|T_VERIFY)' "$log" | sed 's/^/  /'
        printf '  poll_last %s\n' "$(tail -1 "$poll" 2>/dev/null)"
        printf '  log %s\n' "$log"
    } | tee -a "$res"
    return 0
}

# A fresh catch-up needs fresh state: the shadow data dir, the filtered WAL, the
# transaction spill and the manifest are all "how far it got", and reusing them
# would measure a resume instead of a first landing.
cmd_wsreset() {
    docker rm -f $(docker ps -aq --filter "name=^apitap-bench-cdc-ws") >/dev/null 2>&1
    sudo rm -rf "$WS_STATE" "$WS_CONF"
    mkdir -p "$WS_STATE/shadow-data" "$WS_STATE/out" "$WS_STATE/spill" "$WS_CONF/ch-config.d"
    # The container runs as the image's postgres (uid 999) and the shadow refuses to
    # start on a data dir it does not own at 0700; a bind mount can drop the mode.
    sudo chown -R 999:999 "$WS_STATE" "$WS_CONF"
    pgq "SELECT pg_drop_replication_slot(slot_name::text) FROM pg_replication_slots
          WHERE slot_name='walshadow' AND NOT active" >/dev/null 2>&1 || true
    [[ "$(pgq "SELECT count(*) FROM pg_replication_slots WHERE slot_name='walshadow'")" == 0 ]] && \
        pgq "SELECT pg_create_physical_replication_slot('walshadow')" >/dev/null
}

# walshadow's own declaration of what it replicates: the thirty tables, initial
# load through COPY, nothing else in the database. Discrete keys only — `url` is a
# CLI and environment form, not a documented [source] key, and an unknown key in a
# non-[table] section is silently ignored, which would look like "it connected and
# replicated nothing".
ws_config() {
    local tuned=${1:-no} t tmp
    tmp=$(mktemp)
    {
        echo '[source]'
        echo 'host = "127.0.0.1"'
        echo "port = $PG_PORT"
        echo 'user = "postgres"'
        echo 'password = "bench"'
        echo "dbname = \"$PG_DB\""
        echo 'sslmode = "disable"'
        echo 'slot = "walshadow"'
        echo
        echo '[ch]'
        echo 'host = "127.0.0.1"'
        echo "port = $CH_NATIVE"                      # NATIVE, required
        echo 'database = "default"'
        echo 'user = "default"'
        echo 'password = "bench"'
        [[ "$tuned" == tuned ]] && echo 'byte_budget = 33554432'
        echo
        if [[ "$tuned" == tuned ]]; then
            # walshadow's configuration.md, "Memory budget": the default is one half
            # of the cgroup limit WITH A 512 MiB MINIMUM, a floor a 256 MB cage
            # cannot satisfy.
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
            echo 'initial_load = "copy"'
        done
    } > "$tmp"
    sudo install -m 0644 -o 999 -g 999 "$tmp" "$WS_CONF/ch-config.toml"
    rm -f "$tmp"
    echo "  walshadow config: $(grep -c '^\[table' "$WS_CONF/ch-config.toml") table blocks, tuned=$tuned"
}

# ── verification ───────────────────────────────────────────────────────────────
# Rows plus the order-independent checksum, per table, source against destination.
# `--fresh` recomputes the source side, which the end-of-window check needs: the
# cached sums describe the SEED, and by then the seed has taken 2.97M changes.
cmd_verify() {
    local fresh=${1:-} srcfile="$WORK/src.txt" chfile="$WORK/dst.txt"
    local t ok=0 bad=0 s d body=""
    if [[ -n "$fresh" ]]; then
        src_digest_batch | sort > "$srcfile"
    else
        : > "$srcfile"
        for t in "${TABLES[@]}"; do printf '%s\t%s\n' "$t" "$(srcsum_of "$t")" >> "$srcfile"; done
    fi
    # Both sides must be 30 lines of `table<TAB>digest`. A malformed side is a
    # HARNESS fault, not a data verdict, and must never print as thirty MISMATCHes.
    local nsrc ndig
    nsrc=$(wc -l < "$srcfile")
    ndig=$(awk -F'\t' 'length($2) == 0 { n++ } END { print n+0 }' "$srcfile")
    if [[ "$nsrc" != "${#TABLES[@]}" || "$ndig" != 0 ]]; then
        echo "VERIFY_BROKEN the source side is malformed: $nsrc lines (want ${#TABLES[@]}), $ndig with an empty digest (want 0):"
        head -2 "$srcfile" | cat -A | sed "s/^/    /"
        return 2
    fi
    ch_digest_batch 2>/dev/null | sort > "$chfile"
    # One missing table voids a whole ClickHouse query — there is no TRY — so a
    # partial landing (a killed leg) would come back as NO verdicts at all instead
    # of naming the one table that landed. Fall back to per-table queries, built
    # from the same shared definition, for whatever the batch did not return.
    if (( $(wc -l < "$chfile") < ${#TABLES[@]} )); then
        local missing=0 d2
        for t in "${TABLES[@]}"; do
            grep -q "^$t	" "$chfile" && continue
            missing=$((missing + 1))
            d2=$(chsum "$t")
            [[ -n "$d2" ]] && printf '%s\t%s\n' "$t" "$d2" >> "$chfile"
        done
        echo "  (batch covered $(( ${#TABLES[@]} - missing )) of ${#TABLES[@]} tables; $missing checked one at a time)"
        sort -o "$chfile" "$chfile"
    fi
    while IFS=$'\t' read -r t d; do
        s=$(awk -F'\t' -v k="$t" '$1==k{print $2}' "$srcfile")
        if [[ -n "$d" && "$s" == "$d" ]]; then
            body+="$(printf 'VERIFY %s rows=%s MATCH %s' "$t" "${d%%|*}" "$d")"$'\n'
            ok=$((ok+1))
        else
            body+="$(printf 'VERIFY %s MISMATCH\n  src=%s\n  dst=%s' "$t" "$s" "${d:-<no table / no row>}")"$'\n'
            bad=$((bad+1))
        fi
    done < "$chfile"
    printf '%s' "$body" | tee -a "$WORK/verify.log"
    echo "VERIFY_SUMMARY checked=$((ok + bad)) of ${#TABLES[@]} match=$ok mismatch=$bad"
    # A table the destination does not have at all produces no row in the batch, so
    # it would never reach the loop above and "0 mismatch" would be a statement
    # about nothing. Loud, rather than quietly green.
    if (( ok + bad < ${#TABLES[@]} )); then
        echo "VERIFY_INCOMPLETE $(( ${#TABLES[@]} - ok - bad )) of ${#TABLES[@]} tables are not in the destination at all — THIS IS NOT A PASS"
        while IFS=$'\t' read -r t _; do
            grep -q "^$t	" "$chfile" || echo "VERIFY_ABSENT $t"
        done < "$srcfile"
        return 1
    fi
    chq "SELECT concat('TOTAL_ROWS ', sum(total_rows)) FROM system.tables
          WHERE database='default' AND name IN ($(inlist))"
    [[ $bad -eq 0 && $((ok + bad)) -eq ${#TABLES[@]} ]]
}

# ── the cold catch-up (the headline) ───────────────────────────────────────────
# ROUNDS rounds, arms interleaved, each from a pre-seeded source and an EMPTY
# destination, each checksum-verified before any number counts.
cmd_catchup() {
    local rounds=${ROUNDS:-2} r arm
    : > "$RESULTS"; : > "$WORK/verify.log"; : > "$WORK/host-state.log"
    for ((r = 1; r <= rounds; r++)); do
        for arm in "$@"; do
            echo "=== round $r · $arm  ($(date -u +%H:%M:%S))"
            cmd_drop >/dev/null
            cmd_slotdrop >/dev/null
            case "$arm" in
                apitap)  cmd_leg_apitap "r$r" catchup 0 ;;
                wsdef)   cmd_leg_ws "r$r" no ;;
                wstuned) cmd_leg_ws "r$r" tuned ;;
                # a leg name is unique per arm, so two campaigns can share $WORK's
                # log directory without truncating each other
                *) echo "unknown arm $arm" >&2; return 2 ;;
            esac
            cmd_verify
            cmd_drop >/dev/null
            cmd_slotdrop >/dev/null
            df -h / | tail -1 | sed 's/^/  disk /'
        done
    done
    cmd_results
}

# ── the 5-minute window ────────────────────────────────────────────────────────
cmd_window() {
    local i res="$WORK/window-results.txt"
    local wlog="$WORK/window.log" win0 win1
    : > "$res"; : > "$wlog"
    echo "== fresh start for the window: empty destination, no slot, no state =="
    cmd_drop >/dev/null
    cmd_slotdrop >/dev/null
    echo
    echo "== establishing the group (the bootstrap; reported, not the headline) =="
    cmd_leg_apitap window-bootstrap catchup 0 "$res" | tee -a "$wlog"
    echo
    echo "== the bootstrap's landing, checksum-verified BEFORE any traffic =="
    cmd_verify || { echo "WINDOW ABORTED: the bootstrap's landing does not match the source"; return 1; }
    echo
    echo "== the change stream: $TICKS ticks x $TICK_S s, $INS ins + $UPD upd + $DEL del"
    echo "   rows per table per tick, across all ${#TABLES[@]} tables =="
    win0=$(now)
    # The writer is the SOURCE, uncapped by design, and it is generated on the host
    # and piped straight into one psql session inside the source container: every
    # statement is its own implicit transaction and a pg_sleep paces the ticks. Its
    # progress is an artifact (\echo WINDOW_PROGRESS), never a pgrep.
    # ONE redirection around the WHOLE pipeline: a generator that dies on stderr
    # (it did, twice) leaves an empty artifact and nothing to read.
    { python3 "$HERE/bench-capped-pg-ch-cdc-window.py" "$TICKS" "$TICK_S" "${#TABLES[@]}" \
        $INS $UPD $DEL \
      | docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -q ; } > "$WORK/writer.log" 2>&1 &
    local writer=$!
    # Confirm by ARTIFACT, immediately: the writer's own progress lines. A pgrep of
    # the pipeline would match the check itself.
    sleep 5
    echo "   writer host pid $writer; ticks completed so far: $(grep -c WINDOW_PROGRESS "$WORK/writer.log")"

    for ((i = 1; i <= 5; i++)); do
        sleep "$DRAIN_EVERY"
        echo
        echo "=== drain $i of 5  ($(date -u +%H:%M:%S))  writer ticks done: $(grep -c WINDOW_PROGRESS "$WORK/writer.log")"
        local b a
        b=$(chq "SELECT toString(sum(total_rows)) FROM system.tables
                 WHERE database='default' AND name IN ($(inlist))")
        # The fifth drain runs a SECOND, empty drain in the same process: the
        # idempotence of a drain with nothing to do, and a descriptor count across
        # two runs of one process.
        cmd_leg_apitap "drain$i" drain "$([[ $i == 5 ]] && echo 1 || echo 0)" "$res" | tee -a "$wlog"
        a=$(chq "SELECT toString(sum(total_rows)) FROM system.tables
                 WHERE database='default' AND name IN ($(inlist))")
        {
            echo "  window $i: ch_total_rows $b -> $a"
            echo "  slot: $(pgq "SELECT concat_ws(' ', slot_name, 'active=', active,
                    'wal_status=', coalesce(wal_status,'-'), 'retained=',
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)))
                    FROM pg_replication_slots" | tr '\n' ' ')"
            echo "  watermark: $(watermarks)"
            echo "  host: $(tail -1 "$WORK/host-state.log")"
        } | tee -a "$wlog"
    done

    echo
    echo "== waiting for the writer to finish =="
    wait $writer
    win1=$(now)
    if ! grep -q WINDOW_DONE "$WORK/writer.log"; then
        echo "WRITER FAILED: the stream never reached its last tick. Its log:" | tee -a "$wlog"
        tail -20 "$WORK/writer.log" | sed "s/^/    /" | tee -a "$wlog"
        return 1
    fi
    echo "WRITER $(grep WINDOW_DONE "$WORK/writer.log" | head -1)" | tee -a "$wlog"
    echo "WRITER progress lines: $(grep -c WINDOW_PROGRESS "$WORK/writer.log")" | tee -a "$wlog"
    local wwall txs changes
    wwall=$(awk -v a="$win0" -v b="$win1" 'BEGIN{printf "%.1f", b-a}')
    changes=$(( TICKS * (INS + UPD + DEL) * ${#TABLES[@]} ))
    txs=$(( TICKS * ${#TABLES[@]} ))
    echo "WRITER_TOTAL ticks=$TICKS tables=${#TABLES[@]} transactions=$txs changes=$changes" \
         "writer_wall_s=$wwall changes_per_s=$(awk -v c="$changes" -v w="$wwall" 'BEGIN{printf "%.0f", c/w}')" \
         | tee -a "$wlog"
    echo "WRITER source row counts now (must be 1000000 + ${INS}·${TICKS} - ${DEL}·${TICKS} = $(( 1000000 + INS*TICKS - DEL*TICKS ))):" | tee -a "$wlog"
    pgq "SELECT concat('  ', c, ' tables at that count, min=', min(n), ' max=', max(n))
          FROM (SELECT (xpath('/row/c/text()', query_to_xml(
                 format('SELECT count(*) c FROM public.%I', table_name),
                 false,true,'')))[1]::text::bigint AS n
                FROM information_schema.tables
                WHERE table_schema='public' AND table_name LIKE 'cdc\_pg\_t%') x
          GROUP BY 1" | tee -a "$wlog"

    echo
    echo "== the final drain: whatever the writer's last commits produced =="
    cmd_leg_apitap final drain 1 "$res" | tee -a "$wlog"
    echo
    echo "== and the destination against the source, FRESH checksums =="
    cmd_verify fresh | tee -a "$wlog"
}

# Every table's watermark, and how many distinct ones the group holds. A group that
# advances as ONE unit is the property under test, so all thirty equal is the
# expected reading and anything else is a finding.
watermarks() {
    chq "SELECT concat('lsn ', min(watermark), ' .. ', max(watermark),
                       ' distinct=', uniqExact(watermark), ' tables=', count())
          FROM (SELECT dest_table, argMax(watermark, synced_at) AS watermark
                FROM _apitap_state FINAL
                WHERE startsWith(dest_table, 'cdc_pg_t') GROUP BY dest_table)"
}

# ── leak checks ────────────────────────────────────────────────────────────────
cmd_leaks() {
    echo "== descriptors: peak and final, per leg, measured from inside each container =="
    grep -hE '^FD_PEAK|^FD_END|^FDCOUNT|^IDEMPOTENT' "$WORK"/logs/apitap-bench-cdc-*.log 2>/dev/null \
        | sed "s|^|$(printf '%-34s' "$(basename "$WORK")")  |"
    echo
    echo "== replication slots and the WAL they retain =="
    pgq "SELECT concat('  ', slot_name, ' type=', slot_type, ' active=', active,
                      ' wal_status=', coalesce(wal_status,'-'), ' retained=',
                      pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)))
          FROM pg_replication_slots ORDER BY slot_name"
    pgq "SELECT concat('  publications: ', coalesce(string_agg(pubname, ','), '(none)'))
          FROM pg_publication"
    pgq "SELECT concat('  total WAL held by slots: ',
                      pg_size_pretty(coalesce(sum(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)),0)))
          FROM pg_replication_slots"
    pgq "SELECT concat('  spilly transactions on the slot(s): ',
                      coalesce(sum(spill_txns),0), ' txns / ', coalesce(sum(spill_count),0), ' spills / ',
                      pg_size_pretty(coalesce(sum(spill_bytes),0)))
          FROM pg_stat_replication_slots"
    echo
    echo "== _apitap_lease rows: a leaked lease is a table a peer cannot claim =="
    chq "SELECT concat('  rows=', count()) FROM _apitap_lease" 2>/dev/null \
        || echo "  (no _apitap_lease table)"
    chq "SELECT concat('  UNCOLLECTED AND STILL LIVE = ', count())
          FROM (SELECT dest_key, token, argMax(collected, seq) AS c, argMax(expires_at, seq) AS e
                FROM _apitap_lease GROUP BY dest_key, token)
          WHERE c = 0 AND e > now64(6)" 2>/dev/null || true
    echo
    echo "== staging / marker leftovers (run-scoped key tables, staging, locks) =="
    chq "SELECT concat('  ', name, ' engine=', engine) FROM system.tables
          WHERE database='default' AND (position(name, '__apitap') > 0
                                          OR position(name, '__ws') > 0)"
    echo "== _apitap_cdc_pending =="
    chq "SELECT concat('  rows=', count()) FROM _apitap_cdc_pending" 2>/dev/null \
        || echo "  (no _apitap_cdc_pending table)"
    echo "== the group's own state rows =="
    # arrayStringConcat(groupUniqArray(...)), not string_agg(DISTINCT ...): ClickHouse
    # has no string_agg and its parser reads the DISTINCT as a function name.
    chq "SELECT concat('  rows=', count(), ' mode=', arrayStringConcat(groupUniqArray(mode)),
                      ' cursor=', arrayStringConcat(groupUniqArray(cursor_col)),
                      ' distinct_watermarks=', uniqExact(watermark),
                      ' watermark=', min(watermark), ' .. ', max(watermark),
                      ' last_rows_total=', sum(last_rows))
          FROM _apitap_state FINAL WHERE startsWith(dest_table, 'cdc_pg_t')"
}

cmd_results() {
    echo
    printf '%-10s %-18s %10s %12s %-18s %-22s %s\n' ARM TAG WALL_S PEAK_MB STOPPED DOCKER ROWS
    python3 - "$RESULTS" <<'PY'
import re, sys, statistics
legs, cur = [], None
for line in open(sys.argv[1]):
    line = line.rstrip()
    m = re.match(r"LEG (\S+) tag=(\S+)", line)
    if m:
        cur = {"arm": m.group(1), "tag": m.group(2)}
        legs.append(cur)
        continue
    m = re.match(r"\s+(\S+) (.*)", line)
    if m and cur is not None:
        cur[m.group(1)] = m.group(2)
for d in legs:
    rows = d.get("ROWS", "-")
    dm = re.search(r"rows=(\d+)", d.get("DRAIN", ""))
    if dm:
        rows = dm.group(1)
    print(f"{d['arm']:<10} {d['tag']:<18} {d.get('wall_container_s','?'):>10} "
          f"{d.get('cgroup_memory_peak_mb','?'):>12} {d.get('stopped_by','-'):<18} "
          f"{d.get('docker_state','?'):<22} {rows}")
print()
for arm in sorted({d["arm"] for d in legs}):
    ds = [d for d in legs if d["arm"] == arm]
    ws = [float(d["wall_container_s"].split()[0]) for d in ds if "wall_container_s" in d]
    pk = [float(d["cgroup_memory_peak_mb"]) for d in ds
          if d.get("cgroup_memory_peak_mb", "0") not in ("?", "0")]
    if ws:
        line = f"MEDIAN {arm} wall_s {statistics.median(ws):.1f}  rounds {[round(w,1) for w in ws]}"
        if pk:
            line += f"  peak_mb {max(pk):.1f}"
        print(line)
PY
    echo
    echo "checksum verdicts per leg: the VERIFY lines above (the destination is dropped after each)"
}

cmd_state() {
    echo "== this campaign's containers =="
    docker ps --filter "name=apitap-bench-cdc" --format '  {{.Names}}  {{.Image}}  {{.Status}}  {{.Ports}}'
    echo "== this campaign's volumes (on disk) =="
    for v in apitap-bench-cdc-pg-data apitap-bench-cdc-ch-data; do
        echo "  $v $(sudo du -sh /var/lib/docker/volumes/$v/_data 2>/dev/null | cut -f1)"
    done
    echo "== walshadow's own state on disk (its container is gone) =="
    echo "  $(du -sh "$WS_STATE" 2>/dev/null | cut -f1)"
    echo "== the seed (KEPT for the next campaign) =="
    pgq "SELECT concat('  ', COUNT(*), ' tables x ', MIN(c), '-', MAX(c), ' columns, ',
                      pg_size_pretty(SUM(b)))
          FROM (SELECT table_name, (SELECT count(*) FROM information_schema.columns
                                    WHERE table_schema='public' AND table_name=t.table_name) c,
                       pg_total_relation_size(quote_ident(table_name)) b
                FROM information_schema.tables t
                WHERE table_schema='public' AND table_name LIKE 'cdc\_pg\_t%') x"
    pgq "SELECT concat('  rows: ', c, ' tables, total ', sum(n))
          FROM (SELECT (xpath('/row/c/text()', query_to_xml(
                 format('SELECT count(*) c FROM public.%I', table_name),
                 false,true,'')))[1]::text::bigint AS n
                FROM information_schema.tables
                WHERE table_schema='public' AND table_name LIKE 'cdc\_pg\_t%') x
          GROUP BY 1 HAVING count(*) > 0" 2>/dev/null \
        | head -3
    echo "== destination leftovers =="
    chq "SELECT concat('  cdc_pg_t* tables: ', count()) FROM system.tables
          WHERE database='default' AND startsWith(name, 'cdc_pg_t')"
    echo "== replication slots left =="
    pgq "SELECT concat('  ', coalesce(string_agg(slot_name || ' active=' || active, ', '), '(none)'))
          FROM pg_replication_slots"
    echo "== disk =="
    df -h / | tail -1 | sed 's/^/  /'
}

case "${1:-}" in
rig)      cmd_rig ;;
seed)     cmd_seed ;;
srcsum)   cmd_srcsum ;;
drop)     cmd_drop ;;
slotdrop) cmd_slotdrop ;;
wsreset)  cmd_wsreset ;;
catchup)  shift; cmd_catchup "$@" ;;
leg)      shift; cmd_leg_apitap "$@" ;;
wsleg)    shift; cmd_leg_ws "$@" ;;
verify)   shift; cmd_verify "${1:-}" ;;
window)   cmd_window ;;
leaks)    cmd_leaks ;;
wm)       watermarks ;;
results)  cmd_results ;;
state)    cmd_state ;;
agg)      shift; case "${1:-}" in
               src) srcsum "$2" ;;
               dst) chsum "$2" ;;
               *) echo "usage: $0 agg src|dst TABLE" >&2; exit 2 ;;
           esac ;;
*) sed -n 2,22p "$0"; exit 2 ;;
esac