#!/usr/bin/env bash
# Capped MySQL -> ClickHouse **CDC**: apitap 0.57.0 (PyPI) vs ingestr 1.1.61
# (bruin-data) at the capped tier, with a SHORT window.
#
#   30 tables x 1,000,000 rows x 15 columns, ALL 30 in ONE CDC group — one
#   binlog stream, one replication position, one transfer call — each tool inside
#   ONE container capped at --cpus=0.5 --memory=256m --memory-swap=256m.
#
#   ./bench-capped-my-ch-cdc-0.57.sh rig       # this campaign's MySQL source
#   ./bench-capped-my-ch-cdc-0.57.sh seed      # 30 identical tables (KEPT between legs)
#   ./bench-capped-my-ch-cdc-0.57.sh srcsum    # source aggregates (cached per table)
#   ./bench-capped-my-ch-cdc-0.57.sh srcsumx   # ...recomputed: the window moved the source
#   ./bench-capped-my-ch-cdc-0.57.sh catchup ARM...   # interleaved cold catch-up legs
#   ./bench-capped-my-ch-cdc-0.57.sh verify [--fresh]  # rows + checksum, per table
#   ./bench-capped-my-ch-cdc-0.57.sh drop      # destination tables (the SEEDS stay)
#   ./bench-capped-my-ch-cdc-0.57.sh window    # the 5-minute window
#   ./bench-capped-my-ch-cdc-0.57.sh leaks     # the end-of-campaign leak checks
#   ./bench-capped-my-ch-cdc-0.57.sh state     # containers? disk? seeds? binlogs?
#   ./bench-capped-my-ch-cdc-0.57.sh results   # the per-leg tables
#   ./bench-capped-my-ch-cdc-0.57.sh binlog    # coordinates, sizes, who is attached
#   ./bench-capped-my-ch-cdc-0.57.sh wm        # the group's watermark, and is it ONE
#   ./bench-capped-my-ch-cdc-0.57.sh leg TAG MODE [twice]  # one apitap leg, by hand
#   ./bench-capped-my-ch-cdc-0.57.sh ingleg TAG [tuned]   # one ingestr leg, by hand
#   ./bench-capped-my-ch-cdc-0.57.sh agg src|dst TABLE    # one table's digest
#
# ARM is apitap | ingestr | ingestrtuned. RESULTS=<file> redirects the per-leg table;
# ING_DEST_URI=<uri> points the rival at a destination other than ClickHouse, which is
# how the control validates its landing at all (ingestr refuses ClickHouse for CDC).
#
# The rig is this campaign's own: apitap-bench-cdc-my (mysql:8.0, log_bin=ON,
# binlog_format=ROW, binlog_row_image=FULL, its own volume, free host port). The
# DESTINATION is the previous campaign's apitap-bench-cdc-ch, which this campaign
# is explicitly allowed to share: only cdc_my_* objects are ever created or dropped
# there, and the cdc_pg_* rows another campaign left in _apitap_state are left
# alone. No container that existed before this campaign is touched, moved or
# restarted, and nothing outside this campaign's own names is ever dropped.
set -uo pipefail

export MY_C=${MY_C:-apitap-bench-cdc-my}
export CH_C=${CH_C:-apitap-bench-cdc-ch}
export MY_DB=${MY_DB:-bench}
# 3313, not 3312: 3312 is taken by apitap-tls-my, which binds 0.0.0.0 rather than
# 127.0.0.1. A scan that greps for "127.0.0.1:" misses exactly the bindings that
# collide, and docker's answer to that is "port is already allocated" on a
# container left in the Created state.
MY_PORT=${MY_PORT:-3313}
CH_HTTP=${CH_HTTP:-8128}
CH_NATIVE=${CH_NATIVE:-9128}
MY_URL="mysql://root:bench@127.0.0.1:${MY_PORT}/${MY_DB}"
CH_URL="clickhouse://default:bench@127.0.0.1:${CH_HTTP}/default"

# The capped tier. Inheritable, so a wrapper can move the ceiling without editing
# this file — and so nothing can move it by accident.
CAP=${CAP:-"--cpus=0.5 --memory=256m --memory-swap=256m"}
APITAP_SO_MD5=41e9f7c252d3e1eb5403b70f87bf5435
INGESTR_VERSION=1.1.61
SP_APITAP=/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages
ING_BIN=/home/ubuntu/.cache/ingestr/bin/v${INGESTR_VERSION}/Linux_x86_64/ingestr
IMG=python:3.13-slim
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/bench-cdc-my}
SEED_ROWS=${SEED_ROWS:-1000000}
NT=${NT:-30}
TABLES=()
for ((i = 1; i <= NT; i++)); do TABLES+=("$(printf 'cdc_my_t%02d' "$i")"); done
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
# The three disjoint id bands the window writes to. INS clones rows out of the
# first band and inserts them at +2,000,000, so the two never collide.
INS_LO=${INS_LO:-2000000}
UPD_LO=${UPD_LO:-1}
DEL_LO=${DEL_LO:-900001}
RESULTS=${RESULTS:-$WORK/results.txt}
# The seed's fifteen columns, in the schema file's order. Spelled once so the two
# routes into cdc_my_t01 (a local clone and a mysqldump pipe) cannot disagree about
# which columns are being copied.
SEED_COLS="id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, \
float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text"
# Six concurrent source scans: the aggregate is a full 1M-row pass and thirty of
# them in sequence is 30 x 1m32s. This is source-side work, not a measurement.
SRC_JOBS=${SRC_JOBS:-6}
mkdir -p "$WORK/out" "$WORK/logs"

now() { date +%s.%N; }
myq()  { docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" -e "$1" 2>/dev/null; }
myq_() { docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" -e "$1"; }
chq()  { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1" 2>/dev/null; }
chq_() { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1"; }
inlist() { printf "'%s'," "${TABLES[@]}"; }

# ── the rig ────────────────────────────────────────────────────────────────────
# Everything CDC needs from the source is a server setting that needs a restart, so
# they are all command-line arguments. The values that are NOT CDC's business are
# left at the image's defaults, and the two that are worth naming are:
#
#   gtid-mode=ON             required by the RIVAL and not by us: ingestr's MySQL
#                             CDC path refuses to start with "MySQL CDC requires
#                             gtid_mode=ON for lineage-safe checkpoints; current mode
#                             is \"OFF\"". Its documented requirement list does NOT
#                             mention GTID, so this was found by running it. The rig
#                             is therefore configured to the rival's requirement and
#                             not to apitap's — the fair direction — and apitap's own
#                             CDC lane is verified under GTID=ON by the control.
#   innodb-buffer-pool-size 2G  the image default is 128M, which makes a 13 GB
#                              seed's INSERT..SELECT and the CDC snapshot's full
#                              scans crawl. This is a setting on the UNCAPPED
#                              source and it helps both tools identically.
#   innodb-flush-log-at-trx-commit / sync-binlog stay at 1 (the durability
#   defaults), so the window's writer pays for an fsync per statement exactly as the
#   PostgreSQL window did.
#
# binlog_expire_logs_seconds=86400 is the bound that keeps a falling-behind drain
# from filling this shared disk: the binlog is the backlog, and at 24 h the server
# rotates it out from under a stalled reader rather than growing without limit.
cmd_rig() {
    echo "== this campaign's own source container =="
    # Idempotent, and deliberately so: re-running `rig` on a live, answering
    # container must NOT destroy it, because that would throw away the seed and the
    # binlog history for nothing. RECREATE=1 forces it.
    #
    # RECREATE has to be consulted HERE, in the keep-branch's condition. It used to
    # be read only inside the else, which made `RECREATE=1 bash … rig` print the
    # same "keeping it" line as a plain re-run and change nothing — a documented knob
    # that did nothing, discovered the hard way when a gtid_mode=ON rebuild appeared
    # to succeed and the server still answered OFF.
    if [[ "${RECREATE:-0}" != "1" ]] &&
       [[ "$(docker inspect -f '{{.State.Running}}' "$MY_C" 2>/dev/null)" == "true" ]] &&
       myq "SELECT 1" >/dev/null 2>&1; then
        echo "  $MY_C is already up and answering; keeping it (RECREATE=1 to rebuild)"
    else
        RECREATE=1 cmd_rig_create
    fi
    echo "  $MY_C  mysql:8.0  127.0.0.1:${MY_PORT}  db $MY_DB  root/bench"
    echo "== the CDC preconditions, read from the running server =="
    myq "SELECT CONCAT('  ', variable_name, ' = ', variable_value)
          FROM performance_schema.global_variables
          WHERE variable_name IN ('log_bin','binlog_format','binlog_row_image',
              'binlog_row_value_options','server_id','gtid_mode','enforce_gtid_consistency',
              'transaction_compression','innodb_buffer_pool_size',
              'innodb_flush_log_at_trx_commit','sync_binlog','binlog_expire_logs_seconds')
          ORDER BY variable_name"
    myq "SELECT CONCAT('  version ', version())"
    echo "== ingestr's other documented CDC requirements, checked from the server =="
    myq "SELECT CONCAT('  ', table_name, ': columns=', COUNT(*),
              ' pk=', MAX(CASE WHEN column_key='PRI' THEN 1 ELSE 0 END),
              ' enum/set/bit=', SUM(CASE WHEN data_type IN ('enum','set','bit') THEN 1 ELSE 0 END))
          FROM information_schema.columns
          WHERE table_schema='$MY_DB' AND table_name='cdc_my_t01'
          GROUP BY table_name"
    # apitap's OWN precondition, learned from the control leg rather than from the
    # docs: the MySQL CDC lane refuses a native JSON column outright. Asserted here so
    # the schema cannot drift back into a shape the engine declines to run, and so a
    # reader sees the restriction stated as a precondition and not discovered at hour
    # two. MariaDB's JSON is LONGTEXT, which is why the seed stores it as longtext.
    myq "SELECT CONCAT('  apitap MySQL-CDC type gate — json columns in the seed: ',
              COUNT(*), IF(COUNT(*)=0,' (OK: none, the CDC lane will run)',' (REFUSED by the CDC lane)'))
          FROM information_schema.columns
          WHERE table_schema='$MY_DB' AND table_name LIKE 'cdc\_my\_t%'
            AND data_type='json'"
    echo "  root's grants cover SELECT + RELOAD + REPLICATION SLAVE + REPLICATION CLIENT:"
    myq "SHOW GRANTS FOR CURRENT_USER" | sed 's/^/    /'
    echo "== the destination (the previous campaign's container, shared) =="
    docker ps --filter "name=^$CH_C$" --format '  {{.Names}}  {{.Image}}  {{.Status}}  {{.Ports}}'
    chq "SELECT concat('  ClickHouse ', version())"
    echo "== what this campaign must not touch =="
    # WITH an explicit FROM: a bare countIf(...) has nothing to count, the server
    # rejects it, and the suppressed stderr prints as an EMPTY line — which reads as
    # "there are none" when it means "the check never ran".
    chq "SELECT concat('  pre-existing cdc_pg_* _apitap_state rows in the destination: ',
                      countIf(startsWith(dest_table, 'cdc_pg_')), ' (total rows ',
                      count(), ')')
          FROM _apitap_state FINAL"
    df -h / | tail -1 | sed 's/^/  disk /'
}

cmd_rig_create() {
    docker rm -f "$MY_C" >/dev/null 2>&1
    docker volume create apitap-bench-cdc-my-data >/dev/null
    docker run -d --name "$MY_C" \
        -p 127.0.0.1:${MY_PORT}:3306 \
        -e MYSQL_ROOT_PASSWORD=bench -e MYSQL_DATABASE="$MY_DB" -e MYSQL_ROOT_HOST=% \
        -v apitap-bench-cdc-my-data:/var/lib/mysql \
        mysql:8.0 \
            --server-id=3302 \
            --log-bin=mysql-bin \
            --binlog-format=ROW \
            --binlog-row-image=FULL \
            --gtid-mode=ON \
            --enforce-gtid-consistency=ON \
            --binlog-expire-logs-seconds=86400 \
            --innodb-buffer-pool-size=2G \
            --max-connections=200 >/dev/null
    local i
    for i in $(seq 240); do
        myq "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 2
    done
    echo "RIG FAILED: $MY_C never answered after 480 s"
    docker logs "$MY_C" 2>&1 | tail -20
    return 1
}

# ── the seed ──────────────────────────────────────────────────────────────────
# The DDL is bench.bench_my_1m's, verbatim, copied out of the rig's existing MySQL
# (benchmarks/bench-capped-my-ch-cdc-schema.sql) — so the thirty tables are the
# SAME shape the capped MySQL -> CH bulk campaign validated against, and a
# difference can only come from the transfer. t01 is loaded from the rig's
# bench_my_1m over a pipe; t02..t30 are LIKE-clones of t01.
#
# The seed is loaded with SESSION sql_log_bin=0. Two reasons, both stated in the
# report: a row-image binlog of a 13 GB seed would double the disk cost of a box
# that has 39 GB free, and a real pre-seeded table's binlog was rotated away long
# ago — it is the state before the campaign, not part of it. Every leg's CDC run
# therefore starts from an EMPTY binlog and mints its own coordinate, which is
# exactly the position a production source is in. The seed's own content is
# unaffected and both tools see the identical starting state.
cmd_seed() {
    local t0 t
    t0=$(now)
    echo "SEED: $# tables x $SEED_ROWS rows x 15 columns (bench.bench_my_1m's schema, verbatim)"
    if [[ "$(myq "SELECT COUNT(*) FROM information_schema.tables
                   WHERE table_schema='$MY_DB' AND table_name='cdc_my_t01'")" == "1" ]] &&
       [[ "$(myq "SELECT COUNT(*) FROM cdc_my_t01")" == "$SEED_ROWS" ]]; then
        echo "  cdc_my_t01 already at $SEED_ROWS rows"
    else
        myq "DROP TABLE IF EXISTS cdc_my_t01" >/dev/null
        docker exec -i "$MY_C" mysql -uroot -pbench "$MY_DB" \
            < "$HERE/bench-capped-my-ch-cdc-schema.sql"
        echo "  loading cdc_my_t01 (binlog off for the load)"
        # Two routes to the same dataset, and the cheap one is only taken when it is
        # PROVEN to be the same dataset: this rig's own bench_my_1m is compared
        # against apitap-bench-my.bench_my_1m with the campaign's own aggregate
        # first, and only an exact match clones locally. Anything else takes the
        # canonical route — a mysqldump pipe from the rig's container — so the
        # thirty tables can only ever be the bulk arm's table.
        local want got_local
        want=$(MY_C=apitap-bench-my bash "$HERE/bench-capped-my-ch-validator.sh" \
                   src bench_my_1m 2>/dev/null)
        got_local=$(MY_C="$MY_C" bash "$HERE/bench-capped-my-ch-validator.sh" \
                        src bench_my_1m 2>/dev/null)
        if [[ -n "$want" && "$want" == "$got_local" ]]; then
            echo "    bench_my_1m's aggregate matches apitap-bench-my.bench_my_1m exactly"
            echo "    ($want) — cloning it locally"
            myq "SET SESSION sql_log_bin=0;
                 INSERT INTO cdc_my_t01 ($SEED_COLS)
                 SELECT $SEED_COLS FROM bench_my_1m" >/dev/null
        else
            echo "    no local copy, or its aggregate differs ($got_local vs $want)"
            echo "    loading from apitap-bench-my.bench_my_1m over a pipe"
            { echo "SET SESSION sql_log_bin=0;"
              docker exec apitap-bench-my mysqldump -uroot -pbench \
                  --single-transaction --quick --hex-blob --no-tablespaces \
                  --default-character-set=utf8mb4 bench bench_my_1m 2>/dev/null
            } | docker exec -i "$MY_C" mysql -uroot -pbench "$MY_DB"
        fi
        echo "  cdc_my_t01 rows: $(myq "SELECT COUNT(*) FROM cdc_my_t01")  warnings: $(myq "SELECT @@warning_count")"
    fi
    for t in "${TABLES[@]:1}"; do
        if [[ "$(myq "SELECT COUNT(*) FROM information_schema.tables
                      WHERE table_schema='$MY_DB' AND table_name='$t'")" == "1" ]]; then
            [[ "$(myq "SELECT COUNT(*) FROM \`$t\`")" == "$SEED_ROWS" ]] && continue
            echo "  $t exists with the wrong row count, rebuilding"
            myq "DROP TABLE \`$t\`" >/dev/null
        fi
        myq "SET SESSION sql_log_bin=0; CREATE TABLE \`$t\` LIKE cdc_my_t01;
             INSERT INTO \`$t\` SELECT * FROM cdc_my_t01" >/dev/null
    done
    printf 'SEED_SECONDS %.1f\n' "$(echo "$(now) - $t0" | bc -l)"
    echo "== the seed, verified =="
    myq "SELECT CONCAT('  declared: ', COUNT(*), ' tables, columns each ', MIN(c), '-', MAX(c),
              ', ', FORMAT(ROUND(SUM(b)/1073741824, 2), 2), ' GiB on disk')
          FROM (SELECT t.table_name,
                  (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_schema=t.table_schema AND table_name=t.table_name) c,
                  (SELECT data_length+index_length FROM information_schema.tables
                    WHERE table_schema=t.table_schema AND table_name=t.table_name) b
                FROM information_schema.tables t
                WHERE t.table_schema='$MY_DB' AND t.table_name LIKE 'cdc\_my\_t%') x"
    echo "  EXACT row counts (COUNT(*), not information_schema's estimate):"
    exact_counts
    echo "  the estimate, for the record — it is NOT what the rows are:"
    myq "SELECT CONCAT('    information_schema.table_rows would have said: ',
              MIN(table_rows), '-', MAX(table_rows))
          FROM information_schema.tables
          WHERE table_schema='$MY_DB' AND table_name LIKE 'cdc\_my\_t%'"
    echo "== the binlog coordinates before any run =="
    cmd_binlogstate
    echo "  the seed's own digest (srcsum prints the same one 30x = they are clones):"
    echo "    $(bash "$0" agg src cdc_my_t01)"
}

cmd_binlogstate() {
    # SHOW MASTER STATUS, not SHOW BINARY LOG STATUS: this 8.0.46 server answers the
    # latter with a syntax error from the client, and the older spelling is the one
    # that carries the same (file, position) pair. The basename comes from the server
    # rather than being spelled `binlog.*` here, because this rig's --log-bin is
    # mysql-bin and a hardcoded prefix reports "0 files" for a rig with 2.9 MB of
    # binlog on disk.
    # Plain `SHOW MASTER STATUS`, not `FROM (SHOW MASTER STATUS) x`: this server
    # rejects the derived-table form, and the bare statement is the one that carries
    # the (file, position) pair anyway. The basename comes from the server rather
    # than being spelled `binlog.*` here, because this rig's --log-bin is mysql-bin
    # and a hardcoded prefix reports "0 files" for a rig with 2.9 MB of binlog.
    local line
    line=$(myq "SHOW MASTER STATUS" | head -1)
    echo "  SHOW MASTER STATUS: ${line:-<nothing>}"
    local base
    base=$(myq "SELECT REPLACE(@@log_bin_basename, '/var/lib/mysql/', '')")
    echo "  binlogs on disk: $(docker exec "$MY_C" sh -c "ls -1 /var/lib/mysql/${base}.0* 2>/dev/null | wc -l") files, $(docker exec "$MY_C" sh -c "du -ch /var/lib/mysql/${base}.0* 2>/dev/null | tail -1 | cut -f1")"
    # Two questions, asked two ways, because "who is attached" is the leak check:
    # the P_S table only lists a connection that is *running*, while a replication
    # client that has connected and is idling still holds a binlog file open. The
    # processlist sees the second kind.
    echo "  replication_connection_status rows: $(myq "SELECT COUNT(*) FROM performance_schema.replication_connection_status" || echo 'n/a')"
    echo "  binlog-dump / replica-IO threads: $(myq "SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND LIKE 'Binlog Dump%' OR COMMAND LIKE '%Dump%' OR COMMAND LIKE 'Connect%'" || echo 'n/a')"
}

# ── the validator, shared verbatim with the capped MySQL -> CH bulk campaign ──
source "$HERE/bench-capped-my-ch-validator.sh"
myq()  { myq_ "$1"; }
chq()  { chq_ "$1"; }

cmd_srcsum() {
    local key f t
    key=$(printf '%s' "$MY_AGG" | md5sum | cut -c1-8)
    # Six at a time: a full 1M-row pass per table, thirty of them, on an UNCAPPED
    # source that has sixteen cores of its own. This is not a measurement.
    for t in "${TABLES[@]}"; do
        f="$WORK/srcsum-$t-$key"
        [[ -s "$f" ]] || : > "$f"
    done
    printf '%s\n' "${TABLES[@]}" | xargs -P "$SRC_JOBS" -I{} bash -c '
        t="{}"; key="'"$key"'"; f="'"$WORK"'/srcsum-$t-$key"
        [[ -s "$f" ]] && exit 0
        v=$(bash "'"$HERE"'/bench-capped-my-ch-cdc-0.57.sh" agg src "$t" 2>/dev/null)
        [[ -n "$v" ]] && printf "%s" "$v" > "$f"
    '
    for t in "${TABLES[@]}"; do
        f="$WORK/srcsum-$t-$key"
        [[ -s "$f" ]] && echo "$t cached $(cat "$f")" || echo "$t EMPTY — the source scan failed"
    done
}
srcsum_of() {
    local key; key=$(printf '%s' "$MY_AGG" | md5sum | cut -c1-8)
    cat "$WORK/srcsum-$1-$key"
}

# EXACT row counts, six tables at a time.
#
# information_schema.tables.table_rows is InnoDB's ESTIMATE and it is visibly wrong
# here: right after the seed it read 907,529–999,363 for thirty tables that each hold
# exactly 1,000,000 rows. A campaign that reported that range as its seed would be
# reporting an estimate as a measurement — and one of these reports' own hard rules
# (benchmark-fairness-rules.md #8, "verify the trivia too") is about exactly this. So
# every count printed anywhere in this campaign comes from COUNT(*).
#
# COUNT(*) on a 442 MB InnoDB table is a real scan, so it runs six at a time against
# an UNCAPPED source that has sixteen cores of its own. This is verification, not a
# measurement.
exact_counts() {
    printf '%s\n' "${TABLES[@]}" | xargs -P "$SRC_JOBS" -I{} sh -c '
        printf "  %s %s\n" "{}" \
            "$(docker exec -i '"$MY_C"' mysql -uroot -pbench -N -B '"$MY_DB"' \
                -e "SELECT COUNT(*) FROM \`{}\`" 2>/dev/null)"
    ' | sort
}
# The cached sums describe the SEED, so the end-of-window check recomputes them.
cmd_srcsum_fresh() {
    local key; key=$(printf '%s' "$MY_AGG" | md5sum | cut -c1-8)
    rm -f "$WORK"/srcsum-cdc_my_t*-"$key"
    cmd_srcsum
}

# ── destination hygiene ───────────────────────────────────────────────────────
# Every run ends here. SYNC matters: without it ClickHouse keeps a dropped table's
# metadata (and its data on disk) for database_atomic_delay_before_drop_table_sec
# = 480 s so UNDROP can still find it, and CH's own description says the delay is
# IGNORED for a SYNC drop.
#
# Dropping a destination table does NOT clear its watermark — that lives in the
# destination's _apitap_state — so this campaign's own state rows go with the
# tables, or the next run meets its own stale watermark with no binlog position to
# resume from. ONLY cdc_my_* / cdc_my_ctrl rows are touched: the cdc_pg_* rows
# another campaign left in that table are not ours (benchmarks/cdc-test-traps.md #9
# is the same lesson about slots, on a shared rig).
cmd_drop() {
    chq "SELECT concat('DROP TABLE IF EXISTS ', database, '.', name, ' SYNC;')
          FROM system.tables
          WHERE name IN ($(inlist)) OR startsWith(name, 'cdc_my_ctrl')
             OR (position(name, '__apitap') > 0 AND startsWith(name, 'cdc_my'))
             OR startsWith(name, 'cdc_my_t') OR position(name, '__ingestr') > 0
             OR position(name, '__bruin') > 0
          FORMAT TSVRaw" \
        | docker exec -i "$CH_C" clickhouse-client --password bench --multiquery 2>/dev/null
    # ingestr stages through its own database and keeps CDC state in the
    # destination; both are named by this campaign's own table prefix.
    for db in $(chq "SELECT DISTINCT database FROM system.tables
                     WHERE startsWith(name, 'cdc_my') OR startsWith(name, '_bruin')
                        OR position(name, '__ingestr') > 0"); do
        [[ "$db" == "system" || "$db" == "information_schema" ]] && continue
        chq "SELECT concat('DROP DATABASE IF EXISTS ', '\`$db\`', ' SYNC;')
              FROM system.databases WHERE name = '$db' FORMAT TSVRaw" \
          | docker exec -i "$CH_C" clickhouse-client --password bench --multiquery 2>/dev/null
    done
    chq "DELETE FROM _apitap_state
          WHERE startsWith(dest_table, 'cdc_my_t') OR startsWith(dest_table, 'cdc_my_ctrl')" 2>/dev/null
    echo -n "  leftover destination tables: "
    chq "SELECT count() FROM system.tables
          WHERE name IN ($(inlist)) OR startsWith(name, 'cdc_my')
             OR position(name, '__apitap') > 0 OR position(name, '__ingestr') > 0"
}

# ── host state, and a peak that survives an OOM kill ───────────────────────────
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

# ── one timed apitap leg ──────────────────────────────────────────────────────
# MODE=catchup: the destination is EMPTY, so the call is the bootstrap plus the
# group's first drain — the headline, and the comparable shape to the pg CDC arm.
# MODE=drain:   the group is established; the call lands everything the binlog has
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
        -e "APITAP_TABLES=${TABLES[*]}" -e "APITAP_SRC=$MY_URL" -e "APITAP_DST=$CH_URL" \
        -e "APITAP_MODE=$mode" -e "APITAP_RUN_TWICE=$twice" \
        "$IMG" sh /job/bench-capped-my-ch-cdc-leg-apitap.sh >"$log" 2>&1
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

# ── the ingestr arm ───────────────────────────────────────────────────────────
# ingestr's MySQL CDC path, exactly as its own docs describe it
# (docs/supported-sources/mysql.md#change-data-capture):
#
#   mysql+cdc://  binary log, after a consistent snapshot; resumes from durable CDC
#                 state recorded in the DESTINATION
#   requirements  log_bin=ON, binlog_format=ROW, binlog_row_image=FULL, no
#                 PARTIAL_JSON, a primary key on every table, no ENUM/SET/BIT, and
#                 a user with SELECT + RELOAD + REPLICATION SLAVE + REPLICATION
#                 CLIENT
#   multi-table   a comma-separated --source-table is still a multi-table run, and
#                 dest_schema then decides where the output lands
#   server_id     pinned, because overlapping runs need distinct replication ids
#   its own       merge strategy, _bruin_staging, and the _cdc_lsn / _cdc_deleted /
#                 _cdc_synced_at metadata columns
#
# Defaults are left alone: --extract-parallelism 5, --page-size 25000,
# --loader-file-size 25000, --batch-size 512 MiB, --sql-limit 0 (no limit).
# One process for all thirty tables, because a comma-separated --source-table IS
# ingestr's own multi-table CDC mode — this is not apitap's model imposed on it.
# One-shot (no --stream) is its documented catch-up: "by default a CDC run catches
# up to the current log position and exits", which is the same shape apitap's
# one-call catch-up is measured against.
cmd_leg_ingestr() {
    local tag=$1 res=${2:-$RESULTS} tuned=${3:-no}
    local name="apitap-bench-cdc-ing-$tag$([[ "$tuned" == tuned ]] && echo -tuned)"
    local log="$WORK/logs/${name}.log" peak="$WORK/logs/${name}.peak"
    local poll="$WORK/logs/${name}.poll" deadline=${LEG_TIMEOUT:-3600}
    # ING_DEST_URI is overridable so the control can point the rival at a destination
    # ingestr accepts (its ClickHouse refusal is a capability boundary, measured by
    # benchmarks/bench-capped-my-ch-cdc-probe-ingestr.sh, and the control has to be
    # able to prove the tool's landing is READABLE by this campaign's validator
    # somewhere). The campaign's own arm leaves it at ClickHouse.
    local dst_uri=${ING_DEST_URI:-"clickhouse://default:bench@127.0.0.1:${CH_NATIVE}?http_port=${CH_HTTP}"}
    local poll_dst=no
    case "$dst_uri" in clickhouse://*) poll_dst=yes ;; esac
    local t0 t1="" wall rc state stopped mempeak hostpeak waited=0 rows=0 fin_at=""
    local -a envs=(-e "ING_SOURCE_URI=mysql+cdc://root:bench@127.0.0.1:${MY_PORT}/${MY_DB}?server_id=18888"
                   -e "ING_DEST_URI=$dst_uri"
                   -e "ING_SOURCE_TABLE=$(IFS=,; echo "${TABLES[*]}")"
                   -e "ING_DEST_SCHEMA=default"
                   -e "ING_WORK=$WORK/out")
    [[ "$tuned" == tuned ]] && envs+=(-e "ING_SQL_LIMIT=${ING_SQL_LIMIT:-200000}"
                                      -e "ING_EXTRACT_PARALLELISM=1"
                                      -e "ING_BATCH_SIZE=64"
                                      -e "ING_LOADER_FILE_SIZE=5000")
    docker rm -f "$name" >/dev/null 2>&1
    : > "$log"; : > "$poll"
    host_state "before $name"
    # A fresh CDC run needs a fresh position: ingestr's is stored in the
    # destination, and cmd_drop cleared this campaign's rows.
    rm -rf "$WORK/out/$name" ; mkdir -p "$WORK/out/$name"
    t0=$(now)
    docker run --name "$name" --network=host $CAP "${envs[@]}" \
        -v "$ING_BIN:/usr/local/bin/ingestr:ro" -v "$HERE:/job:ro" -v "$WORK/out:/out" \
        "$IMG" sh /job/bench-capped-my-ch-cdc-leg-ingestr.sh >"$log" 2>&1 &
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
        # The row poll only makes sense against the shared ClickHouse destination. A
        # leg pointed elsewhere (the control's rival leg) has no system.tables to read,
        # so it waits for the container to exit and the deadline guards the wait.
        if [[ "$poll_dst" == yes ]]; then
            rows=$(chq "SELECT toString(coalesce(sum(total_rows),0)) FROM system.tables
                        WHERE database='default' AND name IN ($(inlist))")
            rows=${rows:-0}
            printf '%s rows=%s\n' "$(now)" "$rows" >> "$poll"
            sleep 2
            waited=$((waited + 2))
            if (( rows >= EXPECT_ROWS )); then
                local union="" all_rows
                for t in "${TABLES[@]}"; do
                    union+="${union:+ UNION ALL }SELECT count() AS c FROM \`default\`.\`$t\` FINAL"
                done
                all_rows=$(chq "SELECT toString(sum(c)) FROM ( $union )")
                if [[ "${all_rows:-0}" -ge $EXPECT_ROWS ]]; then
                    t1=$(now); stopped="caught_up"
                    printf 'ROWS %s\nT_VERIFY %s\n' "$all_rows" "$t1" >> "$log"
                    break
                fi
            fi
        else
            sleep 2
            waited=$((waited + 2))
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
    state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    if [[ "$stopped" == caught_up ]]; then
        # ingestr's one-shot run has already exited by the time it caught up; if it
        # is somehow still up, stop it and keep the log.
        docker ps --format '{{.Names}}' | grep -qx "$name" && docker stop -t 60 "$name" >>"$log" 2>&1 || true
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
        printf 'LEG ingestr tag=%s tuned=%s\n' "$tag" "$tuned"
        printf '  stopped_by %s\n' "$stopped"
        printf '  wall_container_s %s %s\n' "$wall" \
            "$([[ "$stopped" == caught_up ]] || echo "(container lifetime: it never reached full landing)")"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' \
            "$(awk -v v="${mempeak:-0}" 'BEGIN{printf "%.1f", v/1048576}')"
        printf '  host_sampled_peak_mb %s\n' "$hostpeak"
        grep -E '^(MEMPEAK_BYTES|MEMEVENTS|MEMSTAT|ROWS|T_VERIFY|INGESTR_|EXITCODE|SNAPSHOT)' "$log" | sed 's/^/  /'
        printf '  progress_lines %s\n' "$(grep -c 'PROGRESS' "$log")"
        printf '  errors %s\n' "$(grep -ciE 'level=ERROR|\[ERROR\]|panic:' "$log")"
        printf '  poll_last %s\n' "$(tail -1 "$poll" 2>/dev/null)"
        printf '  log %s\n' "$log"
    } | tee -a "$res"
    return 0
}

# ── verification ──────────────────────────────────────────────────────────────
# Rows plus the order-independent checksum, per table, source against destination.
# `--fresh` recomputes the source side, which the end-of-window check needs: the
# cached sums describe the SEED, and by then the seed has taken the window's
# 2.97M changes.
cmd_verify() {
    local fresh=${1:-} srcfile="$WORK/src.txt" chfile="$WORK/dst.txt"
    local t ok=0 bad=0 seen=0 body=""
    if [[ -n "$fresh" ]]; then
        cmd_srcsum_fresh >/dev/null
    fi
    : > "$srcfile"
    for t in "${TABLES[@]}"; do printf '%s\t%s\n' "$t" "$(srcsum_of "$t")" >> "$srcfile"; done
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
    : > "$chfile"
    for t in "${TABLES[@]}"; do
        d=$(chsum "$t")
        [[ -n "$d" ]] && printf '%s\t%s\n' "$t" "$d" >> "$chfile"
    done
    while IFS=$'\t' read -r t d; do
        s=$(awk -F'\t' -v k="$t" '$1==k{print $2}' "$srcfile")
        if [[ -n "$d" && "$s" == "$d" ]]; then
            body+="$(printf 'VERIFY %s rows=%s MATCH %s' "$t" "${d%%|*}" "$d")"$'\n'
            ok=$((ok+1))
        else
            body+="$(printf 'VERIFY %s MISMATCH\n  src=%s\n  dst=%s' "$t" "$s" "${d:-<no table / no row>}")"$'\n'
            bad=$((bad+1))
        fi
        seen=$((seen+1))
    done < "$chfile"
    printf '%s' "$body" | tee -a "$WORK/verify.log"
    echo "VERIFY_SUMMARY checked=$((ok + bad)) of ${#TABLES[@]} match=$ok mismatch=$bad"
    # A table the destination does not have at all produces no line above, so it
    # would never reach the loop and "0 mismatch" would be a statement about
    # nothing. Loud, rather than quietly green.
    #
    # The test is HOW MANY LINES THE LOOP SAW, not the name of the last variable it
    # read: bash's `read` assigns empty strings to its variables when it reaches EOF
    # with no input, so the previous `[[ "$t" != "${TABLES[-1]}" ]]` was true on
    # EVERY run — it printed "VERIFY_INCOMPLETE 0 of 30 … THIS IS NOT A PASS"
    # directly under a clean 30/30 summary, and returned 1 from a PASSING
    # verification, which is the condition `window` gates its bootstrap on.
    if (( seen != ${#TABLES[@]} )); then
        echo "VERIFY_INCOMPLETE $(( ${#TABLES[@]} - seen )) of ${#TABLES[@]} tables are not in the destination at all — THIS IS NOT A PASS"
        while IFS=$'\t' read -r t _; do
            grep -q "^$t	" "$chfile" || echo "VERIFY_ABSENT $t"
        done < "$srcfile"
        return 1
    fi
    chq "SELECT concat('TOTAL_ROWS ', sum(total_rows)) FROM system.tables
          WHERE database='default' AND name IN ($(inlist))"
    [[ $bad -eq 0 && $((ok + bad)) -eq ${#TABLES[@]} ]]
}

# ── the cold catch-up (the headline) ──────────────────────────────────────────
# ROUNDS rounds, arms interleaved, each from a pre-seeded source and an EMPTY
# destination, each checksum-verified before any number counts.
cmd_catchup() {
    local rounds=${ROUNDS:-2} r arm
    : > "$RESULTS"; : > "$WORK/verify.log"; : > "$WORK/host-state.log"
    for ((r = 1; r <= rounds; r++)); do
        for arm in "$@"; do
            echo "=== round $r · $arm  ($(date -u +%H:%M:%S))"
            cmd_drop >/dev/null
            case "$arm" in
                apitap)  cmd_leg_apitap "r$r" catchup 0 ;;
                ingestr) cmd_leg_ingestr "r$r" ;;
                # The rival also gets a second arm with smaller settings of its own
                # (--sql-limit 200000, --extract-parallelism 1, --batch-size 64 MiB,
                # --loader-file-size 5000), because "ingestr cannot run this" is only
                # an honest sentence if it also cannot run it when told to use less.
                ingestrtuned) cmd_leg_ingestr "r$r" "$RESULTS" tuned ;;
                *) echo "unknown arm $arm" >&2; return 2 ;;
            esac
            cmd_verify
            cmd_drop >/dev/null
            df -h / | tail -1 | sed 's/^/  disk /'
        done
    done
    cmd_results
}

# ── the 5-minute window ───────────────────────────────────────────────────────
cmd_window() {
    local i res="$WORK/window-results.txt"
    local wlog="$WORK/window.log" win0 win1
    : > "$res"; : > "$wlog"
    echo "== fresh start for the window: empty destination, no state, empty binlog tail =="
    cmd_drop >/dev/null
    echo
    echo "== establishing the group (the bootstrap; reported, not the headline) =="
    cmd_binlogstate | tee -a "$wlog"
    cmd_leg_apitap window-bootstrap catchup 0 "$res" | tee -a "$wlog"
    echo
    echo "== the bootstrap's landing, checksum-verified BEFORE any traffic =="
    cmd_verify || { echo "WINDOW ABORTED: the bootstrap's landing does not match the source"; return 1; }
    echo
    echo "== the change stream: $TICKS ticks x $TICK_S s, $INS ins + $UPD upd + $DEL del"
    echo "   rows per table per tick, across all ${#TABLES[@]} tables =="
    win0=$(now)
    # The writer is the SOURCE, uncapped by design, and it is generated on the host
    # and piped straight into one mysql session inside the source container: every
    # statement is its own implicit transaction (autocommit) and a SLEEP paces the
    # ticks. Its progress is an artifact, never a pgrep.
    { python3 "$HERE/bench-capped-my-ch-cdc-window.py" "$TICKS" "$TICK_S" "${#TABLES[@]}" \
        $INS $UPD $DEL $INS_LO $UPD_LO $DEL_LO \
      | docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" ; } > "$WORK/writer.log" 2>&1 &
    local writer=$!
    # Confirm by ARTIFACT, immediately: the writer's own progress lines.
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
            cmd_binlogstate | sed 's/^/  /'
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
    txs=$(( TICKS * ${#TABLES[@]} * 3 ))
    echo "WRITER_TOTAL ticks=$TICKS tables=${#TABLES[@]} statements=$txs changes=$changes" \
         "writer_wall_s=$wwall changes_per_s=$(awk -v c="$changes" -v w="$wwall" 'BEGIN{printf "%.0f", c/w}')" \
         | tee -a "$wlog"
echo "WRITER source row counts now — EXACT COUNT(*), and they must be $SEED_ROWS + ${INS}·${TICKS} - ${DEL}·${TICKS} = $(( SEED_ROWS + INS*TICKS - DEL*TICKS )):" | tee -a "$wlog"
    exact_counts | tee -a "$wlog"

    echo
    echo "== the final drain: whatever the writer's last commits produced =="
    cmd_leg_apitap final drain 1 "$res" | tee -a "$wlog"
    echo
    echo "== and the destination against the source, FRESH checksums =="
    cmd_verify fresh | tee -a "$wlog"
}

# Every table's watermark, and how many distinct ones the group holds. A group that
# advances as ONE unit is the property under test, so all thirty equal is the
# expected reading and anything else is a finding. MySQL's watermark is the packed
# (file_ordinal << 32 | log_pos) coordinate, so it is a u64 and it is one value for
# the whole binlog stream.
watermarks() {
    chq "SELECT concat('pos ', min(watermark), ' .. ', max(watermark),
                       ' distinct=', uniqExact(watermark), ' tables=', count())
          FROM (SELECT dest_table, argMax(watermark, synced_at) AS watermark
                FROM _apitap_state FINAL
                WHERE startsWith(dest_table, 'cdc_my_t') GROUP BY dest_table)"
}

# ── leak checks ───────────────────────────────────────────────────────────────
cmd_leaks() {
    echo "== descriptors: peak and final, per leg, measured from inside each container =="
    grep -hE '^FD_PEAK|^FD_END|^FDCOUNT|^IDEMPOTENT' "$WORK"/logs/apitap-bench-cdc-*.log 2>/dev/null \
        | sed "s|^|$(printf '%-34s' "$(basename "$WORK")")  |"
    echo
    echo "== the source's binlog: files, size, and who is attached =="
    cmd_binlogstate
    myq "SELECT CONCAT('  ', VARIABLE_NAME, ' = ', VARIABLE_VALUE)
          FROM performance_schema.global_status
          WHERE VARIABLE_NAME IN ('Binlog_cache_disk_use','Binlog_cache_use',
              'Binlog_stmt_cache_disk_use','Binlog_stmt_cache_use')
          ORDER BY VARIABLE_NAME" 2>/dev/null || true
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
    chq "SELECT concat('  ', database, '.', name, ' engine=', engine) FROM system.tables
          WHERE position(name, '__apitap') > 0 OR position(name, '__ingestr') > 0
             OR position(name, '__bruin') > 0"
    echo "== _apitap_cdc_pending =="
    chq "SELECT concat('  rows=', count()) FROM _apitap_cdc_pending" 2>/dev/null \
        || echo "  (no _apitap_cdc_pending table)"
    echo "== _bruin_staging (ingestr's own staging schema) =="
    chq "SELECT concat('  tables=', count()) FROM system.tables WHERE database='_bruin_staging'" 2>/dev/null \
        || echo "  (no _bruin_staging database)"
    echo "== the group's own state rows =="
    # Broken down by source_id, because the bare aggregate is misleading. There are
    # 60 rows for 30 tables, in two kinds:
    #   mysql:…               the GROUP's row — the live position, last_rows the rows
    #                         this table contributed, and all thirty at ONE watermark
    #   server-identity:…     written once at bootstrap to pin WHICH server/schema/table
    #                         this is, so a later run can tell a resumed group from a
    #                         different server that happens to have the same table
    #                         name. Its watermark field holds a sentinel, not a
    #                         position, and it is deliberately never advanced.
    # Counting uniqExact(watermark) across BOTH kinds therefore reads 2 on a
    # perfectly healthy group, which is exactly how a real drift would look. The
    # per-window question — "do the thirty group rows agree?" — is what matters, and
    # it is `watermarks()`'s distinct=1.
    chq "SELECT concat('  rows=', count(), ' mode=', arrayStringConcat(groupUniqArray(mode)),
                      ' cursor=', arrayStringConcat(groupUniqArray(cursor_col)),
                      ' last_rows_total=', sum(last_rows))
          FROM _apitap_state FINAL WHERE startsWith(dest_table, 'cdc_my_t')"
    chq "SELECT concat('    group rows        source_id=', source_id,
                      ' n=', count(), ' distinct_watermarks=', uniqExact(watermark),
                      ' watermark=', min(watermark), ' .. ', max(watermark))
          FROM _apitap_state FINAL
          WHERE startsWith(dest_table, 'cdc_my_t') AND startsWith(source_id, 'mysql:')
          GROUP BY dest_table, source_id LIMIT 1"
    chq "SELECT concat('    server-identity rows n=', count(),
                      ' distinct_watermarks=', uniqExact(watermark),
                      ' sentinel=', min(watermark),
                      ' last_rows=', min(last_rows), '..', max(last_rows))
          FROM _apitap_state FINAL
          WHERE startsWith(dest_table, 'cdc_my_t')
            AND startsWith(source_id, 'server-identity:')"
    chq "SELECT concat('    the group watermark, one value across all thirty tables: ',
                      uniqExact(w), ' distinct over ', count(), ' tables at ', min(w))
          FROM (SELECT dest_table, argMax(watermark, synced_at) AS w
                FROM _apitap_state FINAL
                WHERE startsWith(dest_table, 'cdc_my_t') GROUP BY dest_table)"
    echo "== another campaign's rows in the shared _apitap_state, untouched =="
    chq "SELECT concat('  cdc_pg_* rows still there: ',
                      countIf(startsWith(dest_table, 'cdc_pg_'))) FROM _apitap_state FINAL"
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
          f"{d.get('docker_state','?'):>22} {rows}")
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
    echo "== this campaign's volume (on disk) =="
    # `sudo -n ... </dev/null`: without the -n and the redirect sudo falls back to
    # reading the password from stdin, which over `bash -s` is the SCRIPT — so it
    # swallows every line after it and the step looks like it crashed.
    echo "  apitap-bench-cdc-my-data $(sudo -n du -sh /var/lib/docker/volumes/apitap-bench-cdc-my-data/_data </dev/null 2>/dev/null | cut -f1)"
    echo "== the seed (KEPT) =="
    myq "SELECT CONCAT('  declared: ', COUNT(*), ' tables, columns each ', MIN(c), '-', MAX(c),
              ', ', FORMAT(ROUND(SUM(b)/1073741824, 2), 2), ' GiB on disk')
          FROM (SELECT t.table_name,
                  (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_schema=t.table_schema AND table_name=t.table_name) c,
                  (SELECT data_length+index_length FROM information_schema.tables
                    WHERE table_schema=t.table_schema AND table_name=t.table_name) b
                FROM information_schema.tables t
                WHERE t.table_schema='$MY_DB' AND t.table_name LIKE 'cdc\_my\_%') x"
    echo "  EXACT row counts (COUNT(*), never information_schema's estimate):"
    exact_counts
    echo "== the source's binlog =="
    cmd_binlogstate
    echo "== destination leftovers (this campaign only) =="
    chq "SELECT concat('  cdc_my_* tables: ', count()) FROM system.tables
          WHERE startsWith(name, 'cdc_my') OR position(name, '__apitap') > 0
             OR position(name, '__ingestr') > 0"
    chq "SELECT concat('  another campaign cdc_pg_* rows in _apitap_state (untouched): ',
                      countIf(startsWith(dest_table, 'cdc_pg_'))) FROM _apitap_state FINAL"
    echo "== replicas/slaves attached to the source =="
    myq "SELECT CONCAT('  ', COUNT(*)) FROM performance_schema.replication_connection_status"
    echo "== disk =="
    df -h / | tail -1 | sed 's/^/  /'
}

case "${1:-}" in
rig)      cmd_rig ;;
seed)     cmd_seed ;;
srcsum)   cmd_srcsum ;;
srcsumx)  cmd_srcsum_fresh ;;
binlog)   cmd_binlogstate ;;
drop)     cmd_drop ;;
catchup)  shift; cmd_catchup "$@" ;;
leg)      shift; cmd_leg_apitap "$@" ;;
ingleg)   shift; cmd_leg_ingestr "$@" ;;
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
*) sed -n 2,32p "$0"; exit 2 ;;
esac