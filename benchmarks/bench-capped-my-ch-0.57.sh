#!/usr/bin/env bash
# Capped MySQL -> ClickHouse head-to-head: apitap 0.57.0 (PyPI) vs ingestr 1.1.61,
# at the capped tier — 10 tables x 1,000,000 rows x 15 columns, all 10 synced
# CONCURRENTLY in ONE job, each tool inside ONE container capped at
# --cpus=0.5 --memory=256m --memory-swap=256m.
#
#   ./bench-capped-my-ch.sh seed        # 10 identical tables from bench.bench_my_1m (kept)
#   ./bench-capped-my-ch.sh srcsum      # source aggregates (cached per table)
#   ./bench-capped-my-ch.sh leg apitap 1
#   ./bench-capped-my-ch.sh leg ingestr 2
#   ./bench-capped-my-ch.sh verify      # per-table row count + checksum vs the source
#   ./bench-capped-my-ch.sh drop        # destination tables (the SEEDS stay)
#   ./bench-capped-my-ch.sh startup     # what each tool pays in the cage before moving a row
#   ./bench-capped-my-ch.sh run         # 3 interleaved rounds of every arm
#   ./bench-capped-my-ch.sh results     # the per-round table
set -uo pipefail

MY_C=apitap-bench-my            # source, 127.0.0.1:3307, db bench, root/bench
CH_C=apitap-bench-ch            # destination, 127.0.0.1:8124 (http) / 9124 (native)
MY_DB=bench
CAP="--cpus=0.5 --memory=256m --memory-swap=256m"
APITAP_VERSION=0.57.0
APITAP_SO_MD5=41e9f7c252d3e1eb5403b70f87bf5435
INGESTR_VERSION=1.1.61
SP_APITAP=/home/ubuntu/apitap-057-pullback/lib/python3.13/site-packages
ING_BIN=/home/ubuntu/.cache/ingestr/bin/v${INGESTR_VERSION}/Linux_x86_64/ingestr
IMG=python:3.13-slim
HERE="$(cd "$(dirname "$0")" && pwd)"          # the harness (rsynced from the repo)
WORK=${WORK:-$HOME/bench-capped}               # artifacts: logs, per-table ingestr logs, cached sums
TABLES=(cmp_my_t01 cmp_my_t02 cmp_my_t03 cmp_my_t04 cmp_my_t05 cmp_my_t06
        cmp_my_t07 cmp_my_t08 cmp_my_t09 cmp_my_t10)
mkdir -p "$WORK/out" "$WORK/logs"


now() { date +%s.%N; }

# ── the seed ───────────────────────────────────────────────────────────────────────
cmd_seed() {
    local t0 n start
    t0=$(now)
    for t in "${TABLES[@]}"; do
        n=$(myq "SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema='$MY_DB' AND table_name='$t'")
        if [[ "$n" == "0" ]]; then
            myq "CREATE TABLE \`$t\` LIKE bench.bench_my_1m;
                 INSERT INTO \`$t\` SELECT * FROM bench.bench_my_1m;" >/dev/null
        fi
    done
    printf 'SEED_SECONDS %.1f\n' "$(echo "$(now) - $t0" | bc -l)"
    for t in "${TABLES[@]}"; do
        myq "SELECT CONCAT('$t', ' cols=',
              (SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema='$MY_DB' AND table_name='$t'),
              ' rows=', COUNT(*)) FROM \`$t\`"
    done
}

# ── the validator ─────────────────────────────────────────────────────────────────
# ONE definition per engine, shared with the control leg (validator.sh): the
# per-table aggregate every run is scored with. Debug it with
#   bash bench-capped-my-ch-mismatch.sh row TABLE 1
# the validator owns myq/chq (loud) and myq_/chq_ (quiet); the driver wants quiet
source "$HERE/bench-capped-my-ch-validator.sh"
myq() { myq_ "$1"; }
chq() { chq_ "$1"; }

# Cached per table, keyed on the validator text, so the ~1 min MySQL scan per
# table is paid once for the whole campaign (the seed never changes under it).
cmd_srcsum() {
    local t key f
    for t in "${TABLES[@]}"; do
        key=$(printf '%s' "$MY_AGG" | md5sum | cut -c1-8)
        f="$WORK/srcsum-$t-$key"
        if [[ -s "$f" ]]; then echo "$t cached $(cat "$f")"; continue; fi
        printf '%s' "$(srcsum "$t")" > "$f"
        echo "$t computed $(cat "$f")"
    done
}

srcsum_of() {
    local key; key=$(printf '%s' "$MY_AGG" | md5sum | cut -c1-8)
    cat "$WORK/srcsum-$1-$key"
}

# ── destination hygiene ───────────────────────────────────────────────────────────
# Every run ends here: the destination tables this campaign created are dropped,
# in `default` AND in the `_bruin_staging` schema ingestr stages through, and the
# MySQL seeds are never touched.
cmd_drop() {
    chq "SELECT concat('DROP TABLE IF EXISTS ', database, '.', name, ';')
         FROM system.tables
         WHERE (database = 'default' OR database = '_bruin_staging')
           AND (name LIKE '%cmp_my_t%' OR name LIKE '%cmp_ctrl%' OR name LIKE '%cmp_probe%') FORMAT TSVRaw" \
      | docker exec -i "$CH_C" clickhouse-client --password bench --multiquery 2>/dev/null
    chq "SELECT concat(database, '.', name) FROM system.tables
         WHERE (database = 'default' OR database = '_bruin_staging')
           AND (name LIKE '%cmp_my_t%' OR name LIKE '%cmp_ctrl%' OR name LIKE '%cmp_probe%') FORMAT TSVRaw"
}

# ── verification ─────────────────────────────────────────────────────────────────
cmd_verify() {
    local t src got rows ok=0 bad=0
    for t in "${TABLES[@]}"; do
        src=$(srcsum_of "$t")
        got=$(chsum "$t")
        rows=$(chq "SELECT count() FROM \`default\`.\`$t\`")
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
    local in_list
    in_list=$(printf "%s','" "${TABLES[@]}")
    chq "SELECT concat('TOTAL_ROWS ', sum(total_rows)) FROM system.tables
         WHERE database='default' AND name IN ('${in_list}')" 
    [[ $bad -eq 0 ]]
}

# ── one leg ──────────────────────────────────────────────────────────────────────
host_state() {
    local tag=$1
    { printf '%s loadavg=%s memavail_kb=%s cached_kb=%s\n' "$tag" \
        "$(cut -d' ' -f1-3 /proc/loadavg)" \
        "$(awk '/MemAvailable/{print $2}' /proc/meminfo)" \
        "$(awk '/^Cached:/{print $2}' /proc/meminfo)"
    } >> "$WORK/host-state.log"
    tail -1 "$WORK/host-state.log"
}

# Sample the container's OWN cgroup from the host while it runs, so a peak
# survives an OOM kill (the in-container echo cannot run once the kernel has
# killed the process that would have printed it). This tracks the max of
# memory.current at 5 Hz — a LOWER BOUND on memory.peak. The kernel's own
# memory.peak, echoed from inside the container before it exited, is the exact
# one and is what the result reports.
host_peak_watch() {
    local name=$1 out=$2 id="" cg="" v m=0
    : > "$out"
    for _ in $(seq 300); do                       # the cgroup appears a moment late
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
        sleep 0.2
    done
}

cmd_leg() {
    local tool=$1 round=$2
    local name="cmp-${tool}-r${round}"
    local log="$WORK/logs/${name}.log"
    local peakfile="$WORK/logs/${name}.peak"
    local deadline=${LEG_TIMEOUT:-1200}
    local t0 t1 wall rc state stopped cg events mempeak
    docker rm -f "$name" >/dev/null 2>&1
    : > "$log"
    host_state "before $name"
    t0=$(now)

    case "$tool" in
    apitap)
        script=/job/bench-capped-my-ch-leg-apitap.sh
        [[ "$round" == startup ]] && script="/job/bench-capped-my-ch-probes.sh startup apitap"
        docker run --name "$name" --network=host $CAP \
            -v "$SP_APITAP:/py:ro" -e PYTHONPATH=/py \
            -v "$ING_BIN:/usr/local/bin/ingestr:ro" \
            -v "$HERE:/job:ro" \
            "$IMG" sh $script >"$log" 2>&1 &
        ;;
    ingestr)          # ingestr's native model for many tables: one process each
        script=/job/bench-capped-my-ch-leg-ingestr.sh
        [[ "$round" == startup ]] && script="/job/bench-capped-my-ch-probes.sh startup ingestr"
        docker run --name "$name" --network=host $CAP \
            -v "$ING_BIN:/usr/local/bin/ingestr:ro" \
            -v "$HERE:/job:ro" -v "$WORK/out:/out" \
            "$IMG" sh $script >"$log" 2>&1 &
        ;;
    ingestr1)         # ingestr's leanest model: the same 10 tables, one at a time
        docker run --name "$name" --network=host $CAP \
            -v "$ING_BIN:/usr/local/bin/ingestr:ro" \
            -v "$HERE:/job:ro" -v "$WORK/out:/out" \
            "$IMG" sh /job/bench-capped-my-ch-leg-ingestr1.sh >"$log" 2>&1 &
        ;;
    *) echo "unknown arm $tool" >&2; return 2 ;;
    esac
    local dockerpid=$!
    host_peak_watch "$name" "$peakfile" &
    local watchpid=$!

    # A leg that cannot finish gets a DEADLINE, and hitting it is a result, not
    # an accident: at the deadline the kernel's own counters and whatever landed
    # in the destination are recorded before the container goes away.
    stopped="completed"
    local waited=0
    while kill -0 "$dockerpid" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        (( waited < deadline )) || { stopped="deadline_${deadline}s"; break; }
    done
    wait $dockerpid
    rc=$?
    t1=$(now)
    if [[ "$stopped" == completed ]]; then
        kill $watchpid 2>/dev/null
    else
        # evidence first, container second
        cg="/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' "$name" 2>/dev/null).scope"
        events=$(grep -E '^(oom|oom_kill|high|max) ' "$cg/memory.events" 2>/dev/null | tr '\n' ' ')
        {
            printf 'DEADLINE_EVIDENCE round=%s arm=%s\n' "$round" "$tool"
            printf '  memory_events %s\n' "$events"
            printf '  memory_stat %s\n' \
                "$(grep -E '^(anon|file) ' "$cg/memory.stat" 2>/dev/null | tr '\n' ' ')"
            printf '  memory_pressure %s\n' \
                "$(grep -E '^(some|full) ' "$cg/memory.pressure" 2>/dev/null | tr '\n' ' ')"
            printf '  dest_rows_landed %s\n' \
                "$(chq "SELECT sum(total_rows) FROM system.tables
                        WHERE database='default' AND name LIKE 'cmp_my_t%'")"
            printf '  in_cage_rss_kb %s\n' \
                "$(docker exec "$name" sh -c 'awk "/VmRSS/{s+=\$2} END{print s+0}" /proc/*/status 2>/dev/null' 2>/dev/null)"
            printf '  in_cage_utime_ticks %s\n' \
                "$(docker exec "$name" sh -c 'awk "{print \$14+\$15}" /proc/[0-9]*/stat 2>/dev/null | awk "{s+=\$1} END{print s+0}"' 2>/dev/null)"
        } | tee -a "$WORK/results.txt"
        docker rm -f "$name" >/dev/null 2>&1
    fi
    wall=$(echo "$t1 - $t0" | bc -l)
    state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
    mempeak=$(grep -o 'MEMPEAK=[0-9]*' "$log" | tail -1 | cut -d= -f2)
    [[ -n "$mempeak" ]] || mempeak=$(cat "$peakfile" 2>/dev/null || echo 0)
    mempeak_mb=$(awk -v v="$mempeak" 'BEGIN{printf "%.1f", v/1048576}')
    hostpeak_mb=$(awk -v v="$(cat "$peakfile" 2>/dev/null || echo 0)" 'BEGIN{printf "%.1f", v/1048576}')
    if [[ -n "$state" ]]; then docker rm -f "$name" >/dev/null 2>&1; else state="(container removed at the deadline)"; fi
    host_state "after  $name"

    {
        printf 'LEG %s round=%s\n' "$tool" "$round"
        printf '  stopped_by %s\n' "$stopped"
        printf '  wall_container_s %s\n' "$wall"
        printf '  docker_state %s (OOMKilled ExitCode)\n' "$state"
        printf '  cgroup_memory_peak_mb %s\n' "$mempeak_mb"
        printf '  host_sampled_peak_mb %s\n' "$hostpeak_mb"
        grep -E '^(ELAPSED|ELAPSED_MS|MEMPEAK|MEMSTAT|ROWS|PIPE_BUDGET|APITAP_VERSION|INGESTR_VERSION|PROC_ALL_RC|IMPORT_S|RAISED)' "$log" \
            | sed 's/^/  /'
        printf '  proc_lines %s\n' "$(grep -c '^PROC cmp_my_t' "$log")"
        printf '  failed_procs %s\n' "$(grep -c '^PROC cmp_my_t.* rc=[^0]' "$log")"
        printf '  log %s\n' "$log"
    } | tee -a "$WORK/results.txt"
    return 0
}

# The arms, in the order they run inside every round. Interleaved A B C A B C,
# because this box carries other load and a block-of-tools run would confound
# host drift with engine identity.
ARMS=(apitap ingestr ingestr1)

cmd_run() {
    local rounds=${ROUNDS:-3}
    cmd_seed
    cmd_srcsum
    : > "$WORK/results.txt"
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
        done
    done
    cmd_results
}

# How many rows ONE ingestr process can land at this cap (ingestr's own
# --sql-limit). The landing is validated against the matching MySQL subset, so a
# ceiling that is reported is a ceiling that was checksum-verified, then dropped.
cmd_ceiling() {
    local limit
    for limit in "${@:-100000 200000 400000}"; do
        local name="cmp-ceiling-$limit"
        docker rm -f "$name" >/dev/null 2>&1
        docker run --name "$name" --network=host $CAP \
            -v "$ING_BIN:/usr/local/bin/ingestr:ro" -v "$HERE:/job:ro" \
            "$IMG" sh /job/bench-capped-my-ch-probes.sh ceiling "$limit" \
            > "$WORK/logs/${name}.log" 2>&1
        local state mempeak
        state=$(docker inspect -f '{{.State.OOMKilled}} {{.State.ExitCode}}' "$name" 2>/dev/null)
        mempeak=$(grep -o 'MEMPEAK=[0-9]*' "$WORK/logs/${name}.log" | tail -1 | cut -d= -f2)
        printf 'CEILING %s rows docker_state=%s peak_mb=%s\n' "$limit" "$state" \
            "$(awk -v v="$mempeak" 'BEGIN{printf "%.1f", v/1048576}')"
        printf '  dst_sum  %s\n' "$(chsum "cmp_probe_${limit}")"
        printf '  src_sum  %s\n' "$(srcsum cmp_my_t01 "id <= ${limit}")"
        docker rm -f "$name" >/dev/null 2>&1
        cmd_drop >/dev/null
    done
}

# What each tool pays inside the cage before it moves a single row.
cmd_startup() {
    : > "$WORK/results.txt"
    local arm
    cmd_leg apitap startup
    cmd_leg ingestr startup
}

cmd_results() {
    echo
    printf '%-8s %-3s %10s %10s %12s %10s %s\n' TOOL RND WALL_S PEAK_MB ROWS CHECKSUM
    python3 - "$WORK/results.txt" <<'PY'
import re, sys, statistics
rounds = {}
tool = None
for line in open(sys.argv[1]):
    line = line.rstrip()
    m = re.match(r"LEG (\w+) round=(\d+)", line)
    if m:
        tool, rnd = m.group(1), int(m.group(2))
        rounds.setdefault(tool, {})[rnd] = {}
        continue
    m = re.match(r"\s+(\S+) (.*)", line)
    if m and tool:
        rounds[tool][rnd][m.group(1)] = m.group(2)
rows = []
for tool, rs in rounds.items():
    for rnd in sorted(rs):
        d = rs[rnd]
        wall = d.get("wall_container_s", "?")
        peak = d.get("cgroup_memory_peak_mb", "?")
        rows.append((tool, rnd, wall, peak, d))
for tool, rnd, wall, peak, d in rows:
    st = d.get("docker_state", "? ?")
    el = d.get("ELAPSED", d.get("ELAPSED_MS", "?"))
    print(f"{tool:<8} {rnd:<3} {wall:>10} {peak:>10} {'':>12} {st:<10} in_cage={el}")
for tool in sorted(rounds):
    ws = [float(d["wall_container_s"]) for d in rounds[tool].values() if "wall_container_s" in d]
    if ws:
        print(f"MEDIAN {tool} wall_s {statistics.median(ws):.1f}  rounds {[round(w,1) for w in ws]}")
PY
    echo
    echo "checksum verdicts per leg: see the VERIFY lines above (drop after each run)"
}

case "${1:-}" in
seed) cmd_seed ;;
srcsum) cmd_srcsum ;;
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
ceiling) shift; cmd_ceiling "$@" ;;
state)
    echo "== ClickHouse leftovers from this campaign:"
    chq "SELECT concat('  ', count()) FROM system.tables
         WHERE name LIKE '%cmp_my_t%' OR name LIKE '%cmp_probe%' OR name LIKE '%cmp_ctrl%'"
    chq "SELECT concat('  _bruin_staging tables: ', count()) FROM system.tables
         WHERE database = '_bruin_staging'"
    echo "== MySQL seeds (columns per seed, then exact row counts):"
    myq "SELECT CONCAT('  seeds: ', COUNT(*), ' tables, columns each ',
            MIN(c), '-', MAX(c))
         FROM (SELECT table_name, COUNT(*) AS c FROM information_schema.columns
               WHERE table_schema = '$MY_DB' AND table_name LIKE 'cmp_my_t%'
               GROUP BY table_name) x"
    for t in "${TABLES[@]}"; do
        myq "SELECT CONCAT('  $t rows=', COUNT(*)) FROM \`$t\`"
    done
    ;;
*) sed -n 2,20p "$0"; exit 2 ;;
esac