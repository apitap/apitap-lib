#!/usr/bin/env bash
# Assemble the raw receipts for the capped MySQL -> ClickHouse CDC campaign into one
# file: the tools and their provenance, the rig, the seed, the control, BOTH
# interleaved catch-up passes, the five-minute window's writer and drains, the leak
# checks, host state around every leg, and the state of the rig after.
#
# The campaign runs its cold catch-up TWICE, both times fully checksum-validated,
# against two different source sizes, so both are here rather than the better one:
#
#   pass A  the source the brief specifies — 30 x 1,000,000 rows
#   pass B  the same source AFTER the window took its changes, so 30 x 1,036,000 rows
#
# Pass A's numbers are in $WORK/results.txt, pass B's in
# $WORK/results-catchup-final.txt, and the window's in $WORK/window-results.txt.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-my-ch-cdc-0.57.sh"
WORK=${WORK:-$HOME/bench-cdc-my}
CTRL=${CTRL:-$HOME/bench-cdc-my-ctrl}
MY_C=apitap-bench-cdc-my
CH_C=apitap-bench-cdc-ch
OUT=$1

{
    echo "apitap 0.57.0 (PyPI) vs ingestr 1.1.61 — capped MySQL -> ClickHouse, CDC"
    echo "raw log, generated $(date -u +%Y-%m-%dT%H:%M:%SZ) on the bench VPS"
    echo "host: $(nproc) cores, $(awk '/MemTotal/{printf "%.0f GB", $2/1048576}' /proc/meminfo) RAM, shared with production"
    echo

    echo "===== 1. the tools and their provenance"
    echo "\$ ~/apitap-057-pullback/bin/python -c 'import apitap; ...'"
    ~/apitap-057-pullback/bin/python - <<'PY'
import apitap, hashlib, os, sys
so = os.path.join(os.path.dirname(apitap.__file__), "_apitap.abi3.so")
print("  apitap.__version__      ", apitap.__version__)
print("  interpreter             ", sys.version.split()[0], "(abi3 wheel)")
print("  _apitap.abi3.so md5     ", hashlib.md5(open(so, "rb").read()).hexdigest())
PY
    echo "  site-packages is bind-mounted READ-ONLY into the cage at /py and put on"
    echo "  PYTHONPATH, so the PyPI wheel is what runs and nothing is built on this box."
    echo "  every leg re-asserts it from INSIDE the cage; the catch-up legs' own log:"
    grep -hE '^APITAP_(VERSION|SO_MD5|TABLES)' "$WORK"/logs/apitap-bench-cdc-r*.log 2>/dev/null \
        | sort -u | sed 's/^/    /'
    echo
    echo "\$ ingestr --version   (the native binary from its own cache, bind-mounted in)"
    /home/ubuntu/.cache/ingestr/bin/v1.1.61/Linux_x86_64/ingestr --version 2>&1 | sed 's/^/  /'
    ls -la /home/ubuntu/.cache/ingestr/bin/v1.1.61/Linux_x86_64/ingestr 2>&1 | sed 's/^/  /'
    echo "  ingestr 1.1.x is no longer a Python program: the pip package is a thin"
    echo "  wrapper that downloads and execs this 254 MB native binary, which is"
    echo "  mounted read-only, so nothing is downloaded inside a measurement."
    echo

    echo "===== 2. the rig: this campaign's own source, and the SHARED destination"
    bash "$H" rig
    echo
    echo "  the container's own arguments, verbatim from docker inspect:"
    docker inspect "$MY_C" --format '    {{json .Config.Cmd}}' 2>&1
    docker inspect "$MY_C" --format '    ports {{json .HostConfig.PortBindings}}  volume {{range .Mounts}}{{.Name}}{{end}}' 2>&1
    echo "  the seed's declared shape, from the server:"
    docker exec -i "$MY_C" mysql -uroot -pbench -N -B bench -e "SHOW CREATE TABLE cdc_my_t01" 2>&1 | sed 's/^/    /'
    echo

    echo "===== 3. the seed: 30 identical tables, the bulk arm's schema"
    echo "  (ONE declared type differs from the bulk arm's — json_val is longtext, not a"
    echo "   native JSON column, because apitap's MySQL CDC lane refuses the latter."
    echo "   The refusal and the product's own suggested workaround are in section 4.)"
    bash "$H" state
    echo
    echo "  seeding wall time, and the binlog coordinates before any run:"
    grep -E '^SEED_SECONDS' "$WORK"/seed.log 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 4. the control: the validator AND the CDC lane proven before anything was timed"
    echo "  leg 4 is where the rival's fate was found: its CDC landing is validated on a"
    echo "  destination it accepts, and its refusal of THIS campaign's ClickHouse is"
    echo "  recorded verbatim, plus the probe that shows the refusal is not about our"
    echo "  schema."
    cat "$CTRL/control.log" 2>/dev/null \
        || echo "  (run: bash bench-capped-my-ch-cdc-control.sh 2>&1 | tee $CTRL/control.log)"
    echo
    echo "  --- apitap's MySQL CDC lane refuses a native JSON column, verbatim ---"
    grep -h 'RAISED' "$CTRL"/logs/*.log 2>/dev/null | grep -i json | sort -u | sed 's/^/  /'
    echo "  --- where ingestr's CDC path does and does not land ---"
    bash "$HERE/bench-capped-my-ch-cdc-probe-ingestr.sh" probe_mini 2>&1 | sed 's/^/  /'
    echo

    echo "===== 5. PASS A — the cold catch-up on the source the brief specifies"
    echo "  30 tables x 1,000,000 rows x 15 columns, all 30 in ONE CDC group over ONE"
    echo "  binlog stream, arms interleaved: apitap · ingestr · ingestr-tuned, n=2."
    echo "  Every leg checksum-verified before the destination was dropped."
    cat "$WORK/results.txt" 2>/dev/null
    echo
    echo "  --- pass A's own summary table (arm / wall / peak / stopped / rows + medians) ---"
    RESULTS="$WORK/results.txt" bash "$H" results
    echo
    echo "  --- pass A's per-table checksums ---"
    grep -E '^(VERIFY|VERIFY_SUMMARY|TOTAL_ROWS)' "$WORK/verify.log" 2>/dev/null | head -100
    echo

    echo "===== 6. PASS B — the same legs on the source the window left behind"
    cat "$WORK/results-catchup-final.txt" 2>/dev/null
    echo
    echo "  --- pass B's own summary table ---"
    RESULTS="$WORK/results-catchup-final.txt" bash "$H" results
    echo
    grep -E '^(VERIFY_SUMMARY|TOTAL_ROWS)' "$WORK/verify.log" 2>/dev/null | tail -6
    echo

    echo "===== 7. THE FIVE-MINUTE WINDOW"
    echo "  the writer: TICKS ticks x 30 tables x (100 INSERT + 100 UPDATE + 20 DELETE)"
    echo "  rows, every statement its own implicit transaction, SLEEP between ticks,"
    echo "  piped from the host into one mysql session on the source."
    grep -E '^WINDOW_PROGRESS|^WINDOW_DONE' "$WORK/writer.log" 2>/dev/null | tail -8 | sed 's/^/  /'
    echo "  --- the writer's statements, two ticks of two tables, verbatim ---"
    python3 "$HERE/bench-capped-my-ch-cdc-window.py" 2 0.65 2 100 100 20 2>/dev/null \
        | head -30 | sed 's/^/    /'
    echo
    echo "  --- every drain's receipt, the watermark, the binlog ---"
    cat "$WORK/window-results.txt" 2>/dev/null
    echo
    # window-full.log is the run's COMPLETE stdout. $WORK/window.log is written by
    # `tee -a` from inside cmd_window and ends up short of the final drain's block, so
    # reading it here lost the middle binlog sizes and read as if they never happened.
    WLOG="$WORK/window-full.log"; [[ -s "$WLOG" ]] || WLOG="$WORK/window.log"
    # [[:space:]]* rather than two literal spaces: cmd_window re-indents
    # cmd_binlogstate's own two, so those lines carry four, and an anchored two-space
    # pattern silently dropped every binlog size between the first and the last — which
    # made a binlog that grew to 1.7 GB read as if it had jumped there in one step.
    grep -E '^[[:space:]]*(window [0-9]|SHOW MASTER|binlogs on disk|replication_connection|watermark:|host:)|^WRITER' \
        "$WLOG" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- the window's per-drain descriptors, from inside each container ---"
    for f in "$WORK"/logs/apitap-bench-cdc-drain*.log "$WORK"/logs/apitap-bench-cdc-final.log; do
        [[ -f "$f" ]] || continue
        echo "    $(basename "$f"):"
        grep -E '^(DRAIN|IDEMPOTENT|FDCOUNT|FD_PEAK|FD_END)' "$f" | grep -v per_table | sed 's/^/      /'
    done
    echo
    echo "  --- the window's final verification: destination vs source, FRESH digests ---"
    grep -E '^(VERIFY_SUMMARY|TOTAL_ROWS)' "$WLOG" 2>/dev/null | sed 's/^/  /'
    grep -E '^VERIFY cdc_my_t0[12] |^VERIFY cdc_my_t30' "$WLOG" 2>/dev/null | sed 's/^/  /'
    echo "  --- and the source's EXACT row counts after the window (COUNT(*), all thirty) ---"
    grep -E '^  cdc_my_t[0-9]+ [0-9]+$' "$WLOG" 2>/dev/null | sort -u | sed 's/^/  /'
    echo

    echo "===== 8. leak checks, taken with the window's state still intact"
    cat "$WORK/leaks.log" 2>/dev/null
    echo
    echo "  --- what DID exist during a run: the run-scoped artifacts, from ClickHouse's"
    echo "  own dropped-table metadata, which names them ---"
    docker exec "$CH_C" bash -c \
        'ls /var/lib/clickhouse/metadata_dropped 2>/dev/null | sed "s/\.[0-9a-f-]*\.sql$//" | sort -u | head -8 | sed "s/^/    /"' 2>&1
    echo

    echo "===== 9. host state around every leg (loadavg / MemAvailable / Cached / disk free)"
    cat "$WORK/host-state.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 10. the rig afterwards"
    bash "$H" state
    echo
    echo "===== 11. disk"
    df -h / | tail -1 | sed 's/^/  /'
    docker exec "$CH_C" bash -c 'du -sh /var/lib/clickhouse/store 2>/dev/null' | sed 's/^/  CH store: /'
    sudo -n du -sh /var/lib/docker/volumes/apitap-bench-cdc-my-data/_data </dev/null 2>/dev/null \
        | sed 's/^/  this campaign'"'"'s MySQL volume: /'
} > "$OUT" 2>&1
wc -l "$OUT"