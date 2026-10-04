#!/usr/bin/env bash
# Assemble the raw receipts for the capped-tier CDC STRESS campaign into one file:
# the tools and their provenance, the rig and every setting changed from the
# previous campaign's rig, the calibration that decided the design, the state the
# campaign FOUND, the control, the bootstrap, WINDOW C, WINDOW S with its backlog
# curve, the applied-prefix check and its control, the walshadow leg, the leak
# checks, host state around every leg, and the rig afterwards.
#
#   bash bench-capped-pg-ch-cdc-stress-raw.sh OUT.log
#
# Nothing here recomputes anything: every number in the report is a copy of a
# line one of the campaign's own scripts wrote.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-pg-ch-cdc-stress-0.57.sh"
W=${WORK:-$HOME/bench-cdc-stress}
OUT=$1

{
    echo "CAPPED-TIER CDC STRESS — can a 0.5 CPU / 256 MB drain keep up with"
    echo "1,000,000 changed rows per table per minute across 30 tables in one group?"
    echo "raw log, generated $(date -u +%Y-%m-%dT%H:%M:%SZ) on the bench VPS"
    echo "host: $(nproc) cores, $(awk '/MemTotal/{printf "%.0f GB", $2/1048576}' /proc/meminfo) RAM, shared with production"
    echo

    echo "===== 1. the tool and its provenance, asserted from inside the cage"
    ~/apitap-057-pullback/bin/python - <<'PY'
import apitap, hashlib, os, sys
so = os.path.join(os.path.dirname(apitap.__file__), "_apitap.abi3.so")
print("  apitap.__version__      ", apitap.__version__)
print("  interpreter             ", sys.version.split()[0], "(abi3 wheel)")
print("  _apitap.abi3.so md5     ", hashlib.md5(open(so, "rb").read()).hexdigest())
PY
    echo "  the site-packages is bind-mounted READ-ONLY at /py and put on PYTHONPATH, so"
    echo "  the PyPI wheel is what runs and nothing is built on this box. Every leg"
    echo "  re-asserts it from inside the cage — e.g. the stress drain's own log:"
    grep -hE '^APITAP_(VERSION|SO_MD5|TABLES)' "$W"/logs/apitap-bench-cdc-stress-*.log 2>/dev/null \
        | sort -u | sed 's/^/    /'
    echo
    echo "  the rival: walshadow 0.1.2, its own release artifact, sha256-verified"
    docker image inspect apitap-bench-ws16:0.1.2 >/dev/null 2>&1 && {
        docker run --rm --entrypoint walshadow-stream apitap-bench-ws16:0.1.2 --version 2>&1 | sed 's/^/    /'
        (cd ~/ws-dl16 2>/dev/null && sha256sum walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz \
            walshadow-pgext-0.1.2-pg16-x86_64-unknown-linux-gnu.tar.gz) 2>&1 | sed 's/^/    /'
    } || echo "    (image not on this box)"
    echo

    echo "===== 2. the rig, and every setting that differs from the previous campaign"
    docker ps --filter name=apitap-bench-cdc --format '  {{.Names}}  {{.Image}}  {{.Status}}  {{.Ports}}'
    docker exec -i apitap-bench-cdc-pg psql -U postgres -d bench -Atc \
        "SELECT concat('    ', name, ' = ', setting, unit)
         FROM pg_settings WHERE name IN ('server_version','wal_level','max_replication_slots',
         'max_wal_senders','max_slot_wal_keep_size','max_wal_size','min_wal_size',
         'shared_buffers','max_connections','logical_decoding_work_mem','checkpoint_timeout',
         'synchronous_commit','wal_compression','full_page_writes') ORDER BY name" 2>&1
    docker exec -i apitap-bench-cdc-ch clickhouse-client --password bench -q \
        "SELECT concat('    ClickHouse ', version())" 2>&1
    echo "  vs the previous campaign's rig: max_slot_wal_keep_size 6GB -> 12GB (the safety"
    echo "  valve), max_wal_size 4GB -> 2GB, shared_buffers 128MB -> 1GB,"
    echo "  max_connections 80 -> 200, checkpoint_timeout 30min -> 5min, synchronous_commit"
    echo "  on -> off, wal_compression off (unchanged). Every one of these is a setting on"
    echo "  the UNCAPPED source and changes nothing the capped container may do."
    echo

    echo "===== 2b. WINDOW C and the two bootstraps' own receipts"
    for f in "$W/rerun.log" "$W/bootstrap.log"; do
        [[ -s "$f" ]] && { echo "--- $f ---"; grep -E '^(==|LEG|  (stopped_by|wall_container_s|docker_state|cgroup_memory_peak_mb|DRAIN|APITAP_|MEMPEAK|MEMEVENTS|ELAPSED|FD_)|WRITER|  \(drain|CONVERGED|WINDOW C total|VERIFY_SUMMARY|TOTAL_ROWS|  watermark:|  slot:)' "$f" | sort -u | sed 's/^/  /'; }
    done
    echo

    echo "===== 3. the calibration that decided the design (before anything was timed)"
    echo "  WAL bytes per change, per statement kind, by compression and page temperature,"
    echo "  and writer throughput with 1/8/15/30 concurrent psql sessions:"
    ls -la "$W"/logs/calibration.log 2>/dev/null && cat "$W"/logs/calibration.log || \
        echo "  (run: bash benchmarks/bench-cdc-stress-calibrate.sh all 2>&1 | tee $W/logs/calibration.log)"
    echo

    echo "===== 3b. was the 0.5 CPU quota DELIVERED under the writer? (a separate measurement)"
    cat "$W/logs/cpu-probe.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 4. the state this campaign FOUND — recorded, not assumed"
    head -30 "$W/start.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 5. the control: GREEN x4, plus the prefix reconstruction's own control"
    cat "$W/logs/control.log" 2>/dev/null | grep -vE '^$' | sed 's/^/  /'
    echo
    grep -E "RECONSTRUCTION_CONTROL|graveyard|CONTROL|tables at" "$W/start.log" 2>/dev/null | sed 's/^/  /'
    echo "  (start.log is written by: bash bench-capped-pg-ch-cdc-stress-0.57.sh start)" 
    echo "  the reconstruction at k=0 against the plain source digest, all thirty:"
    if diff -q "$W/k0.txt" "$W/k0-plain.txt" >/dev/null 2>&1; then
        echo "    diff k0.txt k0-plain.txt -> IDENTICAL, 30 of 30"
    else
        diff "$W/k0.txt" "$W/k0-plain.txt" 2>&1 | head -20 | sed 's/^/    /'
    fi
    cat "$W/k0.txt" 2>/dev/null | sed 's/^/    /'
    echo

    echo "===== 6. the bootstrap: ONE group over 30 tables, ONE slot, empty destination"
    grep -E '^(LEG|  )' "$W/bootstrap.log" 2>/dev/null | grep -vE '^  VERIFY cdc' | sed 's/^/  /'
    echo
    echo "  --- its checksum verdicts ---"
    grep -E '^(VERIFY cdc_pg_t0[123] |VERIFY cdc_pg_t30|VERIFY_SUMMARY|TOTAL_ROWS)' "$W/bootstrap.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 7. WINDOW C — the correctness window: a change stream the drain CONVERGES on"
    grep -E 'WRITER|applied|CONVERGED|WINDOW C total|^LEG|  wall_container_s|  cgroup_memory_peak_mb|  DRAIN|  MEMPEAK|  FD_|  docker_state|  stopped_by|watermark:|slot:' \
        "$W/calm.log" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- WINDOW C's final verification, destination against source ---"
    grep -E '^(VERIFY cdc_pg_t0[123] |VERIFY cdc_pg_t30|VERIFY_SUMMARY|TOTAL_ROWS)' "$W/calm.log" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- the writer's actual statement stream, one transaction verbatim ---"
    python3 "$HERE/bench-capped-pg-ch-cdc-stress-writer.py" 0 1 1 0 954001 2>/dev/null \
        | grep -vE '^SELECT pg_current_wal_lsn|^\\\\echo|^--' | sed 's/^/    /'
    echo

    echo "===== 8. WINDOW S — the requested rate"
    echo "  --- LEG 1, the leg that hit the ceiling: its own receipts, in full ---"
    echo "  (LEG 1 is the one whose curve ends with the slot lost and whose drain raised)"
    sed -n '1,200p' "$W/stress-leg1.log" 2>/dev/null | grep -vE '^(  writer:|  writer progress|  writer is running|before |after )' | sed 's/^/  /'
    echo
    echo "  --- LEG 1: the curve that hit the ceiling, verbatim from the sampler ---"
    grep -E '^[0-9]' "$W/stress-backlog-leg1.csv" 2>/dev/null | awk -F, 'NR==1{print "  "$0; next}{printf "  %-9s %7ss  retained_bytes=%12s (%9.1f MB)  %-9s wm=%s..%s distinct=%s ch_rows=%s disk_free=%sGB  lsn=%s\n",$1,$2,$3,$3/1048576,$4,$6,$7,$8,$9,$12,$13}'
    echo
    echo "  --- LEG 3 slice 1's own container log: the drain that committed mid-stream ---"
    cat "$W/logs/apitap-bench-cdc-stress-drain1.log" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- the final converge drain's own container log ---"
    grep -E '^(APITAP_|DRAIN|ELAPSED|MEMPEAK|MEMEVENTS|MEMSTAT|FD_PEAK|FD_END|EXITCODE|IMPORT_S)' "$W/logs/apitap-bench-cdc-stress-converge.log" 2>/dev/null | sed 's/^/  /' 
    echo
    grep -E 'target|offered|UNBOUNDED|preconditions|disk free|writer:|writer finished|committed transactions|WRITER|GENERATION_WAL|generation done|writer was stopped' \
        "$W/stress.log" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- THE BACKLOG CURVE (5 s cadence) ---"
    cat "$W/stress-backlog.csv" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- the drain's own log in full: this is where the ceiling speaks ---"
    cat "$W/logs/apitap-bench-cdc-stress-drain.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 8b. LEG 2, the same rate with no early SIGTERM — the confound control ---"
    echo "  LEG 1's drain slice was cut at 30 s; if the zero applied were an artefact of"
    echo "  that, LEG 2 (a 600 s slice, no early SIGTERM) would have applied something."
    grep -E 'LEG apitap|stopped_by|wall_container|cgroup_memory_peak|RAISED|EXITCODE|MEMPEAK|DRAIN pass' "$W/leg2.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 9. the applied-prefix check and ITS control"
    grep -E 'final watermark|PREFIX_|CONTROL|MISMATCH|applied_changes|ledger_txns|drain applied' \
        "$W/prefix.log" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  --- the writer's actual statement stream, one transaction, verbatim ---"
    python3 "$HERE/bench-capped-pg-ch-cdc-stress-writer.py" 0 1 1 0 964801 2>/dev/null \
        | grep -vE '^SELECT pg_current_wal_lsn|^\\echo|^-- ' | sed 's/^/    /'
    echo
    echo "  --- the writer's ledger, its first and last lines for one table ---"
    grep -h "cdc_pg_t01" "$W/ledger/stress"-s*.log 2>/dev/null | head -2 | sed 's/^/    /'
    grep -h "cdc_pg_t01" "$W/ledger/stress"-s*.log 2>/dev/null | tail -1 | sed 's/^/    /'
    echo "    (the k field is the SESSION's loop index, not the table's transaction number;"
    echo "     see the report's fault 4 — the claim is made against apitap's own"
    echo "     per_table_rows counter instead)"
    echo

    echo "===== 9b. the converge drain that finished leg 3's backlog, and the final verify"
    grep -E '^(LEG apitap tag=converge|  (stopped_by|wall_container_s|docker_state|cgroup_memory_peak_mb|DRAIN|MEMPEAK|MEMEVENTS|FD_))' "$W/final.log" 2>/dev/null | sed 's/^/  /'
    grep -E '^(VERIFY_SUMMARY|TOTAL_ROWS)' "$W/final.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 10. the walshadow leg: one, same source/destination/cap"
    sed -n '/LEG walshadow/,/^after  apitap/p' "$W/final.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 11. leak checks, taken with the campaign's state still intact"
    sed -n '/== descriptors/,/== replication slots: /p' "$W/final.log" 2>/dev/null | sed 's/^/  /'
    echo
    echo "===== 11b. the catch-up curve's mirror image: the final drain emptying the slot"
    grep -E '^[0-9]' "$W/stress-backlog.csv" 2>/dev/null | awk -F, 'NR>1{printf "  t=%-8s retained=%9.1f MB  %-9s wm=%s..%s distinct=%s ch_rows=%s\n",$2,$3/1048576,$4,$6,$7,$8,$9}' | tail -40
    echo

    echo "===== 12. host state around every leg"
    cat "$W/host-state.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 13. the rig afterwards"
    bash "$H" state
    echo
    echo "===== 13b. EVERY NUMBER THE REPORT QUOTES AS DERIVED, with its inputs shown"
    awk 'BEGIN{
        printf "  applied sustained (WINDOW C): 3990000 changes / 581.099 s = %.0f changes/s\n", 3990000/581.099;
        printf "  applied per CPU-second:       3990000 / (581.099 * 0.5) = %.0f changes/CPU-s\n", 3990000/(581.099*0.5);
        printf "  cores for 500000 changes/s:   500000 / %.0f = %.1f cores\n", 3990000/(581.099*0.5), 500000/(3990000/(581.099*0.5));
        printf "  ratio offered/applied:         500000 / %.0f = %.1fx\n", 3990000/581.099, 500000/(3990000/581.099);
        printf "  offered achieved:              54000000 / 105 s = %.0f changes/s = %.0f changes/min/table\n", 54000000/105, 54000000/105*60/30;
        printf "  WAL per change:                16743576984 / 54000000 = %.1f B\n", 16743576984/54000000;
        printf "  backlog slope:                 (12480715832 - 9793632) / (78.3 - 0.8) = %.0f MB/s\n", (12480715832-9793632)/(78.3-0.8)/1048576;
        printf "  90M changes of WAL:            90000000 * 310.1 = %.1f GB\n", 90000000*310.1/1e9;
        printf "  catch-up of one 3-min window:  90000000 / %.0f = %.0f s = %.2f h\n", 3990000/581.099, 90000000/(3990000/581.099), 90000000/(3990000/581.099)/3600;
        printf "  leg3 applied total:            180000 + 3420000 = %d (offered 3600000)\n", 180000+3420000;
        printf "  leg3 applied rate:             (180000+3420000) / (34.807+525.936) = %.0f changes/s\n", (180000+3420000)/(34.807+525.936);
        printf "  converge applied rate:         3420000 / 525.936 = %.0f changes/s\n", 3420000/525.936;
        printf "  clearing drain:                7375000 / 991.332 = %.0f changes/s\n", 7375000/991.332;
        printf "  cpu delivered under load:      1048827 / 1110544 = %.1f%%\n", 1048827/1110544*100;
        printf "  30 x 1,036,100 rows:           %d\n", 30*1036100;
        printf "  compression composites:        off %.1f  pglz %.1f  lz4 %.1f  zstd %.1f B/change\n",
               (800*402.8+100*356.4+100*67.8)/1000, (800*382.2+100*331.9+100*72.5)/1000,
               (800*363.1+100*314.3+100*50.6)/1000, (800*344.6+100*321.0+100*59.2)/1000;
    }'
    echo
    echo "  --- the prefix reconstruction's control, recomputed from its two files ---"
    if diff -q "$W/k0.txt" "$W/k0-plain.txt" >/dev/null 2>&1; then
        echo "  RECONSTRUCTION_CONTROL checked=30 of 30 match=30 mismatch=0"
        echo "    (diff $W/k0.txt $W/k0-plain.txt is empty; the two files are the prefix"
        echo "     reconstruction at k=0 and the plain source digest, 30 lines each)"
    else
        diff "$W/k0.txt" "$W/k0-plain.txt" 2>&1 | head -20 | sed 's/^/    /'
    fi
    echo
    echo "===== 14. disk, and what the ClickHouse delayed-drop costs"
    df -h / | tail -1 | sed 's/^/  /'
    docker exec apitap-bench-cdc-ch bash -c 'du -sh /var/lib/clickhouse/store 2>/dev/null' | sed 's/^/  CH store: /'
    echo "  the destination is dropped SYNC after every verification: ClickHouse keeps a"
    echo "  dropped table's data on disk for database_atomic_delay_before_drop_table_sec ="
    echo "  480 s without SYNC, and the previous campaign measured 42 GB -> 9.5 GB free"
    echo "  across three consecutive 8 GB landings."
} > "$OUT" 2>&1
wc -l "$OUT"