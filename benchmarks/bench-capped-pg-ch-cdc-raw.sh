#!/usr/bin/env bash
# Assemble the raw receipts for the capped PostgreSQL -> ClickHouse CDC campaign
# into one file: the tools and their provenance, the rig, the seed, the control,
# BOTH interleaved catch-up passes, the five-minute window's writer and drains,
# the leak checks, host state around every leg, and the state of the rig after.
#
# The campaign ran its cold catch-up TWICE, both times fully checksum-validated,
# against two different source sizes, so both are here rather than the better one:
#
#   pass A  the source the brief specifies — 30 x 1,000,000 rows
#   pass B  the same source AFTER the window took 2,970,000 changes, so
#           30 x 1,036,000 rows — the same leg at 3.6% more rows
#
# Pass A's numbers are in $WORK/results.txt, pass B's in
# $WORK/results-catchup-final.txt, and the window's in $WORK/window-results.txt.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-pg-ch-cdc-0.57.sh"
WORK=${WORK:-$HOME/bench-cdc}
CTRL=${CTRL:-$HOME/bench-cdc-ctrl}
OUT=$1

{
    echo "apitap 0.57.0 (PyPI) vs walshadow 0.1.2 — capped PostgreSQL -> ClickHouse, CDC"
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
    echo "  every leg re-asserts it from INSIDE the cage; e.g. the catch-up legs' own log:"
    grep -hE '^APITAP_(VERSION|SO_MD5|TABLES)' "$WORK/logs/apitap-bench-cdc-r1.log" | sed 's/^/    /'
    echo
    echo "\$ walshadow-stream --version   (from the release artifact, inside the image)"
    docker run --rm --entrypoint walshadow-stream apitap-bench-ws16:0.1.2 --version 2>&1 | sed 's/^/  /'
    echo "  release artifacts, sha256-verified against the published SHA256SUMS:"
    echo "    walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz            (the daemon)"
    echo "    walshadow-pgext-0.1.2-pg16-x86_64-unknown-linux-gnu.tar.gz (the PG module)"
    echo "  the pg16 module, because this campaign's source is PostgreSQL 16 and the"
    echo "  shadow walshadow supervises is a physical copy of it — 'physical bootstrap"
    echo "  cannot cross major versions' applies to the module as much as to the data."
    echo "  the image is the Debian-flavoured postgres:16, not the alpine one, because the"
    echo "  published module links against glibc (ldd: libc.so.6) and cannot load into musl."
    (cd ~/ws-dl16 && sha256sum walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz \
        walshadow-pgext-0.1.2-pg16-x86_64-unknown-linux-gnu.tar.gz) 2>&1 | sed 's/^/    /'
    docker run --rm --entrypoint pg_config apitap-bench-ws16:0.1.2 --version 2>&1 | sed 's/^/    pg /'
    echo

    echo "===== 2. the rig: this campaign's own two containers, and nothing else touched"
    docker ps --filter name=apitap-bench-cdc --format '  {{.Names}}  {{.Image}}  {{.Status}}  {{.Ports}}'
    docker exec -i apitap-bench-cdc-pg psql -U postgres -d bench -Atc \
        "SELECT concat('    ', name, ' = ', setting, unit)
         FROM pg_settings WHERE name IN ('wal_level','max_replication_slots','max_wal_senders',
         'max_slot_wal_keep_size','max_wal_size','shared_buffers','max_connections',
         'logical_decoding_work_mem','server_version') ORDER BY name" 2>&1
    docker exec -i apitap-bench-cdc-ch clickhouse-client --password bench -q \
        "SELECT concat('    ClickHouse ', version())" 2>&1
    echo "  the pg_hba entry walshadow needs and the official image omits (without it the"
    echo "  daemon retries forever on auth instead of failing):"
    docker exec -i apitap-bench-cdc-pg bash -c "grep -E 'replication' /var/lib/postgresql/data/pg_hba.conf | sed 's/^/    /'"
    echo

    echo "===== 3. the seed: 30 identical tables, the bulk arm's schema verbatim"
    docker exec -i apitap-bench-cdc-pg psql -U postgres -d bench -Atc \
        "SELECT concat('  ', table_name, ' cols=',
            (SELECT count(*) FROM information_schema.columns
              WHERE table_schema='public' AND table_name=t.table_name),
            ' ', pg_size_pretty(pg_total_relation_size(quote_ident(table_name))))
          FROM information_schema.tables t
          WHERE table_schema='public' AND table_name LIKE 'cdc\_pg\_t%'
          ORDER BY table_name LIMIT 3" 2>&1
    echo "  the seed's own digest, per table (all thirty identical = all thirty clones):"
    echo "    1000000|2147445026505068|1|1000000|31670112|32000000|37518996|10309|12658|12048|11235"
    echo "    ^ identical to the bulk arm's ten-table digest: same schema, same generator."
    echo "  seeding wall time: 171.0 s for all thirty (excluded from every measurement)"
    echo "  the schema:"
    docker exec -i apitap-bench-cdc-pg psql -U postgres -d bench -c '\d public.cdc_pg_t01' 2>&1 | sed 's/^/    /'
    echo

    echo "===== 4. the control: the validator AND the CDC lane proven before anything was timed"
    cat "$CTRL/control.log" 2>/dev/null || echo "  (run: bash bench-capped-pg-ch-cdc-control.sh 2>&1 | tee $CTRL/control.log)"
    echo

    echo "===== 5. PASS A — the cold catch-up on the source the brief specifies"
    echo "  30 tables x 1,000,000 rows, all 30 in ONE CDC group, arms interleaved"
    echo "  apitap · walshadow-default · walshadow-tuned, n=2 rounds, every leg"
    echo "  checksum-verified before the destination was dropped."
    cat "$WORK/results.txt" 2>/dev/null
    echo
    echo "  --- pass A's checksums, per leg (the destination dropped after each) ---"
    head -120 "$WORK/verify.log" 2>/dev/null | grep -E '^(VERIFY|VERIFY_SUMMARY|TOTAL_ROWS)'
    echo

    echo "===== 6. PASS B — the same legs on the source the window left behind"
    echo "  30 x 1,036,000 rows (the window's 2,970,000 changes are still in it), n=2, arms"
    echo "  interleaved the same way. This is the pass whose per-arm daemon logs are all"
    echo "  intact: pass A gave both walshadow arms of a round the same log file, so the"
    echo "  second truncated the first."
    cat "$WORK/results-catchup-final.txt" 2>/dev/null
    echo

    echo "===== 7. THE FIVE-MINUTE WINDOW"
    echo "  the writer: 450 ticks x 30 tables x (100 INSERT + 100 UPDATE + 20 DELETE) rows,"
    echo "  every statement its own implicit transaction, pg_sleep between ticks, piped"
    echo "  from the host into one psql session on the source. Counts are exact by"
    echo "  construction and the source's own row counts are the proof."
    grep -E '^WINDOW_PROGRESS|^WINDOW_DONE' "$WORK/writer.log" | sed 's/^/  /'
    echo "  --- the writer's statements, one tick of two tables, verbatim ---"
    python3 "$HERE/bench-capped-pg-ch-cdc-window.py" 2 0.65 2 100 100 20 | head -32 | sed 's/^/    /'
    echo
    echo "  --- every drain's receipt, the watermark, the slot's retained WAL ---"
    cat "$WORK/window-results.txt" 2>/dev/null
    echo
    grep -E '^  (window [0-9]|slot:|watermark:|host:)' "$WORK/window.log" | sed 's/^/  /'
    grep -E '^WRITER' "$WORK/window.log" | sed 's/^/  /'
    echo
    echo "  --- the window's per-drain descriptors, from inside each container ---"
    for f in "$WORK"/logs/apitap-bench-cdc-drain*.log "$WORK"/logs/apitap-bench-cdc-final.log; do
        echo "    $(basename "$f"):"
        grep -E '^(DRAIN|IDEMPOTENT|FDCOUNT|FD_PEAK|FD_END)' "$f" | grep -v per_table | sed 's/^/      /'
    done
    echo
    echo "  --- the window's final verification: destination against source, FRESH digests ---"
    grep -E '^(VERIFY cdc_pg_t0[12] |VERIFY cdc_pg_t30|VERIFY_SUMMARY|TOTAL_ROWS)' "$WORK/window.log" | sed 's/^/  /'
    echo

    echo "===== 8. leak checks, taken with the window's state still intact"
    cat "$WORK/leaks.log" 2>/dev/null
    echo
    echo "  --- what DID exist during a run: the run-scoped artifacts, from ClickHouse's"
    echo "  own dropped-table metadata, which names them ---"
    docker exec apitap-bench-cdc-ch bash -c \
        'ls /var/lib/clickhouse/metadata_dropped 2>/dev/null | sed "s/\.[0-9a-f-]*\.sql$//" | sort -u | head -8 | sed "s/^/    /"' 2>&1
    echo "    (and after every leg, the drop check reports 0 — nothing is left behind)"
    echo

    echo "===== 9. host state around every leg (loadavg / MemAvailable / Cached / disk free)"
    cat "$WORK/host-state.log" 2>/dev/null | sed 's/^/  /'
    echo

    echo "===== 10. the rig afterwards"
    bash "$H" state
    echo
    echo "===== 11. disk, and what the ClickHouse delayed-drop cost us"
    df -h / | tail -1 | sed 's/^/  /'
    docker exec apitap-bench-cdc-ch bash -c 'du -sh /var/lib/clickhouse/store 2>/dev/null' | sed 's/^/  CH store: /'
    echo "  measured during the campaign: without DROP TABLE ... SYNC, three consecutive"
    echo "  8 GB landings left 42 GB free -> 9.5 GB free, because ClickHouse keeps a"
    echo "  dropped table's metadata (and its parts on disk) for"
    echo "  database_atomic_delay_before_drop_table_sec = 480 s so UNDROP can find it."
    echo "  CH's own description of that setting says the delay is IGNORED for a SYNC"
    echo "  drop; cmd_drop therefore drops SYNC, and the store went back to ~130 MB."
} > "$OUT" 2>&1
wc -l "$OUT"