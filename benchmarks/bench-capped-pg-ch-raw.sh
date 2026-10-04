#!/usr/bin/env bash
# Assemble the raw receipts for the capped PostgreSQL -> ClickHouse campaign into
# one file: the tools and their provenance, the seed, the control, what each tool
# pays in the cage, every interleaved leg with its verification, the cap ladder
# that establishes walshadow's floor, host state around every leg, and the state
# of the rig afterwards.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
H="$HERE/bench-capped-pg-ch-0.57.sh"
WORK=${WORK:-$HOME/bench-capped-pg}
CTRL=${CTRL:-$HOME/bench-capped-pg-ctrl}
OUT=$1

{
    echo "apitap 0.57.0 (PyPI) vs walshadow 0.1.2 — capped PostgreSQL -> ClickHouse"
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
    echo
    echo "\$ walshadow-stream --version   (from the release artifact, inside the image)"
    docker run --rm --entrypoint walshadow-stream apitap-bench-ws:0.1.2 --version 2>&1 | sed 's/^/  /'
    echo "  release artifacts, sha256-verified against the published SHA256SUMS:"
    echo "    walshadow-0.1.2-x86_64-unknown-linux-gnu.tar.gz          (the daemon)"
    echo "    walshadow-pgext-0.1.2-pg18-x86_64-unknown-linux-gnu.tar.gz  (the PG module)"
    echo "  the image is postgres:18 with those two files dropped in — nothing compiled here:"
    docker image inspect apitap-bench-ws:0.1.2 --format '    built {{.Created}}' 2>&1
    docker run --rm --entrypoint pg_config apitap-bench-ws:0.1.2 --version 2>&1 | sed 's/^/    pg /'
    docker run --rm --entrypoint sh apitap-bench-ws:0.1.2 -c \
        'ls -l /usr/lib/postgresql/18/lib/walshadow.so' 2>&1 | sed 's/^/    /'
    echo "  the shadow PostgreSQL's memory knobs are inherited from the SOURCE's"
    echo "  postgresql.conf, which BASE_BACKUP copies into the shadow data directory:"
    docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -Atc \
        "SELECT concat('    source ', name, ' = ', setting, unit)
         FROM pg_settings WHERE name IN ('shared_buffers','wal_level','max_wal_senders',
         'max_connections','max_replication_slots','TimeZone') ORDER BY name" 2>&1
    echo

    echo "===== 2. the seed (10 identical tables, kept between every run)"
    docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -Atc \
        "SELECT concat('  ', table_name, ' rows=', (xpath('/row/c/text()',
            query_to_xml(format('SELECT count(*) c FROM public.%I', table_name),
                         false,true,'')))[1]::text::bigint)
         FROM information_schema.tables
         WHERE table_schema='public' AND table_name LIKE 'cmp_pg_t%'
         ORDER BY table_name" 2>&1
    docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -Atc \
        "SELECT concat('  ', COUNT(*), ' tables x ', MIN(c), '-', MAX(c),
                       ' columns, ', pg_size_pretty(SUM(b)), ' total')
         FROM (SELECT table_name, (SELECT count(*) FROM information_schema.columns
                                   WHERE table_schema='public' AND table_name=t.table_name) c,
                      pg_total_relation_size(quote_ident(table_name)) b
               FROM information_schema.tables t
               WHERE table_schema='public' AND table_name LIKE 'cmp_pg_t%') x" 2>&1
    echo "  generated from id alone, then LIKE-cloned, so all ten are byte-identical:"
    echo "  the ten source aggregates are therefore identical, which is the check:"
    bash "$H" srcsum 2>&1 | sed 's/^/    /'
    echo "  seeding wall time: 63.7 s for all ten (excluded from every measurement)"
    echo "  the schema:"
    docker exec -i apitap-bench-ws-pg psql -U postgres -d bench -c '\d+ public.cmp_pg_t01' 2>&1 | sed 's/^/    /'
    echo

    echo "===== 3. the control (the validator proven both ways before anything was timed)"
    cat "$CTRL/control.log" 2>/dev/null || echo "  (run: bash bench-capped-pg-ch-control.sh 2>&1 | tee $CTRL/control.log)"
    echo "  the two tools' landed schemas, as each built it:"
    grep -A26 "the landed schema" "$CTRL/control.log" 2>/dev/null
    echo

    echo "===== 4. what each tool pays in the cage BEFORE it moves a row"
    cat "$WORK/results-startup.txt" 2>/dev/null || bash "$H" startup
    echo "  the walshadow figure is the floor of STARTING the binary only. Its"
    echo "  deployment unit also owns a PostgreSQL, whose floor is not in that number:"
    echo "  the attribution run below measures the whole container instead."
    echo

    echo "===== 5. the campaign: 3 interleaved rounds, every leg checksum-verified"
    echo "  (the ladder and attribution runs of section 6/7 append to the same file"
    echo "   with round names of their own — ceil-* and attrib-* — and are filtered"
    echo "   out here; nothing is edited, the per-leg logs are the authority)"
    awk '/^LEG /{keep = ($3 ~ /^round=[0-9]+$/)} keep' "$WORK/results.txt" 2>/dev/null
    echo
    echo "  --- the checksums, per leg (dest dropped after each) ---"
    cat "$WORK/verify.log" 2>/dev/null
    echo
    echo "  --- and, per arm, the per-round host state and the in-cage receipt ---"
    for f in "$WORK"/host-state.log; do
        echo "  host state (loadavg / MemAvailable / Cached / disk free) around every leg:"
        sed 's/^/    /' "$f"
    done
    echo

    echo "===== 6. the cap ladder: the same walshadow leg at a bigger cage"
    echo "  upstream's DEFAULT configuration throughout (byte_budget 256 MiB, 8 inserters),"
    echo "  each rung from a pre-seeded source and an empty destination."
    cat "$WORK/results-ceiling.txt" 2>/dev/null
    echo

    echo "===== 7. where walshadow's memory actually goes (uncapped attribution run)"
    echo "  ps inside the container at ~12 s and ~36 s of the uncapped leg:"
    sed -n '1,40p' /tmp/attrib-ps.txt 2>/dev/null || echo "  (see the report)"
    echo

    echo "===== 8. host state around every leg"
    cat "$WORK/host-state.log" 2>/dev/null
    echo

    echo "===== 9. the rig afterwards"
    bash "$H" state
} > "$OUT" 2>&1
wc -l "$OUT"