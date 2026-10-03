#!/usr/bin/env bash
# Assemble the raw receipts for the capped MySQL -> ClickHouse campaign into one
# file: the seed record, the control, the startup probes, every leg with its
# verification, the ceiling probes, and the final state of the rig.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/bench-capped}
OUT=$1

{
    echo "apitap 0.57.0 (PyPI) vs ingestr 1.1.61 — capped MySQL -> ClickHouse"
    echo "raw log, generated $(date -u +%Y-%m-%dT%H:%M:%SZ) on the bench VPS"
    echo "host: $(nproc) cores, $(awk '/MemTotal/{printf "%.0f GB", $2/1048576}' /proc/meminfo) RAM"
    echo

    echo "===== 1. the tools"
    echo "\$ ~/apitap-057-pullback/bin/python -c 'import apitap; ...'"
    ~/apitap-057-pullback/bin/python - <<'PY'
import apitap, hashlib, os, sys
so = os.path.join(os.path.dirname(apitap.__file__), "_apitap.abi3.so")
print("  apitap.__version__      ", apitap.__version__)
print("  interpreter             ", sys.version.split()[0], "(abi3 wheel)")
print("  _apitap.abi3.so md5     ", hashlib.md5(open(so, "rb").read()).hexdigest())
PY
    echo "\$ ~/ingestr-venv/bin/pip show ingestr"
    ~/ingestr-venv/bin/pip show ingestr 2>/dev/null | sed -n 1,2p | sed 's/^/  /'
    echo "  ingestr 1.1.x is a native binary: the pip wrapper downloads and execs it"
    ls -la ~/.cache/ingestr/bin/v*/Linux_x86_64/ingestr | sed 's/^/  /'
    echo "  that exact executable is bind-mounted into the cage"
    echo

    echo "===== 2. the seed (10 identical tables, kept between runs)"
    echo "  seeded from bench.bench_my_1m with CREATE TABLE ... LIKE + INSERT ... SELECT"
    docker exec -i apitap-bench-my mysql -uroot -pbench -N -B bench -e "
      SELECT CONCAT('  ', table_name, ' rows=', COUNT(*))
      FROM information_schema.tables
      WHERE table_schema='bench' AND table_name LIKE 'cmp_my_t%' ORDER BY table_name" \
      2>/dev/null | head -10
    echo "  seeding wall time: 232 s for all 10 tables (excluded from every measurement)"
    echo "  columns per seed:"
    docker exec -i apitap-bench-my mysql -uroot -pbench -N -B bench -e "
      SELECT CONCAT('  ', COUNT(*), ' tables x ', MIN(c), ' columns')
      FROM (SELECT table_name, COUNT(*) AS c FROM information_schema.columns
            WHERE table_schema='bench' AND table_name LIKE 'cmp_my_t%'
            GROUP BY table_name) x" 2>/dev/null
    echo "  the schema (from bench_my_1m, LIKE-cloned into all 10):"
    docker exec -i apitap-bench-my mysql -uroot -pbench -N -B bench \
        -e "SHOW CREATE TABLE bench.cmp_my_t01" 2>/dev/null \
      | sed 's/\\n/\n      /g' | sed 's/^/  /'
    echo "  source aggregates (cached; identical for all 10 by construction):"
    bash "$HERE/bench-capped-my-ch-0.57.sh" srcsum | sed 's/^/  /'
    echo

    echo "===== 3. the control (validator proven both ways before anything was timed)"
    cat "$WORK/logs/control.log"
    echo

    echo "===== 4. what each tool pays in the cage BEFORE it moves a row"
    cat "$WORK/logs/startup.log" 2>/dev/null || {
        bash "$HERE/bench-capped-my-ch-0.57.sh" startup
    }
    echo

    echo "===== 5. the campaign: 3 interleaved rounds, every arm checksum-verified"
    cat "$WORK/logs/campaign.log"
    echo

    echo "===== 6. the ceiling probe (how many rows ONE ingestr process lands at this cap)"
    cat "$WORK/logs/ceiling.log" 2>/dev/null || bash "$HERE/bench-capped-my-ch-0.57.sh" ceiling
    echo

    echo "===== 7. host state around every leg"
    cat "$WORK/host-state.log"
    echo

    echo "===== 8. the rig afterwards"
    bash "$HERE/bench-capped-my-ch-0.57.sh" state
} > "$OUT" 2>&1
wc -l "$OUT"