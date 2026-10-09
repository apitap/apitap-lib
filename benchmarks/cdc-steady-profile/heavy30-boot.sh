#!/usr/bin/env bash
# Bootstrap the heavy 30-table group into the CH destination under the
# 256MB / 0.5-CPU cage, through the campaign converge harness (which validates
# 30/30 at the end). The source tables must already be seeded.
#   heavy30-boot.sh <pg|my> <tag>
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROUTE="${1:?pg|my}"; TAG="${2:?tag}"
N=${N:-30}
export CPUS="${CPUS:-0.5}"
export SP="${SP:-$HOME/gate-venv/lib/python3.13/site-packages}"
export CH_DST="${CH_DST:-clickhouse://default:bench@127.0.0.1:8126/default}"
export CH_C="${CH_C:-apitap-bench-ch3}"
if [ "$ROUTE" = pg ]; then
  TBL=$(for i in $(seq -w 1 $N); do printf 'public.prof_pg_t%s,' "$i"; done | sed 's/,$//')
else
  TBL=$(for i in $(seq -w 1 $N); do printf 'prof_my_t%s,' "$i"; done | sed 's/,$//')
fi
APITAP_SLOTS="${APITAP_SLOTS:-auto}" bash "$HERE/converge.sh" "boot-$TAG" "$ROUTE" "${BUDGET:-5400}" "$TBL"
