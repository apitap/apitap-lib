#!/usr/bin/env bash
# Source-vs-destination digest for one prof_ table, through the campaign
# validators that already own each engine pair's digest definition:
#   pg  -> benches/bench-capped-pg-ch-validator.sh  (md5 prefix over 15 cols)
#   my  -> benchmarks/bench-capped-my-ch-validator.sh (CRC32 over 14 cols + json)
# Prints SRC/DST lines and MATCH|MISMATCH.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROUTE="${1:?pg|my}"
TABLE="${2:?table}"
V="$(cd "$HERE/.." && pwd)"

if [ "$ROUTE" = pg ]; then
  export PG_C=${PG_C:-apitap-bench-pg-src} PG_DB=${PG_DB:-apitap_bench_src} CH_C=${CH_C:-apitap-bench-ch}
  S=$(bash "$V/bench-capped-pg-ch-validator.sh" src "$TABLE" 2>/dev/null)
  D=$(bash "$V/bench-capped-pg-ch-validator.sh" dst "$TABLE" 2>/dev/null)
else
  export MY_C=${MY_C:-apitap-bench-my} MY_DB=${MY_DB:-bench} CH_C=${CH_C:-apitap-bench-ch}
  S=$(bash "$V/bench-capped-my-ch-validator.sh" src "$TABLE" 2>/dev/null)
  D=$(bash "$V/bench-capped-my-ch-validator.sh" dst "$TABLE" 2>/dev/null)
fi
echo "SRC $ROUTE $TABLE $S"
echo "DST $ROUTE $TABLE $D"
if [ -n "$S" ] && [ "$S" = "$D" ]; then echo "VERDICT MATCH"; else echo "VERDICT MISMATCH"; fi
