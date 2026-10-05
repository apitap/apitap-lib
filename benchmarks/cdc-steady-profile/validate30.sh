#!/usr/bin/env bash
# Source-vs-destination digest for MANY prof_ tables (comma list), one per line
# through the same per-engine validator validate.sh uses. Prints a summary:
#   VALIDATE_ALL route=<r> tables=<n> match=<m> mismatch=<k>
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROUTE="${1:?pg|my}"
TABLES="${2:?comma list}"
match=0; mismatch=0; n=0
IFS=',' read -r -a arr <<< "$TABLES"
for t in "${arr[@]}"; do
  [ -z "$t" ] && continue
  n=$((n + 1))
  out=$(bash "$HERE/validate.sh" "$ROUTE" "$t")
  v=$(printf '%s\n' "$out" | grep -o 'VERDICT [A-Z]*' | tail -1)
  if [ "$v" = "VERDICT MATCH" ]; then
    match=$((match + 1))
  else
    mismatch=$((mismatch + 1))
    printf '%s\n' "$out"
    echo "FAILED_TABLE $t"
  fi
done
echo "VALIDATE_ALL route=$ROUTE tables=$n match=$match mismatch=$mismatch"
[ "$mismatch" -eq 0 ] && exit 0 || exit 1
