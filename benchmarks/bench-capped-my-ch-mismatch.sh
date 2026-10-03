#!/usr/bin/env bash
# What to do when a checksum disagrees: name the offending ROW, then the
# offending COLUMN, using the SAME aggregate the measurement is scored with (a
# debug query built from a copy of it is a debug query that can drift).
#
#   bench-capped-my-ch-mismatch.sh cols  TABLE          per-column diff, both engines
#   bench-capped-my-ch-mismatch.sh row   TABLE ID       one row's digest, both engines
#   bench-capped-my-ch-mismatch.sh bisect TABLE [LO HI] first id whose digest differs
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
V="$HERE/bench-capped-my-ch-validator.sh"

cols() {
    local T=$1
    echo "== MySQL"
    bash "$V" cols src "$T"
    echo "== ClickHouse"
    bash "$V" cols dst "$T"
    echo
    echo "== per-column diff (mysql vs clickhouse)"
    python3 - "$HERE/.percol.my" "$HERE/.percol.ch" <<'PY'
import sys
def rd(p):
    out = {}
    for line in open(p):
        if '=' in line:
            k, v = line.rstrip('\n').split('=', 1)
            out[k.strip()] = v
    return out
my, ch = rd(sys.argv[1]), rd(sys.argv[2])
bad = 0
for k, a in my.items():
    b = ch.get(k)
    if a != b:
        bad += 1
        print(f"  DIFF {k:20} mysql={a!s:<28} ch={b!s:<28}")
print(f"  {len(my) - bad}/{len(my)} per-column aggregates agree")
PY
}

row() { # TABLE ID — the row digest from each engine, split into its 14 fields
    local T=$1 ID=$2
    bash "$V" row src "$T" "$ID" > "$HERE/.row.my"
    bash "$V" row dst "$T" "$ID" > "$HERE/.row.ch"
    python3 - "$HERE/.row.my" "$HERE/.row.ch" <<'PY'
import sys
names = ("id small_str medium_str large_str tiny_int regular_int big_int float_scaled "
         "decimal_scaled bool_val date_val ts_val ts_tz_val extra_text").split()
def rd(p):
    return bytes.fromhex(open(p).read().strip()).decode('utf-8', 'replace')
my, ch = rd(sys.argv[1]), rd(sys.argv[2])
mf, cf = my.split('\x1f'), ch.split('\x1f')
print(f"  fields: mysql={len(mf)} ch={len(cf)}   (14 expected)")
for i, n in enumerate(names):
    a = mf[i] if i < len(mf) else '<MISSING>'
    b = cf[i] if i < len(cf) else '<MISSING>'
    print(f"  {'ok  ' if a == b else 'DIFF'} {n:15} mysql={a!r:<38} ch={b!r}")
PY
}

bisect() { # TABLE [LO HI] — narrow the range until ONE id's digest differs
    local T=$1 lo=${2:-1} hi=${3:-1000000}
    echo "== range $lo..$hi  my=$(bash "$V" src "$T" "id BETWEEN $lo AND $hi")  ch=$(bash "$V" dst "$T" "id BETWEEN $lo AND $hi")"
    while (( hi - lo > 1 )); do
        local mid=$(( (lo + hi) / 2 ))
        local a b
        a=$(bash "$V" src "$T" "id BETWEEN $lo AND $mid")
        b=$(bash "$V" dst "$T" "id BETWEEN $lo AND $mid")
        if [[ "$a" != "$b" ]]; then hi=$mid; else lo=$(( mid + 1 )); fi
    done
    echo "== first disagreeing id: $lo"
    echo "   my $(bash "$V" src "$T" "id = $lo")"
    echo "   ch $(bash "$V" dst "$T" "id = $lo")"
    row "$T" "$lo"
}

case "${1:-}" in
cols) cols "${2:?table}" ;;
row) row "${2:?table}" "${3:?id}" ;;
bisect) bisect "${2:?table}" "${3:-}" "${4:-}" ;;
*) sed -n 2,10p "$0"; exit 2 ;;
esac
rm -f "$HERE"/.percol.my "$HERE"/.percol.ch "$HERE"/.row.my "$HERE"/.row.ch