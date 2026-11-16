#!/usr/bin/env bash
# Release check: the extension a wheel ships links no symbol newer than its
# manylinux tag allows. Today's rule: no undefined `mallinfo2` (glibc 2.33;
# the wheels are manylinux2014, glibc 2.17, and load with `-z now`, so a
# direct call fails `import apitap` on every older glibc — W1 review R1).
# Resolve such symbols at run time (dlsym) instead.
#
# usage: check-wheel-symbols.sh <wheel.whl | _apitap.abi3.so>   (exit 1 on a finding)
set -euo pipefail
IN="${1:?wheel or .so}"
T=""
if [[ "$IN" == *.whl ]]; then
    T=$(mktemp -d)
    python3 -m zipfile -e "$IN" "$T"
    SO=$(find "$T" -name '_apitap*.so' | head -1)
else
    SO="$IN"
fi
[ -f "$SO" ] || { echo "check-wheel-symbols: no extension found in $IN"; exit 1; }
BAD=$(nm -D --undefined-only "$SO" | grep -E '\bmallinfo2\b' || true)
[ -n "$T" ] && rm -rf "$T"
if [ -n "$BAD" ]; then
    echo "check-wheel-symbols: FAIL $IN links $BAD (glibc 2.33; resolve it with dlsym)"
    exit 1
fi
echo "check-wheel-symbols: OK $IN (no undefined mallinfo2)"
