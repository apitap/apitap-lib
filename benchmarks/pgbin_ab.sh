#!/bin/bash
# pgoutput `binary 'true'` A/B — does binary tuple encoding cut the CPU of
# the ONE pegged walsender process that is the measured per-slot CDC ceiling
# (benchmarks/gcp-cdc-100tables.md Part 7)?
#
# Phase A isolates the walsender with pg_recvlogical (no apitap in the
# loop): three slots created BEFORE one 5M-change generation drain the SAME
# WAL bytes — text, binary, and a discards-everything publication (the
# non-shardable decode floor finding 2 needs quantified).
# Phase B is the end-to-end apitap A/B with digest verification: T,B,B,T
# rounds so destination growth cancels across modes.
#
# Every drain ends with a dest drop (standing rule) and the ab_* slots are
# dropped as soon as their phase is done so the WAL is released.
exec > >(tee -a /home/ubuntu/pgbin-ab.log) 2>&1
ts() { date +"[%H:%M:%S]"; }
die() { echo "$(ts) STEP FAILED — aborting"; cleanup_slots; exit 1; }

B=/home/ubuntu/apitap-lib/benchmarks
HOSTPY=/home/ubuntu/ice-run/bin/python
V="-v /home/ubuntu/read-venv:/venv"
PGX="docker exec -i apitap-bench-pg-src psql -U postgres -d apitap_bench_src -tA"
CHANGES=${CHANGES:-5000000}

cleanup_slots() {
  $PGX -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'ab_%'" >/dev/null 2>&1
}

# ── walsender-CPU-instrumented run: $1=outfile-prefix, rest=command ─────────
measured() {
  local pfx=$1; shift
  rm -f "/tmp/ws_$pfx.txt"
  python3 "$B/pgbin_sample.py" "/tmp/ws_$pfx.txt" & local sampler=$!
  local s e
  s=$(date +%s.%N)
  "$@"; local rc=$?
  e=$(date +%s.%N)
  sleep 0.6   # one extra tick so the sampler catches the final /proc read
  kill $sampler 2>/dev/null; wait $sampler 2>/dev/null
  WS_CPU=$(awk '/total_cpu_seconds/{print $2}' "/tmp/ws_$pfx.txt" 2>/dev/null)
  WALL=$(awk -v a="$s" -v b="$e" 'BEGIN{printf "%.1f", b-a}')
  return $rc
}

echo ""
echo "$(ts) ============ pgoutput binary A/B — $CHANGES changes/leg ============"

# ── install + HARD wheel-swap verification ──────────────────────────────────
WHEEL_PATH=${WHEEL_PATH:-$(ls -t /home/ubuntu/apitap-lib/target/wheels/apitap-*.whl | head -1)}
[ -f "$WHEEL_PATH" ] || die
echo "$(ts) wheel: $WHEEL_PATH"
WDIR=$(dirname "$WHEEL_PATH"); WNAME=$(basename "$WHEEL_PATH")
docker run --rm $V -v "$WDIR":/w:ro python:3.11-slim \
  sh -c "/venv/bin/pip install --force-reinstall --no-deps /w/$WNAME" | tail -1
WANT=$(unzip -p "$WHEEL_PATH" "apitap/_apitap*.so" | md5sum | cut -d' ' -f1)
GOT=$(docker run --rm $V python:3.11-slim \
  sh -c "md5sum /venv/lib/python3.11/site-packages/apitap/_apitap*.so" | cut -d' ' -f1)
echo "$(ts) installed .so md5=$GOT (wheel: $WANT)"
[ "$WANT" = "$GOT" ] || die

# ── clean slate ──────────────────────────────────────────────────────────────
echo "$(ts) [1/6] clean slate + seed"
$HOSTPY "$B/pgbin_ab.py" cleanup || die
$HOSTPY "$B/pgbin_ab.py" seed || die

# ── Phase A: three slots over ONE generation ────────────────────────────────
echo "$(ts) [2/6] create ab_ slots + publications, then generate"
$PGX >/dev/null <<'SQL' || die
DROP TABLE IF EXISTS bin_dummy; CREATE TABLE bin_dummy(id int primary key);
DROP PUBLICATION IF EXISTS ab_pub;
DROP PUBLICATION IF EXISTS ab_pub_none;
CREATE PUBLICATION ab_pub FOR TABLE
  bin_t01,bin_t02,bin_t03,bin_t04,bin_t05,bin_t06,bin_t07,bin_t08,bin_t09,bin_t10;
CREATE PUBLICATION ab_pub_none FOR TABLE bin_dummy;
SELECT pg_create_logical_replication_slot('ab_text','pgoutput');
SELECT pg_create_logical_replication_slot('ab_bin','pgoutput');
SELECT pg_create_logical_replication_slot('ab_none','pgoutput');
SQL
GEN_OUT=$($HOSTPY "$B/pgbin_ab.py" gen "$CHANGES") || die
echo "$GEN_OUT"
N=$(echo "$GEN_OUT" | awk '/^GEN /{sub("changes=","",$2); print $2}')
ENDPOS=$($PGX -c "SELECT pg_current_wal_lsn()")
echo "$(ts)   endpos=$ENDPOS changes=$N"

recv_leg() { # $1=slot $2=pub $3=extra-opts $4=label
  local slot=$1 pub=$2 extra=$3 label=$4
  measured "$slot" docker exec apitap-bench-pg-src bash -c \
    "set -o pipefail; pg_recvlogical -U postgres -d apitap_bench_src -S $slot --start --no-loop \
     --endpos=$ENDPOS -o proto_version=2 -o publication_names=$pub $extra -f - | wc -c" \
     > "/tmp/bytes_$slot.txt" || die
  local bytes; bytes=$(tr -d '[:space:]' < "/tmp/bytes_$slot.txt")
  echo "$(ts)   [$label] wall=${WALL}s walsender_cpu=${WS_CPU}s wire_bytes=$bytes"
  echo "A_RESULT leg=$label wall=$WALL ws_cpu=$WS_CPU bytes=$bytes changes=$N"
}

echo "$(ts) [3/6] Phase A — pg_recvlogical over identical WAL"
recv_leg ab_text ab_pub      ""                 "text  "
recv_leg ab_bin  ab_pub      "-o binary=true"   "binary"
recv_leg ab_none ab_pub_none ""                 "discard-all"
cleanup_slots
echo "$(ts)   ab_ slots dropped (WAL released)"

# ── Phase B: apitap end-to-end, digests verified every leg ──────────────────
echo "$(ts) [4/6] Phase B — bootstrap"
$HOSTPY "$B/pgbin_ab.py" dropdest || die
drain() { # $1=label $2=binary(0/1)
  local label=$1 env=""
  [ "$2" = 1 ] && env="-e APITAP_PG_BINARY=1"
  measured "e2e_$label" docker run --rm --network host $env $V \
    -v "$B/pgbin_drain.py:/s.py:ro" python:3.11-slim /venv/bin/python /s.py "$label" || die
  echo "$(ts)   [$label] walsender_cpu=${WS_CPU}s (wall=${WALL}s)"
  echo "B_WALSENDER leg=$label ws_cpu=$WS_CPU wall=$WALL"
}
drain bootstrap 0

round() { # $1=round-name $2=binary(0/1)
  echo "$(ts) [5/6] round $1 (binary=$2) — gen $CHANGES then drain"
  $HOSTPY "$B/pgbin_ab.py" gen "$CHANGES" || die
  drain "$1" "$2"
  drain "$1-catchup" "$2"   # must pick up 0 rows or the timed number lies
  $HOSTPY "$B/pgbin_ab.py" verify || die
}
round T1 0
round B1 1
round B2 1
round T2 0

echo "$(ts) [6/6] cleanup — dest tables dropped, slots dropped, seeds kept"
$HOSTPY "$B/pgbin_ab.py" cleanup || die
echo "$(ts) PGBIN A/B DONE"
