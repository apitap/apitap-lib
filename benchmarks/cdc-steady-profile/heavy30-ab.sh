#!/usr/bin/env bash
# The heavy30 knob sweep: sequential PREBUILD backlog arms on one lane. Each
# arm generates NTX*1000 changes/table/round with NO drain (a real backlog),
# then starts the capped drain and measures pure catch-up (the honest capacity
# metric). Arms share the converged state; the fixed rounds are net-zero, so
# IBASE ranges are reusable across arms.
#
#   heavy30-ab.sh <pg|my> <arm> [<arm> ...]
#   arm := name[:ENV=VAL[,ENV=VAL...]]      e.g. follow30:APITAP_FOLLOW_SECS=30
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROUTE="${1:?pg|my}"; shift
NTX="${NTX:-100}"
CH_CT=${CH_CT:-apitap-bench-ch3}

preclean() { # drop stale apitap lease/staging markers when no drain is running
  if docker ps --format '{{.Names}}' | grep -q '^prof-heavy-'; then
    echo "[preclean] a drain is running — leaving markers alone"
    return 0
  fi
  for t in $(docker exec "$CH_CT" clickhouse-client --password bench -q \
      "SELECT name FROM system.tables WHERE database='default' AND (name LIKE 'prof_%__apitap_staging' OR name LIKE 'prof_%__apitap_lock')" 2>/dev/null); do
    docker exec "$CH_CT" clickhouse-client --password bench -q "DROP TABLE IF EXISTS default.\`$t\`" 2>/dev/null
  done
}

for spec in "$@"; do
  name="${spec%%:*}"; envs=""
  [ "$spec" != "$name" ] && envs="${spec#*:}"
  echo "== ARM $name envs=[${envs:-defaults}] ntx=$NTX $(date -u +%H:%M:%S) =="
  preclean
  # shellcheck disable=SC2086
  env PREBUILD=1 NTX="$NTX" $(printf '%s' "$envs" | tr ',' ' ') \
      bash "$HERE/heavy30-seq.sh" "$ROUTE" "ab-$name" > "/tmp/ab-$name.log" 2>&1
  grep -E "^TAG |^ARM |^LEG_DONE|^MEMPEAK|^MEMEVENTS|^VALIDATE|^DRAIN_STATE" \
      "$HOME/bench-cdc-steady/logs/ab-$name.summary" 2>/dev/null | tr '\n' ' '; echo
done
echo "AB_SWEEP_DONE route=$ROUTE $(date -u +%H:%M:%S)"
