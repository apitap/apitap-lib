#!/usr/bin/env bash
# Campaign driver for the steady-profile rig. One measured shape per call, all
# artifacts under ~/bench-cdc-steady/logs/<tag>.*, one line per measurement in
# ~/bench-cdc-steady/results.tsv.
#
#   campaign.sh prep   <pg|my>                          drop + reseed
#   campaign.sh boot   <pg|my> [cores]                  empty dest -> converged state
#   campaign.sh steady <pg|my> <tag> <cores> <rate> <writer_s> <drain_s> [perf_s]
#   campaign.sh backlog <pg|my> <tag> <cores> <changes> <rate>   # 0 rate = writer max
#   campaign.sh scale  <pg|my> <cores> [cores...]       backlog-drain ceiling per quota
#   campaign.sh purge  <pg|my>                          free WAL/binlog and dest
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
LOGS="$WORK/logs"
RES="$WORK/results.tsv"
mkdir -p "$LOGS"

table_of() { [ "$1" = pg ] && echo prof_pg_m || echo prof_my_m; }

record() { # tag route cores writer_note drain_note
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -u +%H:%M:%S)" "$1" "$2" "$3" "$4" "$5" >> "$RES"
}

purgelogs() { # route
  if [ "$1" = my ]; then
    cur=$(docker exec apitap-bench-my mysql -uroot -pbench -N -B -e "SHOW MASTER STATUS" 2>/dev/null | awk '{print $1}')
    [ -n "$cur" ] && docker exec apitap-bench-my mysql -uroot -pbench -e "PURGE BINARY LOGS TO '$cur'" 2>/dev/null
  else
    docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -c CHECKPOINT >/dev/null 2>&1
  fi
}

cmd="${1:?prep|boot|steady|backlog|scale|purge|purgelogs}"
case "$cmd" in
prep)
  route="${2:?pg|my}"
  bash "$HERE/drop.sh"
  bash "$HERE/prep.sh"
  ;;
boot)
  route="${2:?pg|my}"; cores="${3:-0.5}"
  tag="boot-$route"
  CPUS="$cores" bash "$HERE/converge.sh" "$tag" "$route" 900 > "$LOGS/$tag.out" 2>&1
  bash "$HERE/busyrate.sh" "$LOGS/$tag.converge.log"
  ;;
steady)
  route="${2:?}"; tag="${3:?}"; cores="${4:?}"; rate="${5:?}"; ws="${6:?}"; db="${7:?}"; perf="${8:-0}"
  CPUS="$cores" bash "$HERE/steady.sh" "$tag" "$route" "$rate" "$ws" "$db" 10 "$perf" 0 \
      > "$LOGS/$tag.steady.out" 2>&1
  echo "== $tag: writer"
  grep -E "WRITER_TOTAL|WRITER_WITNESS" "$LOGS/$tag.writer.log" | tail -2
  echo "== $tag: drain"
  bash "$HERE/busyrate.sh" "$LOGS/$tag.drain.log"
  echo "== $tag: validate"
  bash "$HERE/validate.sh" "$route" "$(table_of "$route")" | tee "$LOGS/$tag.validate.out"
  W=$(grep -oE "changes=[0-9]+" "$LOGS/$tag.writer.log" | tail -1 | cut -d= -f2)
  record "$tag" "$route" "$cores" "writer=$W" "$(bash "$HERE/busyrate.sh" "$LOGS/$tag.drain.log" | head -1)"
  purgelogs "$route"
  ;;
backlog)
  route="${2:?}"; tag="${3:?}"; cores="${4:?}"; changes="${5:?}"; rate="${6:-0}"
  # Phase 1: writer alone, capped at <changes> (no drain).
  bash "$HERE/writer-validate.sh" "$tag-writer" "$route" 4 "$rate" 3600 "$changes" \
      > "$LOGS/$tag.writer.out" 2>&1
  echo "== $tag: writer"
  grep -E "WRITER_TOTAL|WRITER_WITNESS|WRITER_(WAL|BINLOG)_BYTES" "$LOGS/$tag-writer.writer.log" | tail -3
  # Phase 2: drain the backlog at the quota, writer stopped.
  CPUS="$cores" bash "$HERE/converge.sh" "$tag" "$route" 5400 > "$LOGS/$tag.out" 2>&1
  bash "$HERE/busyrate.sh" "$LOGS/$tag.converge.log"
  record "$tag" "$route" "$cores" "backlog=$changes" "$(bash "$HERE/busyrate.sh" "$LOGS/$tag.converge.log" | head -1)"
  ;;
scale)
  route="${2:?}"; shift 2
  for cores in "$@"; do
    tag="scale-$route-$cores"
    bash "$HERE/drop.sh" > /dev/null
    bash "$HERE/prep.sh" > /dev/null
    CPUS="$cores" bash "$HERE/converge.sh" "boot-$tag" "$route" 900 > "$LOGS/boot-$tag.out" 2>&1
    bash "$HERE/writer-validate.sh" "$tag-writer" "$route" 4 0 3600 5000000 \
        > "$LOGS/$tag.writer.out" 2>&1
    CPUS="$cores" bash "$HERE/converge.sh" "$tag" "$route" 5400 > "$LOGS/$tag.out" 2>&1
    echo "== scale $route @ ${cores}cpu"
    bash "$HERE/busyrate.sh" "$LOGS/$tag.converge.log"
    record "$tag" "$route" "$cores" "backlog=5000000" "$(bash "$HERE/busyrate.sh" "$LOGS/$tag.converge.log" | head -1)"
    # free the binlog/WAL this point generated before the next one
    if [ "$route" = my ]; then
      cur=$(docker exec apitap-bench-my mysql -uroot -pbench -N -B -e "SHOW MASTER STATUS" 2>/dev/null | awk '{print $1}')
      [ -n "$cur" ] && docker exec apitap-bench-my mysql -uroot -pbench -e "PURGE BINARY LOGS TO '$cur'" 2>/dev/null
    else
      docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -c CHECKPOINT >/dev/null 2>&1
    fi
    df -h / | tail -1
  done
  ;;
purge)
  route="${2:?pg|my}"
  # free the destination tables and the source-side log space this campaign owns
  bash "$HERE/drop.sh"
  if [ "$route" = my ]; then
    cur=$(docker exec apitap-bench-my mysql -uroot -pbench -N -B -e "SHOW MASTER STATUS" 2>/dev/null | awk '{print $1}')
    if [ -n "$cur" ]; then
      docker exec apitap-bench-my mysql -uroot -pbench -e "PURGE BINARY LOGS TO '$cur'" 2>/dev/null
      echo "purged binlogs to $cur ($(docker exec apitap-bench-my mysql -uroot -pbench -N -B -e 'SHOW BINARY LOGS' 2>/dev/null | wc -l) files left)"
    fi
  else
    docker exec apitap-bench-pg-src psql -U postgres -d apitap_bench_src -c CHECKPOINT >/dev/null 2>&1
  fi
  df -h / | tail -1
  ;;
purgelogs)
  purgelogs "${2:?pg|my}"
  df -h / | tail -1
  ;;
*)
  sed -n '2,14p' "$0"; exit 2
  ;;
esac
