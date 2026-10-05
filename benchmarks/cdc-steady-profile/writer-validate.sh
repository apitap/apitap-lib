#!/usr/bin/env bash
# Control-the-test: prove the writer can SUSTAIN an offered rate, with no
# drain attached, using only the source server's own witnesses (WAL LSN +
# pg_stat_user_tables for pg; binlog position + performance_schema
# ROWS_AFFECTED + Com_commit for mysql). The writer's ledger is printed too,
# but it is the server numbers the report quotes.
#
#   writer-validate.sh <tag> <pg|my> <threads> <rate|0=max> <duration_s>
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
export WORK=${WORK:-$HOME/bench-cdc-steady}
mkdir -p "$WORK/logs"
TAG="${1:?tag}"; ROUTE="${2:?pg|my}"; THREADS="${3:?threads}"
RATE="${4:?rate}"; DURATION="${5:?duration_s}"; MAXC="${6:-0}"

if [ "$ROUTE" = pg ]; then
  URL="postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
  TABLE="public.prof_pg_m"; DIALECT=pg
else
  URL="mysql://root:bench@127.0.0.1:3307/bench"
  TABLE="prof_my_m"; DIALECT=mysql
fi
LOG="$WORK/logs/$TAG.writer.log"
rm -f "$LOG"

setsid nohup "$HOME/prof-venv/bin/python" "$HERE/writer.py" --dialect "$DIALECT" \
  --url "$URL" --tables "$TABLE" --threads "$THREADS" --rate "$RATE" \
  --duration "$DURATION" --max-changes "$MAXC" > "$LOG" 2>&1 < /dev/null &
# artifact-first wait: WRITER_TICK exists only once transactions commit
for _ in $(seq 1 90); do grep -q WRITER_TICK "$LOG" 2>/dev/null && break; sleep 0.5; done
grep -q WRITER_TICK "$LOG" || { echo "writer failed to start"; sed -n '1,30p' "$LOG"; exit 1; }
for _ in $(seq 1 $((DURATION + 300))); do grep -q WRITER_TOTAL "$LOG" 2>/dev/null && break; sleep 1; done
grep -q WRITER_TOTAL "$LOG" || { echo "writer never finished"; tail -5 "$LOG"; exit 1; }
echo "===== $TAG writer ====="
grep -E "WRITER_START|WRITER_TICK|WRITER_TOTAL|WRITER_ERROR|WRITER_WITNESS|WRITER_WAL_BYTES|WRITER_BINLOG_BYTES" "$LOG" | tail -60
echo "===== $TAG end ====="
