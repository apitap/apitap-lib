#!/usr/bin/env bash
# Unix-socket vs TCP probe for Postgres logical replication (pgoutput) and the
# COPY plane, all inside apitap-bench-pg-src so the client binary and the kernel
# are identical across transports.
#
#   probe-unix.sh
#
# Creates a temporary publication + two slots on public.prof_pg_m, runs a paced
# writer, streams each slot for 10 s (unix socket vs 127.0.0.1 TCP), times both
# with bash's TIMEFORMAT, then the binary COPY plane 3x per transport.
# Drops the slots and publication when done.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=${WORK:-$HOME/waltcp}
mkdir -p "$WORK/logs"
PG_C=${PG_C:-apitap-bench-pg-src}
PG_DB=${PG_DB:-apitap_bench_src}
TABLE=${TABLE:-public.prof_pg_m}
PUB=waltcp_probe_pub
SLOT_U=waltcp_probe_u
SLOT_T=waltcp_probe_t
RATE=${RATE:-20000}
WS=${WS:-45}
WRITER=${WRITER:-$HOME/apitap-lib/benchmarks/cdc-steady-profile/writer.py}
WPY=${WPY:-$HOME/prof-venv/bin/python}
SRC=${SRC:-postgres://postgres:bench@127.0.0.1:5544/$PG_DB}
OUT="$WORK/logs/probe-unix.log"
exec > >(tee "$OUT") 2>&1

echo "== probe setup: publication + two slots =="
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -v ON_ERROR_STOP=1 -c \
  "DROP PUBLICATION IF EXISTS $PUB; CREATE PUBLICATION $PUB FOR TABLE $TABLE;"
for s in "$SLOT_U" "$SLOT_T"; do
  docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c \
    "SELECT pg_drop_replication_slot('$s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$s' AND NOT active);" >/dev/null
  docker exec "$PG_C" pg_recvlogical -h /var/run/postgresql -p 5432 -U postgres \
    -d "$PG_DB" -S "$s" -P pgoutput -o proto_version=1 \
    -o publication_names="$PUB" --create-slot
done
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "select slot_name||' '||confirmed_flush_lsn||' '||active from pg_replication_slots where slot_name like 'waltcp%'"

echo "== writer: $RATE changes/s for ${WS}s =="
setsid nohup "$WPY" "$WRITER" --dialect pg --url "$SRC" --tables "$TABLE" \
  --threads 2 --rate "$RATE" --duration "$WS" > "$WORK/logs/probe-unix.writer.log" 2>&1 < /dev/null &
WPID=$!
for _ in $(seq 1 120); do grep -q WRITER_TICK "$WORK/logs/probe-unix.writer.log" 2>/dev/null && break; sleep 0.5; done
grep -q WRITER_TICK "$WORK/logs/probe-unix.writer.log" || { echo "writer failed"; exit 1; }
sleep 3

stream() { # tag host slot
  local tag=$1 host=$2 slot=$3
  echo "== stream $tag slot=$slot host=$host =="
  docker exec "$PG_C" bash -c \
    "TIMEFORMAT='real=%R user=%U sys=%S'; time timeout -s INT 10 pg_recvlogical -h '$host' -p 5432 -U postgres -d '$PG_DB' -S '$slot' --start -f /tmp/$tag.out -P pgoutput -o proto_version=1 -o publication_names='$PUB' -s 1 -n" 2>&1
  docker exec "$PG_C" sh -c "wc -c /tmp/$tag.out; rm -f /tmp/$tag.out"
}

# packet count on the TCP leg, inside the PG container's netns
PGPID=$(docker inspect -f '{{.State.Pid}}' "$PG_C")
sudo nsenter -t "$PGPID" -n tcpdump -i lo -nn -s 0 -w "$WORK/logs/probe-unix.pcap" \
  'tcp port 5432' > "$WORK/logs/probe-unix.tcpdump.txt" 2>&1 &
TCPDUMP_PID=$!
sleep 1
stream tcp 127.0.0.1 "$SLOT_T"
sleep 1
sudo kill "$TCPDUMP_PID" 2>/dev/null
sleep 1
echo "== tcpdump =="
tail -3 "$WORK/logs/probe-unix.tcpdump.txt"
python3 "$HERE/pcap_flows.py" "$WORK/logs/probe-unix.pcap" 2>&1 | head -8

stream unix /var/run/postgresql "$SLOT_U"

echo "== COPY plane: binary COPY TO STDOUT, 3x per transport =="
for i in 1 2 3; do
  docker exec "$PG_C" bash -c \
    "TIMEFORMAT='unix real=%R user=%U sys=%S'; time psql -h /var/run/postgresql -U postgres -d '$PG_DB' -c 'COPY $TABLE TO STDOUT (FORMAT binary)' > /dev/null" 2>&1
done
for i in 1 2 3; do
  docker exec "$PG_C" bash -c \
    "TIMEFORMAT='tcp  real=%R user=%U sys=%S'; time psql -h 127.0.0.1 -p 5432 -U postgres -d '$PG_DB' -c 'COPY $TABLE TO STDOUT (FORMAT binary)' > /dev/null" 2>&1
done

wait "$WPID" 2>/dev/null
grep -E "WRITER_TOTAL|WRITER_WAL_BYTES" "$WORK/logs/probe-unix.writer.log" | tail -3

echo "== cleanup: drop slots + publication =="
for s in "$SLOT_U" "$SLOT_T"; do
  docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c \
    "SELECT pg_drop_replication_slot('$s') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$s' AND NOT active);" >/dev/null
done
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -c "DROP PUBLICATION IF EXISTS $PUB;" >/dev/null
docker exec "$PG_C" psql -U postgres -d "$PG_DB" -tAc \
  "select count(*) from pg_replication_slots where slot_name like 'waltcp%'; select count(*) from pg_publication where pubname like 'waltcp%'"
echo "PROBE_DONE"
