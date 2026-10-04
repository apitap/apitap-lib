#!/bin/bash
# Probe 2: pin the two things probe 1 got wrong, on measured values.
#   1. ClickHouse reinterpretAsUInt32 is LITTLE-endian, so a naive 32-bit
#      md5 prefix disagrees with PostgreSQL's bit(32) on the same md5. Build the
#      byte-reversed hex prefix explicitly and check it matches PG.
#   2. toString(Decimal(18,4)) drops trailing zeros ('1234.56' where PG prints
#      '1234.5600'), so a decimal must enter the digest as a scaled integer.
set -u
CH() { docker exec -i apitap-bench-ch clickhouse-client --password bench -q "$1"; }
PG() { docker exec -i apitap-bench-ws-pg psql -U postgres -d smoke -Atc "$1"; }

for s in abc 'hello world' ''; do
  echo "--- input: [$s]"
  pg=$(PG "SELECT ((chr(120)||substr(md5('$s'),1,8))::bit(32)::bigint)")
  ch_naive=$(CH "SELECT toUInt64(reinterpretAsUInt32(unhex(substr(lower(hex(MD5('$s'))),1,8))))")
  ch_rev=$(CH "SELECT toUInt64(reinterpretAsUInt32(unhex(
        substr(lower(hex(MD5('$s'))),7,2)||substr(lower(hex(MD5('$s'))),5,2)
     || substr(lower(hex(MD5('$s'))),3,2)||substr(lower(hex(MD5('$s'))),1,2))))")
  echo "  pg=$pg  ch_naive=$ch_naive  ch_reversed=$ch_rev"
  [[ "$pg" == "$ch_rev" ]] && echo "  REVERSED MATCHES PG" || echo "  *** REVERSED DIFFERS ***"
done

echo
echo "== decimal as a scaled integer on both sides =="
PG "SELECT trunc((1234.5600::numeric(18,4)/100)*10000)::bigint, trunc((0.0001::numeric(18,4))*10000)::bigint"
CH "SELECT toInt64(toDecimal64('12.34',4)*10000), toInt64(toDecimal64('0.0001',4)*10000)"

echo
echo "== bool normalised to 1/0 on both sides =="
PG "SELECT coalesce((bool 't')::int::text,'<NUL>'), coalesce((bool 'f')::int::text,'<NUL>')"
CH "SELECT if(toBool(1),'1','0'), if(toBool(0),'1','0')"

echo
echo "== 1M-row digest sums: both engines in the same integer domain =="
CH "SELECT toUInt64(sum(reinterpretAsUInt32(unhex(
        substr(lower(hex(MD5(toString(number)))),7,2)||substr(lower(hex(MD5(toString(number)))),5,2)
     || substr(lower(hex(MD5(toString(number)))),3,2)||substr(lower(hex(MD5(toString(number)))),1,2))))) AS ch_sum
     FROM numbers(1000000)"
PG "SELECT sum(((chr(120)||substr(md5(number::text),1,8))::bit(32)::bigint)) AS pg_sum
     FROM generate_series(0,999999) AS number"