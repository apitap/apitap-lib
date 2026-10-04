#!/bin/bash
# Throwaway probe: which digest primitives exist on BOTH engines, and do their
# renderings agree byte for byte? Written before the validator so the validator
# is built on measured facts, not on assumptions about either engine's SQL.
set -u
CH() { docker exec -i apitap-bench-ch clickhouse-client --password bench -q "$1"; }
PG() { docker exec -i apitap-bench-ws-pg psql -U postgres -d smoke -Atc "$1"; }

echo "== CH: MD5, hex, unhex, reinterpret =="
CH "SELECT hex(MD5('abc')) AS h, toUInt64(reinterpretAsUInt32(unhex(substr(lower(hex(MD5('abc'))),1,8)))) AS first4"
echo "== PG: md5, same 32-bit prefix =="
PG "SELECT md5('abc'), ((chr(120)||substr(md5('abc'),1,8))::bit(32)::bigint)"
echo
echo "== CH: type renderings =="
CH "SELECT toString(toDateTime64('2024-01-01 00:00:00.000001',6)) AS ts6,
        toString(toDateTime64('2024-01-01 00:00:00.000001',6,'UTC')) AS ts6utc,
        toString(toBool(1)) AS b, toString(toBool(0)) AS b0,
        toString(toDecimal64('1234.5600',4)) AS d,
        toString(toFloat64(1249.875)) AS f,
        toString(toDate32('2024-03-05')) AS dt,
        toString(toInt64(1249)) AS i"
echo "== PG: the same renderings =="
PG "SET TIME ZONE 'UTC';
    SELECT to_char(timestamp '2024-01-01 00:00:00.000001','YYYY-MM-DD HH24:MI:SS.US'),
           (1234.5600::numeric(18,4))::text,
           (1249.875::float8)::text,
           date '2024-03-05'::text,
           1249::bigint::text"
echo
echo "== CH: nullable -> coalesce + Nullable concat behaviour =="
CH "SELECT toTypeName(concat(toString(x))) AS t FROM (SELECT CAST(NULL AS Nullable(String)) AS x) s"
CH "SELECT md5(coalesce(x,'<NUL>')) AS m FROM (SELECT CAST(NULL AS Nullable(String)) AS x) s"
echo "== CH: sum() over UInt32 -> UInt64, 1M rows, no overflow =="
CH "SELECT sum(x) AS s, toTypeName(sum(x)) FROM (SELECT toUInt32(number*7919) AS x FROM numbers(1000000))"
echo "== PG: same range summed in bigint =="
PG "SELECT sum((number*7919)::bigint) FROM generate_series(0,999999) AS number"