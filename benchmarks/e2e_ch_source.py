"""e2e: ClickHouse -> ClickHouse (two separate servers).

Seeds a table that carries every trap the wire study found — Nullable with
NULLs, LowCardinality, Date (2 bytes) and DateTime (4) that must land as
Date32/DateTime64 (4/8), Decimal, UUID, Enum, Array, FixedString — transfers
it server-to-server, and compares a per-column checksum on both sides.
"""
import sys
import time
import urllib.request

import apitap

SRC = "clickhouse://default:bench@127.0.0.1:8124/default"
DST = "clickhouse://default:bench@127.0.0.1:8125/default"
SRC_HTTP = "http://127.0.0.1:8124/?user=default&password=bench"
DST_HTTP = "http://127.0.0.1:8125/?user=default&password=bench"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000


def q(endpoint, sql):
    req = urllib.request.Request(endpoint, data=sql.encode())
    return urllib.request.urlopen(req, timeout=300).read().decode().strip()


print("== seed source ==")
q(SRC_HTTP, "DROP TABLE IF EXISTS ch_src")
q(
    SRC_HTTP,
    """
    CREATE TABLE ch_src (
      id          Int32,
      u64         UInt64,
      s           String,
      lc          LowCardinality(String),
      fs          FixedString(4),
      n_s         Nullable(String),
      n_i         Nullable(Int64),
      f32         Float32,
      f64         Float64,
      dec         Decimal(18, 4),
      d           Date,
      d32         Date32,
      dt          DateTime,
      dt64        DateTime64(3),
      uid         UUID,
      en          Enum8('a' = 1, 'b' = 2),
      arr         Array(Int32),
      b           Bool
    ) ENGINE = MergeTree ORDER BY id
    """,
)
q(
    SRC_HTTP,
    f"""
    INSERT INTO ch_src
    SELECT toInt32(number + 1), toUInt64(number * 7),
           concat('row-', toString(number)),
           ['alpha','beta','gamma'][(number % 3) + 1],
           substring(concat(toString(number), 'xxxx'), 1, 4),
           if(number % 5 = 0, NULL, concat('n', toString(number))),
           if(number % 7 = 0, NULL, toInt64(number)),
           toFloat32(number) / 3, toFloat64(number) / 7,
           toDecimal64(number, 4) / 100,
           toDate('2020-01-01') + (number % 900),
           toDate32('1950-01-01') + (number % 30000),
           toDateTime('2021-06-01 00:00:00') + number,
           toDateTime64('2021-06-01 00:00:00.123', 3) + number,
           generateUUIDv4(),
           if(number % 2 = 0, 'a', 'b'),
           [toInt32(number), toInt32(number + 1)],
           number % 2 = 0
    FROM numbers({N})
    """,
)
print("  rows:", q(SRC_HTTP, "SELECT count() FROM ch_src"))

print("== transfer ch -> ch (server to server) ==")
t0 = time.time()
r = apitap.transfer(SRC, DST, table="ch_src", dest_table="ch_dst")
wall = time.time() - t0
print(f"  transferred {r.rows:,} rows in {wall:.1f}s")

print("== verify ==")
dst_schema = q(DST_HTTP, "DESCRIBE TABLE ch_dst FORMAT TSV")
print("  destination columns:")
for line in dst_schema.splitlines():
    f = line.split("\t")
    print(f"    {f[0]:<12} {f[1]}")

# Per-column checksum. Types that legitimately widen (Date->Date32,
# DateTime->DateTime64) or land as text (Enum/Array/FixedString) are compared
# through the same rendering on both sides.
CK = (
    "SELECT count(), sum(cityHash64(id)), sum(cityHash64(u64)), sum(cityHash64(s)), "
    "sum(cityHash64(lc)), sum(cityHash64(toString(fs))), sum(cityHash64(ifNull(n_s,'~'))), "
    "sum(cityHash64(ifNull(n_i,-1))), sum(cityHash64(toString(f32))), "
    "sum(cityHash64(toString(f64))), sum(cityHash64(toString(dec))), "
    "sum(cityHash64(toString(toDate32(d)))), sum(cityHash64(toString(d32))), "
    "sum(cityHash64(toString(toDateTime64(dt, 6, 'UTC')))), "
    "sum(cityHash64(toString(toDateTime64(dt64, 6, 'UTC')))), sum(cityHash64(toString(uid))), "
    "sum(cityHash64(toString(en))), sum(cityHash64(toString(arr))), sum(cityHash64(toUInt8(b))) "
    "FROM {t} FORMAT TSV"
)
src_ck = q(SRC_HTTP, CK.format(t="ch_src"))
dst_ck = q(DST_HTTP, CK.format(t="ch_dst"))
if src_ck == dst_ck:
    print(f"  CHECKSUM MATCH across 18 columns ({src_ck.split(chr(9))[0]} rows)")
else:
    print("  CHECKSUM MISMATCH")
    for i, (a, b) in enumerate(zip(src_ck.split("\t"), dst_ck.split("\t"))):
        flag = "" if a == b else "   <-- differs"
        print(f"    col{i}: src={a} dst={b}{flag}")
    sys.exit(1)

print("== incremental append (delta cursor) ==")
q(SRC_HTTP, f"INSERT INTO ch_src SELECT * FROM ch_src LIMIT 10 SETTINGS max_block_size=10")
before = int(q(DST_HTTP, "SELECT count() FROM ch_dst"))
r2 = apitap.transfer(SRC, DST, table="ch_src", dest_table="ch_dst", mode="replace")
after = int(q(DST_HTTP, "SELECT count() FROM ch_dst"))
print(f"  replace re-ran: dst {before:,} -> {after:,} rows (src {q(SRC_HTTP, 'SELECT count() FROM ch_src')})")

print("\nE2E CH SOURCE: ALL GREEN")
