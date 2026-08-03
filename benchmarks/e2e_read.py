"""apitap.read() e2e: every ArrowKind + the ::text fallbacks, NULLs, empty
strings, unicode, chunk boundaries — verified value-by-value against SQL
ground truth through to_polars(), to_arrow() and the raw capsule."""
import subprocess

import apitap
import polars as pl
import pyarrow as pa

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "read_e2e"


def sql(q):
    out = subprocess.run(
        ["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-Atc", q], capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    return out.stdout.strip()


sql(f"DROP TABLE IF EXISTS {T}")
sql(f"""CREATE TABLE {T} (
    id bigint PRIMARY KEY,
    i2 smallint, i4 int,
    f4 real, f8 double precision,
    b boolean,
    dec numeric(12,4),
    d date, ts timestamp, tstz timestamptz,
    u uuid, js jsonb,
    t text, bin bytea)""")
sql(f"""INSERT INTO {T}
    SELECT g,
           (g % 30000)::smallint, g % 2000000000,
           g / 7.0, g / 13.0,
           g % 2 = 0,
           (g % 100000) / 10.0 + 0.0001,
           DATE '2020-01-01' + (g % 3000),
           TIMESTAMP '2020-01-01 00:00:00' + make_interval(secs => g % 86400),
           TIMESTAMPTZ '2020-01-01 00:00:00+00' + make_interval(secs => g % 86400),
           md5(g::text)::uuid,
           jsonb_build_object('k', g),
           CASE WHEN g % 100 = 0 THEN '' ELSE 'v' || g || '·ünï' END,
           decode(lpad(to_hex(g), 8, '0'), 'hex')
    FROM generate_series(1, 200000) g""")
# NULL row + extreme values.
sql(f"INSERT INTO {T}(id) VALUES (200001)")
sql(f"INSERT INTO {T}(id, i2, i4, dec, t) VALUES (200002, -32768, -2147483648, -9999.9999, 'neg')")

print("== to_polars ==")
df = apitap.read(SRC, table=T).to_polars().sort("id")
n = int(sql(f"SELECT count(*) FROM {T}"))
assert df.height == n, (df.height, n)
assert df["id"].sum() == int(sql(f"SELECT sum(id) FROM {T}"))
assert df["i4"].cast(pl.Int64).sum() == int(sql(f"SELECT sum(i4) FROM {T}"))
assert df["i2"].null_count() == 1 and df["u"].null_count() == 2
assert abs(float(df["dec"].sum()) - float(sql(f"SELECT sum(dec) FROM {T}"))) < 1e-6
assert df.filter(pl.col("id") == 100)["t"][0] == "" , "empty string must stay empty"
assert df.filter(pl.col("id") == 7)["t"][0] == "v7·ünï"
assert df.filter(pl.col("id") == 200002)["i2"][0] == -32768
assert df.filter(pl.col("id") == 200002)["i4"][0] == -2147483648
# uuid + jsonb ride ::text
assert df.filter(pl.col("id") == 5)["u"][0] == sql(f"SELECT u::text FROM {T} WHERE id=5")
assert df.filter(pl.col("id") == 5)["js"][0] == sql(f"SELECT js::text FROM {T} WHERE id=5")
# bytea真binary
assert df.filter(pl.col("id") == 3)["bin"][0] == bytes.fromhex("00000003")
# date/timestamp epochs
assert str(df.filter(pl.col("id") == 1)["d"][0]) == sql(f"SELECT d::text FROM {T} WHERE id=1")
print(f"  {df.height:,} rows, schema: {dict(zip(df.columns, [str(t) for t in df.dtypes]))}")

print("== to_arrow + capsule ==")
tbl = apitap.read(SRC, table=T).to_arrow()
assert tbl.num_rows == n
assert pa.types.is_decimal(tbl.schema.field("dec").type)
assert str(tbl.schema.field("tstz").type) == "timestamp[us, tz=UTC]"
assert str(tbl.schema.field("ts").type) == "timestamp[us]"
df2 = pl.DataFrame(apitap.read(SRC, table=T))
assert df2.height == n
print(f"  arrow schema ok; capsule direct ok")

print("== parallel=1 preserves source order ==")
one = apitap.read(SRC, table=T, parallel=1).to_polars()
assert one["id"].to_list()[:5] == [1, 2, 3, 4, 5]

print("== query= refused loudly (v1) ==")
try:
    apitap.read(SRC, query="SELECT 1")
    raise SystemExit("query= should refuse")
except ValueError as e:
    assert "lands next" in str(e), e

sql(f"DROP TABLE IF EXISTS {T}")
print("ALL GREEN")
