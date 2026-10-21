"""to_polars() reads a table whose one text column is larger than 2 GiB.

The eager path (to_polars / to_arrow / to_pandas) asks for one unbounded
batch per worker, and Arrow utf8 addresses only i32::MAX bytes. A worker
holding more than 2 GiB of one text column — parallel=1, or a MySQL table
with no integer key — was refused "varlen column past i32 offsets in one
batch" at any memory size, Postgres at the push past 2 GiB, MySQL after the
whole scan (deep review 2, N28). The builder now seals when one varlen
column passes 1 GiB.

Proof (pg read, parallel=1): 9M rows of 256-byte text, ~2.3 GB in one
column. to_polars() returns every row and every byte.
"""
import subprocess
import time

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "to_polars_2gib_column"
N = 9_000_000
W = 256


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    pg(f"DROP TABLE IF EXISTS {T}; CREATE UNLOGGED TABLE {T} (id int, v text); "
       f"ALTER TABLE {T} ALTER COLUMN v SET STORAGE PLAIN; "
       f"INSERT INTO {T} SELECT g, repeat(md5(g::text), {W // 32}) FROM generate_series(1, {N}) g")
    try:
        t0 = time.monotonic()
        df = apitap.read(PG, table=T, parallel=1).to_polars()
        took = time.monotonic() - t0
        total = df["v"].str.len_bytes().sum()
        assert df.height == N, f"{df.height} rows, want {N}"
        assert total == N * W, f"{total} text bytes, want {N * W}"
        print(f"  parallel=1, one text column of {total / 2**30:.2f} GiB: {df.height} rows in {took:.1f} s")
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ to_polars() reads a text column larger than 2 GiB")


if __name__ == "__main__":
    main()
