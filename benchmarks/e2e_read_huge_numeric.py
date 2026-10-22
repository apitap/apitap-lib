"""read() of a NUMERIC wider than 76 digits returns its values, never tiny floats.

A NUMERIC with a declared precision past Decimal's 76 digits is delivered as
Float64, but read() still took its COPY BINARY (numeric send) bytes as a
float8: a zero or a NaN (8 bytes) came back as 1e-323 / ~1.6e-314 with no
error, so `lazy().filter(col == 0)` returned nothing, and any other value
failed the read (deep review 3, R32). Such a column now reads as text.

Proof: amount numeric(100,2) holding 0, NaN and a 50-digit value. to_polars()
returns each as the source's own text.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "read_huge_numeric"


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                        "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    big = "1" + "2" * 49 + ".25"
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, amount numeric(100,2)); "
       f"INSERT INTO {T} VALUES (1, 0), (2, 'NaN'), (3, {big})")
    try:
        want = pg(f"SELECT string_agg(amount::text, ',' ORDER BY id) FROM {T}").split(",")
        df = apitap.read(PG, table=T).to_polars().sort("id")
        got = [str(v) for v in df["amount"].to_list()]
        assert got == want, f"read {got!r}, the source holds {want!r}"
        print(f"  numeric(100,2) reads as its text: {got}")
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ read() of a NUMERIC wider than 76 digits returns its values, never tiny floats")


if __name__ == "__main__":
    main()
