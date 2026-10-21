"""Reader.to_parquet sizes its row groups from the engine's memory limit.

to_parquet read only the root cgroup-v2 memory.max. On cgroup v1, under
--cgroupns=host or a systemd MemoryMax slice, and with APITAP_MEM_BUDGET,
that file is absent or says "max", so a 256 MB process took 128 MB row
groups: up to 128 MiB of Arrow batches held in Python on top of the
engine's full budget, an OOM kill in a capped container (deep review 2,
N29). The engine itself sizes from the real limit.

Proof (pg read on the uncapped host with APITAP_MEM_BUDGET=256M, ~110 MB of
rows): a capped process keeps the per-batch path, so the file has one row
group per engine batch, not the single group 128 MB buffering makes of it.
Every row is in the file.
"""
import os
import subprocess
import sys
import tempfile

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "to_parquet_budget"
N = 1_000_000


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v text); "
       f"INSERT INTO {T} SELECT g, repeat(md5(g::text), 3) FROM generate_series(1, {N}) g")
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, "t.parquet")
        code = ("import apitap, pyarrow.parquet as pq\n"
                f"rows = apitap.read({PG!r}, table={T!r}).to_parquet({out!r})\n"
                f"m = pq.ParquetFile({out!r}).metadata\n"
                "print(rows, m.num_rows, m.num_row_groups)\n")
        try:
            o = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                               env={**os.environ, "APITAP_MEM_BUDGET": "256M"}, timeout=300)
            assert o.returncode == 0, o.stderr[-600:]
            rows, in_file, groups = map(int, o.stdout.split()[-3:])
        finally:
            pg(f"DROP TABLE IF EXISTS {T}")
    assert rows == in_file == N, f"returned {rows}, file holds {in_file}, want {N}"
    assert groups > 1, f"{groups} row group(s): a 256 MB budget took the 128 MB buffering path"
    print(f"  APITAP_MEM_BUDGET=256M: {in_file} rows in {groups} row groups (per-batch path)")
    print("✓ to_parquet sizes its row groups from the engine's memory limit")


if __name__ == "__main__":
    main()
