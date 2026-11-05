"""An explicit read() parallel past the memory limit is clamped, with a note, never left to OOM.

0.65.0 honoured `parallel=` as asked: in a 64 MiB cage a 4-column read with
parallel=6 or 8 was OOM-killed on the spot (measured on the rig, 16.2M rows),
while the planner's own pick for that cage, 2 pipes, ran. A limit that holds the
pipes asked for changes nothing; one that does not reads with the planner's fit
for it and says so on stderr.

Proof, with the limit given as APITAP_MEM_BUDGET=64M (the planner prices pipes
against it exactly as against a cgroup):
  parallel=8: the note names parallel=8, the 64 MiB limit and the pipes used,
              and every row arrives
  parallel=2: no note, every row arrives
"""
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "read_parallel_clamp"
ROWS = 200_000


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                        "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def read(parallel):
    code = ("import apitap\n"
            f"df = apitap.read({PG!r}, table={T!r}, parallel={parallel}).to_polars()\n"
            "print('ROWS', df.height)\n")
    env = {"APITAP_MEM_BUDGET": "64M", "PATH": "/usr/bin:/bin"}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    rows = next((int(l.split()[1]) for l in r.stdout.splitlines() if l.startswith("ROWS")), None)
    return r.returncode, rows, r.stderr


def main():
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, a bigint, b numeric(15,2), "
       f"d date); INSERT INTO {T} SELECT g, g * 7, g / 3.0, DATE '2000-01-01' + g % 9000 "
       f"FROM generate_series(1, {ROWS}) g")
    ok = True
    try:
        code, rows, err = read(8)
        note = next((l for l in err.splitlines() if "parallel=8" in l), "")
        good = code == 0 and rows == ROWS and "64 MiB" in note and "pipe" in note
        print(f"   {'OK' if good else 'XX'} parallel=8 under a 64 MiB limit: exit={code} rows={rows} "
              f"note={note.strip()[:160]!r}")
        ok &= good
        code, rows, err = read(2)
        good = code == 0 and rows == ROWS and "does not fit" not in err
        print(f"   {'OK' if good else 'XX'} parallel=2 under the same limit: exit={code} rows={rows}, no note")
        ok &= good
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")
    return ok


if __name__ == "__main__":
    ok = main()
    print("READ PARALLEL CLAMP E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
