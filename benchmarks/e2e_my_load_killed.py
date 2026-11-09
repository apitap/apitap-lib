"""A MySQL load whose connection is killed mid-LOAD DATA fails, it does not hang.

The loader's task holds its connection until finish/abort releases it. When
the LOAD DATA died under it (a lost connection, a KILL, a dropped staging
table), the next `send` found the channel closed and joined the task, which
was still waiting to be released: the transfer hung forever with no error
(0.67 review, B1; the local_infile pre-check made the e2e leg miss it).

Proof: a 3M-row Postgres table (~200 MB) moved into the bench MySQL, its
LOAD DATA connection KILLed from a second session once it is running, then
again with its staging table dropped: each run fails within seconds with an
error, none is still blocked at the deadline. Not seen RED: at b8dee28 both
modes also ended in an error (the hang needs the load to end between two
sends), so this leg guards the release-before-join rather than proving it.
"""
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "my_load_killed"
N = 3_000_000
DEADLINE = 120


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL, **kw)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    assert o.returncode == 0, o.stderr
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "-e", "MYSQL_PWD=bench", "apitap-bench-my", "mysql", "-uroot", "-N", "-e", sql])
    assert o.returncode == 0, o.stderr
    return o.stdout.strip()


def run(mode):
    """`kill`: KILL the LOAD DATA connection once it runs. `drop`: DROP the
    staging table the moment it exists, so the server refuses the LOAD
    before it asks for the file (the task ends on its own, the producer is
    still sending)."""
    my(f"DROP TABLE IF EXISTS bench.{T}")
    code = ("import apitap\n"
            "try:\n"
            f"    print('ROWS', apitap.transfer({PG!r}, {MY!r}, table={T!r}, mode='replace').rows)\n"
            "except Exception as e:\n"
            "    print('ERR', str(e).splitlines()[0][:300])\n")
    child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             stdin=subprocess.DEVNULL, text=True)
    try:
        killed = None
        t0 = time.time()
        while time.time() - t0 < 60 and child.poll() is None:
            if mode == "kill":
                ids = my("SELECT id FROM information_schema.processlist WHERE info LIKE 'LOAD DATA%'")
                if ids:
                    time.sleep(1.0)  # well into the load
                    killed = ids.split()[0]
                    my(f"KILL {killed}")
                    break
                time.sleep(0.2)
            else:
                st = my(f"SELECT table_name FROM information_schema.tables WHERE table_schema='bench' AND table_name LIKE '{T}%staging%'")
                if st:
                    killed = st.split()[0]
                    my(f"DROP TABLE IF EXISTS bench.`{killed}`")
                    break
        assert killed, f"{mode}: the load was never caught (the run ended first)"
        t1 = time.time()
        try:
            out, _ = child.communicate(timeout=DEADLINE)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate()
            raise AssertionError(f"{mode}: the run hung {DEADLINE}s after {killed}")
        last = out.strip().splitlines()[-1] if out.strip() else ""
        secs = time.time() - t1
        # A drop that came before the load began may let a retry land the
        # rows; either an error or every row is an answer, a hang is not.
        assert last.startswith("ERR") or last == f"ROWS {N}", f"{mode}: got {last!r}"
        print(f"  {mode} {killed}: ended in {secs:.1f}s: {last[:160]}")
    finally:
        if child.poll() is None:
            child.kill()
        for t in my(f"SELECT table_name FROM information_schema.tables WHERE table_schema='bench' AND table_name LIKE '{T}%'").split():
            my(f"DROP TABLE IF EXISTS bench.`{t}`")


def main():
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} AS SELECT g AS id, md5(g::text) || md5((g + 1)::text) AS v "
       f"FROM generate_series(1, {N}) g; ALTER TABLE {T} ADD PRIMARY KEY (id)")
    try:
        for mode in ("kill", "drop"):
            run(mode)
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ a MySQL load whose LOAD DATA dies (killed, or its staging table dropped) ends, no hang")


if __name__ == "__main__":
    main()
