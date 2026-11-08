"""A MySQL destination with local_infile=OFF fails fast and names the fix.

MySQL 8.0+ ships local_infile=OFF. apitap loads a MySQL destination with
LOAD DATA LOCAL INFILE; the server refused it, the refused load never read its
stream, and the producer blocked on the full channel forever: a transfer into a
stock MySQL 8.4 hung with no error (found 2026-11-08 seeding lab_my). Every
bench MySQL/MariaDB has local_infile=1, so no leg saw it.

Proof: a throwaway stock mysql:8.4 (no server flags, capped), a 1M-row
Postgres table (~70 MB, past the loader channel) moved into it. Before the fix the run hangs (the child is killed
at the deadline); after it the run fails within seconds naming local_infile and
SET PERSIST. Then SET PERSIST local_infile=1 and the same run lands every row.
"""
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
NAME, PORT = "apitap-qa-my84-stock", 18313
MY = f"mysql://root:bench@127.0.0.1:{PORT}/bench?ssl-mode=disabled"
T = "my_infile_off"
N = 1_000_000
DEADLINE = 120


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, stdin=subprocess.DEVNULL, **kw)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    assert o.returncode == 0, o.stderr
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "-h127.0.0.1", "-N", "-e", sql])
    assert o.returncode == 0, o.stderr
    return o.stdout.strip()


def transfer():
    """The run in a child process: a hang is killed at the deadline, not inherited."""
    code = ("import apitap, sys\n"
            "try:\n"
            f"    print('ROWS', apitap.transfer({PG!r}, {MY!r}, table={T!r}, mode='replace').rows)\n"
            "except Exception as e:\n"
            "    print('ERR', e)\n")
    t0 = time.time()
    try:
        o = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           stdin=subprocess.DEVNULL, timeout=DEADLINE)
    except subprocess.TimeoutExpired:
        return None, time.time() - t0
    return o.stdout.strip().splitlines()[-1] if o.stdout.strip() else o.stderr[-500:], time.time() - t0


def main():
    sh(["docker", "rm", "-f", NAME])
    r = sh(["docker", "run", "-d", "--rm", "--name", NAME, "--memory", "1g", "--cpus", "1",
            "-p", f"127.0.0.1:{PORT}:3306", "-e", "MYSQL_ROOT_PASSWORD=bench", "-e", "MYSQL_DATABASE=bench",
            "mysql:8.4"])
    assert r.returncode == 0, r.stderr
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} AS SELECT g AS id, md5(g::text) || md5((g + 1)::text) AS v "
       f"FROM generate_series(1, {N}) g; ALTER TABLE {T} ADD PRIMARY KEY (id)")
    try:
        # Over TCP: the entrypoint's init server answers on the socket, then restarts.
        for _ in range(120):
            if sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "-h127.0.0.1",
                   "-e", "SELECT 1"]).returncode == 0:
                break
            time.sleep(1)
        assert my("SELECT @@GLOBAL.local_infile") == "0", "a stock MySQL 8.4 must have local_infile=OFF"

        out, secs = transfer()
        assert out is not None, f"the run into local_infile=OFF hung past {DEADLINE}s"
        assert out.startswith("ERR"), f"the run must fail, got {out!r}"
        assert "local_infile" in out and "SET PERSIST" in out, f"the error must name the fix: {out}"
        assert secs < 60, f"it must fail fast, took {secs:.0f}s"
        print(f"  local_infile=OFF: refused in {secs:.1f}s, naming SET PERSIST local_infile")

        my("SET PERSIST local_infile = 1")
        out, secs = transfer()
        assert out == f"ROWS {N}", f"with local_infile=1 the run must land {N} rows, got {out!r}"
        got = my(f"SELECT COUNT(*) FROM bench.{T}")
        assert got == str(N), f"destination holds {got} rows"
        print(f"  local_infile=ON: {N} rows in {secs:.1f}s")
    finally:
        sh(["docker", "rm", "-f", NAME])
        pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ a MySQL destination with local_infile=OFF fails fast and names the fix")


if __name__ == "__main__":
    main()
