"""An incremental view's drain stays inside a 256 MiB cage (round P p10, L14).

Through p9 a view drain copied each window's change images into a second
vector (a heap value per cell) next to the window, and MySQL kept up to four
windows alive; QA's drain of ~166K updated rows (~332K images) was OOM-killed
at 256m on wheel I and p7. p10 applies a window's images 8K at a time and
prices the windows alive with the view's state.

Every run of the view is its own process in a 0.5 CPU / 256 MiB container;
each prints the container's peak memory (cgroup memory.peak).

  A  Postgres: 50 transactions x 5,000 updates = 500K change images in one
     drain: exit 0, the answer equal to the server's, peak printed (PASS rule)
  B  MySQL side of a Postgres x MySQL join: 50 x 5,000 updates on the MySQL
     table, the same (PASS rule)
  C  the largest single UPDATE a drain survives (L14c bound, release note):
     a ladder of one-transaction updates on Postgres, 25K, 50K, 100K, 166K
     rows, each drained; stops at the first OOM (informational)

Usage: PKG=dir-holding-apitap e2e_sql_live_drain.py
"""
import os
import subprocess
import sys
import tempfile

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
N = 250_000
ok = True

RUNNER = r"""
import apitap, sys, json
try:
    r = apitap.sql(sys.argv[1], sources=json.loads(sys.argv[2]), dest=sys.argv[3], table=sys.argv[4], mode='log_based')
    print('ROWS', r.rows)
except Exception as e:
    print('ERR', str(e)[:300])
try:
    print('PEAK_MB', int(open('/sys/fs/cgroup/memory.peak').read()) >> 20)
except Exception:
    pass
"""


def psql(container, db, sql):
    o = subprocess.run(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-AtF", "|",
                        "-c", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)

def view_slots(run, tables):
    """The apitap_v_ slots (= their publications' names) of views over `tables`:
    a view's publication is FOR TABLE its Postgres table. Only these are this
    leg's to drop; another run's views (QA's, a user's) are never touched.
    Call before the tables are dropped (that empties the publication)."""
    tl = ", ".join(f"'{t}'" for t in tables)
    return set(run(f"SELECT DISTINCT pubname FROM pg_publication_tables WHERE pubname LIKE 'apitap\\_v\\_%' "
                   f"AND tablename IN ({tl})").split())


def drop_view_slots(run, tables):
    for n in view_slots(run, tables):
        run(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{n}' AND NOT active")
        run(f'DROP PUBLICATION IF EXISTS "{n}"')



def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def view(pkg, state, query, sources, table):
    """One run in the cage; (exit code, 'ROWS n' / 'ERR …', peak MB)."""
    import json
    r = subprocess.run(["docker", "run", "--rm", "--name", "w1-live-drain", "--network", "host", "--cpus=0.5", "--memory=256m",
                        "--memory-swap=256m", "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp", "-e", "APITAP_STATE_DIR=/state",
                        "-e", "PYTHONPATH=/w/pkg:/qa/py312", "-v", f"{os.path.expanduser('~/qa')}:/qa:ro", "-v", f"{state}:/state",
                        "-v", f"{pkg}/apitap:/w/pkg/apitap:ro", "python:3.12-slim", "python", "-c", RUNNER, query,
                        json.dumps(sources), PGD, table], capture_output=True, text=True)
    out = [l for l in r.stdout.splitlines() if l.startswith(("ROWS", "ERR"))]
    peak = next((l.split()[1] for l in r.stdout.splitlines() if l.startswith("PEAK_MB")), "?")
    return r.returncode, (out[0] if out else r.stderr.strip()[-200:]), peak


def case(label, good, detail):
    global ok
    ok &= bool(good)
    print(f"  {'ok ' if good else 'BAD'} {label}: {detail}", flush=True)


def cleanup():
    drop_view_slots(src, ["ld_t", "ld_d"])
    src("DROP TABLE IF EXISTS ld_t, ld_d")
    my("DROP TABLE IF EXISTS ld_m")
    dst("DROP TABLE IF EXISTS ld_out_a, ld_out_b, ld_out_c")



def _pkg_dir():
    """The dir holding the apitap package: PKG, else the interpreter's own
    (the release gate runs every leg with the wheel it gates)."""
    import importlib.util
    return os.environ.get("PKG") or os.path.dirname(os.path.dirname(importlib.util.find_spec("apitap").origin))

def main():
    pkg = _pkg_dir()
    cleanup()
    pad = "repeat('p', 60)"
    src(f"CREATE TABLE ld_t (id int PRIMARY KEY, g int, amt numeric(12,2), pad text); ALTER TABLE ld_t REPLICA IDENTITY FULL; "
        f"INSERT INTO ld_t SELECT i, i % 1000, (i % 997) * 1.25, {pad} FROM generate_series(1, {N}) i; "
        "CREATE TABLE ld_d (id int PRIMARY KEY, grp int); INSERT INTO ld_d SELECT i, i % 50 FROM generate_series(0, 999) i; "
        "ALTER TABLE ld_d REPLICA IDENTITY FULL")
    my("CREATE TABLE ld_m (id int PRIMARY KEY, k int, amt decimal(12,2), pad varchar(80)); "
       "SET SESSION cte_max_recursion_depth = 300000; "
       f"INSERT INTO ld_m WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {N}) "
       "SELECT i, i % 1000, (i % 997) * 1.25, REPEAT('p', 60) FROM s")

    # A: Postgres, 500K images in 50 transactions.
    qa = "SELECT g, COUNT(*) AS n, SUM(amt) AS s FROM pg.ld_t GROUP BY g"
    st = tempfile.mkdtemp(prefix="ld-a-", dir=os.path.expanduser("~/w1"))
    rc, out, peak = view(pkg, st, qa, {"pg": PG}, "ld_out_a")
    print(f"  bootstrap A: exit {rc} {out} peak {peak} MB", flush=True)
    for b in range(50):
        lo = b * 5000 + 1
        src(f"UPDATE ld_t SET amt = amt + 1, g = (g + 7) % 1000 WHERE id BETWEEN {lo} AND {lo + 4999}")
    rc, out, peak = view(pkg, st, qa, {"pg": PG}, "ld_out_a")
    same = dst("SELECT g, n, s FROM ld_out_a ORDER BY g") == src("SELECT g, COUNT(*), SUM(amt) FROM ld_t GROUP BY g ORDER BY g")
    case("A Postgres: 500K images in 50 transactions, one drain at 256m", rc == 0 and out.startswith("ROWS") and same,
         f"exit {rc} {out}, equal to the server: {same}, peak {peak} MB")

    # B: the MySQL side of a join, 500K images in 50 transactions.
    qb = "SELECT d.grp, COUNT(*) AS n, SUM(m.amt) AS s FROM pg.ld_d d JOIN my.ld_m m ON m.k = d.id GROUP BY d.grp"
    stb = tempfile.mkdtemp(prefix="ld-b-", dir=os.path.expanduser("~/w1"))
    rc, out, peak = view(pkg, stb, qb, {"pg": PG, "my": MY}, "ld_out_b")
    print(f"  bootstrap B: exit {rc} {out} peak {peak} MB", flush=True)
    for b in range(50):
        lo = b * 5000 + 1
        my(f"UPDATE ld_m SET amt = amt + 1, k = (k + 7) % 1000 WHERE id BETWEEN {lo} AND {lo + 4999}")
    rc, out, peak = view(pkg, stb, qb, {"pg": PG, "my": MY}, "ld_out_b")
    rows = {}
    for l in my("SELECT k, COUNT(*), SUM(amt) FROM ld_m GROUP BY k").splitlines():
        k, n, s = l.split("\t")
        grp = int(k) % 50
        c = rows.setdefault(grp, [0, 0])
        c[0] += int(n)
        c[1] += float(s)
    want = [f"{g}|{n}|{s:.2f}" for g, (n, s) in sorted(rows.items())]
    got = [f"{l.split('|')[0]}|{l.split('|')[1]}|{float(l.split('|')[2]):.2f}" for l in dst("SELECT grp, n, s FROM ld_out_b ORDER BY grp").splitlines()]
    case("B MySQL side of a join: 500K images in 50 transactions, one drain at 256m", rc == 0 and out.startswith("ROWS") and got == want,
         f"exit {rc} {out}, equal to the server: {got == want}, peak {peak} MB")

    # C: the largest single UPDATE a drain survives (informational).
    stc = tempfile.mkdtemp(prefix="ld-c-", dir=os.path.expanduser("~/w1"))
    rc, out, peak = view(pkg, stc, qa, {"pg": PG}, "ld_out_c")
    survived = 0
    for n in (25_000, 50_000, 100_000, 166_000):
        src(f"UPDATE ld_t SET amt = amt + 1 WHERE id <= {n}")
        rc, out, peak = view(pkg, stc, qa, {"pg": PG}, "ld_out_c")
        print(f"  C one transaction of {n} updates ({2 * n} images): exit {rc} {out[:80]} peak {peak} MB", flush=True)
        if rc != 0 or not out.startswith("ROWS"):
            break
        survived = n
    print(f"  C largest single UPDATE drained at 256m: {survived} rows", flush=True)
    cleanup()
    import shutil
    for d in (st, stb, stc):
        shutil.rmtree(d, ignore_errors=True)
    print("SQL LIVE DRAIN E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
