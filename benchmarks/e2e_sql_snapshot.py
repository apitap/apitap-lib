"""apitap.sql() reads every pass of a multi-pass join in ONE Postgres snapshot.

A join past its build budget runs as key-range passes, each a statement of
its own; on a table stored in key order a pass scans only the pages its keys
lay in when the query started (`ctid` spans). Before 0.66 each statement had
its own snapshot: a row an UPDATE moved (a non-HOT update writes the new
tuple at the table's end) after the query started, but before its pass
read it, was in neither span — lost.

  1  a join forced into many passes (APITAP_SQL_BUILD_BYTES=64K, ~60 passes) while another
     session updates random rows in a loop (each update moves its tuple,
     fillfactor 100): every group's COUNT and SUM equal the table's
     (unchanging) contents
  2  the run says it pinned a snapshot and split into passes, and >= 1,000
     rows moved during it, so case 1 tested what it claims

Without the pinning (APITAP_SQL_SNAPSHOT=0) case 1 loses rows: run this file
with --control to see it RED.
"""
import os
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}
TL, TO = "sql_snap_li", "sql_snap_od"
N = 600_000
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                        "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B",
                        "bench", "-e", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main(control):
    # No index on l_k (a key no index leads is asked in page parts), rows
    # stored in key order, pages full: an update cannot stay on its page.
    pg(f"DROP TABLE IF EXISTS {TL}; CREATE TABLE {TL} (l_k bigint, l_v int) WITH (fillfactor = 100, "
       f"autovacuum_enabled = false); INSERT INTO {TL} SELECT g, 1 FROM generate_series(1, {N}) g; ANALYZE {TL}")
    my(f"DROP TABLE IF EXISTS {TO}; CREATE TABLE {TO} (o_k bigint PRIMARY KEY, o_g int); "
       f"SET SESSION cte_max_recursion_depth = {N + 1}; "
       f"INSERT INTO {TO} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < {N}) "
       f"SELECT n, n % 5 FROM s")
    upd = None
    try:
        want = [(g, N // 5, N // 5) for g in range(5)]
        pages0 = int(pg(f"SELECT pg_relation_size('{TL}') / 8192"))
        # The updater: random rows of the original pages, found by a ctid
        # computed first (a TID scan, ~4,000 a second; `random()` in the
        # WHERE is re-drawn per row, a full scan per update, ~2 a second), one
        # commit each, until killed. It starts once the query has asked the
        # page parts' key ranges: a row moved before that lands in the last
        # part and widens its range to every pass (no pass is narrowed then);
        # a row moved after it is the one a narrowed pass missed.
        updater = ["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src", "-c",
                   f"DO $$ DECLARE t tid; BEGIN LOOP t := format('(%s,%s)', floor(random() * {pages0})::int, "
                   f"1 + floor(random() * 180)::int)::tid; UPDATE {TL} SET l_v = l_v WHERE ctid = t; "
                   f"COMMIT; END LOOP; END $$"]
        q = (f"SELECT o.o_g, COUNT(*) AS n, SUM(l.l_v) AS s FROM pg.{TL} l JOIN my.{TO} o ON l.l_k = o.o_k "
             f"GROUP BY 1 ORDER BY 1")
        code = ("import apitap\n"
                f"r = apitap.sql({q!r}, sources={SRC!r}).to_polars()\n"
                "print('ROWS', [tuple(x) for x in r.iter_rows()])\n")
        env = dict(os.environ, APITAP_SQL_BUILD_BYTES="64K", APITAP_DEBUG="1")
        if control:
            env["APITAP_SQL_SNAPSHOT"] = "0"
        run = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env)
        err = []
        for line in run.stderr:
            err.append(line.rstrip("\n"))
            if upd is None and ("probe range known" in line or "past" in line):
                upd = subprocess.Popen(updater, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                       stdin=subprocess.DEVNULL)
        stdout = run.stdout.read()
        run.wait()
        r = subprocess.CompletedProcess(run.args, run.returncode, stdout, "\n".join(err))
        moved = pg(f"SELECT count(*) FROM {TL} WHERE ctid >= '({pages0},0)'::tid")
        out = next((l[5:] for l in r.stdout.splitlines() if l.startswith("ROWS ")), "")
        ranges = [l for l in r.stderr.splitlines() if "past" in l and "range" in l]
        pinned = [l for l in r.stderr.splitlines() if "snapshot(s) pinned" in l]
        case("1: a multi-pass join under concurrent updates equals the table, every group",
             r.returncode == 0 and out == repr(want),
             f"got {out[:160]} want {want} ({moved} tuples past the original pages) {r.stderr[-300:] if r.returncode else ''}")
        case("2: the run pinned a snapshot, split into passes, and rows moved under it",
             (control or len(pinned) == 1) and len(ranges) >= 1 and int(moved) >= 1000,
             f"{pinned[:1]} {ranges[:1]} moved={moved}")
    finally:
        if upd is not None:
            upd.kill()
            upd.wait()
            pg(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'DO $$%{TL}%' "
               "AND pid <> pg_backend_pid()")
        pg(f"DROP TABLE IF EXISTS {TL}")
        my(f"DROP TABLE IF EXISTS {TO}")


if __name__ == "__main__":
    control = "--control" in sys.argv
    try:
        main(control)
    except Exception as e:  # noqa: BLE001
        print(f"   XX {type(e).__name__}: {e}")
        ok = False
    print("SQL SNAPSHOT E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
