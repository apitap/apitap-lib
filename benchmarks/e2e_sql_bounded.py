"""apitap.sql() memory bounded by construction (W1): two TPC-H queries on the 10g
seeds in the 0.5 core / 256 MiB cage, with the apitap this interpreter imports.

- Q13 (customer LEFT JOIN orders, a grace spill of 69M orders over key-range
  passes): the cgroup's memory.peak stays at or under 240 MiB and the answer
  equals DuckDB's (36 rows). 0.67.2 peaked 257-261 MiB: its spill writers each
  kept 16 MiB written and cached, priced nowhere.
- Q1 (lineitem alone, eight Postgres pipes): the cgroup's socket memory stays
  within what the engine reserved for its read sockets' receive buffers
  (`[mem] ... rcvbuf reserved=`, the most at any mark) plus 4 MiB, and the
  answer equals DuckDB's. 0.67.2 asked 4 MiB on every socket (8 MiB in the
  kernel): 63 MiB of `sock`, nothing reserved.
- Both: no `[mem]` mark is OVER_RESERVED. At each phase mark the engine sets
  what it holds at the cgroup's high mark since the mark before against what
  it reserved over that interval (the larger of the two ends) plus a slack of
  8 MiB and a twentieth, at most 16 MiB. What it holds: the cgroup's anon less
  what glibc keeps on its free lists there (`held = anon - min(glibc free,
  anon - live)`, live = mallinfo2 in use + mmapped: the rule the planner
  prices its pipes by), plus kernel and sock. glibc's free is not a reserved
  term (B8 decision, architect 2026-11-14): every mark over raw anon on both
  seeds was covered by glibc free at that mark, and none had another cause;
  free heap is bounded by the cage itself, which the peak condition (<= 240
  MiB) guards, both queries.

- Q13F: Q13 beside a 40 MiB polars frame (memory glibc never sees): the
  answer equal and no OOM. The subquery's pipes are printed, not asserted:
  the held rule (RSS less glibc's free list) takes more pipes beside
  non-glibc memory than 0.67.2 did (5-6 against 2 at 40 MiB and at 20 MiB,
  W1 review R4, observed), yet answered 5/5 at a 238-258 MiB peak where
  0.67.2 was OOM-killed 2/5. The fix is B9 (pipes priced from the ledger's
  reservations, next round).

- Q9D: TPC-H Q9 with the probe side's key range held back 90 s (the debug
  lever APITAP_SQL_PROBE_RANGE_DELAY_MS), as it arrives from a slow server
  (TPC-H SF50: ~39 s), in the 256 MiB cage and again in 128 MiB: answered
  (175 rows, their profit sum DuckDB's) or a named refusal, never an OOM. The
  build's direct array, turned into pairs at once when the range came, was
  priced nowhere: SF50 Q9 filled 256 MB, 512 MB and 1 GB cages in under a
  second on 0.67.2 and on the W1 wheel H (W1b).

RED on the 0.67.2 wheel (gate-venv): Q13 peak 257+ MiB, Q1 sock 63 MiB > 4.
RED with `--raw` (the engine checks raw anon, `APITAP_LEDGER_RAW=1`): Q13 has
marks over, the freed heap between its statements counted as held.
Needs the bench seeds (pg lineitem_10g/customer_10g, my orders_10g), docker
and the apitap-duck:d2 image (python 3.12) on the bench box.

Usage: e2e_sql_bounded.py [Q13,Q1,Q13F,Q9D] [--raw]
"""

import os
import re
import subprocess
import sys
import tempfile

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
IMAGE = "apitap-duck:d2"
PEAK_MAX_MIB = 240
# Q13F: memory glibc never sees (a polars frame) held beside Q13: answered,
# not OOM-killed. 0.67.2 took 2 pipes for the subquery beside it (printed).
FRAME_MIB = int(os.environ.get("E2E_FRAME_MIB", "40"))
FRAME_PIPES_0672 = 2
SOCK_SLACK_MIB = 4

Q13 = """SELECT c_count, count(*) AS custdist FROM (
  SELECT c_custkey, count(o_orderkey) FROM pg.customer_10g
  LEFT OUTER JOIN my.orders_10g ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  GROUP BY c_custkey) AS c_orders (c_custkey, c_count)
GROUP BY c_count ORDER BY custdist DESC, c_count DESC"""

Q9 = """SELECT nation, o_year, SUM(amount) AS sum_profit FROM
  (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
   FROM my.part_10g, my.supplier_10g, pg.lineitem_10g, pg.partsupp_10g, my.orders_10g, my.nation
   WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
     AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
     AND p_name LIKE '%green%') AS profit
  GROUP BY nation, o_year ORDER BY nation, o_year DESC"""
# DuckDB's answer on the same seeds (~/baseline.log): 175 rows, these ends, this sum.
Q9_ROWS, Q9_FIRST, Q9_LAST, Q9_SUM = 175, ("ALGERIA", "1998"), ("VIETNAM", "1992"), 61812474616.2669
Q9_DELAY_MS = 90000

Q1 = """SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, count(*) AS count_order
FROM pg.lineitem_10g WHERE l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"""

# DuckDB's answers on the same seeds (~/p82-sf10.log on the bench box).
Q13_TRUTH = [(0, 3302700), (7, 553905), (8, 535388), (6, 524506), (9, 492561), (10, 451517), (5, 434246), (11, 426334),
             (12, 411433), (13, 397957), (14, 376749), (15, 341912), (4, 301577), (16, 294247), (17, 238583), (18, 183994),
             (3, 168053), (19, 134031), (20, 92251), (2, 69605), (21, 61035), (22, 38062), (23, 22536), (1, 19497),
             (24, 12852), (25, 7035), (26, 3837), (27, 1918), (28, 910), (29, 420), (30, 208), (31, 77), (32, 43),
             (33, 15), (34, 4), (35, 1)]
Q1_TRUTH = [("A", "F", "379368393.00", "568882882085.52", "540435127340.9816", 14876600),
            ("N", "F", "9901232.00", "14848683405.00", "14105675435.4285", 387942),
            ("N", "O", "746740074.00", "1119807704145.54", "1063812691765.9647", 29286146),
            ("R", "F", "379590539.00", "569168073341.83", "540716970232.3730", 14880900)]

# Runs inside the cage: the query, a 50 ms sampler of the cgroup's socket
# memory, the answer rows and one RESULT line.
RUNNER = r'''
import os, sys, threading, time, apitap
PG, MY, q = sys.argv[1], sys.argv[2], sys.argv[3]
# Memory glibc never sees, held beside the query (a polars frame: its own
# allocator), when FRAME_MIB is set.
frame = None
if os.environ.get("FRAME_MIB"):
    import polars as pl
    frame = pl.DataFrame({"x": pl.int_range(0, int(os.environ["FRAME_MIB"]) << 17, eager=True)})
mx = [0]
def samp():
    while True:
        try:
            for l in open("/sys/fs/cgroup/memory.stat"):
                if l.startswith("sock "):
                    mx[0] = max(mx[0], int(l.split()[1]))
        except OSError:
            pass
        time.sleep(0.05)
threading.Thread(target=samp, daemon=True).start()
rows = apitap.sql(q, sources={"pg": PG, "my": MY}).to_polars().rows()
for r in rows:
    print("ROW", "|".join(str(x).strip() if isinstance(x, str) else str(x) for x in r))
peak = int(open("/sys/fs/cgroup/memory.peak").read())
print(f"RESULT peak={peak} sock={mx[0]}", flush=True)
'''


RAW = "--raw" in sys.argv


def run(name, query, frame_mib=0, mem="256m", env=()):
    pkg = os.path.dirname(apitap.__file__)
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(RUNNER)
        runner = f.name
    os.chmod(runner, 0o644)
    cname = f"e2e-sql-bounded-{name}-{os.getpid()}"
    cmd = ["docker", "run", "--rm", "--name", cname, "--network", "host", "--cpus=0.5", f"--memory={mem}", f"--memory-swap={mem}", *[x for kv in env for x in ("-e", kv)],
           "-e", "APITAP_DEBUG=1", *(["-e", "APITAP_LEDGER_RAW=1"] if RAW else []), *(["-e", f"FRAME_MIB={frame_mib}"] if frame_mib else []), "-v", f"{pkg}:/usr/local/lib/python3.12/site-packages/apitap:ro", "-v", f"{runner}:/run.py:ro",
           IMAGE, "python", "/run.py", PG, MY, query]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True)
    finally:
        os.unlink(runner)
        subprocess.run(["docker", "rm", "-f", cname], capture_output=True)
    out = p.stdout + p.stderr
    m = re.search(r"RESULT peak=(\d+) sock=(\d+)", out)
    if p.returncode != 0 or not m:
        return None, out
    rows = [l[4:].split("|") for l in p.stdout.splitlines() if l.startswith("ROW ")]
    reserved = [float(x) for x in re.findall(r"rcvbuf reserved=([\d.]+)", out)]
    over = len(re.findall(r"OVER_RESERVED", out))
    pipes = [int(x) for x in re.findall(r"subquery over \S+: (\d+) pipe", out)]
    return dict(peak=int(m.group(1)) >> 20, sock=int(m.group(2)) >> 20, reserved=max(reserved, default=0.0), over=over, rows=rows,
                pipes=pipes), out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    want = args[0].split(",") if args else ["Q13", "Q1", "Q13F", "Q9D"]
    print(f"apitap {getattr(apitap, '__version__', '?')} from {os.path.dirname(apitap.__file__)}")
    fails = 0
    if "Q13" in want:
        r, out = run("q13", Q13)
        if r is None:
            print(f"Q13: no answer (OOM-killed or failed):\n{out[-2000:]}")
            fails += 1
        else:
            got = [(int(a), int(b)) for a, b in r["rows"]]
            ok_rows = got == Q13_TRUTH
            ok_peak = r["peak"] <= PEAK_MAX_MIB
            print(f"Q13: {len(got)} rows {'equal' if ok_rows else 'DIFFER'}; cgroup peak {r['peak']} MiB (<= {PEAK_MAX_MIB}: {ok_peak}); "
                  f"marks over their reservation: {r['over']}")
            fails += (not ok_rows) + (not ok_peak) + (r["over"] > 0)
    if "Q1" in want:
        r, out = run("q1", Q1)
        if r is None:
            print(f"Q1: no answer (OOM-killed or failed):\n{out[-2000:]}")
            fails += 1
        else:
            got = [(a, b, c, d, e, int(n)) for a, b, c, d, e, n in r["rows"]]
            ok_rows = got == Q1_TRUTH
            ok_sock = r["sock"] <= r["reserved"] + SOCK_SLACK_MIB
            ok_peak = r["peak"] <= PEAK_MAX_MIB
            print(f"Q1: {len(got)} rows {'equal' if ok_rows else 'DIFFER'}; socket memory {r['sock']} MiB, "
                  f"receive buffers reserved {r['reserved']:.1f} MiB (sock <= reserved + {SOCK_SLACK_MIB}: {ok_sock}); "
                  f"cgroup peak {r['peak']} MiB (<= {PEAK_MAX_MIB}: {ok_peak}); marks over their reservation: {r['over']}")
            fails += (not ok_rows) + (not ok_sock) + (not ok_peak) + (r["over"] > 0)
    if "Q13F" in want:
        r, out = run("q13f", Q13, frame_mib=FRAME_MIB)
        if r is None:
            print(f"Q13 beside a {FRAME_MIB} MiB polars frame: no answer (OOM-killed or failed):\n{out[-2000:]}")
            fails += 1
        else:
            got = [(int(a), int(b)) for a, b in r["rows"]]
            ok_rows = got == Q13_TRUTH
            print(f"Q13 beside a {FRAME_MIB} MiB polars frame: {len(got)} rows {'equal' if ok_rows else 'DIFFER'}; "
                  f"subquery pipes {r['pipes']} (0.67.2 took {FRAME_PIPES_0672}; not asserted until B9); cgroup peak {r['peak']} MiB")
            fails += not ok_rows
    if "Q9D" in want:
        for mem in ("256m", "128m"):
            r, out = run(f"q9d-{mem}", Q9, mem=mem, env=[f"APITAP_SQL_PROBE_RANGE_DELAY_MS={Q9_DELAY_MS}"])
            refused = re.search(r"ERROR sql: (.*?) is not supported", out) or re.search(r"(sql: .*?) is not supported", out)
            if r is None and refused and "OOM" not in out:
                print(f"Q9D at {mem}: a named refusal: {refused.group(1)[:160]}")
                continue
            if r is None:
                print(f"Q9D at {mem}: no answer (OOM-killed or failed):\n{out[-1500:]}")
                fails += 1
                continue
            rows = r["rows"]
            ok = (len(rows) == Q9_ROWS and tuple(rows[0][:2]) == Q9_FIRST and tuple(rows[-1][:2]) == Q9_LAST
                  and abs(sum(float(x[2]) for x in rows) - Q9_SUM) < 0.01)
            print(f"Q9D at {mem} (probe range held {Q9_DELAY_MS // 1000} s): {len(rows)} rows {'equal' if ok else 'DIFFER'}; cgroup peak {r['peak']} MiB")
            fails += not ok
    print("PASSED" if fails == 0 else f"FAILED ({fails})")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
