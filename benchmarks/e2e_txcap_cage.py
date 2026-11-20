"""One big transaction in a 256 MiB cage: an exact answer or a named refusal, never exit 137 (round P, p12, L14b).

A log_based drain holds a transaction whole until its commit. Through p11 the
cap on it (APITAP_TX_BUF_BYTES) defaulted to 256 MiB counted in wire-size
units, the whole of a 256 MiB cage, and a changelog body (a view's, a
changelog=True lane's) was built at the commit on top of the buffer: QA's
case M, one ~166K-row UPDATE under an incremental view, was OOM-killed (exit
137) on wheel I, p10 and p11, and only larger ones met the named refusal.
p12 derives the cap from the cage (7/8 of the memory limit minus what the
process holds and what the drain plans beside the buffer) and charges
resident bytes at the commit's peak.

Every run is its own process in a 0.5 CPU / 256 MiB container (memory-swap
the same); each prints the cgroup's memory.peak.

  M  QA's case-M shape, self-contained: an incremental view
       SELECT o_orderpriority, COUNT(*), SUM(l_quantity), SUM(l_extendedprice * (1 - l_discount))
       FROM pg.tc_li l JOIN my.tc_ord o ON l.l_orderkey = o.o_orderkey WHERE l.l_shipmode <> 'MAIL'
       GROUP BY o_orderpriority
     over a lineitem-shaped Postgres table with no primary key (REPLICA
     IDENTITY FULL: every update is a key move) and a MySQL orders table. A
     ladder of single UPDATEs, 50K .. 400K rows, each drained once: exit 137
     anywhere is BAD; each step exact (equal to the server) or refused by name
     (APITAP_TX_BUF_BYTES and how the cap was derived); the ladder stops at
     the first refusal. The largest exact size is the release note's bound.
  C  pg -> ClickHouse log_based, a PK table, single UPDATEs of 100K .. 1M
     rows: the default lane spills past the cap (A2) and must land exact at
     every step; the changelog=True lane lands exact or refuses by name.
  Y  MySQL -> ClickHouse log_based (binlog), a PK table, single UPDATEs of
     100K .. 1M rows: the same rules as C (round P, p13: the release note's
     MySQL bound).
  E  APITAP_TX_BUF_BYTES above the cage (1G at 256m) is refused by name
     before anything is read: a CDC bootstrap run and a view's first run
     (RED p12: both ran, the cap was only checked when a drain began).

Each run also prints the cgroup's highest anon (sampled from the host every
50 ms): memory.peak counts the spill files' page cache, anon is the bound
the release note states.

LEG_TXBUF=<size> runs every container under that explicit cap (to measure
the real peak past the derived cap; not the gate's rule). LEG_DEBUG=1 runs the containers with APITAP_DEBUG=1 (else the variable is
not set at all: an empty value still enables it). STEPS_M / STEPS_C narrow
the ladders (comma-separated row counts).

Usage: PKG=dir-holding-apitap e2e_txcap_cage.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
LI = 1_600_000  # 400K orders x 4 lines
CN = 1_000_000
PRIO = "ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW']"
ok = True

RUNNER = r"""
import apitap, sys, json
a = json.loads(sys.argv[1])
try:
    if a['op'] == 'view':
        r = apitap.sql(a['q'], sources=a['sources'], dest=a['dest'], table=a['table'], mode='log_based')
    else:
        r = apitap.transfer(a['src'], a['dest'], table=a['table'], dest_table=a['dest_table'], mode='log_based', **a['kw'])
    print('ROWS', r.rows)
except Exception as e:
    print('ERR', ' '.join(str(e).split())[:600])
try:
    print('PEAK_MB', int(open('/sys/fs/cgroup/memory.peak').read()) >> 20)
except Exception:
    pass
"""


def sh(a, **kw):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **kw)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql])
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
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "-B", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def case(label, good, detail):
    global ok
    ok &= bool(good)
    print(f"  {'ok ' if good else 'BAD'} {label}: {detail}", flush=True)


def _pkg_dir():
    import importlib.util
    return os.environ.get("PKG") or os.path.dirname(os.path.dirname(importlib.util.find_spec("apitap").origin))


def anon_sampler():
    """Start sampling the w1-txcap container's cgroup anon; call the result
    to stop and get the highest anon in MiB ('?' if never seen)."""
    stop, top = threading.Event(), [None]

    def run():
        cid = None
        while not stop.is_set():
            if cid is None:
                cid = sh(["docker", "ps", "-q", "--no-trunc", "--filter", "name=^w1-txcap$"]).stdout.strip() or None
            if cid:
                try:
                    for l in open(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/memory.stat"):
                        if l.startswith("anon "):
                            top[0] = max(top[0] or 0, int(l.split()[1]))
                            break
                except OSError:
                    pass
            time.sleep(0.05)

    th = threading.Thread(target=run, daemon=True)
    th.start()

    def done():
        stop.set()
        th.join()
        return "?" if top[0] is None else str(top[0] >> 20)

    return done


def caged(args, state=None, env=None):
    """One run in the cage: (exit code, 'ROWS n' / 'ERR …', 'peak MB / anon MiB')."""
    pkg = _pkg_dir()
    e = []
    if os.environ.get("LEG_DEBUG") == "1":
        e += ["-e", "APITAP_DEBUG=1"]
    if os.environ.get("LEG_TXBUF"):
        # Measurement only (not the gate's rule): run under an explicit cap.
        e += ["-e", f"APITAP_TX_BUF_BYTES={os.environ['LEG_TXBUF']}"]
    for k, v in (env or {}).items():
        e += ["-e", f"{k}={v}"]
    vol = ["-v", f"{state}:/state", "-e", "APITAP_STATE_DIR=/state"] if state else []
    anon = anon_sampler()
    r = sh(["docker", "run", "--rm", "--name", "w1-txcap", "--network", "host", "--cpus=0.5", "--memory=256m", "--memory-swap=256m",
            "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp", "-e", "PYTHONPATH=/w/pkg:/qa/py312",
            "-v", f"{os.path.expanduser('~/qa')}:/qa:ro", "-v", f"{pkg}/apitap:/w/pkg/apitap:ro", *vol, *e,
            "python:3.12-slim", "python", "-c", RUNNER, json.dumps(args)])
    out = [l for l in r.stdout.splitlines() if l.startswith(("ROWS", "ERR"))]
    peak = next((l.split()[1] for l in r.stdout.splitlines() if l.startswith("PEAK_MB")), "?")
    return r.returncode, (out[0] if out else r.stderr.strip()[-200:]), f"{peak} / anon {anon()}"


def named(out):
    return out.startswith("ERR") and "APITAP_TX_BUF_BYTES" in out and "MiB" in out




def cleanup():
    for pub in set(src("SELECT pubname FROM pg_publication_tables WHERE tablename LIKE 'tc\\_%'").split()):
        if not pub.startswith("apitap_v_"):
            src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
            src(f'DROP PUBLICATION IF EXISTS "{pub}"')
    drop_view_slots(src, ["tc_li", "tc_c"])
    src("DROP TABLE IF EXISTS tc_li, tc_c")
    my("DROP TABLE IF EXISTS tc_ord, tc_y")
    dst("DROP TABLE IF EXISTS tc_view, tc_view_env")
    for t in ("tc_c_plain", "tc_c_cl", "tc_c_env", "tc_y_plain", "tc_y_cl"):
        for x in (t, t + "__apitap_cl"):
            ch(f"DROP TABLE IF EXISTS {x}")
        ch(f"DROP VIEW IF EXISTS {t}__current")
        # A killed run (exit 137, the RED this leg looks for) leaves its lock
        # and lease behind; the next run would answer "locked".
        for x in ch(f"SELECT name FROM system.tables WHERE database = currentDatabase() AND startsWith(name, '{t}_') "
                    "AND (endsWith(name, '__apitap_lock') OR endsWith(name, '__apitap_staging'))").split():
            ch(f"DROP TABLE IF EXISTS `{x}`")
        for meta, col in (("_apitap_state", "dest_table = '{t}'"), ("_apitap_lease", "dest_key LIKE '%.{t}'")):
            if ch(f"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{meta}'") != "0":
                ch(f"ALTER TABLE {meta} DELETE WHERE {col.format(t=t)} SETTINGS mutations_sync = 1")


def steps(var, dflt):
    v = os.environ.get(var)
    return [int(x) for x in v.split(",")] if v else dflt


def ladder_m():
    print("=== M: QA's case-M view shape, single UPDATEs at 256m", flush=True)
    src(f"CREATE TABLE tc_li (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber int, l_quantity numeric(15,2), "
        "l_extendedprice numeric(15,2), l_discount numeric(15,2), l_tax numeric(15,2), l_returnflag char(1), l_linestatus char(1), "
        "l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar(44)); "
        "ALTER TABLE tc_li REPLICA IDENTITY FULL; "
        f"INSERT INTO tc_li SELECT i / 4, i % 200000, i % 10000, i % 4 + 1, i % 50 + 1, (i % 99991) * 1.17, (i % 11) / 100.0, (i % 9) / 100.0, "
        "chr(65 + i % 3), chr(70 + i % 2), date '1995-01-01' + i % 2000, date '1995-02-01' + i % 2000, date '1995-03-01' + i % 2000, "
        "(ARRAY['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'])[i % 4 + 1], "
        "(ARRAY['MAIL','SHIP','AIR','TRUCK','RAIL','FOB','REG AIR'])[i % 7 + 1], left(md5(i::text) || md5((i + 1)::text), 10 + i % 34) "
        f"FROM generate_series(0, {LI - 1}) i")
    my("CREATE TABLE tc_ord (o_orderkey bigint PRIMARY KEY, o_orderpriority varchar(15)); SET SESSION cte_max_recursion_depth = 500000; "
       f"INSERT INTO tc_ord WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < {LI // 4 - 1}) "
       "SELECT i, ELT(i % 5 + 1, '1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW') FROM s")
    q = ("SELECT o_orderpriority, COUNT(*) AS n, SUM(l_quantity) AS q, SUM(l_extendedprice * (1 - l_discount)) AS rev "
         "FROM pg.tc_li l JOIN my.tc_ord o ON l.l_orderkey = o.o_orderkey WHERE l.l_shipmode <> 'MAIL' GROUP BY o_orderpriority")
    truth = ("SELECT p || '|' || count(*) || '|' || round(sum(l_quantity), 2) || '|' || round(sum(l_extendedprice * (1 - l_discount)), 2) FROM "
             f"(SELECT l.*, ({PRIO})[l_orderkey % 5 + 1] AS p FROM tc_li l) x WHERE l_shipmode <> 'MAIL' GROUP BY p ORDER BY p")
    st = tempfile.mkdtemp(prefix="tc-m-", dir=os.path.expanduser("~/w1"))
    view = {"op": "view", "q": q, "sources": {"pg": PG, "my": MY}, "dest": PGD, "table": "tc_view"}
    rc, out, peak = caged(view, st)
    print(f"  bootstrap: exit {rc} {out[:80]} peak {peak} MiB", flush=True)
    largest = 0
    for n in steps("STEPS_M", [50_000, 100_000, 166_000, 250_000, 400_000]):
        src(f"UPDATE tc_li SET l_quantity = l_quantity + 1 WHERE l_orderkey < {n // 4}")
        rc, out, peak = caged(view, st)
        got = dst("SELECT o_orderpriority || '|' || n || '|' || round(q::numeric, 2) || '|' || round(rev::numeric, 2) FROM tc_view ORDER BY 1")
        exact = rc == 0 and out.startswith("ROWS") and got == src(truth)
        case(f"M one UPDATE of {n} rows ({2 * n} images): exact or named, never 137", rc != 137 and (exact or named(out)),
             f"exit {rc} peak {peak} MiB, {'exact' if exact else out[:400]}")
        if not exact:
            break
        largest = n
    print(f"  M largest single UPDATE drained exact at 256m: {largest} rows", flush=True)
    shutil.rmtree(st, ignore_errors=True)


def ladder_c():
    print("=== C: pg -> ClickHouse log_based, single UPDATEs at 256m", flush=True)
    src("CREATE TABLE tc_c (id int PRIMARY KEY, a bigint, b numeric(15,2), c text, d date); "
        f"INSERT INTO tc_c SELECT i, i * 7, (i % 99991) * 1.17, md5(i::text) || md5((i * 3)::text), date '2000-01-01' + i % 9000 "
        f"FROM generate_series(1, {CN}) i")
    lanes = {"plain": ("tc_c_plain", {}), "changelog": ("tc_c_cl", {"changelog": True})}
    live = dict(lanes)
    for lane, (t, kw) in lanes.items():
        rc, out, peak = caged({"op": "transfer", "src": PG, "dest": CH, "table": "tc_c", "dest_table": t, "kw": kw})
        print(f"  bootstrap {lane}: exit {rc} {out[:80]} peak {peak} MiB", flush=True)
    largest = {k: 0 for k in lanes}
    for n in steps("STEPS_C", [100_000, 200_000, 400_000, 1_000_000]):
        src(f"UPDATE tc_c SET a = a + 1, c = c || 'u' WHERE id <= {n}")
        want = src("SELECT count(*) || '|' || sum(a) || '|' || sum(length(c)) FROM tc_c")
        for lane, (t, kw) in list(live.items()):
            rc, out, peak = caged({"op": "transfer", "src": PG, "dest": CH, "table": "tc_c", "dest_table": t, "kw": kw})
            view_t = t + "__current" if kw else t
            got = ch(f"SELECT toString(count()) || '|' || toString(sum(a)) || '|' || toString(sum(length(c))) FROM {view_t}")
            exact = rc == 0 and out.startswith("ROWS") and got == want
            rule = exact if lane == "plain" else (exact or named(out))
            case(f"C {lane}: one UPDATE of {n} rows, {'exact' if lane == 'plain' else 'exact or named'}, never 137", rc != 137 and rule,
                 f"exit {rc} peak {peak} MiB, {'exact' if exact else out[:300] + f' (src {want} / ch {got})'}")
            if exact:
                largest[lane] = n
            else:
                del live[lane]
    print(f"  C largest single UPDATE landed exact at 256m: {largest}", flush=True)


def ladder_y():
    print("=== Y: MySQL -> ClickHouse log_based, single UPDATEs at 256m", flush=True)
    my("CREATE TABLE tc_y (id int PRIMARY KEY, a bigint, b decimal(15,2), c varchar(80), d date); SET SESSION cte_max_recursion_depth = 2000000; "
       f"INSERT INTO tc_y WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < {CN}) "
       "SELECT i, i * 7, (i % 99991) * 1.17, CONCAT(MD5(i), MD5(i * 3)), DATE_ADD('2000-01-01', INTERVAL i % 9000 DAY) FROM s")
    lanes = {"plain": ("tc_y_plain", {}), "changelog": ("tc_y_cl", {"changelog": True})}
    live = dict(lanes)
    for lane, (t, kw) in lanes.items():
        rc, out, peak = caged({"op": "transfer", "src": MY, "dest": CH, "table": "tc_y", "dest_table": t, "kw": kw})
        print(f"  bootstrap {lane}: exit {rc} {out[:80]} peak {peak} MiB", flush=True)
    largest = {k: 0 for k in lanes}
    for n in steps("STEPS_Y", [100_000, 200_000, 400_000, 1_000_000]):
        my(f"UPDATE tc_y SET a = a + 1, c = CONCAT(c, 'u') WHERE id <= {n}")
        want = my("SELECT CONCAT(COUNT(*), '|', SUM(a), '|', SUM(CHAR_LENGTH(c))) FROM tc_y")
        for lane, (t, kw) in list(live.items()):
            rc, out, peak = caged({"op": "transfer", "src": MY, "dest": CH, "table": "tc_y", "dest_table": t, "kw": kw})
            view_t = t + "__current" if kw else t
            got = ch(f"SELECT toString(count()) || '|' || toString(sum(a)) || '|' || toString(sum(length(c))) FROM {view_t}")
            exact = rc == 0 and out.startswith("ROWS") and got == want
            rule = exact if lane == "plain" else (exact or named(out))
            case(f"Y {lane}: one UPDATE of {n} rows, {'exact' if lane == 'plain' else 'exact or named'}, never 137", rc != 137 and rule,
                 f"exit {rc} peak {peak} MiB, {'exact' if exact else out[:300] + f' (src {want} / ch {got})'}")
            if exact:
                largest[lane] = n
            else:
                del live[lane]
    print(f"  Y largest single UPDATE landed exact at 256m: {largest}", flush=True)


def env_above_cage():
    # Its own destination: a lane an earlier step left leased (a killed run)
    # would answer "locked" first (seen on p11).
    rc, out, peak = caged({"op": "transfer", "src": PG, "dest": CH, "table": "tc_c", "dest_table": "tc_c_env", "kw": {}},
                          env={"APITAP_TX_BUF_BYTES": "1G"})
    case("E APITAP_TX_BUF_BYTES=1G at 256m is refused by name (CDC bootstrap)", rc != 137 and out.startswith("ERR") and "above this process's memory limit" in out,
         f"exit {rc} {out[:200]}")
    # A view's first run (its bootstrap) too, in a state dir of its own. The
    # view needs REPLICA IDENTITY FULL, or every wheel refuses it for that
    # first and the case shows nothing about the cap (seen on p12).
    src("ALTER TABLE tc_c REPLICA IDENTITY FULL")
    st = tempfile.mkdtemp(prefix="tc-e-", dir=os.path.expanduser("~/w1"))
    view = {"op": "view", "q": "SELECT d, COUNT(*) AS n FROM pg.tc_c GROUP BY d", "sources": {"pg": PG}, "dest": PGD, "table": "tc_view_env"}
    rc, out, peak = caged(view, st, env={"APITAP_TX_BUF_BYTES": "1G"})
    case("E2 APITAP_TX_BUF_BYTES=1G at 256m is refused by name (a view's first run)",
         rc != 137 and out.startswith("ERR") and "above this process's memory limit" in out, f"exit {rc} {out[:200]}")
    shutil.rmtree(st, ignore_errors=True)


def main():
    cleanup()
    try:
        if "M" in os.environ.get("CASES", "MCYE"):
            ladder_m()
        cases = os.environ.get("CASES", "MCYE")
        if "C" in cases or "E" in cases:
            if "C" in cases:
                ladder_c()
            else:
                src("CREATE TABLE tc_c (id int PRIMARY KEY, a bigint, b numeric(15,2), c text, d date); "
                    "INSERT INTO tc_c SELECT i, i, 0, 'x', date '2000-01-01' FROM generate_series(1, 1000) i")
            if "E" in cases:
                env_above_cage()
        if "Y" in cases:
            ladder_y()
    finally:
        cleanup()
    print("TXCAP CAGE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
