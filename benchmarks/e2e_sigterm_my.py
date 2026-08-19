"""The same SIGTERM contract, on the MySQL binlog plane.

The flag is shared, but the two drains are not: Postgres stops between
walsender messages with an LSN, MySQL stops between binlog events with a
(file, position). A stop that reported the wrong position would not fail
loudly — it would skip the events between where the run really got to and
where it said it got to, and the destination would simply be missing rows
that nobody ever asks about again. That is what leg 3 is for.

Legs mirror `e2e_sigterm.py` and lean on the same two lessons: the backlog is
written as many separate transactions (autocommit, one per INSERT) because the
window budget is only consulted at a commit boundary, and the moment to signal
is taken from the destination rather than from a sleep.

  leg 0  the control       — APITAP_GRACEFUL_STOP=0: the signal must still kill
  leg 1  the graceful stop — exit 0, progress made, work left over
  leg 2  the resume        — count AND key sum reach the source exactly

Rig: `apitap-bench-my` on :3307 (MySQL 8.0), ClickHouse on :8124.
"""
import os
import subprocess
import sys
import time

MY = os.environ.get("MY_URL", "mysql://root:bench@127.0.0.1:3307/bench")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
MY_C = "apitap-bench-my"
T = "sigterm_my"

SEED = 100
BACKLOG = 200_000
TXN = 5_000
WINDOW = "4000000"

ok = True
notes = []


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def my(sql, check=True):
    o = sh(["docker", "exec", "-i", MY_C, "mysql", "-uroot", "-pbench",
            "-N", "-D", "bench", "-e", sql])
    if check and o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def ch_count():
    v = ch(f"SELECT count() FROM {T}")
    return int(v) if v.isdigit() else -1


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def backlog(frm, n):
    """n rows as n/TXN separate transactions.

    MySQL's autocommit gives one transaction per statement, which is the point:
    the drain's byte budget can only end a window at a commit boundary, so a
    backlog written as one giant INSERT produces exactly one window and leaves
    no mid-drain for a signal to land in.
    """
    parts = []
    for i in range(n // TXN):
        lo = frm + i * TXN
        vals = ",".join(f"({lo + k},REPEAT('x',120))" for k in range(TXN))
        parts.append(f"INSERT INTO {T} (id,v) VALUES {vals};")
    # Over stdin, not `-e`: a few hundred multi-row INSERTs is megabytes of
    # SQL and argv has a hard kernel limit that this walked straight into.
    o = sh(["docker", "exec", "-i", MY_C, "mysql", "-uroot", "-pbench",
            "-N", "-D", "bench"], input="\n".join(parts))
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])


RUN = (
    "import apitap\n"
    f"r = apitap.transfer({MY!r}, {CH!r}, table={T!r}, mode='log_based')\n"
    "print('ROWS', r.rows, flush=True)\n"
)


def start(graceful=True):
    env = dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW)
    if not graceful:
        env["APITAP_GRACEFUL_STOP"] = "0"
    return subprocess.Popen([sys.executable, "-c", RUN], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for_first_window(proc, floor, limit=300.0):
    t0 = time.time()
    while time.time() - t0 < limit:
        if proc.poll() is not None:
            return None
        c = ch_count()
        if c > floor:
            return c
        time.sleep(0.2)
    return None


def reap(p, timeout):
    try:
        return p.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
        return 999


# ---------------------------------------------------------------------------
print("== setup: seed, bootstrap, then a backlog worth interrupting ==")
my(f"DROP TABLE IF EXISTS {T}")
my(f"CREATE TABLE {T} (id INT PRIMARY KEY, v TEXT)")
my(f"INSERT INTO {T} (id,v) SELECT n, CONCAT('seed',n) FROM "
   f"(SELECT @r := @r + 1 AS n FROM information_schema.columns, "
   f"(SELECT @r := 0) x LIMIT {SEED}) s")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")

boot = sh([sys.executable, "-c", RUN], env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW))
if boot.returncode:
    case("bootstrap", False, boot.stderr.strip()[-400:])
    print("\nSIGTERM MYSQL E2E: FAILED")
    raise SystemExit(1)
case("bootstrap landed the seed", ch_count() == SEED, str(ch_count()))

backlog(1_000, BACKLOG)
src_total = int(my(f"SELECT count(*) FROM {T}"))
case("a backlog is waiting in the binlog", src_total == SEED + BACKLOG, str(src_total))

# ---------------------------------------------------------------------------
print("== leg 0: the control — mechanism OFF, the signal must still kill ==")
p = start(graceful=False)
first = wait_for_first_window(p, SEED)
if first is None:
    case("the control run reached a window before we signalled", False,
         "run ended first — raise BACKLOG or lower APITAP_CDC_WINDOW_BYTES")
    p.kill(); p.wait()
else:
    p.terminate()
    rc = reap(p, 120)
    case("with APITAP_GRACEFUL_STOP=0 the process dies by signal", rc == -15, f"rc={rc}")
    notes.append(f"control was killed with {ch_count()} of {src_total} landed")

# ---------------------------------------------------------------------------
print("== leg 1: the graceful stop — exit 0, partial progress, work left ==")
floor = ch_count()
if floor >= src_total:
    case("there is still a backlog to interrupt", False, "the control run drained it all")
else:
    p = start()
    first = wait_for_first_window(p, floor)
    if first is None:
        case("the run reached a window before we signalled", False, "run ended first")
        p.kill(); p.wait()
    else:
        p.terminate()
        rc = reap(p, 300)
        out, err = p.stdout.read(), p.stderr.read()
        landed = ch_count()
        case("SIGTERM did not kill the run", rc == 0, f"rc={rc} {err.strip()[-200:]}")
        # Against `first` (the count at the moment we signalled), not `floor`:
        # the leg only signals once the count has passed floor, so comparing to
        # floor would be asserting its own precondition. See the same note in
        # e2e_sigterm.py.
        case("the window in flight was landed, not discarded", landed > first,
             f"{first} at the signal -> {landed} after it")
        case("and the run stopped SHORT of the backlog", landed < src_total,
             f"{landed} of {src_total}")

# ---------------------------------------------------------------------------
print("== leg 2: the resume is exact — the binlog position was told the truth ==")
floor = ch_count()
r = sh([sys.executable, "-c", RUN], env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW))
case("the resume run succeeds", r.returncode == 0, r.stderr.strip()[-200:])
final = ch_count()
case("the destination reaches the source's count", final == src_total,
     f"{final} vs {src_total} (resumed from {floor})")
src_sum = my(f"SELECT COALESCE(SUM(id),0) FROM {T}")
dst_sum = ch(f"SELECT sum(id) FROM {T}")
case("and its key sum matches exactly", src_sum == dst_sum, f"my={src_sum} ch={dst_sum}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
my(f"DROP TABLE IF EXISTS {T}")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")

for n in notes:
    print(f"   .. {n}")
print("\nSIGTERM MYSQL E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
