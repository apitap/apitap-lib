"""A SIGTERM must land the window in flight, not throw it away.

Every scheduler that stops a job sends SIGTERM and follows it with SIGKILL a
few seconds later — Kubernetes on eviction, Airflow on a cleared run, systemd
on `stop`. Before this, apitap died on the first one. That was never a
correctness problem: a CDC watermark is written after the rows it covers and a
replay is idempotent, so a killed run loses nothing permanently. It was a cost
problem — everything the in-flight window had drained went back to the WAL and
was read again on the next run, which on a busy table is minutes of work per
redeploy.

The legs are built around one question: what would this file print if the
handler were reverted? Leg 0 answers it directly by running the SAME scenario
with the mechanism switched off, so the file carries its own control rather
than asking to be trusted.

  leg 0  the control            — APITAP_GRACEFUL_STOP=0, same signal, same
                                  moment: the process must die BY SIGNAL. If
                                  this leg ever passes with exit 0, every leg
                                  below it is meaningless.
  leg 1  the graceful stop      — same signal, mechanism on: exit 0, and the
                                  destination sits STRICTLY between where it
                                  started and where the source is. Both bounds
                                  matter. The lower one says a window really
                                  landed; the upper one says the run stopped
                                  early instead of simply finishing.
  leg 2  twice to insist        — a second SIGTERM restores the default and
                                  re-raises, so an operator who wants the
                                  process gone now gets it gone now.
  leg 3  the resume is exact    — re-running with no signal must reach the
                                  source's own count AND key sum. A graceful
                                  stop that left the watermark ahead of the
                                  rows it applied would show up here as
                                  missing rows, which is the failure that
                                  would actually matter.
  leg 4  request_stop()         — the public door, called from another thread,
                                  for hosts whose own signal handling apitap
                                  deliberately refuses to touch (SA_SIGINFO,
                                  SIG_IGN).

Two things this file learned the hard way, both of which made an earlier
version pass while testing nothing:

* **The backlog must be many transactions.** The window budget is checked at
  COMMIT boundaries only — one transaction always buffers whole, by design, so
  a backlog written as a single `INSERT ... generate_series` produces exactly
  one window no matter how small the budget is. There is then no "mid-drain"
  for a signal to land in, and the run finishes before the leg can interrupt
  it. The seeder below writes it as thousands of separate statements.
* **The trigger comes from the destination, not from a sleep.** The leg waits
  until the destination row count has actually moved before it signals, so the
  signal is known to arrive with a window applied and more work outstanding. A
  fixed sleep would make the whole file pass or fail on how busy the box is.

Rig: `apitap-bench-pg-src` on :5544, ClickHouse on :8124.
"""
import os
import subprocess
import sys
import time

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
PG_C = os.environ.get("PG_CONTAINER", "apitap-bench-pg-src")
T = "sigterm_demo"

SEED = 100
BACKLOG = 400_000
TXN = 5_000            # rows per transaction — small enough to give many windows
# Small windows, so a drain over the backlog takes many of them and a signal
# has somewhere to land.
WINDOW = "4000000"

ok = True
notes = []


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
            "-d", PG.rsplit("/", 1)[-1], "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg_script(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
            "-d", PG.rsplit("/", 1)[-1], "-q", "-v", "ON_ERROR_STOP=1"], input=sql)
    if o.returncode:
        raise RuntimeError(o.stderr[-500:])


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
def _slots_now():
    return set(pg("SELECT slot_name FROM pg_replication_slots").split())


# Slots that existed BEFORE this leg started. Anything else is ours.
#
# The blanket `SELECT pg_drop_replication_slot(slot_name) ... WHERE NOT active`
# that used to be here is a live grenade on a shared rig: a CDC job that is
# merely BETWEEN drains has an inactive slot, and dropping it destroys its WAL
# continuity. It took out a running 24 h soak on 2026-08-20. Scope the cleanup
# to what this leg made.
_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for s in sorted(_slots_now() - _SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{s}')"
           f" FROM pg_replication_slots WHERE slot_name='{s}' AND NOT active")



def backlog(frm, n):
    """n rows as n/TXN separate transactions.

    psql outside a BEGIN block commits each statement on its own, which is the
    whole point: the drain's byte budget only gets a chance to stop the window
    at a commit boundary.
    """
    stmts = "".join(
        f"INSERT INTO {T} SELECT g, repeat('x',120) FROM "
        f"generate_series({frm + i * TXN},{frm + (i + 1) * TXN - 1}) g;\n"
        for i in range(n // TXN)
    )
    pg_script(stmts)


RUN = (
    "import apitap\n"
    f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')\n"
    "print('ROWS', r.rows, flush=True)\n"
)


def start(graceful=True, window=WINDOW):
    env = dict(os.environ, APITAP_CDC_WINDOW_BYTES=window)
    if not graceful:
        env["APITAP_GRACEFUL_STOP"] = "0"
    return subprocess.Popen([sys.executable, "-c", RUN], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for_first_window(proc, floor, limit=300.0):
    """Block until the run has APPLIED a window, so the signal lands mid-drain.

    Returns the count seen, or None if the process ended first — in which case
    the leg that called it has nothing to test and must say so rather than
    reporting a pass.
    """
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
pg(f"DROP TABLE IF EXISTS {T}")
pg(f"CREATE TABLE {T} (id int primary key, v text)")
pg(f"INSERT INTO {T} SELECT g, 'seed'||g FROM generate_series(1,{SEED}) g")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")
drop_our_slots()

boot = sh([sys.executable, "-c", RUN], env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW))
if boot.returncode:
    case("bootstrap", False, boot.stderr.strip()[-400:])
    print("\nSIGTERM E2E: FAILED")
    raise SystemExit(1)
case("bootstrap landed the seed", ch_count() == SEED, str(ch_count()))

backlog(1_000, BACKLOG)
src_total = int(pg(f"SELECT count(*) FROM {T}"))
case("a backlog is waiting in the WAL", src_total == SEED + BACKLOG, str(src_total))

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
    # Python reports a signal death as a negative return code.
    case("with APITAP_GRACEFUL_STOP=0 the process dies by signal", rc == -15, f"rc={rc}")
    notes.append(f"control was killed with {ch_count()} of {src_total} landed")

# The control run advanced the watermark by whatever it confirmed before dying.
# That is fine: the next leg measures its own floor.
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
        case("the run reported its rows", "ROWS" in out, out.strip()[-80:])
        # `first` is the count at the moment the signal went out. Comparing
        # against `floor` instead would be true by construction — the leg only
        # signals once the count has already moved past floor, so that version
        # asserted its own precondition and would print OK even if the graceful
        # stop threw the in-flight window away. Against `first` it is the real
        # claim: rows that were NOT yet applied when we signalled got applied
        # because we let the run finish.
        case("the window in flight was landed, not discarded", landed > first,
             f"{first} at the signal -> {landed} after it")
        case("and the run stopped SHORT of the backlog", landed < src_total,
             f"{landed} of {src_total}")

# ---------------------------------------------------------------------------
print("== leg 2: twice to insist - the second SIGTERM is not absorbed ==")
# The two signals go out back to back, ~0.1 s apart, and the leg checks the
# process is STILL RUNNING in between. That check is what makes the leg valid:
# without it, a second signal delivered to an already-finished process would
# report the exit code of a clean run and read as a broken contract rather
# than a lost race.
#
# Back-to-back is not a shortcut, it is the only shape that fits. The graceful
# path is fast by construction - the drain breaks at the very next commit
# boundary, so a run usually has well under a second of work left when the
# first signal lands. Waiting "a couple of seconds to be sure it is still
# busy" would be waiting for something this design deliberately does not do.
#
# A second SIGTERM arriving while the first handler is still executing is held
# pending (SIGTERM is masked for the duration of its own handler) and delivered
# the moment that handler returns, so the ordering the contract needs holds
# either way.
twice_ok = False
for attempt in range(3):
    backlog(2_000_000 + attempt * 1_000_000, BACKLOG)
    src_total = int(pg(f"SELECT count(*) FROM {T}"))
    floor = ch_count()
    p = start()
    first = wait_for_first_window(p, floor)
    if first is None:
        p.kill(); p.wait()
        continue
    p.terminate()
    time.sleep(0.1)
    if p.poll() is not None:
        # Lost the race: the run finished between the two signals. Nothing is
        # proven either way, so try again rather than record a verdict.
        notes.append(f"leg 2 attempt {attempt + 1} lost the race "
                     f"(the run exited within 0.1s), retried")
        continue
    p.terminate()
    rc = reap(p, 120)
    out = p.stdout.read()
    case("the second SIGTERM ends the process", rc == -15,
         f"rc={rc} (attempt {attempt + 1})")
    # rc == -15 on its own does not say WHICH SIG_DFL killed it: the handler's
    # own restore-and-re-raise, or the one Guard::drop puts back when the run
    # finishes. From outside, the two deaths are identical. The transfer not
    # having returned is the discriminator available here — the ROWS line is
    # printed after `transfer()` returns, and the guard is dropped inside it.
    # (The residual is the sub-millisecond gap between the guard's drop and
    # that print. The decisive proof of the re-raise branch is the forked unit
    # test `shutdown::tests::the_second_signal_is_not_absorbed`, which can tell
    # an absorbed signal from an honoured one because its child reaches
    # `_exit(3)` in the first case; no e2e leg can.)
    case("and it died with the transfer still running", "ROWS" not in out,
         out.strip()[-60:] or "(no output — transfer had not returned)")
    twice_ok = True
    break
if not twice_ok:
    case("the run stayed alive long enough to take a second signal", False,
         "three attempts all finished inside 0.1s of the first signal")

# ---------------------------------------------------------------------------
print("== leg 3: the resume is exact — no rows skipped by the early stops ==")
floor = ch_count()
r = sh([sys.executable, "-c", RUN], env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW))
case("the resume run succeeds", r.returncode == 0, r.stderr.strip()[-200:])
final = ch_count()
case("the destination reaches the source's count", final == src_total,
     f"{final} vs {src_total} (resumed from {floor})")
src_sum = pg(f"SELECT coalesce(sum(id::bigint),0)::text FROM {T}")
dst_sum = ch(f"SELECT sum(id) FROM {T}")
case("and its key sum matches exactly", src_sum == dst_sum, f"pg={src_sum} ch={dst_sum}")

# ---------------------------------------------------------------------------
print("== leg 4: request_stop() from another thread ==")
# The public door, for hosts whose signal handling apitap will not touch. It
# runs off-thread because that is the shape it exists for: the main thread
# stays in the interpreter and decides to wind the job down.
# 9M, clear of leg 2's retry ranges (2M, 3M, 4M). They overlapped: a single
# leg 2 retry made this INSERT hit a duplicate primary key, and the uncaught
# error ended the script before the cleanup block.
backlog(9_000_000, BACKLOG)
src_total = int(pg(f"SELECT count(*) FROM {T}"))
floor = ch_count()
code = (
    "import threading, time, subprocess, apitap\n"
    "res = {}\n"
    "def go():\n"
    "    try:\n"
    f"        res['r'] = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')\n"
    "    except BaseException as e:\n"
    "        res['err'] = repr(e)\n"
    "t = threading.Thread(target=go); t.start()\n"
    "def cnt():\n"
    "    o = subprocess.run(['docker','exec','-i','apitap-bench-ch','clickhouse-client',"
    "'--user','default','--password','bench','-q',"
    f"'SELECT count() FROM {T}'], capture_output=True, text=True).stdout.strip()\n"
    "    return int(o) if o.isdigit() else -1\n"
    f"floor = {floor}\n"
    "t0 = time.time()\n"
    "while time.time() - t0 < 300 and t.is_alive() and cnt() <= floor:\n"
    "    time.sleep(0.2)\n"
    "stopped_a_live_run = t.is_alive()\n"
    "apitap.request_stop()\n"
    "t.join(300)\n"
    "print('LIVE', stopped_a_live_run, 'ALIVE', t.is_alive(),"
    " 'ROWS', res['r'].rows if 'r' in res else 'RAISED', res.get('err',''))\n"
)
r = sh([sys.executable, "-c", code], env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW))
case("request_stop() was called on a RUNNING transfer", "LIVE True" in r.stdout,
     r.stdout.strip()[-120:])
# "returned normally" has to mean it RETURNED A REPORT. The earlier version
# checked only the exit code and "ALIVE False", both of which a thread that
# died inside apitap.transfer() also satisfies — the child printed "ROWS None"
# and the leg called it a pass.
case("and the transfer returned a report, not an exception",
     r.returncode == 0 and "ALIVE False" in r.stdout and "RAISED" not in r.stdout,
     (r.stdout.strip() + " " + r.stderr.strip()[-160:]).strip()[-220:])
landed = ch_count()
case("a window landed", landed > floor, f"{floor} -> {landed}")
case("and the run stopped short of the new backlog", landed < src_total,
     f"{landed} of {src_total}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
sh([sys.executable, "-c", RUN], env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=WINDOW))
case("the final resume is exact too", ch_count() == src_total,
     f"{ch_count()} vs {src_total}")
pg(f"DROP TABLE IF EXISTS {T}")
ch(f"DROP TABLE IF EXISTS {T}")
# The CDC apply also leaves a delete-marker sidecar next to the
# destination. Dropping only the main table left one behind on the
# shared bench rig after every run.
ch(f"DROP TABLE IF EXISTS `{T}__apitap_cdc_del`")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")
drop_our_slots()

for n in notes:
    print(f"   .. {n}")
print("\nSIGTERM E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
