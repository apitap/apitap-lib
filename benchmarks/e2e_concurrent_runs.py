"""Two runs of one destination table must not publish each other's work.

Before this, every run of a table used the same staging object, and `prepare`
began by dropping it. So run A could stream N rows in, run B's prepare could
DROP that object and CREATE a fresh empty one, and A's finalize would then
rename B's empty table over the destination — returning a successful report
with A's row count. A green run and an empty table.

The fix puts the run's identity IN the staging name, so a run can only publish
an object it minted. `prepare` no longer drops anything blindly: it lists what
is there, collects only what it can PROVE is dead, and refuses to start beside
anything else.

"Provably dead" is a narrow set on purpose, and leg 3 is where that shows. The
token records when the RUN started, not when the object was created, so on a
long multi-table load `now - token` is only an UPPER bound on an object's age
and can never prove it is old — while the run that owns it is still writing.
An age horizon was tried and removed for exactly that reason. The one name
nothing living can own is the un-tokenized one an older apitap wrote, and that
is the only thing collected.

  leg 1  the refusal        — a second replace, while the first is mid-flight,
                              must fail with Error::Locked and NOT touch the
                              destination
  leg 2  the survivor       — the first run then finishes normally and the
                              destination holds ITS rows, whole
  leg 1c the control        — the same race, with the guard bypassed by
                              pointing both runs at DIFFERENT source URLs for
                              the same table, which the matrix permits: both
                              must succeed. If this leg fails, leg 1 is
                              refusing everything rather than refusing
                              collisions.
  leg 3  nothing is collected — BOTH leftovers are refused: the un-tokenized
                              one (an apitap <0.55.0 may be loading into it)
                              and a TOKENIZED one however ancient its token
                              looks (the token is the RUN's start time, not the
                              object's). Dropping them by hand — the recovery
                              each error prescribes — makes the run work again
  leg 4  NOT WRITTEN        — fan-in (two appends from two different sources
                              into one table) is the one matrix row that says
                              "allowed", and no leg here proves it end to end.
                              It is covered as a unit test instead
                              (naming::tests::the_matrix_refuses_what_collides_
                              and_permits_fan_in), which exercises peer_blocks
                              directly. A live-server version has to seed the
                              second source's _apitap_state row first, or it
                              trips a separate, older guard that refuses a new
                              source on a destination that already carries
                              state — see leg 1c's note. Worth writing; do not
                              read this list as if it were written.
  leg 5  the refusal's TYPE  — it arrives as apitap.LockedError, catchable as a
                              class rather than by matching the message, and
                              still a RuntimeError subclass
  leg 6  the window left     — two runs starting in the SAME INSTANT both pass
                              a check-then-act guard. Neither the outcome nor
                              the debris is asserted (both vary); the one
                              INVARIANT is that the destination is whole and
                              the failure, if any, is loud

Leg 1c and leg 4 are what stop leg 1 from being a fix that simply refuses
everything.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545.
"""
import ast
import os
import subprocess
import sys
import threading
import time

import _rig

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
T = "conc_runs"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def dst(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-dst", "psql", "-U", "postgres",
            "-d", "apitap_bench_dst", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def run(mode="replace", url=None, cursor=None, env_extra=None):
    kw = f", cursor={cursor!r}" if cursor else ""
    code = ("import apitap\n"
            f"r = apitap.transfer({url or SRC!r}, {DST!r}, table={T!r}, "
            f"mode={mode!r}{kw})\n"
            "print('ROWS', r.rows, flush=True)\n")
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    return sh([sys.executable, "-c", code], env=env)


def staging_names():
    # Every apitap artifact, not only `%staging`: since 0.56.0 a run also writes
    # a `__apitap_lock` before it scans, and a leftover of EITHER kind refuses
    # the next run. A sweep that saw only one of them left the other behind and
    # the following leg failed for the wrong reason.
    return [n for n in dst(
        "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        f"WHERE n.nspname='public' AND c.relkind='r' AND relname LIKE '{T}%\\_\\_apitap\\_%' "
        "ESCAPE '\\'"
    ).split() if n]


def reset(rows=400_000):
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    for n in staging_names():
        dst(f'DROP TABLE IF EXISTS "{n}"')
    dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")
    src(f"CREATE TABLE {T} (id bigint PRIMARY KEY, v text)")
    src(f"INSERT INTO {T} SELECT g, repeat('x',200) FROM generate_series(1,{rows}) g")


# ---------------------------------------------------------------------------
print("== leg 1+2: a second replace must refuse, and the first must survive ==")
reset()
results = {}


def worker(tag, gate=None):
    if gate is not None:
        gate.wait(120)
    results[tag] = run("replace")


# The overlap is taken from the SERVER, not from a sleep. A first draft gave B
# a 0.8s head start against a 400k-row load — which finishes in 0.2s, so the
# two never overlapped and the leg reported that nothing was refused. Waiting
# until A's staging table EXISTS in the destination catalog is proof A is
# mid-flight; a timer is a guess about how fast the box is today.
a_started = threading.Event()


def watch_for_a_staging():
    for _ in range(1200):
        if staging_names():
            a_started.set()
            return
        time.sleep(0.05)
    a_started.set()   # give up waiting; the assertions below will say so


threads = [threading.Thread(target=worker, args=("A",)),
           threading.Thread(target=watch_for_a_staging),
           threading.Thread(target=worker, args=("B", a_started))]
for t in threads:
    t.start()
for t in threads:
    t.join(600)
case("(rig) A's staging really existed before B started", a_started.is_set())

a, b = results.get("A"), results.get("B")
case("both runs finished", a is not None and b is not None)
if a is not None and b is not None:
    winners = [t for t, r in (("A", a), ("B", b)) if r.returncode == 0]
    losers = [t for t, r in (("A", a), ("B", b)) if r.returncode != 0]
    case("exactly one run succeeded", len(winners) == 1,
         f"succeeded={winners} failed={losers}")
    if losers:
        err = results[losers[0]].stderr
        case("the loser refused with a lock error", "locked:" in err.lower(),
             (err.strip().splitlines() or [""])[-1][:170])
        # The TYPE, not the text. A scheduler is supposed to branch on the
        # class; if this only ever asserted the message we would not notice
        # the day the binding flattens it back into a bare RuntimeError.
        case("and it is apitap.LockedError, not a bare RuntimeError",
             "apitap.LockedError" in err or "LockedError:" in err,
             (err.strip().splitlines() or [""])[-1][:170])
        case("and the refusal names how to avoid it",
             "one at a time" in err or "max_active_runs" in err,
             (err.strip().splitlines() or [""])[-1][:170])
    if winners:
        n = dst(f"SELECT count(*) FROM {T}")
        want = src(f"SELECT count(*) FROM {T}")
        case("the surviving run's rows are all there", n == want,
             f"dest {n} vs source {want}")

case("no staging object was left behind", staging_names() == [],
     f"left: {staging_names()}")

# ---------------------------------------------------------------------------
print("== leg 1c CONTROL: concurrency itself must still be allowed ==")
# The guard is scoped to ONE destination table. Two replaces of DIFFERENT
# tables, started together, must both succeed — otherwise leg 1 is not a fix,
# it is a ban on running two transfers at once.
#
# (A first draft tried two appends from two source URLs into ONE table as the
# control, which collides with a separate, pre-existing check: apitap refuses a
# destination that carries state rows from other sources but none for this one.
# That guard is unrelated and predates this work, so the control was testing
# the wrong thing.)
T2 = T + "_two"
src(f"DROP TABLE IF EXISTS {T2}")
dst(f"DROP TABLE IF EXISTS {T2}")
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T2}', 'public.{T2}')")
src(f"CREATE TABLE {T2} (id bigint PRIMARY KEY, v text)")
src(f"INSERT INTO {T2} SELECT g, repeat('z',200) FROM generate_series(1,200000) g")
reset(rows=200_000)

results = {}


def two_tables(tag, table):
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {DST!r}, table={table!r}, mode='replace')\n"
            "print('ROWS', r.rows, flush=True)\n")
    results[tag] = sh([sys.executable, "-c", code])


threads = [threading.Thread(target=two_tables, args=("P", T)),
           threading.Thread(target=two_tables, args=("Q", T2))]
for t in threads:
    t.start()
for t in threads:
    t.join(600)

p_, q_ = results.get("P"), results.get("Q")
both = p_ is not None and q_ is not None and p_.returncode == 0 and q_.returncode == 0
case("CONTROL: two tables loading at once both succeed", both,
     "" if both else
     f"P={(p_.stderr.strip().splitlines() or [''])[-1][:130] if p_ else 'none'} "
     f"Q={(q_.stderr.strip().splitlines() or [''])[-1][:130] if q_ else 'none'}")
if both:
    case("CONTROL: and each landed its own rows",
         dst(f"SELECT count(*) FROM {T}") == "200000"
         and dst(f"SELECT count(*) FROM {T2}") == "200000",
         f"{T}={dst(f'SELECT count(*) FROM {T}')} {T2}={dst(f'SELECT count(*) FROM {T2}')}")
src(f"DROP TABLE IF EXISTS {T2}")
dst(f"DROP TABLE IF EXISTS {T2}")
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T2}', 'public.{T2}')")

# ---------------------------------------------------------------------------
print("== leg 3: NOTHING is collected — every leftover is refused ==")
reset(rows=1000)
r = run("replace")
case("a clean run", r.returncode == 0, r.stderr.strip()[-200:])

# (a) The un-tokenized leftover, the layout an apitap older than 0.55.0 writes.
#     This leg asserted until 0.55.1 that it was COLLECTED — "no current run
#     mints this name, so nothing living can own it". That reasoning holds right
#     up until an upgrade, which is the one time both versions exist: a ≤0.54.0
#     run is USING that name while it loads. Deleting it mid-load is loud on
#     Postgres and SILENT on BigQuery and the object stores, which is the defect
#     0.55.0 exists to remove. So it is refused like everything else.
dst(f'CREATE TABLE "{T}__apitap_staging" (id bigint)')
case("(rig) the un-tokenized leftover is in place", len(staging_names()) == 1,
     f"{staging_names()}")
r = run("replace")
case("an un-tokenized leftover is REFUSED, not collected",
     r.returncode != 0 and "locked" in (r.stderr or "").lower(),
     (r.stderr.strip().splitlines() or [""])[-1][:200])
case("and the refusal says what to check before removing it",
     "older than 0.55.0" in (r.stderr or ""), (r.stderr or "")[-220:])
case("and it is still there", staging_names() == [f"{T}__apitap_staging"],
     f"{staging_names()}")
dst(f'DROP TABLE IF EXISTS "{T}__apitap_staging"')

# (b) A TOKENIZED leftover whose token says 1970. It looks maximally dead, and
#     it must STILL not be collected: the token is the run's start time, not the
#     object's creation time, so age cannot tell a crashed run from table 40 of
#     a slow one. Exactly 16 bytes or it does not parse as a token at all and
#     the test proves nothing: _ + 7 start + 1 mode + 3 source + 4 nonce. A
#     first draft wrote 15 and then reported the guard as broken.
ancient = f"{T}_0000000r000abcd__apitap_staging"
dst(f'CREATE TABLE "{ancient}" (id bigint)')
r = run("replace")
case("an ancient TOKENIZED leftover is refused, not collected",
     r.returncode != 0 and "locked" in (r.stderr or "").lower(),
     (r.stderr.strip().splitlines() or [""])[-1][:200])
case("and it is still there — refusing is the safe action",
     staging_names() == [ancient], f"{staging_names()}")

# (c) The recovery the error message actually prescribes: drop it, re-run.
#     If this fails, the refusal is a dead end rather than a speed bump.
case("the refusal names the object to drop", ancient in (r.stderr or ""),
     (r.stderr or "")[-200:])
dst(f'DROP TABLE IF EXISTS "{ancient}"')
r = run("replace")
case("after dropping it by hand the run works again", r.returncode == 0,
     (r.stderr.strip().splitlines() or [""])[-1][:170])

# ---------------------------------------------------------------------------
print("== leg 5: the refusal is a catchable TYPE, not a message to regex ==")
# docs/stability.md commits to two things about this class: it exists, and it
# subclasses RuntimeError so code written before it still catches it.
#
# The overlap is GATED the same way leg 1 gates it — B starts only once A has a
# staging table. That is not the test being soft on itself: an ungated start is
# a different scenario with a different answer, and leg 6 is where it lives.
reset(rows=400_000)
probe = f"""
import apitap, threading, time, subprocess
assert issubclass(apitap.LockedError, RuntimeError), "must stay catchable as RuntimeError"
SRC, DST, T = {SRC!r}, {DST!r}, {T!r}
def staged():
    o = subprocess.run(["docker","exec","-i","apitap-bench-pg-dst","psql","-U","postgres",
        "-d","apitap_bench_dst","-Atc",
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relkind='r' AND relname LIKE '" + T + "%staging'"],
        capture_output=True, text=True)
    return o.stdout.strip() not in ("0", "")
hit, gate = [], threading.Event()
def a():
    try: apitap.transfer(SRC, DST, table=T, mode="replace"); hit.append(("A","ok"))
    except Exception as e: hit.append(("A", type(e).__name__))
def watch():
    for _ in range(3000):
        if staged(): break
        time.sleep(0.02)
    gate.set()
def b():
    gate.wait(180)
    try: apitap.transfer(SRC, DST, table=T, mode="replace"); hit.append(("B","ok"))
    except apitap.LockedError: hit.append(("B","LockedError"))
    except Exception as e: hit.append(("B", type(e).__name__))
ts = [threading.Thread(target=f) for f in (a, watch, b)]
[t.start() for t in ts]; [t.join(900) for t in ts]
print("CAUGHT", sorted(hit))
"""
r = sh([sys.executable, "-c", probe])
case("apitap.LockedError exists and subclasses RuntimeError",
     "AssertionError" not in r.stderr,
     (r.stderr.strip().splitlines() or [""])[-1][:170])
case("the loser was caught BY TYPE, not by message",
     "('B', 'LockedError')" in r.stdout,
     (r.stdout.strip() or r.stderr.strip()[-170:]))

# ---------------------------------------------------------------------------
print("== leg 6: two runs starting in the SAME INSTANT ==")
# This used to be the window the guard could not see. `prepare` listed the
# catalog and THEN created its staging table, so two runs starting inside that
# gap both saw an empty catalog and both proceeded — check-then-act, and this
# was the act it could not see. The outcome was genuinely two-valued and this
# leg asserted only the invariants around it.
#
# 0.56.0 inverts the order: a run writes a tokenized `__apitap_lock` FIRST and
# scans second, so it only ever proceeds on a scan taken after its own
# announcement. What that buys is the assertion below — NEVER TWO WINNERS — and
# what it costs is the other branch: when each run sees the other, BOTH yield,
# and nothing is written at all. That is the documented trade, and it is why the
# destination is checked as "whole or untouched" rather than "whole".
reset(rows=400_000)
want = src(f"SELECT count(*) FROM {T}")
burst = f"""
import apitap, threading
out = []
def go(tag):
    try:
        r = apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode="replace")
        out.append((tag, "ok", r.rows))
    except Exception as e:
        out.append((tag, type(e).__name__))
ts = [threading.Thread(target=go, args=(t,)) for t in ("A", "B")]
[t.start() for t in ts]; [t.join(900) for t in ts]
print("BURST", sorted(out))
"""
r = sh([sys.executable, "-c", burst])
line = (r.stdout or "").strip()
print(f"      outcome: {line[:160]}")
try:
    pairs = ast.literal_eval(line.split("BURST ", 1)[1].splitlines()[0])
except Exception as e:                                  # noqa: BLE001
    pairs = []
    print(f"      (could not parse the burst outcome: {e}; stderr: {r.stderr[-200:]})")
winners = [p for p in pairs if len(p) == 3]
refusals = [p[1] for p in pairs if len(p) == 2]

# THE property announce-then-check buys, and the one that was NOT true before:
# a run proceeds only on a scan taken after its own announcement, so a
# concurrent pair cannot both miss each other.
case("INVARIANT: never two winners", len(pairs) == 2 and len(winners) <= 1,
     f"{len(winners)} of 2 runs proceeded: {pairs}")
# A run that yields must do it by the guard, with the typed error — not by
# colliding at RENAME with a catalog message about an object nobody created.
case("a run that yields does so with LockedError",
     bool(pairs) and all(e == "LockedError" for e in refusals),
     f"refusals: {refusals or 'none'}")
# Whole if one landed; untouched if neither did (`reset` dropped it). Never
# short — a short table is the original defect returning.
# The table may legitimately not exist: when both runs yield, nothing is
# written at all, and asking for a count would raise instead of reporting it.
got = (dst(f"SELECT count(*) FROM {T}")
       if dst(f"SELECT to_regclass('public.{T}') IS NOT NULL") == "t" else "absent")
case("INVARIANT: the destination is whole, or untouched — never short",
     got == want if winners else got in ("absent", "0", ""),
     f"dest {got} vs source {want}, {len(winners)} winner(s)")
# NOT an invariant, and an earlier draft of this leg wrongly asserted it was:
# when the loser dies at RENAME it has already built its staging, and no error
# path runs for it, so the object is orphaned — and being orphaned it refuses
# the NEXT run of this table until someone drops it. That is the same-instant
# window's real operational cost and it belongs in the record, not in a
# green-or-red assertion, because whether it happens is not deterministic.
orphans = staging_names()
print(f"      (orphaned by the race, not asserted: {orphans or 'none this time'})")
for n in orphans:
    dst(f'DROP TABLE IF EXISTS "{n}"')

# ---------------------------------------------------------------------------
print("== leg 7: the guard still works where no drain has ever run ==")
# A refusal now asks the lease store whether the peer is still alive. On a
# destination that has never run a `log_based` drain that store does not exist —
# which is the DEFAULT state of every bulk-only deployment, not an edge case —
# and "not there" has to read as "nothing is leased", never as an error.
#
# It was an error once: the missing-table check matched the SQLSTATE text, which
# sqlx does not put in its message, so every peer refusal on such a destination
# came back as a bare RuntimeError instead of a typed LockedError. The gate hid
# it, because an earlier leg had already created the table.
#
# The overlap is taken from the SERVER. An earlier version started two threads
# and accepted "nothing yielded this time" — `all([])` is True, so a burst in
# which neither run ever met the other passed without testing anything. Now A
# runs alone until pg_class lists its staging, and only then does B start.
reset(rows=2_000_000)
dst('DROP TABLE IF EXISTS "_apitap_lease"')
case("(rig) the lease store really is absent",
     dst("SELECT to_regclass('public._apitap_lease') IS NULL") == "t",
     "dropped")
a = subprocess.Popen([sys.executable, "-c",
                      "import apitap\n"
                      f"r = apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='replace')\n"
                      "print('ROWS', r.rows, flush=True)\n"],
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
if not _rig.wait_for(lambda: a.poll() is not None or any(
        n.endswith("__apitap_staging") for n in staging_names()), 120, step=0.05) \
        or a.poll() is not None:
    a.kill(); a.wait()
    _rig.rig_fail("A's staging never appeared in pg_class while A was running")
b = sh([sys.executable, "-c",
        "import apitap\n"
        "try:\n"
        f"    apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='replace')\n"
        "    print('B ok')\n"
        "except Exception as e:\n"
        "    print('B', type(e).__name__, str(e).splitlines()[0][:160])\n"])
a_alive_after_b = a.poll() is None
a_out, a_err = a.communicate(timeout=900)
b_line = (b.stdout or "").strip().splitlines()[-1:] or [b.stderr.strip()[-160:]]
case("(rig) A was still running when B was refused", a_alive_after_b,
     "B's refusal must have met A, not an empty table")
case("B is refused by TYPE with no lease store: LockedError",
     b_line[0].startswith("B LockedError"), b_line[0])
case("and no run died of the store's absence", "RuntimeError" not in b_line[0], b_line[0])
case("A finished and its rows are all there",
     a.returncode == 0 and dst(f"SELECT count(*) FROM {T}") == src(f"SELECT count(*) FROM {T}"),
     f"rc={a.returncode} {(a_err or '').strip()[-160:]}")
for n in staging_names():
    dst(f'DROP TABLE IF EXISTS "{n}"')

# 7b is the half of the leg that actually READS the absent store. A bulk run
# takes its lock back at the end of `prepare`, so B above met A's staging —
# which is never collectable and so never asks about a lease. What does ask is
# a drain's lock: a CDC lock with no lease row behind it (an apitap older than
# the lease, or an operator's plant). With no store at all, that read must mean
# "nothing is leased" and end in a typed refusal — the path the SQLSTATE bug
# turned into a bare RuntimeError.
PLANT = f"{T}_0000001l000abcd__apitap_lock"
dst(f'CREATE TABLE "{PLANT}" ()')
b = sh([sys.executable, "-c",
        "import apitap\n"
        "try:\n"
        f"    apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='replace')\n"
        "    print('B ok')\n"
        "except Exception as e:\n"
        "    print('B', type(e).__name__, str(e).splitlines()[0][:220])\n"])
b_line = ((b.stdout or "").strip().splitlines()[-1:] or [b.stderr.strip()[-160:]])[0]
case("(rig) the lease store is still absent when the drain's lock is met",
     dst("SELECT to_regclass('public._apitap_lease') IS NULL") == "t")
case("a drain's lock with no lease store is refused by TYPE: LockedError",
     b_line.startswith("B LockedError"), b_line)
case("and it is never collected — no record of liveness means refuse",
     PLANT in staging_names(), f"{PLANT} still listed")
dst(f'DROP TABLE IF EXISTS "{PLANT}"')
for n in staging_names():
    dst(f'DROP TABLE IF EXISTS "{n}"')

# ---------------------------------------------------------------------------
print("== cleanup ==")
src(f"DROP TABLE IF EXISTS {T}")
dst(f"DROP TABLE IF EXISTS {T}")
for n in staging_names():
    dst(f'DROP TABLE IF EXISTS "{n}"')
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")

print("\nCONCURRENT RUNS E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
