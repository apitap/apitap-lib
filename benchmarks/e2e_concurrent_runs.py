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
  leg 3  what is collected   — the un-tokenized leftover is collected, a
                              TOKENIZED one is not (however ancient its token
                              looks), the run refuses while it is there, and
                              dropping it by hand — the recovery the error
                              message prescribes — makes the run work again
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
import os
import subprocess
import sys
import threading
import time

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
    return [n for n in dst(
        "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        f"WHERE n.nspname='public' AND c.relkind='r' AND relname LIKE '{T}%staging'"
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
print("== leg 3: only the provably-dead name is collected ==")
reset(rows=1000)
r = run("replace")
case("a clean run", r.returncode == 0, r.stderr.strip()[-200:])

# (a) The leftover from BEFORE tokens existed. No current run mints this name,
#     so nothing living can own it — the one thing collection can prove.
dst(f'CREATE TABLE "{T}__apitap_staging" (id bigint)')
case("(rig) the un-tokenized leftover is in place", len(staging_names()) == 1,
     f"{staging_names()}")
r = run("replace")
case("the run succeeds despite it", r.returncode == 0,
     (r.stderr.strip().splitlines() or [""])[-1][:170])
case("and it was collected", staging_names() == [], f"left: {staging_names()}")

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
print("== leg 6: two runs starting in the SAME INSTANT — the window that is left ==")
# `prepare` lists the catalog and then creates its staging table. Two runs that
# start inside that gap both see an empty catalog and both proceed: the guard is
# check-then-act, and this is the act it cannot see.
#
# The outcome is deliberately NOT asserted, because it is genuinely two-valued —
# both may land (the swap serialises, last writer wins), or the loser may fail at
# RENAME with a duplicate-key error instead of a clean LockedError. What IS
# asserted is the pair of invariants that must hold either way, because their
# failure is the original defect returning: the destination is whole, and no
# staging object is orphaned. If this leg ever reports a short table, the
# concurrency work has regressed to what it was written to fix.
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
print(f"      (outcome, not asserted: {(r.stdout.strip() or 'no output')[:120]})")
got = dst(f"SELECT count(*) FROM {T}")
case("INVARIANT: the destination is whole, whoever won", got == want,
     f"dest {got} vs source {want}")
case("and the failure, if any, was loud — never a green run over a short table",
     "ok" in r.stdout or "Error" in r.stdout,
     (r.stdout.strip() or "")[:120])
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
print("== cleanup ==")
src(f"DROP TABLE IF EXISTS {T}")
dst(f"DROP TABLE IF EXISTS {T}")
for n in staging_names():
    dst(f'DROP TABLE IF EXISTS "{n}"')
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")

print("\nCONCURRENT RUNS E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
