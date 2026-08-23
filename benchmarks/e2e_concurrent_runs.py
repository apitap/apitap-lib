"""Two runs of one destination table must not publish each other's work.

Before this, every run of a table used the same staging object, and `prepare`
began by dropping it. So run A could stream N rows in, run B's prepare could
DROP that object and CREATE a fresh empty one, and A's finalize would then
rename B's empty table over the destination — returning a successful report
with A's row count. A green run and an empty table.

The fix puts the run's identity IN the staging name, so a run can only publish
an object it minted. `prepare` no longer drops anything blindly: it lists what
is there, reaps what is provably dead (the age is in the token, because
Postgres stores no table creation time), and refuses to start beside a live
peer it cannot coexist with.

  leg 1  the refusal        — a second replace, while the first is mid-flight,
                              must fail with Error::Locked and NOT touch the
                              destination
  leg 2  the survivor       — the first run then finishes normally and the
                              destination holds ITS rows, whole
  leg 1c the control        — the same race with APITAP_STAGING_REAP_SECS
                              irrelevant and the guard bypassed by pointing
                              both runs at DIFFERENT source URLs for the same
                              table, which the matrix permits: both must
                              succeed. If this leg fails, leg 1 is refusing
                              everything rather than refusing collisions.
  leg 3  the reap           — a leftover staging object older than the horizon
                              is collected rather than mistaken for a peer,
                              and a leftover from BEFORE tokens existed is
                              collected too
  leg 4  fan-in survives    — two appends from two different sources into one
                              table still both land, because that is a
                              capability the manual advertises and a guard
                              that refused it would be a regression

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
print("== leg 3: dead leftovers are reaped, not mistaken for peers ==")
reset(rows=1000)
r = run("replace")
case("a clean run", r.returncode == 0, r.stderr.strip()[-200:])

# A leftover from a crashed run of THIS version: a tokenized name with an old
# timestamp. '0000000' is 1970, which is past any horizon.
# Exactly 16 bytes, or it does not parse as a token at all and the reap
# correctly ignores it: _ + 7 start + 1 mode + 3 source + 4 nonce. A first
# draft wrote 15 and then reported the reap as broken.
dst(f'CREATE TABLE "{T}_0000000r000abcd__apitap_staging" (id bigint)')
# And one from BEFORE tokens existed, which carries no age at all.
dst(f'CREATE TABLE "{T}__apitap_staging" (id bigint)')
case("(rig) two leftovers are in place", len(staging_names()) == 2,
     f"{staging_names()}")

r = run("replace")
case("the next run succeeds despite them", r.returncode == 0,
     (r.stderr.strip().splitlines() or [""])[-1][:170])
case("and both leftovers were collected", staging_names() == [],
     f"left: {staging_names()}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
src(f"DROP TABLE IF EXISTS {T}")
dst(f"DROP TABLE IF EXISTS {T}")
for n in staging_names():
    dst(f'DROP TABLE IF EXISTS "{n}"')
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")

print("\nCONCURRENT RUNS E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
