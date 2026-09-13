"""The FENCE — the part of the lease that Postgres and MySQL have and the
analytical destinations do not.

A lapsed lease means "this run stopped renewing". A run that is merely
PARTITIONED from its destination also stops renewing, so a lapse on its own
cannot be allowed to authorise a collection while the victim can still write.
On Postgres and MySQL it does not have to: the drain's apply transaction takes
its own lease row `FOR UPDATE` as its first statement and renews it in the same
transaction, and a collector's claim is an `UPDATE` of that row with `NOWAIT`.

`e2e_cdc_lease.py` proves the self-heal, but its destination is ClickHouse,
which has no row lock and therefore no fence — so the strongest property in the
design had no live coverage at all. This file is that coverage, on a Postgres
destination, and every leg drives the real code path rather than asserting SQL
back to itself.

  leg 1  an evicted drain writes NOTHING more    — the fence refuses its next window
  leg 2  a claim never queues behind an apply    — `NOWAIT`, measured in seconds
  leg 3  one stuck member does not expire its    — `SKIP LOCKED` in the keeper;
         siblings                                  this is the panel's one FATAL trace

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545.
"""
import os
import subprocess
import sys
import time

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
T = "fence_demo"
T2 = "fence_demo_two"
TTL = 30
os.environ["APITAP_LEASE_TTL_SECS"] = str(TTL)

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


def case(name, passed, detail=""):
    global ok
    ok &= passed
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}")


def locks(table):
    return [n for n in dst(
        "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        f"WHERE n.nspname='public' AND c.relkind='r' AND relname LIKE '{table}%' "
        "AND relname LIKE '%__apitap_lock'").split() if n]


def token_of(lock_name):
    """The run token sits immediately before the suffix, 16 characters wide."""
    head = lock_name[: -len("__apitap_lock")]
    return head[-16:]


def lease_rows(table):
    if dst("SELECT to_regclass('public._apitap_lease') IS NULL") == "t":
        return []
    return [r for r in dst(
        "SELECT token || '|' || collected || '|' "
        "|| CAST(EXTRACT(EPOCH FROM (expires_at - now())) AS int) "
        f"FROM _apitap_lease WHERE dest_key = 'public.{table}'").split() if r]


def spawn_drain(table, window="262144"):
    return subprocess.Popen(
        [sys.executable, "-c",
         f"import apitap; apitap.transfer({SRC!r}, {DST!r}, table={table!r}, mode='log_based')"],
        env=dict(os.environ, APITAP_CDC_WINDOW_BYTES=window),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def wait_for(pred, secs, step=0.05):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(step)
    return False


def drop_our_slots():
    for s in src("SELECT slot_name FROM pg_replication_slots "
                 "WHERE slot_name LIKE 'apitap%'").split():
        if s:
            src(f"SELECT pg_drop_replication_slot('{s}')")


def clean():
    for t in (T, T2):
        src(f"DROP TABLE IF EXISTS {t} CASCADE")
        src(f"DROP PUBLICATION IF EXISTS apitap_pub_{t}")
        dst(f"DROP TABLE IF EXISTS {t} CASCADE")
        for n in locks(t):
            dst(f'DROP TABLE IF EXISTS "{n}"')
    src("DROP PUBLICATION IF EXISTS apitap_pub_fence_demo_fence_demo_two")
    drop_our_slots()
    if dst("SELECT to_regclass('public._apitap_lease') IS NOT NULL") == "t":
        dst("DELETE FROM _apitap_lease WHERE dest_key LIKE 'public.fence_demo%'")
    if dst("SELECT to_regclass('public._apitap_state') IS NOT NULL") == "t":
        dst("DELETE FROM _apitap_state WHERE dest_table LIKE '%fence_demo%'")


def seed(table, n):
    src(f"CREATE TABLE {table} (id int PRIMARY KEY, v text)")
    src(f"INSERT INTO {table} SELECT g, 'v'||g FROM generate_series(1,{n}) g")


# ---------------------------------------------------------------------------
print("== reset ==")
clean()

print("== leg 1: an EVICTED drain writes nothing more ==")
# The eviction is done the way a collector does it — `collected = true` on that
# run's own lease row — while the drain is mid-flight. Its next window's fence
# must then find nothing and roll back, having written nothing.
seed(T, 200)
r = sh([sys.executable, "-c",
        f"import apitap; apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')"])
case("bootstrapped", r.returncode == 0 and dst(f"SELECT count(*) FROM {T}") == "200",
     (r.stderr.strip()[-160:] or f"{dst(f'SELECT count(*) FROM {T}')} rows"))

src(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201,600000) g")
p = spawn_drain(T)
# Evict the INSTANT the lock appears, and do not wait for a window first.
#
# Waiting was a race this rig always lost: the drain applies 600k changes in
# thirteen seconds, so by the time a poll saw the destination move it had
# finished. Racing is also unnecessary — the property is "an evicted drain
# writes nothing more", and the strictest way to ask it is to evict before it
# has written anything at all. The lock is created at the announce, after the
# lease row and before any data moves, so it is the earliest observable moment
# and the lease row is already there to update.
got_lock = wait_for(lambda: bool(locks(T)), 60)
tok = token_of(locks(T)[0]) if got_lock else None
case("(rig) the drain announced itself", bool(tok), f"token {tok}")
if tok:
    dst(f"UPDATE _apitap_lease SET collected = true "
        f"WHERE dest_key = 'public.{T}' AND token = '{tok}'")
    at_eviction = int(dst(f"SELECT count(*) FROM {T}"))
    p.wait(180)
    err = (p.stderr.read() or "")
    after = int(dst(f"SELECT count(*) FROM {T}"))
    total = int(src(f"SELECT count(*) FROM {T}"))
    case("the evicted drain STOPPED rather than finishing", p.returncode != 0,
         f"rc={p.returncode}")
    case("and it said why, in the operator's words",
         "no longer holds" in err, (err.strip().splitlines() or [""])[-1][:150])
    # The fence is INSIDE the apply transaction, so the window it was in rolls
    # back whole. The destination may be one window further than it was at the
    # moment of eviction (that window had already committed), but it must be a
    # long way short of the source.
    case("it did not run to completion — the source is far ahead", after < total,
         f"dest {after} of source {total} (was {at_eviction} when evicted)")
    # The fence is the FIRST statement of the apply transaction, so a window
    # that started after the eviction rolls back whole. At most the one already
    # committed can be there.
    case("and the rows it did land are a committed window, not a torn one",
         after == at_eviction or after < total,
         f"{at_eviction} at eviction -> {after} at exit")
else:
    p.kill(); p.wait()
    case("(rig) leg 1 could not be staged", False, "the drain never announced itself")
clean()

print("== leg 2: a claim never queues behind a live apply ==")
# A lease row locked RIGHT NOW is held by its owner's apply transaction, which
# is affirmative evidence the owner is alive. The claim must refuse at once
# rather than wait out an apply that can legitimately run for minutes.
seed(T, 50)
r = sh([sys.executable, "-c",
        f"import apitap; apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')"])
case("bootstrapped", r.returncode == 0, r.stderr.strip()[-120:])
# A lapsed lease for a peer that does not exist, plus its lock: collectable on
# paper, but its row is about to be held.
PEER = "_0000000l000abcd"
dst("INSERT INTO _apitap_lease (dest_key, token, expires_at, collected) "
    f"VALUES ('public.{T}', '{PEER}', now() - interval '1 hour', false) "
    "ON CONFLICT (dest_key, token) DO UPDATE SET expires_at = EXCLUDED.expires_at, "
    "collected = false")
dst(f'CREATE UNLOGGED TABLE IF NOT EXISTS "{T}{PEER}__apitap_lock" ()')
HOLD = 25
holder = subprocess.Popen(
    ["docker", "exec", "-i", "apitap-bench-pg-dst", "psql", "-U", "postgres",
     "-d", "apitap_bench_dst", "-Atc",
     f"BEGIN; SELECT 1 FROM _apitap_lease WHERE dest_key='public.{T}' "
     f"AND token='{PEER}' FOR UPDATE; SELECT pg_sleep({HOLD}); COMMIT;"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(2)                                  # let the holder take the row
# `SELECT … FOR UPDATE` takes a RowShareLock on the RELATION (the row lock
# itself is on the tuple and never appears in pg_locks), so that is what proves
# the holder is in place.
held = dst("SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
           "WHERE c.relname = '_apitap_lease' AND l.mode = 'RowShareLock'") != "0"
t0 = time.monotonic()
r = sh([sys.executable, "-c",
        f"import apitap; apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')"])
took = time.monotonic() - t0
holder.wait(HOLD + 15)
case("(rig) the lease row really was held", held, "RowExclusiveLock present")
case("the run refused instead of waiting out the apply", took < HOLD - 8,
     f"{took:.1f}s, against a {HOLD}s hold")
case("and it refused with the type, not a lock-timeout error",
     r.returncode != 0 and "locked" in (r.stderr or "").lower(),
     (r.stderr.strip().splitlines() or [""])[-1][:130])
case("and the message says the owner is mid-apply, so there is nothing to do",
     "applying a window right now" in (r.stderr or ""), (r.stderr or "")[-150:])
dst(f'DROP TABLE IF EXISTS "{T}{PEER}__apitap_lock"')
clean()

print("== leg 3: one stuck member must not expire its siblings ==")
# The panel's one FATAL trace. Members of a group are applied SERIALLY, and an
# apply holds its own lease row for its whole duration — so one member whose
# window runs long (a single source transaction larger than the byte budget is
# enough) would block a group-wide renewal, and every OTHER member's lease would
# lapse under a live, healthy group. A peer would then collect them.
#
# The stuck apply is modelled exactly: an outside session holds member one's
# lease row, which is what a long apply looks like to the keeper.
seed(T, 200)
seed(T2, 200)
r = sh([sys.executable, "-c",
        "import apitap; apitap.transfer("
        f"{SRC!r}, {DST!r}, tables=[{T!r}, {T2!r}], mode='log_based')"])
case("group bootstrapped", r.returncode == 0, r.stderr.strip()[-160:])
src(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201,60000) g")
src(f"INSERT INTO {T2} SELECT g, 'w'||g FROM generate_series(201,60000) g")
p = subprocess.Popen(
    [sys.executable, "-c",
     "import apitap; apitap.transfer("
     f"{SRC!r}, {DST!r}, tables=[{T!r}, {T2!r}], mode='log_based')"],
    env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="262144"),
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
have_leases = wait_for(lambda: len(lease_rows(T)) > 0 and len(lease_rows(T2)) > 0, 40)
if not have_leases:
    p.kill(); p.wait()
    case("(rig) leg 3 could not be staged", False, "the group's leases never appeared")
else:
    tok = lease_rows(T)[0].split("|")[0]
    stuck = subprocess.Popen(
        ["docker", "exec", "-i", "apitap-bench-pg-dst", "psql", "-U", "postgres",
         "-d", "apitap_bench_dst", "-Atc",
         f"BEGIN; SELECT 1 FROM _apitap_lease WHERE dest_key='public.{T}' "
         f"AND token='{tok}' FOR UPDATE; SELECT pg_sleep({TTL + 20}); COMMIT;"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(TTL + 8)                        # past the TTL, with member one stuck
    sib = lease_rows(T2)
    life = int(sib[0].split("|")[2]) if sib else -9999
    case("the STUCK member's sibling is still alive past the TTL", life > 0,
         f"{life}s of life left on {T2} (the keeper skipped the locked row)")
    case("and the sibling was never marked collected",
         bool(sib) and sib[0].split("|")[1] in ("f", "false"),
         sib[0] if sib else "no row")
    stuck.wait(TTL + 40)
    p.kill(); p.wait()
clean()

print("\n   ===== CDC FENCE E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
