"""A hard-killed CDC drain must stop blocking the pipeline by itself.

0.56.0 gave the drain a tokenized `__apitap_lock` so a drain and a bulk run
refuse each other. It also gave it a wedge: a drain killed OUTRIGHT (SIGKILL, an
OOM kill, a node that goes away) runs no cleanup, so its lock survived and every
later run of that table was refused until a human dropped it. Unattended CDC
pipelines are exactly the thing that cannot afford that.

The lease is the fix. Every drain writes a `_apitap_lease` row per table it
holds, whose `expires_at` is stamped AND compared by the DESTINATION server, and
renews it on a timer. A peer may drop a blocking lock if and only if a lease row
exists for that exact peer token and the destination itself says it has lapsed.

What that buys, and what this leg asserts:
  1. a killed drain really does leave a lock AND a lease — no plant, a real kill
  2. the immediate re-run is still refused, by TYPE, and the refusal names a
     deadline rather than a chore
  3. after the TTL the next run collects it and RESUMES FROM THE WATERMARK,
     with no human step
  4. a lock with NO lease row is never collected, at any age — every artifact
     written before the lease existed, every operator plant
  5. a LIVE drain's lease is never collectable, so the fix cannot eat a
     healthy run

Run with a short TTL (`APITAP_LEASE_TTL_SECS`) so the leg does not sleep five
minutes; the clamp floor is 30s.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-ch` on :8124.
"""
import os
import subprocess
import sys
import time

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cdc_lease"
TTL = 30
os.environ["APITAP_LEASE_TTL_SECS"] = str(TTL)


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
                        "--user", "default", "--password", "bench", "-q", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def locks():
    return sorted(n for n in ch(
        "SELECT name FROM system.tables WHERE database = currentDatabase() "
        f"AND startsWith(name, '{T}') AND endsWith(name, '__apitap_lock')").split() if n)


def leases():
    """LIVE leases only.

    The ClickHouse store is append-only by design — a released lease is a row
    stamped into the past with `collected = 1`, swept later by the table's TTL,
    not a row that disappears. Counting those as leases would make every
    assertion here read "a finished run left a lease behind" for a whole day.
    """
    if ch("SELECT count() FROM system.tables WHERE name = '_apitap_lease'") == "0":
        return []
    return [r for r in ch(
        "SELECT token FROM (SELECT token, argMax(collected, seq) AS c, "
        "                          argMax(expires_at, seq) AS e "
        f"                   FROM `_apitap_lease` WHERE dest_key LIKE '%.{T}' "
        "                    GROUP BY token) "
        "WHERE c = 0 AND e > now64(6)").splitlines() if r]


def watermark():
    return ch(f"SELECT watermark FROM `_apitap_state` FINAL WHERE dest_table = '{T}' "
              f"AND source_id NOT LIKE 'server-identity:%'")


def clean():
    pg(f"DROP TABLE IF EXISTS {T} CASCADE")
    pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
    pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM "
       "pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
    for n in locks():
        ch(f"DROP TABLE IF EXISTS `{n}`")
    ch(f"DROP VIEW IF EXISTS {T}__current")
    ch(f"DROP TABLE IF EXISTS {T}")
    for t, w in (("_apitap_state", f"dest_table = '{T}'"),
                 ("_apitap_lease", f"dest_key LIKE '%.{T}'")):
        if ch(f"SELECT count() FROM system.tables WHERE name = '{t}'") != "0":
            ch(f"ALTER TABLE `{t}` DELETE WHERE {w} SETTINGS mutations_sync = 1")


ok = True


def case(name, passed, detail=""):
    global ok
    ok &= passed
    print(f"   {'✓' if passed else '✗'} {name}: {detail}")


def refusal(fn):
    try:
        fn()
        return None
    except Exception as e:                                    # noqa: BLE001
        return f"{type(e).__name__}: {e}"


def drain():
    return apitap.transfer(PG, CH, table=T, mode="log_based")


print("== reset and bootstrap ==")
clean()
pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
pg(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1,200) g")
drain()
case("bootstrapped", ch(f"SELECT count() FROM {T}") == "200",
     f"{ch(f'SELECT count() FROM {T}')} rows")
case("a finished run leaves no lock and no lease",
     locks() == [] and leases() == [],
     f"locks {locks() or 'none'}, leases {leases() or 'none'}")

print("== a drain killed OUTRIGHT leaves its lock — and its lease ==")
# A real kill, not a plant. A plant cannot prove a live run writes one, and the
# whole mechanism rests on it doing so.
pg(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201,40000) g")
wm_before = watermark()
p = subprocess.Popen(
    [sys.executable, "-c",
     f"import apitap; apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')"],
    env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="262144"),
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
deadline = time.monotonic() + 20
while time.monotonic() < deadline and not locks():
    time.sleep(0.05)
had_lock = bool(locks())
p.kill()
p.wait()
case("the killed drain left a lock", had_lock, f"{locks() or 'none'}")
case("…and a lease row, which is what makes it recoverable",
     len(leases()) == 1, f"{leases() or 'none'}")

print("== the immediate re-run is still refused — the guard has not gone soft ==")
e = refusal(drain)
case("refused by type while the lease is fresh",
     e is not None and "LockedError" in e, (e or "it was ALLOWED")[:120])
case("and the refusal names a deadline instead of a chore",
     bool(e) and "nothing for you to do" in e, (e or "")[-160:])

print(f"== after the {TTL}s TTL it collects itself, with no human step ==")
time.sleep(TTL + 3)
e = refusal(drain)
case("the next run proceeds", e is None, e or "collected and drained")
case("and the dead run's lock is gone", locks() == [], f"{locks() or 'none'}")
wm_after = watermark()
case("it RESUMED — the watermark moved on from where the kill left it",
     wm_after != "" and wm_after != wm_before, f"{wm_before} -> {wm_after}")
src = pg(f"SELECT count(*)||'|'||coalesce(sum(id::bigint),0) FROM {T}")
dst = ch(f"SELECT toString(count()) || '|' || toString(sum(toInt64(id))) FROM {T}")
case("and the destination caught up exactly", src == dst, f"src {src} vs dst {dst}")

print("== a lock with NO lease is never collected, at any age ==")
# Everything written before the lease existed, and everything an operator plants.
# `_0000000l000abcd` is an epoch-zero CDC token: as old as a token can look.
STALE = f"{T}_0000000l000abcd__apitap_lock"
ch(f"CREATE TABLE `{STALE}` (t UInt8) ENGINE = Memory")
e = refusal(drain)
case("refused", e is not None and "LockedError" in e, (e or "it was ALLOWED")[:120])
case("and the refusal still says nothing collects it",
     bool(e) and "nothing collects it on its own" in e, (e or "")[-150:])
case("and it is still there — refusing is the safe action",
     STALE in locks(), f"{locks()}")
ch(f"DROP TABLE IF EXISTS `{STALE}`")

print("== a LIVE drain's lease is never collectable ==")
# The failure this mechanism must not have: eating a healthy run.
pg(f"INSERT INTO {T} SELECT g, 'z'||g FROM generate_series(40001,90000) g")
p = subprocess.Popen(
    [sys.executable, "-c",
     f"import apitap; apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')"],
    env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="262144"),
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
deadline = time.monotonic() + 20
while time.monotonic() < deadline and not locks():
    time.sleep(0.05)
live = bool(locks())
e = refusal(lambda: apitap.transfer(PG, CH, table=T, dest_table=T, mode="replace")) if live else "skipped"
p.wait(300)
if not live:
    case("(rig) a live drain was observable", False,
         "the drain finished before its lock could be seen — raise the backlog")
else:
    case("a replace is refused beside a LIVE drain",
         e is not None and "LockedError" in e, (e or "it was ALLOWED")[:120])
    case("and the refusal says to wait, not to remove",
         bool(e) and "nothing for you to do" in e, (e or "")[-160:])
    case("the live drain finished normally and cleaned up after itself",
         p.returncode == 0 and locks() == [] and leases() == [],
         f"rc={p.returncode}, locks {locks() or 'none'}, leases {leases() or 'none'}")

print("== cleanup ==")
clean()

print("\n   ===== CDC LEASE E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
