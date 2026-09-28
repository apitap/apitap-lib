"""An evicted ClickHouse drain stays evicted.

ClickHouse has no row lock, so a collector's claim is a new row-version of the
victim's lease, and 0.56.0 lost it three ways:

  * the victim's keeper renewed UNCONDITIONALLY, with a plain timestamp `seq`,
    so its next tick after a pause out-ranked the claim — the flag flipped back
    within seconds and the evicted drain carried on writing;
  * the lease table had a `TTL … DELETE`, so a drain killed on Friday had no
    lease row by Monday and its lock read as "nothing collects it";
  * that TTL is per part: a merge could drop a collected version while an
    older, unclaimed one survived.

0.57.0: claim and close versions carry `seq + 2^62`, a renewal only writes
while the row exists and is not collected, and the TTL is removed on first
contact. What this leg asks the server:

  schema    after a 0.57 run, `engine_full` of `_apitap_lease` has no TTL
            (the table is given 0.56.0's TTL first, as an upgrade would find it)
  eviction  drain A is paused mid-backlog past its TTL; B collects it and
            finishes; A is resumed → A exits non-zero, "no longer holds", and
            for 3 renewal periods its row reads collected every second
  ttl-merge a collected version aged 2 days survives `OPTIMIZE … FINAL`
  stale     a lock whose lease row is 2 days lapsed (never collected) is
            collected by the next run, which resumes from the watermark

    python benchmarks/e2e_cdc_evict_ch.py

Source MariaDB :3309 (a binlog drain), destination ClickHouse :8124. TTL 30.
RED: 0.56.0 — schema keeps its TTL; the eviction sample flips within 3 s and A
exits 0; the aged version is merged away; the stale run is refused.
"""
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import _rig

SRC = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "evict_ch"
TTL = 30
RENEW = max(3, TTL // 10)
BIAS = 1 << 62
os.environ["APITAP_LEASE_TTL_SECS"] = str(TTL)
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def ma(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench",
                        "-N", "-D", "bench", "-e", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


ch = _rig.clickhouse


def has(table):
    return ch(f"SELECT count() FROM system.tables WHERE database = currentDatabase() "
              f"AND name = '{table}'") != "0"


def engine_full():
    return ch("SELECT engine_full FROM system.tables WHERE database = currentDatabase() "
              "AND name = '_apitap_lease' FORMAT TabSeparatedRaw")


def watermark():
    return ch(f"SELECT watermark FROM `_apitap_state` FINAL WHERE dest_table = '{T}' "
              "AND source_id NOT LIKE 'server-identity:%'")


def dest_counts():
    if not has(T):
        return 0, 0
    # No FINAL: the CDC table is a plain MergeTree, and a row written twice is
    # exactly what this must see.
    c, u = ch(f"SELECT count(), uniqExact(id) FROM `{T}`").split()
    return int(c), int(u)


def fast_count():
    """Over HTTP, not `docker exec`: the drain lands a window every few tens of
    milliseconds, and a 150 ms client round trip watches it finish instead."""
    q = urllib.parse.urlencode({"user": "default", "password": "bench",
                                "query": f"SELECT count() FROM `{T}`"})
    try:
        return int(urllib.request.urlopen(f"http://127.0.0.1:8124/?{q}", timeout=5).read())
    except Exception:                                         # noqa: BLE001 — not there yet
        return 0


def collected(tok):
    return ch("SELECT toUInt8(count() > 0 AND argMax(collected, seq) = 1) FROM `_apitap_lease` "
              f"WHERE token = '{tok}'")


def drain_env():
    # The floor (1 MiB): the most windows, so the most moments to pause in.
    return dict(os.environ, APITAP_CDC_WINDOW_BYTES="1048576")


def spawn():
    return subprocess.Popen([sys.executable, "-c",
                             f"import apitap; apitap.transfer({SRC!r}, {CH!r}, table={T!r}, mode='log_based')"],
                            env=drain_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def run():
    return subprocess.run([sys.executable, "-c",
                           f"import apitap; apitap.transfer({SRC!r}, {CH!r}, table={T!r}, mode='log_based')"],
                          env=drain_env(), capture_output=True, text=True, timeout=600)


def clean():
    ma(f"DROP TABLE IF EXISTS {T}")
    for n in ch("SELECT name FROM system.tables WHERE database = currentDatabase() "
                f"AND startsWith(name, '{T}')").split():
        ch(f"DROP TABLE IF EXISTS `{n}`")
    for t, w in (("_apitap_state", f"dest_table = '{T}'"), ("_apitap_lease", f"dest_key = 'default.{T}'")):
        if has(t):
            ch(f"ALTER TABLE `{t}` DELETE WHERE {w} SETTINGS mutations_sync = 1")


print("== reset ==", flush=True)
clean()
# The table as 0.56.0 left it: with its TTL.
if has("_apitap_lease"):
    if " TTL " not in engine_full():
        ch("ALTER TABLE `_apitap_lease` MODIFY TTL toDateTime(expires_at) + INTERVAL 1 DAY DELETE "
           "SETTINGS mutations_sync = 1")
else:
    ch("CREATE TABLE `_apitap_lease` (dest_key String, token String, expires_at DateTime64(6, 'UTC'), "
       "collected UInt8, seq UInt64) ENGINE = ReplacingMergeTree(seq) ORDER BY (dest_key, token) "
       "TTL toDateTime(expires_at) + INTERVAL 1 DAY DELETE")
if " TTL " not in engine_full():
    _rig.rig_fail("could not give _apitap_lease 0.56.0's TTL")
ma(f"CREATE TABLE {T} (id INT PRIMARY KEY, v VARCHAR(120))")
ma(f"INSERT INTO {T} SELECT seq, CONCAT('v', seq) FROM seq_1_to_200")
a = None
try:
    print("== schema ==", flush=True)
    r = run()
    case("bootstrapped", r.returncode == 0 and dest_counts()[0] == 200, r.stderr.strip()[-200:])
    case("_apitap_lease has no TTL after a run", " TTL " not in engine_full(), engine_full()[-90:])

    print("== eviction ==", flush=True)
    # Many source transactions, not one: a window never splits a transaction,
    # so one big INSERT would land as one window with nothing to pause inside.
    ma("; ".join(f"INSERT INTO {T} SELECT seq, REPEAT('w', 100) FROM seq_{lo}_to_{lo + 9999}"
                 for lo in range(201, 1_500_201, 10_000)))
    a = spawn()
    moved = _rig.wait_for(lambda: a.poll() is not None or fast_count() > 200, 120, step=0.005)
    if not moved or a.poll() is not None:
        _rig.rig_fail(f"drain A never moved the destination (rc={a.poll()})")
    _rig.pause(a)
    lk = _rig.locks_ch(T)
    tok = _rig.token_of(lk[0]) if lk else None
    if not tok:
        _rig.rig_fail("drain A has no lock to be collected")
    print(f"      A = {tok}, paused at {dest_counts()[0]} rows; sleeping {TTL + 3}s", flush=True)
    time.sleep(TTL + 3)
    b = run()
    case("B collects A and finishes", b.returncode == 0 and "collected" in b.stderr,
         (b.stderr.strip().splitlines() or ["(nothing)"])[-1][:160])
    wm_b = watermark()
    case("A's row reads collected", collected(tok) == "1")

    print("== ttl-merge ==", flush=True)
    # A collected version aged two days: a TTL would delete it on merge.
    ch("INSERT INTO `_apitap_lease` (dest_key, token, expires_at, collected, seq) "
       f"SELECT 'default.{T}', '{tok}', now64(6) - INTERVAL 2 DAY, 1, "
       f"toUnixTimestamp64Micro(now64(6)) + {BIAS}")
    ch("OPTIMIZE TABLE `_apitap_lease` FINAL")
    case("the aged collected version survives OPTIMIZE FINAL", collected(tok) == "1",
         ch(f"SELECT count() FROM `_apitap_lease` WHERE token = '{tok}'") + " version(s)")

    print("== resume A ==", flush=True)
    _rig.resume(a)
    flips = []
    for i in range(3 * RENEW):
        v = collected(tok)
        if v != "1":
            flips.append(i)
        time.sleep(1)
    case(f"A's row stays collected for {3 * RENEW}s after resume", not flips,
         f"flipped at second(s) {flips}" if flips else "every sample collected")
    try:
        _, a_err = a.communicate(timeout=180)
    except subprocess.TimeoutExpired:
        a.kill()
        _, a_err = a.communicate()
    case("A exits non-zero", a.returncode != 0, f"rc={a.returncode}")
    case("and says it no longer holds the table", "no longer holds" in a_err,
         (a_err.strip().splitlines() or [""])[-1][:160])
    a = None
    c, u = dest_counts()
    total = int(ma(f"SELECT COUNT(*) FROM {T}"))
    case("dest == source, no duplicates", c == total and u == total, f"dest {c}/{u} of {total}")
    # The one statement A had in flight may land (it carries its window's
    # start LSN, and the replay appends nothing for it); its watermark may not.
    case("the watermark is still B's", watermark() == wm_b, f"B left {wm_b}, now {watermark()}")

    print("== stale ==", flush=True)
    s_tok = _rig.fresh_token("l", "5ta1")
    ch(f"CREATE TABLE `{T}{s_tok}__apitap_lock` (t UInt8) ENGINE = Memory")
    ch("INSERT INTO `_apitap_lease` (dest_key, token, expires_at, collected, seq) "
       f"SELECT 'default.{T}', '{s_tok}', now64(6) - INTERVAL 2 DAY, 0, toUnixTimestamp64Micro(now64(6))")
    ch("OPTIMIZE TABLE `_apitap_lease` FINAL")
    left = ch(f"SELECT count() FROM `_apitap_lease` WHERE token = '{s_tok}'")
    case("the 2-day-lapsed lease row survives OPTIMIZE FINAL", left != "0", f"{left} row(s)")
    ma(f"INSERT INTO {T} SELECT seq, 's' FROM seq_1500201_to_1500300")
    r = run()
    case("the next run collects it and resumes", r.returncode == 0 and "collected" in r.stderr,
         (r.stderr.strip().splitlines() or ["(nothing)"])[-1][:160])
    c, u = dest_counts()
    case("from the watermark: dest == source", c == u == 1500300, f"dest {c}/{u}")
    case("the stale lock is gone", not _rig.locks_ch(T), str(_rig.locks_ch(T)))
finally:
    print("== cleanup ==", flush=True)
    if a is not None and a.poll() is None:
        _rig.resume(a)
        a.kill()
        a.wait()
    clean()

print("\nCDC EVICT CH E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
