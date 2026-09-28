"""The MySQL lease, asked of the MySQL server.

A MySQL destination has a row lock, so its lease is a fence the way Postgres's
is: every apply transaction takes the drain's own lease row `FOR UPDATE` first
and renews it before COMMIT, and a collector's claim is a locked READ of that
row. This leg drives real drains against planted and real peers and asks
`information_schema`, `_apitap_lease` and the server's own counters what
happened.

  case 1  evicted at the lock's appearance, the drain writes NOTHING more
  case 2  a claim never queues behind a live apply: refused in seconds, by type
  case 3  one member held for longer than the TTL does not expire its sibling
  case 4  a claim already TAKEN (an hour of lease left) by a collector that
          died before dropping the lock: the next drain finishes it and runs
  case 5  an evicted member's TRUNCATE never runs: the table keeps its rows
  case 6  as 4, with the lease lapsed a second ago
  case 7  a TRUNCATE of 2M rows stays O(1) — no row-by-row delete
  case 8  a SIGKILLed drain's lock and marker clear by themselves after the TTL

0.56.0: case 1 passes (it had a fence); 4 and 6 are refused for ever (its claim
read `affected_rows`, which counts CHANGED rows); 5 empties the collector's rows
(its TRUNCATE ran before the fence); 8 leaves the marker it never had.

    python benchmarks/e2e_cdc_fence_my.py

Source Postgres :5544, destination MySQL :3307.
"""
import os
import signal
import subprocess
import sys
import time

import _rig

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "mysql://root:bench@127.0.0.1:3307/bench"
T, T2, T5, T7 = "fence_my", "fence_my_two", "fence_my_tr", "fence_my_big"
TTL = 30
os.environ["APITAP_LEASE_TTL_SECS"] = str(TTL)
ok = True
my = _rig.mysql
_SLOTS = set(_rig.psql("SELECT slot_name FROM pg_replication_slots", _rig.PG_SRC).split())


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def key(t):
    return f"bench.{t}"


def pg(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def has(table):
    return my("SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
              f"AND table_name = '{table}'") != "0"


def names(t):
    return [n for n in my("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() "
                          f"AND table_name LIKE '{t}%'").splitlines() if n]


def count(t):
    return int(my(f"SELECT COUNT(*) FROM `{t}`"))


def code(tables):
    arg = f"table={tables[0]!r}" if len(tables) == 1 else f"tables={list(tables)!r}"
    return f"import apitap; apitap.transfer({SRC!r}, {DST!r}, {arg}, mode='log_based')"


def drain(*tables):
    return subprocess.run([sys.executable, "-c", code(tables)], capture_output=True, text=True, timeout=900)


def spawn(*tables, **env):
    return subprocess.Popen([sys.executable, "-c", code(tables)],
                            env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="1048576", **env),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def last(stderr):
    return (stderr.strip().splitlines() or ["(nothing)"])[-1][:200]


def ensure_lease_table():
    my("CREATE TABLE IF NOT EXISTS _apitap_lease (dest_key VARCHAR(320) NOT NULL, "
       "token VARCHAR(64) NOT NULL, expires_at DATETIME(6) NOT NULL, "
       "collected TINYINT NOT NULL DEFAULT 0, PRIMARY KEY (dest_key, token)) ENGINE=InnoDB")


def plant(t, tok, expires, collected):
    """A peer drain's lock and marker, and its lease row."""
    left = [f"{t}{tok}__apitap_lock", f"{t}{tok}__apitap_staging"]
    for n in left:
        my(f"CREATE TABLE `{n}` (t TINYINT) ENGINE=MEMORY")
    ensure_lease_table()
    my(f"INSERT INTO _apitap_lease VALUES ('{key(t)}', '{tok}', {expires}, {collected})")
    return left


def lease_life(t, tok):
    """(collected, seconds of life left) of one lease row, or None."""
    r = my(f"SELECT collected, TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), expires_at) FROM _apitap_lease "
           f"WHERE dest_key = '{key(t)}' AND token = '{tok}'")
    return tuple(int(x) for x in r.split()) if r else None


def pg_clean(t):
    for p in pg(f"SELECT DISTINCT pubname FROM pg_publication_tables WHERE tablename = '{t}'").split():
        pg(f"DROP PUBLICATION IF EXISTS {p}")
    pg(f"DROP TABLE IF EXISTS {t} CASCADE")


def clean():
    for t in (T, T2, T5, T7):
        pg_clean(t)
    for s in set(pg("SELECT slot_name FROM pg_replication_slots").split()) - _SLOTS:
        pg(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
           f"WHERE slot_name = '{s}' AND NOT active")
    my("DROP TRIGGER IF EXISTS fence_my_slow")
    for t in (T, T2, T5, T7):
        for n in names(t):
            my(f"DROP TABLE IF EXISTS `{n}`")
        for lt, w in (("_apitap_lease", f"dest_key = '{key(t)}'"), ("_apitap_state", f"dest_table = '{t}'")):
            if has(lt):
                my(f"DELETE FROM {lt} WHERE {w}")


def seed(t, n):
    pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {t} SELECT g, 'v'||g FROM generate_series(1, {n}) g")


def lock_token(t):
    lk = _rig.locks_my(t)
    return _rig.token_of(lk[0]) if lk else None


def holder(t, tok, secs):
    """A session that holds one lease row the way a long apply does."""
    return subprocess.Popen(
        ["docker", "exec", "-i", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-D", "bench", "-e",
         f"BEGIN; SELECT 1 FROM _apitap_lease WHERE dest_key = '{key(t)}' AND token = '{tok}' FOR UPDATE; "
         f"SELECT SLEEP({secs}); COMMIT;"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def row_locks():
    return int(my("SELECT COUNT(*) FROM performance_schema.data_locks "
                  "WHERE object_name = '_apitap_lease' AND lock_type = 'RECORD'") or 0)


def holds(t):
    """Is some transaction holding `t`'s lease row right now?"""
    return f"'{key(t)}'" in my("SELECT lock_data FROM performance_schema.data_locks "
                               "WHERE object_name = '_apitap_lease' AND lock_type = 'RECORD'")


procs = []
print("== reset ==", flush=True)
clean()
try:
    print("== case 1: evicted at the lock's appearance, the drain writes nothing more ==", flush=True)
    seed(T, 200)
    r = drain(T)
    case("case 1: bootstrapped", r.returncode == 0 and count(T) == 200, last(r.stderr))
    pg(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201, 600000) g")
    p = spawn(T)
    procs.append(p)
    tok = _rig.wait_for(lambda: lock_token(T), 60, step=0.05) and lock_token(T)
    if not tok:
        _rig.rig_fail("case 1: the drain never announced itself")
    # Queues behind the drain's apply transaction whenever that holds the row,
    # so when it returns every window that will EVER commit has committed.
    my(f"UPDATE _apitap_lease SET collected = 1 WHERE dest_key = '{key(T)}' AND token = '{tok}'")
    at_eviction = count(T)
    _, err = p.communicate(timeout=300)
    after = count(T)
    case("case 1: the evicted drain stopped", p.returncode != 0, f"rc={p.returncode}")
    case("case 1: and said why", "no longer holds" in err, last(err))
    case("case 1: not one row landed after the eviction", after == at_eviction,
         f"{at_eviction} at eviction -> {after} at exit")
    clean()

    print("== case 2: a claim never queues behind a live apply ==", flush=True)
    seed(T, 50)
    r = drain(T)
    case("case 2: bootstrapped", r.returncode == 0, last(r.stderr))
    peer = _rig.fresh_token("l", "c2c2")
    plant(T, peer, "UTC_TIMESTAMP(6) - INTERVAL 1 HOUR", 0)
    HOLD = 25
    h = holder(T, peer, HOLD)
    procs.append(h)
    held = _rig.wait_for(lambda: row_locks() > 0, 10, step=0.1)
    t0 = time.monotonic()
    r = drain(T)
    took = time.monotonic() - t0
    h.wait(HOLD + 15)
    case("case 2: (rig) the peer's lease row really was held", held)
    case("case 2: refused instead of waiting out the apply", took < 17, f"{took:.1f}s against a {HOLD}s hold")
    case("case 2: by type, saying the owner is mid-apply",
         r.returncode != 0 and "applying a window right now" in r.stderr, last(r.stderr))
    clean()

    print("== case 3: a member held past the TTL does not expire its sibling ==", flush=True)
    seed(T, 200)
    seed(T2, 200)
    r = drain(T, T2)
    case("case 3: group bootstrapped", r.returncode == 0, last(r.stderr))
    pg(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201, 60000) g")
    pg(f"INSERT INTO {T2} SELECT g, 'w'||g FROM generate_series(201, 60000) g")
    p = spawn(T, T2)
    procs.append(p)
    tok = _rig.wait_for(lambda: lock_token(T), 60, step=0.05) and lock_token(T)
    if not tok:
        _rig.rig_fail("case 3: the group never announced itself")
    h = holder(T, tok, TTL + 20)
    procs.append(h)
    time.sleep(TTL + 8)
    sib = lease_life(T2, tok)
    case("case 3: the sibling is still alive past the TTL", bool(sib) and sib[1] > 0,
         f"{sib[1] if sib else 'no row'}s of life left on {T2}")
    case("case 3: and it was never collected", bool(sib) and sib[0] == 0, str(sib))
    h.wait(TTL + 40)
    p.kill()
    p.wait()
    clean()

    for label, expires in (("case 4", "UTC_TIMESTAMP(6) + INTERVAL 3600 SECOND"),
                           ("case 6", "UTC_TIMESTAMP(6) - INTERVAL 1 SECOND")):
        print(f"== {label}: a claim already taken, lease {'an hour ahead' if '+' in expires else 'lapsed'} ==",
              flush=True)
        seed(T, 200)
        r = drain(T)
        case(f"{label}: bootstrapped", r.returncode == 0, last(r.stderr))
        tok = _rig.fresh_token("l", label[-1] * 4)
        left = plant(T, tok, expires, 1)
        pg(f"INSERT INTO {T} SELECT g, 'n'||g FROM generate_series(201, 250) g")
        r = drain(T)
        case(f"{label}: the drain proceeds", r.returncode == 0, last(r.stderr))
        gone = [n for n in left if n in names(T)]
        case(f"{label}: the peer's lock and marker are gone", not gone, str(gone or "none left"))
        row = lease_life(T, tok)
        case(f"{label}: the peer's row is still there, collected", bool(row) and row[0] == 1, str(row))
        case(f"{label}: and it resumed: MySQL count = Postgres count",
             count(T) == int(pg(f"SELECT count(*) FROM {T}")), f"{count(T)}")
        clean()

    print("== case 5: an evicted member's TRUNCATE never runs ==", flush=True)
    # One window carries T5's TRUNCATE and T's inserts; the group applies T
    # first, and every insert into T sleeps 10 ms at the destination. While
    # the drain is inside T's unit, T5's claim is taken. 0.56.0 then ran
    # T5's TRUNCATE before its fence and emptied the table; the fence
    # refused the rest.
    seed(T, 200)
    pg(f"CREATE TABLE {T5} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {T5} SELECT g, 'v'||g FROM generate_series(1, 200) g")
    r = drain(T, T5)
    case("case 5: group bootstrapped", r.returncode == 0 and count(T5) == 200, last(r.stderr))
    my(f"CREATE TRIGGER fence_my_slow BEFORE INSERT ON `{T}` FOR EACH ROW SET @apitap_slow = SLEEP(0.01)")
    pg(f"TRUNCATE {T5}")
    pg(f"INSERT INTO {T5} SELECT g, 'n'||g FROM generate_series(1, 5000) g")
    pg(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201, 2200) g")
    a = spawn(T, T5)
    procs.append(a)
    inside = _rig.wait_for(lambda: a.poll() is not None or holds(T), 120, step=0.05)
    tok = lock_token(T5)
    if not inside or a.poll() is not None or not tok or count(T5) != 200:
        _rig.rig_fail(f"case 5: the drain was not inside T's unit (rc={a.poll()}, T5 {count(T5)})")
    my(f"UPDATE _apitap_lease SET collected = 1 WHERE dest_key = '{key(T5)}' AND token = '{tok}'")
    _, err = a.communicate(timeout=300)
    case("case 5: the drain stops at T5", a.returncode != 0 and "no longer holds" in err, last(err))
    case("case 5: and T5 was not truncated", count(T5) == 200, f"{count(T5)} rows (200 before the window)")
    my("DROP TRIGGER IF EXISTS fence_my_slow")
    pg_clean(T5)
    clean()

    print("== case 7: a TRUNCATE of 2M rows is O(1) ==", flush=True)
    seed(T7, 2_000_000)
    r = drain(T7)
    case("case 7: bootstrapped 2M rows", r.returncode == 0 and count(T7) == 2_000_000, last(r.stderr))
    pg(f"TRUNCATE {T7}")
    pg(f"INSERT INTO {T7} SELECT g, 'n'||g FROM generate_series(1, 10) g")

    def deleted():
        return int(my("SHOW GLOBAL STATUS LIKE 'Innodb_rows_deleted'").split()[-1])

    d0 = deleted()
    r = drain(T7)
    d1 = deleted()
    case("case 7: applied", r.returncode == 0 and count(T7) == 10, f"{count(T7)} rows: {last(r.stderr)}")
    case("case 7: without deleting row by row", d1 - d0 < 2_000_000, f"Innodb_rows_deleted +{d1 - d0}")
    clean()

    print("== case 8: a SIGKILLed drain clears itself after the TTL ==", flush=True)
    seed(T, 200)
    r = drain(T)
    case("case 8: bootstrapped", r.returncode == 0, last(r.stderr))
    pg(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series(201, 300000) g")
    p = spawn(T)
    procs.append(p)
    tok = _rig.wait_for(lambda: lock_token(T), 60, step=0.05) and lock_token(T)
    os.kill(p.pid, signal.SIGKILL)
    p.wait()
    if not tok:
        _rig.rig_fail("case 8: the drain never announced itself")
    left = [n for n in names(T) if tok in n]
    case("case 8: (rig) the kill left its lock and marker", len(left) == 2, str(left))
    time.sleep(TTL + 3)
    r = drain(T)
    case("case 8: the next run collects it", r.returncode == 0 and "collected" in r.stderr, last(r.stderr))
    case("case 8: lock and marker are gone", not [n for n in names(T) if tok in n], str(names(T)))
    case("case 8: MySQL count = Postgres count", count(T) == int(pg(f"SELECT count(*) FROM {T}")), f"{count(T)}")
finally:
    print("== cleanup ==", flush=True)
    for q in procs:
        if q.poll() is None:
            try:
                _rig.resume(q)
            except Exception:                                 # noqa: BLE001 — a holder is not ours to resume
                pass
            q.kill()
            q.wait()
    clean()

print("\nCDC FENCE MY E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
