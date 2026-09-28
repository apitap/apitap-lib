"""A BigQuery drain's lease and fence, asked of BigQuery.

0.56.0 gave BigQuery drains a lease and nothing that read it: a drain whose
claim a peer had collected carried on committing MERGEs and watermarks beside
the peer. 0.57.0 fences every apply transaction on the run's own table,
`_apitap_fence<token>`: the script's first statement updates its one row
`WHERE NOT claimed`, and a collector marks that row claimed (then deletes the
table) before it proceeds. Scratch is per run: staging is `<T><token>__apitap_cdc`.

  1. a drain killed OUTRIGHT leaves its lock, its marker, a live lease row, its
     fence table and its staging — a real kill, not a plant
  2. the immediate re-run is refused, by type, naming a deadline
  3. after the TTL the next run collects it: lock and marker gone, the dead
     run's lease row reads collected (a collector never deletes it), its fence
     and its staging gone, and the run RESUMES from the watermark
  4. a lock with NO lease row is refused at any age and left in place
  5. a LIVE drain's lease is renewed, never collected; a bulk replace beside it
     is refused; it finishes and leaves no lock, lease, staging or fence
  6. eviction (MariaDB source, so a paused drain holds no walsender): drain A
     is paused after one window landed; past the TTL, drain B collects it and
     finishes; resumed, A exits non-zero ("no longer holds"), the state rows
     for the table are exactly what B left, A's row never reads unclaimed
     again, and the table has one row per key and the source's count

    python benchmarks/e2e_cdc_lease_bq.py

Rig: pg-src :5544, MariaDB :3309, BigQuery dataset `apitap_cdc_e2e` (BQ_SA).
TTL 30. RED: 0.56.0 — no fence table, untokenized staging, and in case 6 A
keeps committing (n2 > n1) and exits 0.
"""
import os
import subprocess
import sys
import time

import apitap

import _rig

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MA = "mysql://root:bench@127.0.0.1:3309/bench"
BQ = _rig.bq_url()
P, D = _rig.BQ_PROJECT, _rig.BQ_DATASET
T = "cdc_lease_bq"
E = "evict_bq"
TTL = 30
os.environ["APITAP_LEASE_TTL_SECS"] = str(TTL)
ok = True
seen_tokens = set()


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ma(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench",
                        "-N", "-D", "bench", "-e", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


bq = _rig.bq


def fq(t):
    return f"`{P}.{D}.{t}`"


def one(sql):
    rows = bq(sql)
    return rows[0][0] if rows and rows[0] else None


def fence(tok):
    return f"_apitap_fence{tok}"


def staging(table, tok):
    return f"{table}{tok}__apitap_cdc"


def lease_row(table, tok):
    """(collected, seconds left) of one run's row, or None when there is none."""
    rows = bq(f"SELECT CAST(collected AS STRING), TIMESTAMP_DIFF(expires_at, CURRENT_TIMESTAMP(), SECOND) "
              f"FROM {fq('_apitap_lease')} WHERE ENDS_WITH(dest_key, '.{table}') AND token = '{tok}'")
    return (rows[0][0] == "true", int(rows[0][1])) if rows else None


def state_rows(table):
    if "_apitap_state" not in _rig.bq_tables():
        return 0
    return int(one(f"SELECT COUNT(*) FROM {fq('_apitap_state')} WHERE dest_table = '{table}'"))


def watermark(table):
    return one(f"SELECT watermark FROM {fq('_apitap_state')} WHERE dest_table = '{table}' "
               "AND source_id NOT LIKE 'server-identity:%' ORDER BY synced_at DESC LIMIT 1")


def dest_counts(table):
    r = bq(f"SELECT COUNT(*), COUNT(DISTINCT id), IFNULL(SUM(id), 0) FROM {fq(table)}")[0]
    return int(r[0]), int(r[1]), int(r[2])


def lock_token(table):
    lk = _rig.locks_bq(table)
    if lk:
        seen_tokens.add(_rig.token_of(lk[0]))
    return _rig.token_of(lk[0]) if lk else None


def refusal(fn):
    try:
        fn()
        return None
    except Exception as e:                                    # noqa: BLE001
        return f"{type(e).__name__}: {e}"


def clean_bq(table):
    names = _rig.bq_tables()
    for n in names:
        if n == table or n.startswith(table + "_") or n == f"{table}__current":
            _rig.bq_delete_table(n)
    for tok in seen_tokens:
        if fence(tok) in names:
            _rig.bq_delete_table(fence(tok))
    for t, w in (("_apitap_state", f"dest_table = '{table}'"),
                 ("_apitap_lease", f"ENDS_WITH(dest_key, '.{table}')")):
        if t in names:
            bq(f"DELETE FROM {fq(t)} WHERE {w}")


def clean_pg():
    pg(f"DROP TABLE IF EXISTS {T} CASCADE")
    pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
    pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
       "WHERE slot_name LIKE 'apitap_%' AND NOT active")


def pg_backlog(lo, n):
    """`n` rows from id `lo`, one source transaction per 500: a window never
    splits a transaction, so one statement would be one window."""
    pg(" ".join(f"BEGIN; INSERT INTO {T} SELECT g, 'w'||g FROM generate_series({a},{min(a + 499, lo + n - 1)}) g; "
                "COMMIT;" for a in range(lo, lo + n, 500)))


def spawn(src, table):
    return subprocess.Popen([sys.executable, "-c",
                             f"import apitap; apitap.transfer({src!r}, {BQ!r}, table={table!r}, mode='log_based')"],
                            env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="262144"),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def run(src, table):
    return subprocess.run([sys.executable, "-c",
                           f"import apitap; apitap.transfer({src!r}, {BQ!r}, table={table!r}, mode='log_based')"],
                          env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="262144"),
                          capture_output=True, text=True, timeout=900)


def drain():
    return apitap.transfer(PG, BQ, table=T, mode="log_based")


a = None
try:
    print("== reset and bootstrap (pg -> BigQuery) ==", flush=True)
    clean_pg()
    clean_bq(T)
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1,200) g")
    drain()
    case("bootstrapped", dest_counts(T)[0] == 200, f"{dest_counts(T)[0]} rows")
    case("a finished run leaves no lock and no live lease",
         _rig.locks_bq(T) == [] and _rig.live_leases_bq(T) == [],
         f"locks {_rig.locks_bq(T) or 'none'}, leases {_rig.live_leases_bq(T) or 'none'}")

    print("== 1. a drain killed OUTRIGHT leaves its lock, lease, fence and staging ==", flush=True)
    pg_backlog(201, 20_000)
    wm_before = watermark(T)
    p = spawn(PG, T)
    if not _rig.wait_for(lambda: lock_token(T) is not None, 60, step=0.5):
        _rig.rig_fail("the drain never announced its lock")
    dead = lock_token(T)
    # Killed once its staging exists: the collector must have one to sweep.
    # (0.56.0's untokenized staging counts as "loading" too, so the leg runs on
    # through its RED instead of stopping at the rig check.)
    def staged():
        return bool({staging(T, dead), f"{T}__apitap_cdc"} & set(_rig.bq_tables()))
    if not _rig.wait_for(lambda: p.poll() is not None or staged(), 120, step=0.5) or p.poll() is not None:
        _rig.rig_fail(f"the drain finished (rc={p.poll()}) before its staging could be seen — raise the backlog")
    p.kill()
    p.wait()
    names = _rig.bq_tables()
    case("the killed drain left a lock", bool(_rig.locks_bq(T)), f"{_rig.locks_bq(T) or 'none'}")
    case("…and its staging marker", bool(_rig.markers_bq(T)), f"{_rig.markers_bq(T) or 'none'}")
    row = lease_row(T, dead)
    case("…and a live lease row", row is not None and not row[0] and row[1] > 0, f"{row}")
    case("…and its fence table", fence(dead) in names, fence(dead))
    case("…and its run-scoped staging", staging(T, dead) in names, staging(T, dead))

    print("== 2. the immediate re-run is refused ==", flush=True)
    e = refusal(drain)
    case("refused by type while the lease is fresh",
         e is not None and "LockedError" in e, (e or "it was ALLOWED")[:120])
    case("and the refusal names a deadline instead of a chore",
         bool(e) and "nothing for you to do" in e, (e or "")[-160:])

    print(f"== 3. after the {TTL}s TTL it collects itself ==", flush=True)
    time.sleep(TTL + 3)
    e = refusal(drain)
    case("the next run proceeds", e is None, e or "collected and drained")
    names = _rig.bq_tables()
    case("the dead run's lock is gone", _rig.locks_bq(T) == [], f"{_rig.locks_bq(T) or 'none'}")
    case("and its marker — one claim collects both", _rig.markers_bq(T) == [], f"{_rig.markers_bq(T) or 'none'}")
    row = lease_row(T, dead)
    case("its lease row reads collected — a collector never deletes it", row is not None and row[0], f"{row}")
    case("its fence table is gone", fence(dead) not in names, fence(dead))
    case("its staging is swept", staging(T, dead) not in names, staging(T, dead))
    wm_after = watermark(T)
    case("it RESUMED — the watermark moved on", wm_after and wm_after != wm_before, f"{wm_before} -> {wm_after}")
    src = pg(f"SELECT count(*)||'|'||count(DISTINCT id)||'|'||coalesce(sum(id::bigint),0) FROM {T}")
    dst = "|".join(map(str, dest_counts(T)))
    case("and the destination caught up exactly, one row per key", src == dst, f"src {src} vs dst {dst}")

    print("== 4. a lock with NO lease is never collected, at any age ==", flush=True)
    stale = f"{T}_0000000l000abcd__apitap_lock"
    _rig.bq_create_table(stale)
    e = refusal(drain)
    case("refused", e is not None and "LockedError" in e, (e or "it was ALLOWED")[:120])
    case("and the refusal says nothing collects it", bool(e) and "nothing collects it on its own" in e,
         (e or "")[-150:])
    case("and it is still there", stale in _rig.locks_bq(T), f"{_rig.locks_bq(T)}")
    _rig.bq_delete_table(stale)

    print("== 5. a LIVE drain's lease is never collectable ==", flush=True)
    pg_backlog(20_201, 20_000)
    p = spawn(PG, T)
    live = _rig.wait_for(lambda: p.poll() is not None or lock_token(T) is not None, 60, step=0.5) \
        and p.poll() is None
    if not live:
        _rig.rig_fail(f"a live drain was not observable (rc={p.poll()})")
    ltok = lock_token(T)
    e = refusal(lambda: apitap.transfer(PG, BQ, table=T, dest_table=T, mode="replace"))
    row = lease_row(T, ltok)
    case("a replace is refused beside a LIVE drain", e is not None and "LockedError" in e,
         (e or "it was ALLOWED")[:120])
    case("and the refusal says to wait, not to remove", bool(e) and "nothing for you to do" in e, (e or "")[-160:])
    case("the live drain's lease is uncollected and ahead of now", row is not None and not row[0] and row[1] > 0,
         f"{row}")
    _, err = p.communicate(timeout=900)
    names = _rig.bq_tables()
    case("it finished normally", p.returncode == 0, f"rc={p.returncode} {err.strip()[-160:]}")
    case("and left no lock, live lease, staging or fence",
         _rig.locks_bq(T) == [] and _rig.live_leases_bq(T) == []
         and staging(T, ltok) not in names and fence(ltok) not in names,
         f"locks {_rig.locks_bq(T) or 'none'}, leases {_rig.live_leases_bq(T) or 'none'}")

    print("== 6. an evicted drain writes nothing more (MariaDB -> BigQuery) ==", flush=True)
    ma(f"DROP TABLE IF EXISTS {E}")
    clean_bq(E)
    ma(f"CREATE TABLE {E} (id INT PRIMARY KEY, v VARCHAR(120))")
    ma(f"INSERT INTO {E} SELECT seq, CONCAT('v', seq) FROM seq_1_to_200")
    r = run(MA, E)
    case("bootstrapped", r.returncode == 0 and dest_counts(E)[0] == 200, r.stderr.strip()[-200:])
    # Many small source transactions: a window never splits one, and A must
    # have windows left to try after its first one lands.
    ma("; ".join(f"INSERT INTO {E} SELECT seq, REPEAT('w', 100) FROM seq_{lo}_to_{lo + 1999}"
                 for lo in range(201, 60_201, 2000)))
    n0 = state_rows(E)
    a = spawn(MA, E)
    if not _rig.wait_for(lambda: a.poll() is not None or state_rows(E) > n0, 300, step=1):
        _rig.rig_fail("drain A never landed a window")
    if a.poll() is not None:
        _rig.rig_fail(f"drain A finished (rc={a.returncode}) before it could be paused — raise the backlog")
    _rig.pause(a)
    atok = lock_token(E)
    if not atok:
        _rig.rig_fail("drain A has no lock to be collected")
    print(f"      A = {atok}, paused after {state_rows(E) - n0} window(s); sleeping {TTL + 3}s", flush=True)
    time.sleep(TTL + 3)
    b = run(MA, E)
    case("B collects A and finishes", b.returncode == 0 and "collected" in b.stderr,
         (b.stderr.strip().splitlines() or ["(nothing)"])[-1][:160])
    n1 = state_rows(E)
    row = lease_row(E, atok)
    case("A's row reads collected", row is not None and row[0], f"{row}")
    case("A's fence table is gone", fence(atok) not in _rig.bq_tables(), fence(atok))
    _rig.resume(a)
    # Until A exits and a little after: its row never reads unclaimed again.
    # (A's own release deletes its row on the way out; gone is not unclaimed.)
    live_again, end = [], time.monotonic() + 600
    while time.monotonic() < end:
        rc = a.poll()
        row = lease_row(E, atok)
        if row is not None and not row[0]:
            live_again.append(row)
        if rc is not None:
            break
        time.sleep(1)
    try:
        _, a_err = a.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        a.kill()
        _, a_err = a.communicate()
    case("A's row never reads unclaimed after the claim", not live_again, f"{live_again or 'never'}")
    case("A exits non-zero", a.returncode not in (0, None), f"rc={a.returncode}")
    case("and says it no longer holds the table", "no longer holds" in a_err,
         (a_err.strip().splitlines() or [""])[-1][:160])
    a = None
    n2 = state_rows(E)
    case("A committed nothing after the claim: the state rows are B's", n2 == n1, f"n1 {n1}, n2 {n2}")
    c, u, _ = dest_counts(E)
    total = int(ma(f"SELECT COUNT(*) FROM {E}"))
    case("one row per key, and the source's count", c == u == total, f"dest {c}/{u} of {total}")
    case("A's staging is not left behind", staging(E, atok) not in _rig.bq_tables(), staging(E, atok))
finally:
    print("== cleanup ==", flush=True)
    if a is not None and a.poll() is None:
        _rig.resume(a)
        a.kill()
        a.wait()
    for fn in (clean_pg, lambda: clean_bq(T), lambda: ma(f"DROP TABLE IF EXISTS {E}"), lambda: clean_bq(E)):
        try:
            fn()
        except Exception as ex:                               # noqa: BLE001
            print(f"   cleanup: {ex}", flush=True)

print("\nCDC LEASE BQ E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
