"""A `log_based` drain and a bulk run must refuse each other.

apitap's concurrency matrix says a CDC drain is exclusive against ANYTHING —
that row was printed in the 0.55.0 release note and on apitap.dev, and it was
false: the drain returned before the bulk dispatcher, so it never minted a run
identity, never wrote an artifact, and never scanned for one. Two drains of a
table were not refused, and neither was a drain running beside a `replace`, in
either direction.

0.56.0 gives the drain the same announcement the bulk lane writes — a tokenized
`__apitap_lock`, minted by the same call, judged by the same rule — and makes it
scan for both kinds. This leg proves the two lanes can now see each other, in
both directions, by planting a live-looking peer artifact and asking for the
refusal.

Planted rather than raced, for the same reason `e2e_concurrent_runs.py` leg 3
plants one: a real overlap needs a run slow enough to still be holding its
artifact when the other starts, and "slow enough" is not a property a test can
assert. The artifact is what the guard reads, so the artifact is what is put in
front of it. What a plant CANNOT prove is that a live run really writes one —
that is the other half, and it is asserted here too, by watching a bootstrap's
own lock appear and then be gone when the run ends.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-ch` on :8124.
"""
import os
import subprocess
import sys
import urllib.parse

import apitap

import _rig

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cdc_guard"

# A run token is `_` + 7 base36 chars of start time + one letter for how the run
# LANDS rows + 7 chars of source hash. The letter is the whole point here: `l` is
# a CDC drain, `r` is a replace, and the matrix refuses that pair in both
# directions. The start time is deliberately ancient — nothing ages out, and a
# peer that looks old must still be refused rather than collected.
CDC_TOKEN = "_0000000l000abcd"
SWAP_TOKEN = "_0000000r000abcd"


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


def artifacts():
    """Everything apitap made beside this table — for the sweep."""
    return sorted(n for n in ch(
        "SELECT name FROM system.tables WHERE database = currentDatabase() "
        f"AND position(name, '__apitap_') > 0 AND startsWith(name, '{T}')").split() if n)


def locks():
    """Just the announcements. `__apitap_cdc_del` is the changelog apply's own
    delete-marker sidecar and lives as long as the destination does — it is not
    a leftover, and an assertion that swept it up would be asserting the wrong
    thing."""
    return [n for n in artifacts() if n.endswith("__apitap_lock")]


def clean():
    pg(f"DROP TABLE IF EXISTS {T} CASCADE")
    pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
    pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM "
       "pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
    for n in artifacts():
        ch(f"DROP TABLE IF EXISTS {n}")
    ch(f"DROP VIEW IF EXISTS {T}__current")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE `_apitap_state` DELETE WHERE dest_table = '{T}' "
           f"SETTINGS mutations_sync = 1")


ok = True


def case(name, passed, detail=""):
    global ok
    ok &= passed
    print(f"   {'✓' if passed else '✗'} {name}: {detail}")


def refusal(fn):
    """Run it, and report the exception TYPE and message rather than a bool."""
    try:
        fn()
        return None
    except Exception as e:                                    # noqa: BLE001
        return f"{type(e).__name__}: {e}"


print("== reset, and a bootstrapped CDC table to work against ==")
clean()
pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
pg(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1,100) g")
apitap.transfer(PG, CH, table=T, mode="log_based")
case("bootstrapped", ch(f"SELECT count() FROM {T}") == "100",
     f"{ch(f'SELECT count() FROM {T}')} rows")
case("and the run took its own announcement back", locks() == [],
     f"locks left behind: {locks() or 'nothing'}")

print("== a live DRAIN's lock refuses a bulk replace ==")
# What a drain in flight looks like to anything else that opens this table.
ch(f"CREATE TABLE `{T}{CDC_TOKEN}__apitap_lock` (t UInt8) ENGINE = Memory")
e = refusal(lambda: apitap.transfer(PG, CH, table=T, dest_table=T, mode="replace"))
case("a replace is refused while a drain holds the table",
     e is not None and "LockedError" in e, (e or "it was ALLOWED")[:150])
case("and the refusal says the peer is draining",
     bool(e) and "draining changes into it" in e, (e or "")[:200])
case("and it names the object to remove",
     bool(e) and f"{T}{CDC_TOKEN}__apitap_lock" in e, (e or "")[-120:])
ch(f"DROP TABLE IF EXISTS `{T}{CDC_TOKEN}__apitap_lock`")

print("== and the other direction: a bulk run's artifact refuses a DRAIN ==")
# A bulk `replace` holds its staging for the whole load; that is what a drain
# opening the same table has to see. Until 0.56.0 the drain scanned for nothing.
ch(f"CREATE TABLE `{T}{SWAP_TOKEN}__apitap_staging` (id Int32, v String) "
   f"ENGINE = MergeTree ORDER BY id")
pg(f"UPDATE {T} SET v = 'changed' WHERE id = 1")
e = refusal(lambda: apitap.transfer(PG, CH, table=T, mode="log_based"))
case("a drain is refused while a replace holds the table",
     e is not None and "LockedError" in e, (e or "it was ALLOWED")[:150])
case("and the refusal says the peer is replacing",
     bool(e) and "replacing the whole table" in e, (e or "")[:200])
ch(f"DROP TABLE IF EXISTS `{T}{SWAP_TOKEN}__apitap_staging`")

print("== a second DRAIN is refused too ==")
ch(f"CREATE TABLE `{T}{CDC_TOKEN}__apitap_lock` (t UInt8) ENGINE = Memory")
e = refusal(lambda: apitap.transfer(PG, CH, table=T, mode="log_based"))
case("two drains of one table cannot both proceed",
     e is not None and "LockedError" in e, (e or "it was ALLOWED")[:150])
ch(f"DROP TABLE IF EXISTS `{T}{CDC_TOKEN}__apitap_lock`")

print("== CONTROL: with nothing in the way, the drain still works ==")
# Without this, "refuse everything" passes every assertion above.
r = apitap.transfer(PG, CH, table=T, mode="log_based")
got = ch(f"SELECT v FROM {T} WHERE id = 1")
case("the drain applies its window once the peers are gone",
     got == "changed", f"id=1 is {got!r} after {r.rows} change(s)")
case("and it left no announcement of its own behind", locks() == [],
     f"locks left behind: {locks() or 'nothing'}")

print("== CONTROL: a DIFFERENT table's lock is none of this table's business ==")
ch(f"CREATE TABLE `{T}_other{CDC_TOKEN}__apitap_lock` (t UInt8) ENGINE = Memory")
pg(f"UPDATE {T} SET v = 'again' WHERE id = 1")
e = refusal(lambda: apitap.transfer(PG, CH, table=T, mode="log_based"))
case("a prefix-sharing sibling does not refuse this table", e is None,
     e or f"drained; id=1 is {ch(f'SELECT v FROM {T} WHERE id = 1')!r}")
ch(f"DROP TABLE IF EXISTS `{T}_other{CDC_TOKEN}__apitap_lock`")

# ---------------------------------------------------------------------------
# Postgres under a search_path that is not `public`.
#
# 0.56.0 resolved an unqualified destination against the live search_path in
# the bulk lane and assumed `public` in the CDC lane. So a drain put its lock in
# `current_schema()`, scanned `public`, and keyed its lease `public.<t>`, while a
# bulk run of the same table looked in the schema the table really lives in.
# Each leg holds a REAL drain mid-flight (stopped with SIGSTOP the moment its
# lock appears), then asks the catalog where the lock is and asks two runs to
# start beside it: a `replace` (the bulk scan) and a second drain (the drain's
# own scan). Both must be refused BY TYPE.
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
SP = "cdc_guard_sp"


def dst(sql):
    return _rig.psql(sql, _rig.PG_DST)


def with_path(search_path):
    return PGD + "?options=" + urllib.parse.quote(f"-c search_path={search_path}")


_SP_SLOTS = set(pg("SELECT slot_name FROM pg_replication_slots").split())


def sp_clean():
    pg(f"DROP TABLE IF EXISTS {SP} CASCADE")
    for s in set(pg("SELECT slot_name FROM pg_replication_slots").split()) - _SP_SLOTS:
        pg(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
           f"WHERE slot_name = '{s}' AND NOT active")
    for p in pg(f"SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap%'").split():
        if p and pg(f"SELECT count(*) FROM pg_publication_tables WHERE pubname = '{p}' "
                    f"AND tablename <> '{SP}'") == "0" and \
                pg(f"SELECT count(*) FROM pg_publication_tables WHERE pubname = '{p}' "
                   f"AND tablename = '{SP}'") != "0":
            pg(f"DROP PUBLICATION IF EXISTS {p}")
    for rel in dst(f"SELECT format('%I.%I', n.nspname, c.relname) FROM pg_class c "
                   f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                   f"WHERE c.relkind = 'r' AND c.relname LIKE '{SP}%'").splitlines():
        if rel:
            dst(f"DROP TABLE IF EXISTS {rel} CASCADE")
    for sch in dst("SELECT nspname FROM pg_namespace WHERE nspname IN ('cdcdest', 'postgres')").split():
        for t in ("_apitap_state", "_apitap_lease"):
            dst(f'DROP TABLE IF EXISTS "{sch}".{t}')
        dst(f'DROP SCHEMA IF EXISTS "{sch}" CASCADE')
    for t, w in (("_apitap_state", f"dest_table LIKE '%{SP}'"),
                 ("_apitap_lease", f"dest_key LIKE '%.{SP}'")):
        if dst(f"SELECT to_regclass('public.{t}') IS NOT NULL") == "t":
            dst(f"DELETE FROM public.{t} WHERE {w}")


def lock_rows():
    return [r for r in dst(
        "SELECT n.nspname || '.' || c.relname FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE c.relname LIKE '{SP}%' AND c.relname LIKE '%\\_\\_apitap\\_lock'").splitlines() if r]


def sp_leg(label, url, before_boot, after_boot):
    """Bootstrap, reshape the schemas, then hold a drain and try two peers."""
    print(f"== search_path {label} ==")
    sp_clean()
    pg(f"CREATE TABLE {SP} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {SP} SELECT g, 'v'||g FROM generate_series(1,100) g")
    before_boot()
    apitap.transfer(PG, url, table=SP, mode="log_based")
    after_boot()
    home = dst(f"SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
               f"WHERE c.relname = '{SP}' AND c.relkind = 'r'")
    for lo in range(101, 300_101, 50_000):
        pg(f"INSERT INTO {SP} SELECT g, 'w'||g FROM generate_series({lo},{lo + 49_999}) g")
    a = subprocess.Popen(
        [sys.executable, "-c",
         f"import apitap; apitap.transfer({PG!r}, {url!r}, table={SP!r}, mode='log_based')"],
        env=dict(os.environ, APITAP_CDC_WINDOW_BYTES="262144"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        if not _rig.wait_for(lambda: a.poll() is not None or bool(lock_rows()), 60, step=0.02):
            a.kill(); a.wait()
            _rig.rig_fail(f"{label}: drain A never announced itself")
        if a.poll() is not None:
            _rig.rig_fail(f"{label}: drain A finished (rc={a.returncode}) before it could be held")
        _rig.pause(a)
        locks_now = lock_rows()
        case(f"{label}: the drain's lock is in the table's own schema ({home})",
             len(locks_now) == 1 and locks_now[0].startswith(home + "."), f"{locks_now}")
        e = refusal(lambda: apitap.transfer(PG, url, table=SP, mode="replace"))
        case(f"{label}: a replace beside the held drain is refused by TYPE",
             e is not None and e.startswith("LockedError"), (e or "it was ALLOWED")[:160])
        e = refusal(lambda: apitap.transfer(PG, url, table=SP, mode="log_based"))
        case(f"{label}: a second drain beside it is refused by TYPE",
             e is not None and e.startswith("LockedError"), (e or "it was ALLOWED")[:160])
    finally:
        if a.poll() is None:
            _rig.resume(a)
            try:
                a.wait(300)
            except subprocess.TimeoutExpired:
                a.kill(); a.wait()
    sp_clean()


try:
    # (a) the table lives in the FIRST search_path entry.
    sp_leg("(a) cdcdest first", with_path("cdcdest,public"),
           lambda: dst("CREATE SCHEMA cdcdest"), lambda: None)
    # (b) the table lives only in the SECOND entry: bootstrapped while cdcdest
    # did not exist (so it landed in public), then cdcdest appears in front.
    sp_leg("(b) table only in public", with_path("cdcdest,public"),
           lambda: None, lambda: dst("CREATE SCHEMA cdcdest"))
    # (c) the default search_path with a "$user" schema present: current_schema()
    # becomes `postgres`, while the table is still found in public.
    sp_leg('(c) "$user" schema present', PGD,
           lambda: None, lambda: dst("CREATE SCHEMA postgres"))
finally:
    sp_clean()

print("== cleanup ==")
clean()

print("\n   ===== CDC GUARD E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
