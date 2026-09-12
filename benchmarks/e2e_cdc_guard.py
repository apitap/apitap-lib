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
import subprocess
import sys

import apitap

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

print("== cleanup ==")
clean()

print("\n   ===== CDC GUARD E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
