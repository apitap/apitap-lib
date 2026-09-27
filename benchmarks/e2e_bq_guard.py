"""A BigQuery bulk run meets a drain's announcement.

BigQuery is the one guarded destination whose names are decorated: a bulk run's
workers each write `<staging>_<i>`. 0.57.0 moved its guard onto the shared loop
(`guard.rs` over `BqGuard`), which lists every raw id beside the canonical name
a run mints, classifies the canonical one and deletes the raw one. This leg asks
the dataset listing (`tables.list`), never an exit code, what each case did.

  1  a drain's lock with no lease behind it — an apitap older than the lease,
     or an operator's plant — refuses a `replace` BY TYPE and is left in place
     (`bq.bulk-meets-cdc-lock`); removing it lets the control run through
  2  a DEAD drain — its lock, its staging marker and a lapsed lease — is
     collected by the bulk run: both markers go, the lease row stays
     `collected`, the run lands its rows. 0.56.0 refused here: it could
     collect a drain's lock but not its staging marker, which a drain did not
     write until 0.57.0 (a 0.55.1 reader needs it to see the drain at all).

Rig: `apitap-bench-pg-src` on :5544 and the gate's BigQuery dataset (BQ_SA).
"""
import sys

import apitap

import _rig

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = _rig.bq_url()
T = "bq_guard_demo"
KEY = f"{_rig.BQ_DATASET}.{T}"
LEASE = f"`{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}._apitap_lease`"
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}")


def refusal(fn):
    try:
        fn()
        return None
    except Exception as e:                                    # noqa: BLE001
        return f"{type(e).__name__}: {e}"


def mine():
    return sorted(n for n in _rig.bq_tables() if n.startswith(T))


def leases():
    if "_apitap_lease" not in _rig.bq_tables():
        return []
    return _rig.bq(f"SELECT token, collected FROM {LEASE} WHERE dest_key = '{KEY}'")


def clean():
    for n in mine():
        _rig.bq_delete_table(n)
    if "_apitap_lease" in _rig.bq_tables():
        _rig.bq(f"DELETE FROM {LEASE} WHERE dest_key = '{KEY}'")


def replace():
    return apitap.transfer(PG, BQ, table=T, mode="replace")


print("== reset ==")
_rig.psql(f"DROP TABLE IF EXISTS {T}", _rig.PG_SRC)
_rig.psql(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)", _rig.PG_SRC)
_rig.psql(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1,100) g", _rig.PG_SRC)
clean()
try:
    print("== 1. a drain's lock with no lease refuses a replace, and stays ==")
    lock = f"{T}_0000000l000abcd__apitap_lock"
    _rig.bq_create_table(lock)
    e = refusal(replace)
    case("the replace is refused BY TYPE", bool(e) and e.startswith("LockedError"),
         (e or "it was ALLOWED")[:170])
    case("and the lock is still listed — nothing collects what has no lease",
         lock in _rig.bq_tables(), f"{mine()}")
    _rig.bq_delete_table(lock)
    e = refusal(replace)
    case("CONTROL: with the lock gone, the replace runs", e is None, e or "ran")
    n = _rig.bq(f"SELECT COUNT(*) FROM `{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}.{T}`")
    case("and landed every row", n and n[0][0] == "100", f"{n}")

    print("== 2. a dead drain's lock AND marker are collected by a bulk run ==")
    tok = "_0000001l000dead"
    dead_lock, dead_marker = f"{T}{tok}__apitap_lock", f"{T}{tok}__apitap_staging"
    _rig.bq_create_table(dead_lock)
    _rig.bq_create_table(dead_marker)
    if "_apitap_lease" not in _rig.bq_tables():
        _rig.bq_create_table("_apitap_lease", [
            {"name": "dest_key", "type": "STRING"}, {"name": "token", "type": "STRING"},
            {"name": "expires_at", "type": "TIMESTAMP"}, {"name": "collected", "type": "BOOL"}])
    _rig.bq(f"INSERT INTO {LEASE} (dest_key, token, expires_at, collected) VALUES "
            f"('{KEY}', '{tok}', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), FALSE)")
    case("(rig) the dead drain is on the server", {dead_lock, dead_marker} <= set(mine()),
         f"{mine()}")
    e = refusal(replace)
    case("the replace collects the dead drain and runs", e is None, e or "ran")
    left = mine()
    case("its lock and its marker are both gone",
         dead_lock not in left and dead_marker not in left, f"{left}")
    rows = [r for r in leases() if r[0] == tok]
    case("and the victim's lease row stays, collected — never deleted by a collector",
         rows == [[tok, "true"]], f"{rows}")
finally:
    print("== cleanup ==")
    clean()
    _rig.psql(f"DROP TABLE IF EXISTS {T}", _rig.PG_SRC)

print("\nBQ GUARD E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
