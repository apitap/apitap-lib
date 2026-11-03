"""A drain refuses a collapsing engine over ORDER BY tuple().

A 0.62.x bootstrap could leave a Replacing-family ClickHouse destination
ORDER BY tuple() — a composite or varchar key with no order_by fell back to
the empty key. D16's fix shapes only NEW bootstraps, and a lane with state
never re-bootstraps, so the upgraded lane drains straight into the damage:
every row of the table shares the one (empty) sorting key, and the engine's
own merges fold them into one row — even the rebuild into this shape folds
the table on the spot, which is the damage demonstrated live below — on
green runs, with no note at all for plain ReplacingMergeTree or
ReplacingMergeTree(ver) (deep review 3, D16 finding 3 / R2).

The drain now refuses on the first apply, before any window, naming the
only repair. Harmless lookalikes stay allowed: plain MergeTree over
tuple() collapses nothing, and a keyed collapsing engine is the D9 note's
business.

Proof (pg -> ch): bootstrap two rows, rebuild the destination into the
0.62.x shape and watch it lose a row to its own part write (the damage,
rig-proven), change the source, and the drain REFUSES with the remedy, its
window still pending. Two controls make the refusal mean the right thing:
MergeTree over tuple() drains and lands the pending change, and
ReplacingMergeTree ORDER BY id applies an update. Then the refusal's own
remedy is executed — clear the state row, re-bootstrap — and the
destination is exact again, the folded row restored.
"""
import os
import subprocess
import sys

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
T = "ch_legacy_tuple_engine"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def drain():
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')\n"
            "print('ROWS', r.rows, flush=True)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def rebuild(engine_and_order):
    """Swap the destination into a 0.62.x-era shape, state row untouched."""
    ch(f"CREATE TABLE {T}_leg (id Int32, v Nullable(Int32)) ENGINE = {engine_and_order} "
       f"AS SELECT id, v FROM {T}")
    ch(f"EXCHANGE TABLES {T} AND {T}_leg")
    ch(f"DROP TABLE {T}_leg")


def rows():
    return ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', "
              f"toString(v))), ',') FROM (SELECT id, v FROM {T} ORDER BY id)")


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}_leg")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v int)")
        pg(f"INSERT INTO {T} VALUES (1, 10), (2, 20)")
        r = drain()
        case("the bootstrap lands both rows", r.returncode == 0 and rows() == "1:10,2:20",
             (r.stdout + r.stderr).strip()[-300:])

        # The 0.62.x shape: a collapsing engine over the empty sorting key.
        # The rebuild itself is the damage, live: the part write sorts by the
        # empty key and folds both rows into one.
        rebuild("ReplacingMergeTree() ORDER BY tuple()")
        case("the 0.62.x shape folds the table by itself (the R2 damage, rig-proven)",
             rows() == "1:10", rows())
        pg(f"INSERT INTO {T} VALUES (3, 30)")
        r = drain()
        err = (r.stdout + r.stderr)
        case("the drain REFUSES the collapsing engine over tuple()",
             r.returncode != 0 and "ORDER BY tuple()" in err and "Re-bootstrap" in err,
             err.strip().splitlines()[-1][:160] if err.strip() else "")
        case("and nothing landed (the window stays pending)", rows() == "1:10", rows())

        # Control 1: the same empty key on a plain MergeTree collapses
        # nothing — allowed, and the pending change lands through it.
        rebuild("MergeTree() ORDER BY tuple()")
        r = drain()
        case("control: MergeTree over tuple() drains, and lands the pending row",
             r.returncode == 0 and rows() == "1:10,3:30",
             (r.stdout + r.stderr).strip()[-200:] if r.returncode else "")

        # Control 2: a collapsing engine WITH its key — the D9 note's
        # business, not a refusal.
        pg(f"UPDATE {T} SET v = 11 WHERE id = 1")
        rebuild("ReplacingMergeTree() ORDER BY id")
        r = drain()
        case("control: ReplacingMergeTree ORDER BY id drains and applies",
             r.returncode == 0 and rows() == "1:11,3:30",
             (r.stdout + r.stderr).strip()[-200:] if r.returncode else "")

        # The refusal's remedy, executed: clear the state row, re-bootstrap
        # with the engine the table was created with, and the folded row
        # comes back — the destination exact again.
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")
        code = ("import apitap\n"
                f"apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based', "
                "engine='ReplacingMergeTree')\n")
        r = sh([sys.executable, "-c", code], timeout=300)
        want = pg(f"SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM {T}")
        case("the remedy repairs it: a re-bootstrap restores the folded row",
             r.returncode == 0 and rows() == want,
             f"{rows()} vs {want}; rc={r.returncode} "
             + (r.stderr.strip().splitlines()[-1][:120] if r.returncode else ""))
    finally:
        wipe()
    print("\nCH LEGACY TUPLE ENGINE E2E: " + ("PASSED" if ok else "FAILED"))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
