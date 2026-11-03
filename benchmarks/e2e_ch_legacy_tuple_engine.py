"""A drain refuses a collapsing engine over ORDER BY tuple().

A 0.62.x bootstrap could leave a Replacing-family ClickHouse destination
ORDER BY tuple() — a composite or varchar key with no order_by fell back to
the empty key. D16's fix shapes only NEW bootstraps, and a lane with state
never re-bootstraps, so the upgraded lane drains straight into the damage:
every row of the table shares the one (empty) sorting key, and the engine's
own background merges fold them into one row per partition — silent
destination loss on green runs, with no note at all for plain
ReplacingMergeTree or ReplacingMergeTree(ver) (deep review 3, D16 finding 3
/ R2).

The drain now refuses on the first apply, naming the only repair —
re-bootstrap, which rebuilds the table ordered by its key.

Proof (pg -> ch): bootstrap, rebuild the destination into the 0.62.x shape
(plain ReplacingMergeTree over tuple()), change the source, and the drain
must REFUSE with that remedy, its window still pending behind it. Two
controls make the refusal mean the right thing: MergeTree over tuple()
(same empty key, nothing collapses) drains fine, and ReplacingMergeTree
ORDER BY id (a collapsing engine WITH a key) drains fine too — and the
control run lands the change the refusal left pending.
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
        case("the bootstrap lands both rows", r.returncode == 0
             and ch(f"SELECT count() FROM {T}") == "2",
             (r.stdout + r.stderr).strip()[-300:])

        # The 0.62.x shape: a collapsing engine over the empty sorting key.
        rebuild("ReplacingMergeTree() ORDER BY tuple()")
        engine = ch(f"SELECT engine_full FROM system.tables WHERE database = 'default' AND name = '{T}'")
        case("(rig) the destination is ReplacingMergeTree over an empty sorting key",
             "ReplacingMergeTree" in engine and ch(
                 f"SELECT length(sorting_key) FROM system.tables "
                 f"WHERE database = 'default' AND name = '{T}'") == "0", engine)

        pg(f"INSERT INTO {T} VALUES (3, 30)")
        r = drain()
        err = (r.stdout + r.stderr)
        case("the drain REFUSES the collapsing engine over tuple()",
             r.returncode != 0 and "ORDER BY tuple()" in err and "Re-bootstrap" in err,
             err.strip().splitlines()[-1][:160] if err.strip() else "")
        case("and nothing landed (the window stays pending)",
             ch(f"SELECT count() FROM {T}") == "2")

        # Control 1: the same empty key on a plain MergeTree collapses
        # nothing — allowed, and the pending change lands through it.
        rebuild("MergeTree() ORDER BY tuple()")
        r = drain()
        case("control: MergeTree over tuple() drains, and lands the pending row",
             r.returncode == 0 and ch(f"SELECT count() FROM {T}") == "3",
             (r.stdout + r.stderr).strip()[-200:] if r.returncode else "")

        # Control 2: a collapsing engine WITH its key — the D9 note's
        # business, not a refusal.
        pg(f"UPDATE {T} SET v = 11 WHERE id = 1")
        rebuild("ReplacingMergeTree() ORDER BY id")
        r = drain()
        case("control: ReplacingMergeTree ORDER BY id drains and applies",
             r.returncode == 0 and ch(f"SELECT v FROM {T} WHERE id = 1") == "11",
             (r.stdout + r.stderr).strip()[-200:] if r.returncode else "")
        want = pg(f"SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM {T}")
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', "
                 f"toString(v))), ',') FROM (SELECT id, v FROM {T} ORDER BY id)")
        case("the destination agrees with the source", got == want, f"{want} vs {got}")
    finally:
        wipe()
    print("\nCH LEGACY TUPLE ENGINE E2E: " + ("PASSED" if ok else "FAILED"))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
