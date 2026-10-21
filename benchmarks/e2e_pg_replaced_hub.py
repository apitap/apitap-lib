"""A CDC drain refuses a source table that a replace swapped under it.

Pipeline A replaces a table on a Postgres hub; pipeline B drains that table
by log_based. A's replace swaps a new table in (the DROP emits nothing to
B's slot) and then puts it back into every publication the old one was in,
so B's membership check passed, the drain mapped the new oid onto the old
table by name, and B went on green without the rows A loaded (deep review
2, N7). B now records the source oid at bootstrap, beside its state on the
destination, and refuses a member whose oid moved; a member with no record
is adopted, and then the stream itself is checked: a Relation under the
member's name with another oid is refused too.

Proof (hub pg :5544, B -> pg :5545, A's source pg :5545):
1. B bootstraps ids 1-3; A replaces the hub table with 10-11; the hub gets
   12. B's next drain must refuse, naming the re-bootstrap; after it, B
   equals the hub.
2. The hub gets 13 (old table), A replaces again, and B's record is
   deleted (a pipeline bootstrapped before the record existed). B's drain
   must still refuse: 13's change carries the old oid.
"""
import subprocess

import apitap

HUB = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_replaced_hub"
SEED = "pg_replaced_hub_seed"
OUT = "pg_replaced_hub_cdc"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(c, db, sql):
    o = sh(["docker", "exec", c, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def hub(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def ids(q, t):
    return q(f"SELECT COALESCE(string_agg(id::text, ',' ORDER BY id), '') FROM {t}")


def clear_state():
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_state WHERE dest_table IN ('{OUT}', 'public.{OUT}'); END IF; END $$")


def clear_record():
    dst(f"DO $$ BEGIN IF to_regclass('public._apitap_source_rel') IS NOT NULL THEN "
        f"DELETE FROM public._apitap_source_rel WHERE dest_table = '{OUT}'; END IF; END $$")


def replace_hub():
    apitap.transfer(DST, HUB, table=SEED, dest_table=T, mode="replace")


def drain_refused(case):
    try:
        apitap.transfer(HUB, DST, table=T, dest_table=OUT, mode="log_based")
    except Exception as e:  # noqa: BLE001 — the message is what is checked
        msg = str(e)
        assert "is not the table" in msg and "Re-bootstrap" in msg, f"[{case}] refused for the wrong reason: {msg}"
        print(f"  [{case}] refused: {msg[:110]}...")
        return
    h, d = ids(hub, T), ids(dst, OUT)
    raise AssertionError(f"[{case}] a green drain over a swapped table: destination {d!r}, hub {h!r}")


def main():
    slots_before = set(hub("SELECT slot_name FROM pg_replication_slots").split())
    pubs_before = set(hub("SELECT pubname FROM pg_publication").split())
    hub(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {OUT}; DROP TABLE IF EXISTS {SEED}")
    clear_state()
    clear_record()
    try:
        hub(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} VALUES (1,'a'),(2,'a'),(3,'a')")
        dst(f"CREATE TABLE {SEED} (id int PRIMARY KEY, v text); INSERT INTO {SEED} VALUES (10,'b'),(11,'b')")
        apitap.transfer(HUB, DST, table=T, dest_table=OUT, mode="log_based")
        assert ids(dst, OUT) == "1,2,3", "(rig) bootstrap"

        replace_hub()
        hub(f"INSERT INTO {T} VALUES (12,'c')")
        drain_refused("recorded oid")

        clear_state()
        apitap.transfer(HUB, DST, table=T, dest_table=OUT, mode="log_based")
        assert ids(dst, OUT) == ids(hub, T) == "10,11,12", f"re-bootstrap: {ids(dst, OUT)!r}"
        print("  after the re-bootstrap the destination equals the hub: 10,11,12")

        hub(f"INSERT INTO {T} VALUES (13,'d')")
        replace_hub()
        clear_record()
        drain_refused("no record, old oid in the stream")
    finally:
        for s in set(hub("SELECT slot_name FROM pg_replication_slots WHERE NOT active").split()) - slots_before:
            hub(f"SELECT pg_drop_replication_slot('{s}')")
        for p in set(hub("SELECT pubname FROM pg_publication").split()) - pubs_before:
            hub(f"DROP PUBLICATION IF EXISTS {p}")
        hub(f"DROP TABLE IF EXISTS {T}")
        hub(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
            f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")
        dst(f"DROP TABLE IF EXISTS {OUT}; DROP TABLE IF EXISTS {SEED}")
        clear_state()
        clear_record()
    print("✓ a CDC drain refuses a source table a replace swapped under it")


if __name__ == "__main__":
    main()
