"""A run names the other inactive apitap slots that hold WAL for its tables.

A slot's name follows the run's identity — destination, group membership,
slots=N, and the pre-0.59 spelling — so changing any of them starts a new
slot and leaves the old one retaining WAL until the source's disk fills, with
nothing pointing at it (MASTER review M-23). It cannot be dropped
automatically: another destination of the same table owns a slot that looks
the same from the source. Every run now names each such slot, what it
retains, and the statement that drops it.

Proof: one source table drained into a Postgres and then a ClickHouse
destination (two slots); the ClickHouse run names the Postgres run's slot,
as a `slot.other` gauge and in words with its pg_drop_replication_slot.
"""
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "pg_other_slots"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


def psql(c, db, sql):
    o = sh(["docker", "exec", c, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def ch(sql):
    return sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--password", "bench", "-q", sql]).stdout.strip()


def run(dst):
    code = f"import apitap\napitap.transfer({PG!r}, {dst!r}, table={T!r}, mode='log_based')\n"
    r = sh([sys.executable, "-c", code], timeout=240)
    assert r.returncode == 0, r.stderr[-600:]
    return r.stdout + r.stderr


def wipe():
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    psql("apitap-bench-pg-dst", "apitap_bench_dst", f"DROP TABLE IF EXISTS {T}")
    psql("apitap-bench-pg-dst", "apitap_bench_dst",
         f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
         f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} VALUES (1, 'a')")
    run(PGD)
    pg_slot = [p.removesuffix("_pub") for p in
               src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()]
    assert len(pg_slot) == 1, pg_slot
    out = run(CH)
    named = [l for l in out.splitlines() if pg_slot[0] in l]
    assert any("slot.other" in l for l in named), f"the gauge must name {pg_slot[0]}:\n{out[-1500:]}"
    assert any(f"pg_drop_replication_slot('{pg_slot[0]}')" in l for l in named), \
        f"the note must name the drop statement:\n{out[-1500:]}"
    print(f"  the ClickHouse run names the Postgres run's slot {pg_slot[0]} and how to drop it")
    wipe()
    print("✓ a run names the other inactive apitap slots that hold WAL for its tables")


if __name__ == "__main__":
    main()
