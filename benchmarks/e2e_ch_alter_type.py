"""A source column that changes number class mid-CDC never lands as reinterpreted bits.

The ClickHouse RowBinary body byte-swaps each fixed-width cell, and the check
that picked it compared WIDTHS only: Int64, UInt64 and Float64 are all eight
bytes. After `ALTER COLUMN v TYPE float8` on a Postgres int8 column, 2.5 was
written into the destination's Int64 column as its IEEE bits,
4612811918334230528 — silently (MASTER review M-28). The body now needs the
same number class; anything else takes the text path, where ClickHouse parses
the value and refuses one that does not fit.

Proof: bootstrap an int8 column, drain an integer, ALTER it to float8 and
drain a fraction — the run is refused, the Int64 column never holds the bits,
and an integral float still lands as its value.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_alter_type"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def drain():
    return apitap.transfer(PG, CH, table=T, mode="log_based")


def wipe():
    # Slot names are hashes: find this table's through its publication.
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v int8)")
    pg(f"INSERT INTO {T} VALUES (1, 10)")
    drain()
    pg(f"INSERT INTO {T} VALUES (2, -20)")
    drain()
    assert ch(f"SELECT toString(groupArray(v)) FROM (SELECT v FROM {T} ORDER BY id)") == "[10,-20]"
    ty = ch(f"SELECT type FROM system.columns WHERE database = 'default' AND table = '{T}' AND name = 'v'")
    print(f"  bootstrap + drain into {ty}: exact")

    pg(f"ALTER TABLE {T} ALTER v TYPE float8")
    pg(f"INSERT INTO {T} VALUES (3, 2.5)")
    try:
        drain()
        raise AssertionError("2.5 into an Int64 column must be refused, not stored as bits")
    except RuntimeError as e:
        print(f"  a fraction after ALTER TYPE float8: refused ({str(e)[:90]}...)")
    assert ch(f"SELECT count() FROM {T} WHERE id = 3") == "0", "nothing may land for the refused window"
    assert ch(f"SELECT count() FROM {T} WHERE abs(v) > 1000000") == "0", "no reinterpreted bits anywhere"

    pg(f"UPDATE {T} SET v = 3 WHERE id = 3")       # the same window now carries an integral value
    drain()
    got = ch(f"SELECT toString(groupArray(v)) FROM (SELECT v FROM {T} ORDER BY id)")
    assert got == "[10,-20,3]", f"an integral float lands as its value, got {got}"
    print("  an integral float8 after the ALTER: lands as its value")
    wipe()
    print("✓ a column that changes number class mid-CDC never lands as reinterpreted bits")


if __name__ == "__main__":
    main()
