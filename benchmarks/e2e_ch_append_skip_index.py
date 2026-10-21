"""A ClickHouse append lands in a table with a skip index and a projection.

An append stages the delta in a table that mirrors the destination and
attaches its parts, and ATTACH PARTITION refuses tables whose skip indexes
or projections differ. Staging was built from the columns and keys alone,
so every append into a table with a bloom_filter index or a projection
streamed the whole delta and then failed (deep review 2, N21).

Proof (pg -> ch append, cursor=id, into a pre-created table with an INDEX
and a PROJECTION): two appends land every source row, and the table keeps
its index and projection.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_append_skip_index"


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


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id bigint PRIMARY KEY, u text NOT NULL); "
           f"INSERT INTO {T} SELECT g, 'u' || (g % 5) FROM generate_series(1, 100) g")
        ch(f"CREATE TABLE {T} (id Int64, u String, INDEX bf u TYPE bloom_filter GRANULARITY 4, "
           f"PROJECTION by_u (SELECT u, count() GROUP BY u)) ENGINE = MergeTree ORDER BY id")
        apitap.transfer(PG, CH, table=T, mode="append", cursor="id")
        pg(f"INSERT INTO {T} SELECT g, 'u' || (g % 5) FROM generate_series(101, 150) g")
        apitap.transfer(PG, CH, table=T, mode="append", cursor="id")
        n = ch(f"SELECT count() FROM {T}")
        assert n == "150", f"{n} rows after two appends, want 150"
        create = ch(f"SHOW CREATE TABLE {T} FORMAT TSVRaw")
        assert "INDEX bf" in create and "PROJECTION by_u" in create, "the table lost its index or projection"
        print(f"  two appends into a table with a skip index and a projection: {n} rows, both kept")
    finally:
        wipe()
    print("✓ a ClickHouse append lands in a table with a skip index and a projection")


if __name__ == "__main__":
    main()
