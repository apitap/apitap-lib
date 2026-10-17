"""A ClickHouse replace without engine= never silently strips the table it replaces.

A replace without `engine=` builds a plain MergeTree and EXCHANGEs it in.
Over an existing table with PARTITION BY, a TTL, column codecs or another
engine, that threw all of them away while reporting success (MASTER review
M-29). It is refused before anything is copied, naming what would be lost.

Proof: a destination with PARTITION BY + TTL + a codec, and one with
ReplacingMergeTree(v) — each replace is refused naming the loss, the table
keeps its definition and rows, nothing is left behind; with engine= the
replace goes through; a plain MergeTree destination still replaces.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_lossy"


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


def full():
    return ch(f"SELECT engine_full FROM system.tables WHERE database = 'default' AND name = '{T}'")


def others():
    return ch(f"SELECT count() FROM system.tables WHERE database = 'default' AND name LIKE '%{T}%' AND name != '{T}'")


def refused(ddl, words):
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(ddl)
    ch(f"INSERT INTO {T} VALUES (1, 'kept', now()), (2, 'kept', now())")
    was = full()
    try:
        apitap.transfer(PG, CH, table=T, mode="replace")
        raise AssertionError(f"replace over {was} must be refused")
    except ValueError as e:
        for w in words:
            assert w in str(e), f"the refusal must name {w!r}: {e}"
    assert full() == was, f"the table definition must be untouched, now {full()}"
    assert ch(f"SELECT count() FROM {T}") == "2", "its rows must be untouched"
    assert others() == "0", "nothing may be left behind"


def main():
    pg(f"DROP TABLE IF EXISTS {T}")
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text NOT NULL, ts timestamptz NOT NULL); "
       f"INSERT INTO {T} SELECT g, 'new', now() FROM generate_series(1, 50) g")
    refused(f"CREATE TABLE {T} (id Int32, v String CODEC(ZSTD(3)), ts DateTime64(6, 'UTC')) "
            f"ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY id TTL toDateTime(ts) + INTERVAL 1 YEAR",
            ["PARTITION BY", "TTL", "codecs"])
    print("  PARTITION BY + TTL + codec: refused naming all three, table intact")
    refused(f"CREATE TABLE {T} (id Int32, v String, ts DateTime64(6, 'UTC')) "
            f"ENGINE = ReplacingMergeTree(ts) ORDER BY id", ["ReplacingMergeTree"])
    print("  ReplacingMergeTree(ts): refused naming the engine, table intact")

    apitap.transfer(PG, CH, table=T, mode="replace", engine="ReplacingMergeTree(ts)")
    assert ch(f"SELECT count() FROM {T} FINAL") == "50" and full().startswith("ReplacingMergeTree(ts)")
    print("  with engine= said out loud: replaced")

    ch(f"DROP TABLE IF EXISTS {T}")
    apitap.transfer(PG, CH, table=T, mode="replace")
    pg(f"INSERT INTO {T} VALUES (51, 'new', now())")
    apitap.transfer(PG, CH, table=T, mode="replace")
    assert ch(f"SELECT count() FROM {T}") == "51", "a plain MergeTree destination still replaces"
    print("  plain MergeTree: replaces as before")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    print("✓ a ClickHouse replace without engine= never silently strips the table it replaces")


if __name__ == "__main__":
    main()
