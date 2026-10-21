"""A multi-table run into ClickHouse orders each table by its own order_by entry.

`order_by={"a": "v, id", "b": "k"}` on a multi-table run reached only the
changelog rebuild: every bulk table, and every replica CDC bootstrap, fell
back to the cursor or tuple(), and under a Replacing engine a table ordered
by tuple() merges into one row (deep review 2, N12).

Proof (pg -> ch, tables=[a, b], mode='replace'): each destination's sorting
key is its own entry; b's text key has no cursor to fall back to.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
A, B = "ch_obpt_a", "ch_obpt_b"


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


def key(t):
    return ch(f"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = '{t}'")


def wipe():
    for t in (A, B):
        pg(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {A} (id int PRIMARY KEY, v int NOT NULL); INSERT INTO {A} VALUES (1, 1), (2, 2); "
           f"CREATE TABLE {B} (k text PRIMARY KEY, n int); INSERT INTO {B} VALUES ('x', 1), ('y', 2)")
        apitap.transfer(PG, CH, tables=[A, B], mode="replace", order_by={A: "v, id", B: "k"})
        got = (key(A), key(B))
        assert got == ("v, id", "k"), f"sorting keys {got}, want each table's own entry ('v, id', 'k')"
        print(f"  {A} ORDER BY ({got[0]}), {B} ORDER BY ({got[1]})")
    finally:
        wipe()
    print("✓ a multi-table run orders each table by its own order_by entry")


if __name__ == "__main__":
    main()
