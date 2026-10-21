"""A replace into a MySQL table refuses to drop what only that table has.

A replace rebuilds the destination from the source's columns and key, swaps
it in and drops the old table, and MySQL drops a table's triggers, indexes
and foreign keys with it. A reporting table's secondary index, audit trigger
and column defaults were gone after the first green run (deep review 2,
N24). The ClickHouse sink refuses the same loss; so does this one now.

Proof (pg -> mysql replace into a pre-created table with a secondary
index, a trigger and a DEFAULT): the run must refuse, naming them, and the
table must still have them. A table apitap created replaces as before.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_lossy_replace"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def dst(sql):
    o = sh(["docker", "exec", "apitap-bench-my-dst", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def facts():
    idx = dst(f"SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'bench' "
              f"AND TABLE_NAME = '{T}' AND INDEX_NAME = 'by_customer'")
    trg = dst(f"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_TABLE = '{T}'")
    return idx, trg


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}; DROP TABLE IF EXISTS {T}_audit")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, customer_id int, status text); "
           f"INSERT INTO {T} VALUES (1, 7, 'new'), (2, 8, 'paid')")
        dst(f"CREATE TABLE {T} (id int PRIMARY KEY, customer_id int, status varchar(20) DEFAULT 'new', "
            f"KEY by_customer (customer_id)); CREATE TABLE {T}_audit (id int); "
            f"CREATE TRIGGER {T}_ai AFTER INSERT ON {T} FOR EACH ROW INSERT INTO {T}_audit VALUES (NEW.id)")
        assert facts() == ("1", "1"), f"(rig) the table starts with its index and trigger: {facts()}"
        try:
            apitap.transfer(PG, MY, table=T, mode="replace")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "by_customer" in msg and f"{T}_ai" in msg, f"refused for the wrong reason: {msg}"
            assert facts() == ("1", "1"), f"a refused replace changed the table: {facts()}"
            print(f"  a lossy replace is refused, the table untouched: {msg[:110]}...")
        else:
            raise AssertionError(f"a green replace dropped what the table had: index, trigger = {facts()}")

        # A table apitap created itself replaces as before.
        dst(f"DROP TABLE {T}")
        for _ in range(2):
            apitap.transfer(PG, MY, table=T, mode="replace")
        assert dst(f"SELECT COUNT(*) FROM {T}") == "2", "an apitap-created table must replace"
        print("  an apitap-created table replaces twice")
    finally:
        wipe()
    print("✓ a replace into a MySQL table refuses to drop what only that table has")


if __name__ == "__main__":
    main()
