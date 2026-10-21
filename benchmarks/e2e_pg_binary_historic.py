"""A ClickHouse CDC lane survives an enum column altered to text mid-backlog.

The ClickHouse lane asks for binary pgoutput when every tracked column has a
binary renderer, and it decided that from the CURRENT catalog. The stream
replays changes under the HISTORIC one: after `ALTER COLUMN status TYPE text`
the changes made before it still carry the enum's type OID, their cells
arrive in send format, and every run failed "no text renderer for type OID"
at the same watermark until someone set APITAP_PG_BINARY=0 (deep review,
D15). A binary session now stops at such a Relation and the run drains again
on text from where each member stands.

Proof (pg -> ch, log_based): a table with an enum column bootstraps (text
session: the enum has no renderer). Rows 2-3 change while it is an enum, the
column becomes text, row 4 changes after. The next drain must land all of
it: the destination equals the source.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "pg_binary_historic"
E = "pg_binary_historic_mood"


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


def wipe(slots_before, pubs_before):
    for s in set(pg("SELECT slot_name FROM pg_replication_slots WHERE NOT active").split()) - slots_before:
        pg(f"SELECT pg_drop_replication_slot('{s}')")
    for p in set(pg("SELECT pubname FROM pg_publication").split()) - pubs_before:
        pg(f"DROP PUBLICATION IF EXISTS {p}")
    pg(f"DROP TABLE IF EXISTS {T}; DROP TYPE IF EXISTS {E}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    slots_before = set(pg("SELECT slot_name FROM pg_replication_slots").split())
    pubs_before = set(pg("SELECT pubname FROM pg_publication").split())
    wipe(slots_before, pubs_before)
    try:
        pg(f"CREATE TYPE {E} AS ENUM ('sad', 'ok', 'happy'); "
           f"CREATE TABLE {T} (id int PRIMARY KEY, mood {E}, n int); INSERT INTO {T} VALUES (1, 'ok', 1)")
        apitap.transfer(PG, CH, table=T, mode="log_based")
        pg(f"INSERT INTO {T} VALUES (2, 'happy', 2), (3, 'sad', 3); UPDATE {T} SET n = 10 WHERE id = 1")
        pg(f"ALTER TABLE {T} ALTER COLUMN mood TYPE text USING mood::text; INSERT INTO {T} VALUES (4, 'meh', 4)")
        apitap.transfer(PG, CH, table=T, mode="log_based")
        want = pg(f"SELECT string_agg(id || ':' || mood || ':' || n, ',' ORDER BY id) FROM {T}")
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', toString(mood), ':', toString(n))), ',') "
                 f"FROM (SELECT * FROM {T} ORDER BY id)")
        assert got == want, f"destination {got!r}, source {want!r}"
        print(f"  enum -> text mid-backlog: {got}")
    finally:
        wipe(slots_before, pubs_before)
    print("✓ a ClickHouse CDC lane survives an enum column altered to text mid-backlog")


if __name__ == "__main__":
    main()
