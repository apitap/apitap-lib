"""A STORED generated column never lands NULL or stale through CDC.

pgoutput on PG 12-17 never sends a generated column, and on PG 18 only when
the publication says publish_generated_columns = stored. The bootstrap copied
the column like any other; afterwards every CDC insert landed it NULL (or 0)
and every update left it stale, on ClickHouse, MySQL, Iceberg and BigQuery,
on green runs. Only a Postgres destination recomputes it (deep review 3,
R21). Now a non-Postgres log_based run refuses such a table on PG <= 17,
naming the column, and on PG 18 the publication carries the column.

Proof (-> ClickHouse): orders(id, qty, price, total GENERATED ALWAYS AS
(qty * price) STORED). On PG 16 the first run must refuse naming `total`.
On PG 18, after a bootstrap, an insert and an update, the destination's
total must equal the source's.
"""
import subprocess

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
SOURCES = [
    ("apitap-bench-pg-src", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src", True),
    ("apitap-bench-pg18", "postgres://postgres:bench@127.0.0.1:5547/apitap_bench_src", False),
]
T = "pg_generated_cdc"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(c, sql):
    o = sh(["docker", "exec", c, "psql", "-U", "postgres", "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1",
            "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe(c, before):
    for s in set(psql(c, "SELECT slot_name FROM pg_replication_slots WHERE NOT active").split()) - before[0]:
        psql(c, f"SELECT pg_drop_replication_slot('{s}')")
    for p in set(psql(c, "SELECT pubname FROM pg_publication").split()) - before[1]:
        psql(c, f"DROP PUBLICATION IF EXISTS {p}")
    psql(c, f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def case(c, url, refuse):
    before = (set(psql(c, "SELECT slot_name FROM pg_replication_slots").split()),
              set(psql(c, "SELECT pubname FROM pg_publication").split()))
    ver = psql(c, "SHOW server_version")
    wipe(c, before)
    try:
        psql(c, f"CREATE TABLE {T} (id int PRIMARY KEY, qty int, price int, "
                f"total int GENERATED ALWAYS AS (qty * price) STORED); "
                f"INSERT INTO {T} (id, qty, price) VALUES (1, 1, 5)")
        try:
            apitap.transfer(url, CH, table=T, mode="log_based")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            assert refuse, f"PG {ver}: refused although the WAL can carry the column: {e}"
            msg = str(e)
            assert "generated" in msg and "total" in msg, f"PG {ver}: refused for the wrong reason: {msg}"
            print(f"  PG {ver}: refused: {msg[:120]}...")
            return
        psql(c, f"INSERT INTO {T} (id, qty, price) VALUES (2, 2, 5); UPDATE {T} SET qty = 3 WHERE id = 1")
        apitap.transfer(url, CH, table=T, mode="log_based")
        want = psql(c, f"SELECT string_agg(id || ':' || total, ',' ORDER BY id) FROM {T}")
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', ifNull(toString(total), 'NULL'))), ',') "
                 f"FROM (SELECT id, total FROM {T} ORDER BY id)")
        assert got == want, f"PG {ver}: destination total {got!r}, source {want!r}"
        assert not refuse, f"PG {ver}: the run went through, so it must also have been exact ({got})"
        print(f"  PG {ver}: the generated column follows the source: {got}")
    finally:
        wipe(c, before)


def main():
    for c, url, refuse in SOURCES:
        case(c, url, refuse)
    print("✓ a STORED generated column never lands NULL or stale through CDC")


if __name__ == "__main__":
    main()
