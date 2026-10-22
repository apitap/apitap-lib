"""A changelog group takes a per-table order_by dict that names its meta columns.

order_by={"a": "id, _apitap_lsn, _apitap_seq", ...} is the documented
multi-table form of a changelog's ORDER BY, and it may name the meta columns
the rebuild adds. Since c399c05 the bootstrap's bulk leg re-resolved the dict:
the bulk CREATE got the changelog's clause and failed "Missing columns" on
every run (BigQuery refused the dict outright). The bulk leg now takes only
the member's resolved entry (deep review 3, R3/R14/R23).

Proof (pg -> ch and mysql -> ch, log_based, changelog=True, two tables): the
bootstrap and a drain land, and each changelog table is ordered by its own
entry, meta columns included.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
TABLES = ["clob_dict_a", "clob_dict_b"]


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
            "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe(src_sql, before=None):
    if before is not None:
        for s in set(pg("SELECT slot_name FROM pg_replication_slots WHERE NOT active").split()) - before[0]:
            pg(f"SELECT pg_drop_replication_slot('{s}')")
        for p in set(pg("SELECT pubname FROM pg_publication").split()) - before[1]:
            pg(f"DROP PUBLICATION IF EXISTS {p}")
    for t in TABLES:
        src_sql(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP VIEW IF EXISTS {t}__current")
        ch(f"DROP TABLE IF EXISTS {t}")
        if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{t}' SETTINGS mutations_sync = 1")


def case(name, url, src_sql, ddl, ins, before=None):
    wipe(src_sql, before)
    try:
        for t in TABLES:
            src_sql(ddl.format(t=t))
            src_sql(ins.format(t=t, id=1))
        order_by = {"clob_dict_a": "id, _apitap_lsn, _apitap_seq", "clob_dict_b": "_apitap_lsn, id"}
        kw = dict(tables=TABLES, mode="log_based", changelog=True, order_by=order_by)
        apitap.transfer(url, CH, **kw)
        for t in TABLES:
            src_sql(ins.format(t=t, id=2))
        apitap.transfer(url, CH, **kw)
        for t in TABLES:
            key = ch(f"SELECT sorting_key FROM system.tables WHERE database = 'default' AND name = '{t}'")
            want = order_by[t]
            assert key.replace(" ", "") == want.replace(" ", ""), f"[{name}] {t} ORDER BY ({key}), want ({want})"
            n = ch(f"SELECT count() FROM {t}")
            assert int(n) >= 2, f"[{name}] {t} holds {n} changelog rows, want the bootstrap row and the insert"
        print(f"  [{name}] two changelog tables, each ordered by its own entry")
    finally:
        wipe(src_sql, before)


def main():
    before = (set(pg("SELECT slot_name FROM pg_replication_slots").split()),
              set(pg("SELECT pubname FROM pg_publication").split()))
    case("pg", PG, pg, "CREATE TABLE {t} (id int PRIMARY KEY, v text)", "INSERT INTO {t} VALUES ({id}, 'x')",
         before)
    case("mysql", MY, my, "CREATE TABLE {t} (id int PRIMARY KEY, v varchar(10))", "INSERT INTO {t} VALUES ({id}, 'x')")
    print("✓ a changelog group takes a per-table order_by dict that names its meta columns")


if __name__ == "__main__":
    main()
