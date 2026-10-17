"""Postgres CDC state lives in the destination's schema, where a replace clears it.

The CDC lane wrote its watermark to an UNQUALIFIED `_apitap_state` (whatever
search_path picked, usually public), keyed by the bare table name; the bulk
lane keeps state in the destination schema's own `_apitap_state`. For a
destination outside public that meant two things (MASTER review M-65):
`sales.orders` and `public.orders` fed from one source shared one row, and a
bulk replace of `sales.orders` cleared `sales._apitap_state` while the drain's
row in `public._apitap_state` survived — the next drain resumed an old
position on a replaced table.

Proof: drain a source table into `sales.<t>`; the state row is in
`sales._apitap_state` and not in public. Replace `sales.<t>` with the bulk
lane: no state row for it survives anywhere. The next drain bootstraps and
the destination equals the source.
"""
import subprocess

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_state_schema"
D = f"sales.{T}"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def state_rows():
    out = []
    for s in ("public", "sales"):
        if dst(f"SELECT to_regclass('{s}._apitap_state') IS NOT NULL") == "t":
            for r in dst(f"SELECT dest_table || '/' || mode FROM {s}._apitap_state "
                         f"WHERE dest_table IN ('{T}', '{D}')").split():
                out.append(f"{s}:{r}")
    return out


def wipe():
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    dst("CREATE SCHEMA IF NOT EXISTS sales")
    dst(f"DROP TABLE IF EXISTS {D}")
    for s in ("public", "sales"):
        if dst(f"SELECT to_regclass('{s}._apitap_state') IS NOT NULL") == "t":
            dst(f"DELETE FROM {s}._apitap_state WHERE dest_table IN ('{T}', '{D}')")


def agree(stage):
    s = src(f"SELECT coalesce(string_agg(id || ':' || v, ',' ORDER BY id), '') FROM {T}")
    d = dst(f"SELECT coalesce(string_agg(id || ':' || v, ',' ORDER BY id), '') FROM {D}")
    assert s == d, f"{stage}: source {s!r} vs destination {d!r}"
    print(f"  {stage}: equal")


def main():
    wipe()
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} VALUES (1, 'a'), (2, 'b')")
    apitap.transfer(PG, PGD, table=T, dest_table=D, mode="log_based")
    src(f"INSERT INTO {T} VALUES (3, 'c')")
    apitap.transfer(PG, PGD, table=T, dest_table=D, mode="log_based")
    agree("drain into sales")
    rows = state_rows()
    assert any(r.startswith("sales:") for r in rows) and not any(r.startswith("public:") for r in rows), \
        f"the drain's state must live in sales._apitap_state only, found {rows}"
    print(f"  the drain's state row: {rows}")

    apitap.transfer(PG, PGD, table=T, dest_table=D, mode="replace")
    rows = state_rows()
    assert not any(r.endswith("/log_based") for r in rows), \
        f"a bulk replace must leave no CDC watermark behind, found {rows}"
    print("  after a bulk replace: no CDC watermark survives")

    src(f"UPDATE {T} SET v = 'B' WHERE id = 2; INSERT INTO {T} VALUES (4, 'd')")
    apitap.transfer(PG, PGD, table=T, dest_table=D, mode="log_based")
    agree("the next drain bootstraps")
    wipe()
    print("✓ Postgres CDC state lives in the destination's schema, where a replace clears it")


if __name__ == "__main__":
    main()
