"""A slots=N first run bootstraps one table at a time across its groups.

slots=N runs N groups at once, each on its own thread. On a first run every
group bootstraps, and each bootstrap is a full load that sizes its pipes and
chunks to the WHOLE memory limit, so N loads ran at once, each budgeted for
the cage alone: the shape that OOM-killed the 30-table bootstrap (deep
review D18). Bootstraps now take a process-wide turn.

Proof (pg -> pg, two 1M-row tables, slots=2, first run): the source is
sampled every 50 ms while the run lasts; it must never serve COPYs of both
tables at the same moment. Both tables must land whole.
"""
import os
import threading
import time

import psycopg2

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
TABLES = ["slots_boot_a", "slots_boot_b"]
N = 1_000_000


def q(url, sql):
    with psycopg2.connect(url) as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None


def wipe():
    for t in TABLES:
        for (pub,) in q(SRC, f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{t}'"):
            q(SRC, f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                   f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
            q(SRC, f"DROP PUBLICATION IF EXISTS {pub}")
        q(SRC, f"DROP TABLE IF EXISTS {t}")
        q(DST, f"DROP TABLE IF EXISTS {t}")
        q(DST, f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
               f"DELETE FROM public._apitap_state WHERE dest_table IN ('{t}', 'public.{t}'); END IF; END $$")


def main():
    wipe()
    try:
        for t in TABLES:
            q(SRC, f"CREATE TABLE {t} (id int PRIMARY KEY, v text); "
                   f"INSERT INTO {t} SELECT g, md5(g::text) || md5(g::text) FROM generate_series(1, {N}) g")
        done = threading.Event()
        err = []
        seen = {t: 0 for t in TABLES}
        both = 0

        def run():
            try:
                apitap.transfer(SRC, DST, tables=TABLES, mode="log_based", slots=2)
            except Exception as e:  # noqa: BLE001
                err.append(e)
            finally:
                done.set()

        th = threading.Thread(target=run)
        th.start()
        while not done.is_set():
            rows = q(SRC, "SELECT query FROM pg_stat_activity WHERE state = 'active' "
                          "AND query LIKE 'COPY (SELECT%' AND pid <> pg_backend_pid()")
            live = {t for t in TABLES for (sql,) in rows if f'"{t}"' in sql or f" {t} " in sql}
            for t in live:
                seen[t] += 1
            both += len(live) == len(TABLES)
            time.sleep(0.05)
        th.join()
        assert not err, f"the run failed: {err[0]}"
        assert all(seen.values()), f"(rig) the sampler must see each table's COPY: {seen}"
        assert both == 0, f"both tables' bootstraps read at once in {both} samples (each table seen: {seen})"
        for t in TABLES:
            n = q(DST, f"SELECT count(*) FROM {t}")[0][0]
            assert n == N, f"{t} landed {n} rows, want {N}"
        print(f"  slots=2 first run: bootstraps one at a time (samples per table {seen}), both tables whole")
    finally:
        wipe()
    print("✓ a slots=N first run bootstraps one table at a time")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")
    main()
