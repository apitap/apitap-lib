"""APITAP_PARALLEL_APPLY checks EVERY window, not only the first.

Parallel apply is only sound for plain insert-only windows. The gate ran on
the window that entered the parallel arm; every later window — including
one carrying a TRUNCATE or TOAST-masked residue — then applied concurrently
with its predecessors (MASTER review M-9): a fast truncate could land before
a slower insert window, whose rows then survived it.

Proof: an insert-only ClickHouse destination, depth 4, 1 MiB windows, and a
backlog drained in ONE run: wide inserts spanning several windows, then a
TRUNCATE, then rows with a TOASTed column, then an update that leaves the
TOASTed column unchanged (a masked residue). The destination under FINAL
must equal the source exactly (id, small value, md5 of the big value).
"""
import os
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "pa_barrier"
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"
ROUNDS = 3


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    # Our slot is the one whose publication covers our tables — asked of the
    # server BEFORE the tables go (a dropped table leaves its publication
    # empty and the slot unfindable, holding WAL for ever).
    for pub in set(pg("SELECT pubname FROM pg_publication_tables "
                       f"WHERE tablename IN ({', '.join(repr(t) for t in (T,))})").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database='default' "
          "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def run(engine=None):
    env = dict(os.environ, APITAP_PARALLEL_APPLY="1", APITAP_WINDOW_DEPTH="4",
               APITAP_CDC_WINDOW_BYTES=str(1 << 20))
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based'"
            + (f", engine={engine!r}" if engine else "") + ")\nprint('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300, env=env)


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, small text, big text);"
       f"ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL;"
       f"INSERT INTO {T} VALUES (0, 'seed', 'x')")
    r = run(engine=ENGINE)
    assert r.returncode == 0, r.stderr[-1500:]
    for rnd in range(ROUNDS):
        # ~3 MiB of wide inserts (several 1 MiB windows), one tx per 100 rows.
        for b in range(9):
            pg(f"INSERT INTO {T} SELECT g, 'w{rnd}', repeat(md5(g::text), 100) "
               f"FROM generate_series({1 + b * 100}, {100 + b * 100}) g "
               f"ON CONFLICT (id) DO UPDATE SET small = excluded.small, big = excluded.big")
        pg(f"TRUNCATE {T}")
        pg(f"INSERT INTO {T} SELECT g, 'after{rnd}', repeat(md5((g * 7)::text), 200) "
           f"FROM generate_series(2001, 2020) g")
        # Leaves `big` (TOASTed, EXTERNAL) unchanged: a masked update.
        pg(f"UPDATE {T} SET small = 'masked{rnd}' WHERE id % 2 = 0")
        r = run()
        assert r.returncode == 0, f"round {rnd}: drain failed:\n{r.stderr[-1500:]}"
        want = pg(f"SELECT id || ':' || small || ':' || md5(big) FROM {T} ORDER BY id")
        got = ch(f"SELECT concat(toString(id), ':', small, ':', lower(hex(MD5(big)))) "
                 f"FROM {T} FINAL WHERE _apitap_deleted = 0 ORDER BY id FORMAT TSV")
        assert got == want, (f"round {rnd}: FINAL differs from the source — "
                             f"{len(got.splitlines())} vs {len(want.splitlines())} rows")
        print(f"  round {rnd}: {len(want.splitlines())} rows exact after insert→truncate→masked")
    wipe()
    print("✓ parallel apply: truncate and masked windows are barriers, FINAL exact")


if __name__ == "__main__":
    main()
