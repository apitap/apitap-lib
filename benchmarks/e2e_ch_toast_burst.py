"""A burst of unchanged-TOAST updates drains into a Replacing ClickHouse table in sets.

An UPDATE that leaves a TOASTed column alone sends no value for it, so the
drain reads the value back from the destination before it writes the row.
Through 0.67.2 that ran key by key: a readback, a journal insert, a row insert
and a DELETE under the lease check per key, ~0.5 s each, ~2 keys/s (QA p12:
1,192-1,288 keys in 600 s on 0.67.2 and on the W1 wheel alike), so a burst of
ten thousand such updates took over an hour while the reader stopped reading.
On an insert-only (`ReplacingMergeTree(_apitap_ver, _apitap_deleted)`) table
the tail now runs in sets: one batched readback, one journal INSERT, the final
images and tombstones as two INSERTs, no per-key DELETE (W1 round P, L6).

The burst mixes what keeps a key in the ordered tail: updates of a plain
column under an unchanged 4 KiB TOASTed text (stored EXTERNAL, so it really is
out of line), a second update of some of the same keys in another transaction,
updates of the TOASTed text itself, deletes of tail keys and inserts. The
drain must take it in under a minute and land exactly the source: the count
and a checksum over every column, read under FINAL.

Usage: e2e_ch_toast_burst.py [keys]   (default 3000)
"""
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "toast_burst"
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"
WALL_MAX_S = 60


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
    for pub in set(pg("SELECT pubname FROM pg_publication_tables "
                      f"WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def run(engine=None):
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based'"
            + (f", engine={engine!r}" if engine else "") + ")\nprint('ROWS', r.rows)\n")
    t0 = time.time()
    r = sh([sys.executable, "-c", code])
    return r, time.time() - t0


def digest_pg():
    return pg(f"SELECT count(*) || '|' || sum(v) || '|' || sum(length(txt)) || '|' || "
              f"md5(string_agg(id || ':' || v || ':' || md5(txt) || ':' || coalesce(note, '~'), ',' ORDER BY id)) FROM {T}")


def digest_ch():
    return ch(f"SELECT toString(count()) || '|' || toString(sum(v)) || '|' || toString(sum(length(txt))) || '|' || "
              f"lower(hex(MD5(arrayStringConcat(arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((id, "
              f"toString(id) || ':' || toString(v) || ':' || lower(hex(MD5(txt))) || ':' || ifNull(note, '~'))))), ','))))"
              f" FROM {T} FINAL WHERE _apitap_deleted = 0")


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v bigint NOT NULL, txt text, note text); "
       f"ALTER TABLE {T} ALTER COLUMN txt SET STORAGE EXTERNAL; "
       f"INSERT INTO {T} SELECT g, g, repeat(md5(g::text), 128), 'n' || g FROM generate_series(1, {n + 1000}) g")
    r, _ = run(ENGINE)
    assert r.returncode == 0, r.stderr[-800:]
    # The burst: each statement its own transaction.
    pg(f"UPDATE {T} SET v = v + 1 WHERE id <= {n}")                       # unchanged TOAST
    pg(f"UPDATE {T} SET v = v + 10, note = NULL WHERE id % 3 = 0 AND id <= {n}")  # the same keys again
    pg(f"UPDATE {T} SET txt = repeat(md5((id * 7)::text), 128) WHERE id % 10 = 1 AND id <= {n}")
    pg(f"DELETE FROM {T} WHERE id % 50 = 7 AND id <= {n}")
    pg(f"INSERT INTO {T} SELECT g, g, repeat(md5(g::text), 128), 'new' FROM generate_series({n + 1001}, {n + 1300}) g")
    pg(f"UPDATE {T} SET note = 'back' WHERE id % 3 = 0 AND id <= {n}")    # NULL then set again
    r, wall = run()
    ok_run = r.returncode == 0
    want, got = digest_pg(), (digest_ch() if ok_run else "")
    ok = ok_run and want == got and wall <= WALL_MAX_S
    print(f"  {n} keys of unchanged-TOAST updates (and the rest of the burst) drained in {wall:.1f}s "
          f"(<= {WALL_MAX_S}: {wall <= WALL_MAX_S}); exact: {want == got}")
    if not ok_run:
        print(r.stderr[-1500:])
    elif want != got:
        print(f"    pg {want}\n    ch {got}")
    wipe()
    print("CH TOAST BURST E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
