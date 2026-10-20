"""A table 0.62.x filled with versions reads back its newest one after the upgrade.

0.62.x applied to a ReplacingMergeTree(_apitap_ver, _apitap_deleted) table
by tombstones even when its ORDER BY was not the key, writing one version
per update. The D9 fix sends such a table to the general path, and that
path's masked readback had no FINAL: it read an arbitrary version and
re-landed a stale TOAST value (deep review 2, N2).

Proof (pg -> ch log_based, ORDER BY (tenant_id, id) over key id, no merge
selected so the versions stay side by side): APITAP_PY_0621 (0.62.1)
bootstraps and applies four full updates of every key's TOASTed body; then
this build applies an update that leaves the bodies alone. FINAL must show
each key's newest body.
"""
import os
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ch_tombstone_upgrade"
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"
OLD_PY = os.environ.get("APITAP_PY_0621", "")


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


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


def run(py):
    code = (f"import apitap\napitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based', "
            f"engine={ENGINE!r}, order_by='tenant_id, id')\n")
    r = sh([py, "-c", code], timeout=300)
    assert r.returncode == 0, r.stderr[-800:]


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP VIEW IF EXISTS {T}__current")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def body(v):
    # md5 chains do not compress: the body stays out of line.
    return f"(SELECT string_agg(md5(i::text || '{v}'), '') FROM generate_series(1, 200) i)"


def main():
    if not OLD_PY:
        print("SKIP: APITAP_PY_0621 is not set")
        return
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, tenant_id int NOT NULL, small text, big text); "
           f"ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL; "
           f"INSERT INTO {T} SELECT g, 42, 's', {body('v0')} FROM generate_series(1, 10) g")
        run(OLD_PY)
        # No merge is picked (no part is under 1 byte), but a mutation, which
        # the apply's DELETE is, still runs; STOP MERGES would block it.
        ch(f"ALTER TABLE {T} MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1")
        # Every key gets versions: a readback that picks an arbitrary one
        # picks the newest for all ten by luck only.
        for v in ("v1", "v2", "v3", "v4"):
            pg(f"UPDATE {T} SET big = {body(v)} || id")
            run(OLD_PY)
        versions = ch(f"SELECT count() FROM {T}")
        assert int(versions) >= 30, f"(rig) 0.62.1 must have left several versions per key, has {versions} rows"
        print(f"  0.62.1 left {versions} versions of 10 keys side by side")

        pg(f"UPDATE {T} SET small = 'x'")
        run(sys.executable)
        want = pg(f"SELECT string_agg(id || ':' || md5(big), ',' ORDER BY id) FROM {T}")
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', lower(hex(MD5(big))))), ',') "
                 f"FROM (SELECT id, big FROM {T} FINAL WHERE _apitap_deleted = 0 ORDER BY id)")
        stale = [a for a, b in zip(got.split(","), want.split(",")) if a != b]
        assert not stale, f"{len(stale)} of 10 keys kept a stale body after the masked update, first {stale[0]}"
        print("  the masked update after the upgrade kept the newest body of every key")
    finally:
        wipe()
    print("✓ a table 0.62.x filled with versions reads back its newest one after the upgrade")


if __name__ == "__main__":
    main()
