"""A masked-update readback on a 0.62.x-shaped table takes the newest version.

FINAL collapses by the table's SORTING key, not by the key the readback's
predicate asks for. A 0.62.x apply on a ReplacingMergeTree(_apitap_ver,
_apitap_deleted) table whose ORDER BY is not the replica identity (the D9
legacy shape) can have left one key live twice — a re-key wrote the row under
its new sorting key and the old one never left. The masked-update readback
then sees both rows, and 0.63.0's `lines().next()` took whichever the part
scan reached first: a coin flip that could re-land the stale value of the
untouched column (deep review 3, N2).

The readback now names the row it wants — ORDER BY _apitap_ver DESC LIMIT 1,
the newest live version, the same rule the tombstone path reads by — and the
apply's key-wide DELETE sweeps the duplicate on its way, so the key leaves
the window exactly once: the invariant 0.62.x broke, repaired.

Proof (pg -> ch): bootstrap two rows with a TOAST-external column, rebuild
the destination into the legacy shape (ORDER BY a non-key column), plant the
key live twice with the STALE row sorting FIRST (so the unfixed first-line
pick takes it), then one UPDATE that does not touch the TOAST column. The
readback must return the newest version's value, and the key must leave the
window once. The second key carries the same masked update with no
duplicate planted: the control that says the fix fixed the duplicate case,
not the masked path.
"""
import os
import subprocess
import sys

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
T = "ch_legacy_dup_readback"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def drain():
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')\n"
            "print('ROWS', r.rows, flush=True)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS `{T}__apitap_cdc_del`")
    for meta in ("_apitap_state", "_apitap_residue_journal"):
        if ch(f"SELECT count() FROM system.tables WHERE database = 'default' AND name = '{meta}'") != "0":
            ch(f"ALTER TABLE {meta} DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, note text, blob text)")
        # EXTERNAL forces the value out of line, so an UPDATE that does not
        # touch it really does produce UnchangedToast (the toast_rekey leg's
        # lesson: without this the leg passes while testing nothing).
        pg(f"ALTER TABLE {T} ALTER COLUMN blob SET STORAGE EXTERNAL")
        pg(f"INSERT INTO {T} VALUES (1, 'note1', repeat('z', 40000)), (2, 'note2', repeat('z', 40000))")
        stored = pg(f"SELECT pg_column_size(blob) FROM {T} WHERE id = 1")
        case("the blob column is stored out of line", int(stored) > 2000, f"{stored} bytes")

        r = drain()
        case("the bootstrap lands both rows", r.returncode == 0
             and ch(f"SELECT count() FROM {T} FINAL WHERE _apitap_deleted = 0") == "2",
             (r.stdout + r.stderr).strip()[-300:])

        # Rebuild the destination into the 0.62.x legacy shape: the tombstone
        # columns with an ORDER BY that is NOT the key. One statement, so the
        # two planted rows below land in one part, and inside a part rows are
        # sorted by the ORDER BY column — which is why the stale row's note
        # sorts FIRST: the unfixed first-line readback takes it.
        ch(f"CREATE TABLE {T}_leg (id Int32, note Nullable(String), blob Nullable(String), "
           f"_apitap_ver UInt64 DEFAULT 0, _apitap_deleted UInt8 DEFAULT 0) "
           f"ENGINE = ReplacingMergeTree(_apitap_ver, _apitap_deleted) ORDER BY note "
           f"AS SELECT id, note, blob FROM {T}")
        ch(f"EXCHANGE TABLES {T} AND {T}_leg")
        ch(f"DROP TABLE {T}_leg")
        engine = ch(f"SELECT engine_full FROM system.tables WHERE database = 'default' AND name = '{T}'")
        case("the destination is the legacy shape (tombstone columns, ORDER BY not the key)",
             "ReplacingMergeTree(_apitap_ver, _apitap_deleted)" in engine,
             engine)

        ch(f"DELETE FROM {T} WHERE id = 1")
        # The key live twice: the stale row (ver 10, a different blob) sorted
        # first, the current row (ver 20, the source's blob) after it.
        ch(f"INSERT INTO {T} (id, note, blob, _apitap_ver, _apitap_deleted) VALUES "
           f"(1, 'aaa-stale', repeat('s', 39999), 10, 0), "
           f"(1, 'zzz-current', repeat('z', 40000), 20, 0)")
        notes = ch(f"SELECT arrayStringConcat(groupArray(note), ',') FROM {T} FINAL "
                   f"WHERE id = 1 AND _apitap_deleted = 0")
        case("the key is live twice and the stale row reads first (the 0.62.x state)",
             notes == "aaa-stale,zzz-current", notes)

        # One transaction, two masked updates: id=1 past the duplicate, id=2
        # the control with a single live row.
        pg(f"BEGIN; UPDATE {T} SET note = 'updated' WHERE id = 1; "
           f"UPDATE {T} SET note = 'updated2' WHERE id = 2; COMMIT")
        r = drain()
        case("the drain succeeds", r.returncode == 0, (r.stdout + r.stderr).strip()[-300:])

        live1 = ch(f"SELECT count() FROM {T} FINAL WHERE id = 1 AND _apitap_deleted = 0")
        case("the duplicated key leaves the window exactly once", live1 == "1", live1)
        got = ch(f"SELECT note, length(blob) FROM {T} FINAL WHERE id = 1 AND _apitap_deleted = 0")
        case("the masked update kept the NEWEST version's blob (40000 z's, not the stale 39999 s's)",
             got == "updated\t40000", got)
        got2 = ch(f"SELECT note, length(blob) FROM {T} FINAL WHERE id = 2 AND _apitap_deleted = 0")
        case("control: the unduplicated key's masked update kept its blob",
             got2 == "updated2\t40000", got2)
        want = pg(f"SELECT string_agg(id || ':' || note || ':' || length(blob), ',' ORDER BY id) FROM {T}")
        got_all = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', note, ':', "
                     f"toString(length(ifNull(blob, ''))))), ',') FROM "
                     f"(SELECT id, note, blob FROM {T} FINAL WHERE _apitap_deleted = 0 ORDER BY id)")
        case("the destination agrees with the source", got_all == want, f"{want} vs {got_all}")
    finally:
        wipe()
    print("\nCH LEGACY DUP READBACK E2E: " + ("PASSED" if ok else "FAILED"))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
