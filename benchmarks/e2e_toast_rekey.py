"""A key-changing UPDATE on a row with an unchanged TOAST column.

Postgres does not re-send a large (externally TOASTed) value when an UPDATE does
not touch it — the new tuple carries `UnchangedToast` in that slot. apitap's
collapser handles that by putting the key on a "residue" tail and emitting a
MaskedUpdate: an `UPDATE ... SET <the columns it does have> WHERE <key>`.

That is right when the key did not move. When it DID move, the same `update()`
call has already queued a DELETE for the OLD key, and the MaskedUpdate targets
the NEW key — which has never existed at the destination. On ClickHouse the
applier reads the row back first and raises "masked update for a row missing at
the destination". On a Postgres or MySQL destination it is a plain UPDATE whose
WHERE matches nothing, nobody checks rows-affected, the window commits and the
watermark advances.

So the question this leg asks is exactly: after a re-key, is the row still there?

  row 1  key change, TOAST untouched   — the case under test
  row 2  key change, TOAST rewritten   — the control: no UnchangedToast, so the
                                         ordinary upsert path runs and the row
                                         must survive. If row 2 also vanishes
                                         the leg is testing re-keying in
                                         general, not the masked path.
  row 3  no key change, TOAST untouched — the other control: MaskedUpdate on a
                                         key that DOES exist, which is the case
                                         the residue tail was built for.

Rows 2 and 3 are what make row 1 mean something. Without them a failure could
be "apitap cannot re-key at all" or "apitap cannot do masked updates at all",
and the fix would be aimed at the wrong place.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545.
"""
import os
import subprocess
import sys

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
SRC_C = "apitap-bench-pg-src"
DST_C = "apitap-bench-pg-dst"
T = "toast_rekey"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def _psql(container, db, sql):
    o = sh(["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", db, "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def src(sql):
    return _psql(SRC_C, "apitap_bench_src", sql)


def dst(sql):
    return _psql(DST_C, "apitap_bench_dst", sql)


def _slots_now():
    return set(src("SELECT slot_name FROM pg_replication_slots").split())


_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for s in sorted(_slots_now() - _SLOTS_BEFORE):
        src(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
            f"WHERE slot_name='{s}' AND NOT active")


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def drain():
    code = (
        "import apitap\n"
        f"r = apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')\n"
        "print('ROWS', r.rows, flush=True)\n"
    )
    return sh([sys.executable, "-c", code])


# ---------------------------------------------------------------------------
print("== setup: a table whose big column really is TOASTed out of line ==")
src(f"DROP TABLE IF EXISTS {T}")
# EXTERNAL storage forces the value out of line and uncompressed, so an UPDATE
# that does not touch it produces UnchangedToast rather than an inline value.
src(f"CREATE TABLE {T} (id int PRIMARY KEY, note text, blob text)")
src(f"ALTER TABLE {T} ALTER COLUMN blob SET STORAGE EXTERNAL")
src(f"INSERT INTO {T} SELECT g, 'note'||g, repeat('z', 40000) FROM generate_series(1,3) g")
toasted = src(f"SELECT count(*) FROM pg_class c JOIN pg_class t ON t.oid = c.reltoastrelid "
              f"WHERE c.relname = '{T}' AND t.reltuples <> 0 OR c.relname = '{T}'")
stored = src(f"SELECT pg_column_size(blob) FROM {T} WHERE id = 1")
case("the big column is stored out of line", int(stored) > 2000, f"{stored} bytes on-row")

dst(f"DROP TABLE IF EXISTS {T}")
dst("DELETE FROM _apitap_state WHERE dest_table = %s" .replace("%s", f"'{T}'")
    ) if dst("SELECT count(*) FROM information_schema.tables WHERE table_name='_apitap_state'") == "1" else None
drop_our_slots()

r = drain()
if r.returncode:
    case("bootstrap", False, r.stderr.strip()[-400:])
    print("\nTOAST REKEY E2E: FAILED")
    raise SystemExit(1)
case("bootstrap landed all three rows", dst(f"SELECT count(*) FROM {T}") == "3")

# ---------------------------------------------------------------------------
print("== the three updates, in one transaction ==")
src(f"""
BEGIN;
UPDATE {T} SET id = 101, note = 'rekeyed-masked'   WHERE id = 1;
UPDATE {T} SET id = 102, note = 'rekeyed-full', blob = repeat('y', 40000) WHERE id = 2;
UPDATE {T} SET note = 'same-key-masked'            WHERE id = 3;
COMMIT;
""")
src_ids = src(f"SELECT string_agg(id::text, ',' ORDER BY id) FROM {T}")
case("the source now holds exactly the re-keyed rows", src_ids == "3,101,102", src_ids)

r = drain()
case("the drain succeeds", r.returncode == 0, r.stderr.strip()[-300:])

# ---------------------------------------------------------------------------
print("== what survived at the destination ==")
dst_ids = dst(f"SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM {T}")
print(f"   source: {src_ids}")
print(f"   dest:   {dst_ids}")

case("row 2 survived — key change with the TOAST column REWRITTEN (control)",
     "102" in dst_ids.split(","), dst_ids)
case("row 3 survived — masked update on a key that did NOT move (control)",
     "3" in dst_ids.split(","), dst_ids)
case("row 1 survived — key change with the TOAST column UNTOUCHED",
     "101" in dst_ids.split(","), dst_ids)

if "101" in dst_ids.split(","):
    n1 = dst(f"SELECT note FROM {T} WHERE id = 101")
    b1 = dst(f"SELECT length(blob) FROM {T} WHERE id = 101")
    case("and it carries both the new note and the old blob",
         n1 == "rekeyed-masked" and b1 == "40000", f"note={n1} blob_len={b1}")

case("the destination agrees with the source exactly", dst_ids == src_ids,
     f"src {src_ids} vs dst {dst_ids}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
src(f"DROP TABLE IF EXISTS {T}")
dst(f"DROP TABLE IF EXISTS {T}")
dst(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")
drop_our_slots()

print("\nTOAST REKEY E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
