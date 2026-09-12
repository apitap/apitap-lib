"""A changelog window that lands and then gets replayed must not double the log.

`changelog=True` is an append-only audit trail, and the destinations that hold
one — ClickHouse and BigQuery — have no transaction that can cover both the
INSERT of the window's rows and the write of its watermark. So the window CAN
be applied twice:

  (a) the process dies between the two round-trips, or
  (b) a sibling table in the same group fails its apply, and the next run
      restarts every member from the group MINIMUM watermark.

Until 0.56.0 each replay appended every event a SECOND time under a DIFFERENT
`_apitap_lsn` — the stamp was `outcome.end_lsn`, the window's stop-line, which
a re-drain recomputes from whatever has arrived since. `<table>__current` still
converged (the later stamp won), so nothing looked broken; the raw log — the
entire product of changelog=True — silently held every event twice, under
stamps that made `(lsn, seq)` useless for de-duplicating it.

This leg reproduces (a) exactly, and without racing anything: apply a window,
then rewind the destination's watermark to where the window STARTED. That is
the same state a crash between the INSERT and `write_state` leaves behind, and
the next drain re-delivers the identical events.

A MariaDB source, not Postgres: rewinding a destination watermark below a
logical replication slot's confirmed-flush point is refused outright (and
Postgres would not re-send the window anyway), while a binlog file+offset can
simply be read again.

Rig: `apitap-bench-mariadb` on :3309 (root/bench), `apitap-bench-ch` on :8124.
"""
import subprocess
import sys

import apitap

MA = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cl_replay"


def ma(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb",
         "-uroot", "-pbench", "-N", "-D", "bench", "-e", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def drain():
    return apitap.transfer(MA, CH, table=T, mode="log_based", changelog=True)


# `_apitap_state` holds TWO rows per CDC table: the watermark itself, and a
# `server-identity:` marker recording which MySQL server the binlog coordinate
# belongs to. Only the first is the position.
WM = (f"`_apitap_state` FINAL WHERE dest_table = '{T}' "
      f"AND source_id NOT LIKE 'server-identity:%'")
# …and the same row, with room for one more predicate.
WM_AND = WM + " AND "
SNAP = "`_apitap_wm_snap`"


def watermark():
    """The binlog position this table has been applied to.

    MySQL stores it as `position\nserver_id`, so the position is the first line
    — and the whole value never travels through Python, because a tab-separated
    reply escapes that newline and writing the escaped form back silently
    corrupts the row (measured).
    """
    return ch(f"SELECT splitByChar(char(10), watermark)[1] FROM {WM}")


def snapshot_watermark():
    """Copy the watermark row aside, in SQL, so it can be put back verbatim."""
    ch(f"DROP TABLE IF EXISTS {SNAP}")
    ch(f"CREATE TABLE {SNAP} ENGINE = MergeTree ORDER BY tuple() AS "
       f"SELECT dest_table, source_id, cursor_col, watermark, mode, last_rows FROM {WM}")
    n = ch(f"SELECT count() FROM {SNAP}")
    if n != "1":
        raise RuntimeError(f"expected exactly one watermark row to snapshot, found {n}")


def rewind():
    """Put the watermark back where the window started.

    Exactly the state a crash between the window's INSERT and its `write_state`
    leaves behind: the rows are in the table, the watermark never moved.
    """
    ch(f"INSERT INTO `_apitap_state` "
       f"(dest_table, source_id, cursor_col, watermark, mode, last_rows) "
       f"SELECT * FROM {SNAP}")
    if ch(f"SELECT count() FROM {WM_AND} watermark = (SELECT watermark FROM {SNAP})") != "1":
        raise RuntimeError("rewind did not take: the state row still holds the new position")


def digest():
    """`__current` as the source sees it, so a replay cannot hide behind it."""
    return ch(f"SELECT concatWithSeparator('|', toString(id), ifNull(v, '<N>')) "
              f"FROM {T}__current ORDER BY id")


def src_digest():
    return ma(f"SELECT CONCAT_WS('|', id, IFNULL(v, '<N>')) FROM bench.{T} ORDER BY id")


ok = True

print("== reset ==")
ma(f"DROP TABLE IF EXISTS bench.{T}")
for obj in (f"{T}__current",):
    ch(f"DROP VIEW IF EXISTS {obj}")
ch(f"DROP TABLE IF EXISTS {T}")
if ch("SELECT count() FROM system.tables WHERE name = '_apitap_state'") != "0":
    ch(f"ALTER TABLE `_apitap_state` DELETE WHERE dest_table = '{T}' "
       f"SETTINGS mutations_sync = 1")
if ch("SELECT count() FROM system.tables WHERE name = '_apitap_cdc_pending'") != "0":
    ch(f"ALTER TABLE `_apitap_cdc_pending` DELETE WHERE dest_table = '{T}' "
       f"SETTINGS mutations_sync = 1")
ch(f"DROP TABLE IF EXISTS {SNAP}")

print("== bootstrap ==")
ma(f"CREATE TABLE bench.{T} (id BIGINT PRIMARY KEY, v VARCHAR(64))")
ma(f"INSERT INTO bench.{T} VALUES (1,'a'),(2,'b'),(3,'c')")
drain()
print(f"   baseline rows in the log: {ch(f'SELECT count() FROM {T}')}")

print("== window 1: eleven operations, applied once ==")
start = watermark()
snapshot_watermark()
ma(f"UPDATE bench.{T} SET v = 'a1' WHERE id = 1")
ma(f"UPDATE bench.{T} SET v = 'a2' WHERE id = 1")
ma(f"UPDATE bench.{T} SET v = 'a3' WHERE id = 1")
ma(f"DELETE FROM bench.{T} WHERE id = 3")
ma(f"INSERT INTO bench.{T} VALUES (4,'d'),(5,'e')")
ma(f"UPDATE bench.{T} SET v = 'b1' WHERE id = 2")
drain()
after_window = watermark()
n1 = int(ch(f"SELECT count() FROM {T}"))
lsns1 = int(ch(f"SELECT uniqExact(_apitap_lsn) FROM {T}"))
cur1 = digest()
print(f"   log rows {n1}, distinct _apitap_lsn {lsns1}, watermark {start} -> {after_window}")
if cur1 != src_digest():
    ok = False
    print(f"   ✗ __current already disagrees with the source before any replay\n"
          f"     src: {src_digest()}\n     dst: {cur1}")

print("== the crash: rewind the watermark to where the window began, drain again ==")
rewind()
drain()
n2 = int(ch(f"SELECT count() FROM {T}"))
lsns2 = int(ch(f"SELECT uniqExact(_apitap_lsn) FROM {T}"))
# Baseline rows are excluded: the bootstrap snapshot writes every one of them
# with seq 0, so they share a pair by construction. The property under test is
# that no captured OPERATION shares its identity with another.
dup_pairs = int(ch(f"SELECT count() FROM (SELECT _apitap_lsn, _apitap_seq FROM {T} "
                   f"WHERE _apitap_op != 'B' GROUP BY 1, 2 HAVING count() > 1)"))
dup_ins = ch(f"SELECT concatWithSeparator(' ', toString(id), toString(count())) FROM {T} "
             f"WHERE _apitap_op = 'I' GROUP BY id HAVING count() > 1 ORDER BY id")

print(f"   log rows {n1} -> {n2}   (pre-0.56.0 predicts {n1 + (n1 - 3)}: the window again)")
print(f"   distinct _apitap_lsn {lsns1} -> {lsns2}")
print(f"   (_apitap_lsn, _apitap_seq) pairs appearing more than once: {dup_pairs}")

if n2 == n1:
    print("   ✓ the replayed window appended nothing: the events were already there")
else:
    ok = False
    print(f"   ✗ the replay appended {n2 - n1} rows — the audit trail now "
          f"double-counts them")
if dup_pairs == 0:
    print("   ✓ every (_apitap_lsn, _apitap_seq) is unique: it identifies one event")
else:
    ok = False
    print(f"   ✗ {dup_pairs} (lsn, seq) pairs are shared by two or more rows")
if dup_ins == "":
    print("   ✓ no source INSERT is recorded twice")
else:
    ok = False
    print(f"   ✗ these ids carry more than one 'I' record: {dup_ins!r}")

cur2 = digest()
if cur2 == src_digest() == cur1:
    print("   ✓ __current still equals the MariaDB table")
else:
    ok = False
    print(f"   ✗ __current changed or drifted\n     src: {src_digest()}\n"
          f"     before: {cur1}\n     after:  {cur2}")

print("== and the stamp is the window's START, so it survives the re-drain ==")
# The whole reason the replay can be recognised: `_apitap_lsn` is the watermark
# the window was drained FROM. `end_lsn` — what it used to be — is recomputed by
# every re-drain, so the same event came back under a new number each time.
stamp = ch(f"SELECT DISTINCT toString(_apitap_lsn) FROM {T} WHERE _apitap_op != 'B' "
           f"AND _apitap_lsn > 0 ORDER BY 1")
print(f"   stamps in the log (excluding the baseline): {stamp.splitlines()}")
if start in stamp.splitlines():
    print(f"   ✓ the window is stamped with its start watermark ({start})")
else:
    ok = False
    print(f"   ✗ no row carries the window's start watermark {start} — the stamp "
          f"is still the moving end-of-window")

print("== a baseline row and a window event can now share a stamp ==")
# The stamp is the window's START, and the FIRST window after a bootstrap starts
# exactly where the baseline snapshot was taken — so a baseline row (seq 0) and
# that window's first event for the same key carry the identical (lsn, seq).
# `__current` has to prefer the event; without the tie-break the winner is
# whichever row the engine happens to return first.
B = f"{T}_tie"
ma(f"DROP TABLE IF EXISTS bench.{B}")
ch(f"DROP VIEW IF EXISTS {B}__current")
ch(f"DROP TABLE IF EXISTS {B}")
if ch("SELECT count() FROM system.tables WHERE name = '_apitap_state'") != "0":
    ch(f"ALTER TABLE `_apitap_state` DELETE WHERE dest_table = '{B}' "
       f"SETTINGS mutations_sync = 1")
ma(f"CREATE TABLE bench.{B} (id BIGINT PRIMARY KEY, v VARCHAR(64))")
ma(f"INSERT INTO bench.{B} VALUES (1,'before')")
apitap.transfer(MA, CH, table=B, mode="log_based", changelog=True)
ma(f"UPDATE bench.{B} SET v = 'after' WHERE id = 1")    # ONE event: seq 0
apitap.transfer(MA, CH, table=B, mode="log_based", changelog=True)
stamps = ch(f"SELECT concatWithSeparator('/', _apitap_op, toString(_apitap_lsn), "
            f"toString(_apitap_seq)) FROM {B} ORDER BY _apitap_op")
got = ch(f"SELECT v FROM {B}__current WHERE id = 1")
print(f"   rows in the log: {stamps.splitlines()}")

# The outcome alone is NOT a control: an ORDER BY that leaves the two rows tied
# still returns one of them, and it may well be the right one — measured, on a
# wheel with no tie-break at all. So the view's ordering is read back and
# checked for being TOTAL, which is the property the outcome depends on.
ddl = ch(f"SELECT create_table_query FROM system.tables WHERE name = '{B}__current'")
tied = "ORDER BY _apitap_lsn DESC, _apitap_seq DESC, _apitap_op" in ddl.replace("`", "")
if tied:
    print("   ✓ the view orders by (lsn, seq, op) — the tie has a defined winner")
else:
    ok = False
    print(f"   ✗ the view still orders only by (lsn, seq), so the winner of a "
          f"shared stamp is whichever row the engine returns first:\n     {ddl[:400]}")
if got == "after":
    print("   ✓ …and the update outranks the baseline it shares a stamp with")
else:
    ok = False
    print(f"   ✗ __current shows {got!r} — the baseline won the tie")
ma(f"DROP TABLE IF EXISTS bench.{B}")
ch(f"DROP VIEW IF EXISTS {B}__current")
ch(f"DROP TABLE IF EXISTS {B}")

print("== a fresh window after the replay still applies normally ==")
ma(f"UPDATE bench.{T} SET v = 'z' WHERE id = 5")
drain()
n3 = int(ch(f"SELECT count() FROM {T}"))
if n3 == n2 + 1 and digest() == src_digest():
    print(f"   ✓ one more operation, one more row ({n2} -> {n3}), __current matches")
else:
    ok = False
    print(f"   ✗ the pipeline did not resume cleanly: {n2} -> {n3}\n"
          f"     src: {src_digest()}\n     dst: {digest()}")

print("== cleanup ==")
ma(f"DROP TABLE IF EXISTS bench.{T}")
ch(f"DROP VIEW IF EXISTS {T}__current")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"DROP TABLE IF EXISTS {SNAP}")

print("\n   ===== CHANGELOG REPLAY E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
