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

And a replay is not always the same window (0.57.0, `replay::replay_plan`):

  T2   a SHORTER replay: 100 updates of ~30 KB applied in one 64 MiB window,
       rewound, and re-drained at the 1 MiB minimum, so the first replay window
       ends after a third of them. 0.56.0 skipped what the replay carried, wrote
       the replay's end, and the next window appended the rest AGAIN (~166).
  T2b  a replay that does not carry a table: a group [U, T], U's 100 big
       updates then T's 100 inserts, one window; both rewound, re-drained at
       1 MiB — the first window holds U only. 0.56.0 wrote T's watermark past
       its 100 rows and appended them all again later.
  T3   an older version's rows at this window's stamp (0.55.x stamped a
       window with its END, the next one's start): the new event numbers
       ABOVE them. 0.56.0 restarted at seq 0, and `__current` kept the old
       value (the planted `old2`, seq 2).
  T3b  a watermark rewound past an attempt (two windows back) is refused,
       "moved backwards", and nothing is appended. 0.56.0 appended again.

A MariaDB source, not Postgres: rewinding a destination watermark below a
logical replication slot's confirmed-flush point is refused outright (and
Postgres would not re-send the window anyway), while a binlog file+offset can
simply be read again.

Rig: `apitap-bench-mariadb` on :3309 (root/bench), `apitap-bench-ch` on :8124.
RED: `~/gate-0560-venv/bin/python benchmarks/e2e_changelog_replay.py`.
"""
import os
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


def drain(table=T, budget=None, **kw):
    """One log_based run, in this process; `budget` = APITAP_CDC_WINDOW_BYTES."""
    if budget:
        os.environ["APITAP_CDC_WINDOW_BYTES"] = str(budget)
    target = kw if "tables" in kw else dict(kw, table=table)
    try:
        return apitap.transfer(MA, CH, mode="log_based", changelog=True, **target)
    finally:
        os.environ.pop("APITAP_CDC_WINDOW_BYTES", None)


def wm_of(table):
    # `_apitap_state` holds TWO rows per CDC table: the watermark itself, and a
    # `server-identity:` marker recording which MySQL server the binlog
    # coordinate belongs to. Only the first is the position.
    return (f"`_apitap_state` FINAL WHERE dest_table = '{table}' "
            f"AND source_id NOT LIKE 'server-identity:%'")


WM = wm_of(T)
SNAP = "`_apitap_wm_snap`"


def watermark(table=T):
    """The binlog position this table has been applied to.

    MySQL stores it as `position\nserver_id`, so the position is the first line
    — and the whole value never travels through Python, because a tab-separated
    reply escapes that newline and writing the escaped form back silently
    corrupts the row (measured).
    """
    return ch(f"SELECT splitByChar(char(10), watermark)[1] FROM {wm_of(table)}")


def snapshot_watermark(table=T, snap=SNAP):
    """Copy the watermark row aside, in SQL, so it can be put back verbatim."""
    ch(f"DROP TABLE IF EXISTS {snap}")
    ch(f"CREATE TABLE {snap} ENGINE = MergeTree ORDER BY tuple() AS "
       f"SELECT dest_table, source_id, cursor_col, watermark, mode, last_rows FROM {wm_of(table)}")
    n = ch(f"SELECT count() FROM {snap}")
    if n != "1":
        raise RuntimeError(f"expected exactly one watermark row to snapshot, found {n}")


def rewind(table=T, snap=SNAP):
    """Put the watermark back where the window started.

    Exactly the state a crash between the window's INSERT and its `write_state`
    leaves behind: the rows are in the table, the watermark never moved.
    """
    ch(f"INSERT INTO `_apitap_state` "
       f"(dest_table, source_id, cursor_col, watermark, mode, last_rows) "
       f"SELECT * FROM {snap}")
    if ch(f"SELECT count() FROM {wm_of(table)} AND watermark = (SELECT watermark FROM {snap})") != "1":
        raise RuntimeError("rewind did not take: the state row still holds the new position")


def forget(table):
    """Every destination object and bookkeeping row of `table`."""
    ch(f"DROP VIEW IF EXISTS `{table}__current`")
    ch(f"DROP TABLE IF EXISTS `{table}`")
    for t in ("_apitap_state", "_apitap_cdc_pending"):
        if ch(f"SELECT count() FROM system.tables WHERE name = '{t}'") != "0":
            ch(f"ALTER TABLE `{t}` DELETE WHERE dest_table = '{table}' SETTINGS mutations_sync = 1")


def pairs_unique(table):
    """Every captured operation carries its own (lsn, seq): count = distinct."""
    n, u = ch(f"SELECT count(), uniqExact((_apitap_lsn, _apitap_seq)) FROM `{table}` "
              f"WHERE _apitap_op != 'B'").split("\t")
    return int(n), int(u)


def digest():
    """`__current` as the source sees it, so a replay cannot hide behind it."""
    return ch(f"SELECT concatWithSeparator('|', toString(id), ifNull(v, '<N>')) "
              f"FROM {T}__current ORDER BY id")


def src_digest():
    return ma(f"SELECT CONCAT_WS('|', id, IFNULL(v, '<N>')) FROM bench.{T} ORDER BY id")


ok = True

SHORT, GU, GT = f"{T}_short", f"{T}_gu", f"{T}_gt"
MINE = (T, f"{T}_tie", SHORT, GU, GT)
SNAPS = (SNAP, "`_apitap_wm_snap2`", "`_apitap_wm_snap_gu`", "`_apitap_wm_snap_gt`")


def reset():
    for t in MINE:
        ma(f"DROP TABLE IF EXISTS bench.{t}")
        forget(t)
    for s in SNAPS:
        ch(f"DROP TABLE IF EXISTS {s}")


print("== reset ==")
reset()

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

n_ops, n_ids = pairs_unique(T)
print(f"   log rows {n1} -> {n2}   (pre-0.56.0 predicts {n1 + (n1 - 3)}: the window again)")
print(f"   distinct _apitap_lsn {lsns1} -> {lsns2}")
print(f"   (_apitap_lsn, _apitap_seq) pairs appearing more than once: {dup_pairs}")

if n2 == n1:
    print("   ✓ the replayed window appended nothing: the events were already there")
else:
    ok = False
    print(f"   ✗ the replay appended {n2 - n1} rows — the audit trail now "
          f"double-counts them")
if dup_pairs == 0 and n_ops == n_ids:
    print(f"   ✓ every (_apitap_lsn, _apitap_seq) is unique: it identifies one event "
          f"(count {n_ops} = uniqExact {n_ids})")
else:
    ok = False
    print(f"   ✗ {dup_pairs} (lsn, seq) pairs are shared by two or more rows "
          f"(count {n_ops}, uniqExact {n_ids})")
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

print("== T3: an older version's rows at this window's stamp ==")
# A 0.55.x window stamped its rows with its END — the watermark, which is where
# the next window STARTS. Planted exactly so: three rows at the current
# watermark, seq 0..2, and no marker (0.55.x wrote none).
W = watermark()
ch(f"INSERT INTO `{T}` (id, v, _apitap_op, _apitap_lsn, _apitap_seq, _apitap_at) VALUES "
   f"(1,'old0','U',{W},0,now64(3)),(1,'old1','U',{W},1,now64(3)),(1,'old2','U',{W},2,now64(3))")
ch(f"ALTER TABLE `_apitap_cdc_pending` DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")
ma(f"UPDATE bench.{T} SET v = 'new' WHERE id = 1")
drain()
first = ch(f"SELECT toString(min(_apitap_seq)) FROM `{T}` WHERE _apitap_lsn = {W} AND v = 'new'")
cur = ch(f"SELECT v FROM `{T}__current` WHERE id = 1")
print(f"   stamp {W}: the new event's seq {first}, __current id=1 {cur!r}")
if first == "3":
    print("   ✓ the new event numbers above the three rows already at its stamp")
else:
    ok = False
    print(f"   ✗ the new event is at seq {first}, under or among the older rows")
if cur == "new":
    print("   ✓ __current shows the new value")
else:
    ok = False
    print(f"   ✗ __current shows {cur!r}: an older version's row outranks the newest event")

print("== T3b: a watermark rewound past an attempt is refused ==")
SNAP2 = SNAPS[1]
snapshot_watermark(T, SNAP2)
ma(f"UPDATE bench.{T} SET v = 'r1' WHERE id = 2")
drain()
ma(f"UPDATE bench.{T} SET v = 'r2' WHERE id = 4")
drain()
before = ch(f"SELECT count() FROM `{T}`")
rewind(T, SNAP2)
try:
    drain()
    err = None
except Exception as e:  # the refusal is the point
    err = str(e)
after = ch(f"SELECT count() FROM `{T}`")
print(f"   log rows {before} -> {after}; the drain said: {(err or 'nothing, rc 0')[:240]!r}")
if err and "moved backwards" in err:
    print("   ✓ refused: the destination's newest attempt is past the rewound start")
else:
    ok = False
    print("   ✗ the rewound drain was not refused as a rewind")
if after == before:
    print("   ✓ nothing was appended")
else:
    ok = False
    print(f"   ✗ it appended {int(after) - int(before)} rows")


def big_updates(table, n, ids):
    """`n` autocommit updates of ~30 KB each, one statement (= one binlog
    transaction) apiece, each value distinct."""
    ma(";".join(f"UPDATE bench.{table} SET v = CONCAT('{i}:', REPEAT('x', 30000)) "
                f"WHERE id = {1 + i % ids}" for i in range(n)))


def src_md5(table):
    return ma(f"SELECT CONCAT_WS('|', id, UPPER(MD5(IFNULL(v, '<N>')))) FROM bench.{table} ORDER BY id")


def dst_md5(table):
    return ch(f"SELECT concatWithSeparator('|', toString(id), hex(MD5(ifNull(v, '<N>')))) "
              f"FROM `{table}__current` ORDER BY id")


def once_each(table, n, what):
    """`n` events recorded, each exactly once."""
    global ok
    got = int(ch(f"SELECT count() FROM `{table}` WHERE _apitap_op != 'B'"))
    ops, ids = pairs_unique(table)
    twice = ch(f"SELECT count() FROM (SELECT v FROM `{table}` WHERE _apitap_op = 'U' "
               f"GROUP BY v HAVING count() > 1)")
    dup_ins = ch(f"SELECT count() FROM (SELECT id FROM `{table}` WHERE _apitap_op = 'I' "
                 f"GROUP BY id HAVING count() > 1)")
    good = got == n and ops == ids and twice == "0" and dup_ins == "0"
    ok &= good
    print(f"   {'✓' if good else '✗'} {table}: {got} {what} recorded (want {n}); "
          f"count {ops} vs uniqExact(lsn, seq) {ids}; updates twice {twice}; "
          f"ids inserted twice {dup_ins}")


def same_as_source(table):
    global ok
    good = dst_md5(table) == src_md5(table)
    ok &= good
    print(f"   {'✓' if good else '✗'} {table}__current equals the MariaDB table")


print("== T2: a shorter replay appends nothing twice ==")
ma(f"CREATE TABLE bench.{SHORT} (id BIGINT PRIMARY KEY, v VARCHAR(40000)) DEFAULT CHARSET=latin1")
ma(f"INSERT INTO bench.{SHORT} SELECT seq, 'b' FROM seq_1_to_10")
drain(SHORT)
snapshot_watermark(SHORT)
big_updates(SHORT, 100, 10)
drain(SHORT, budget=64 << 20)
once_each(SHORT, 100, "updates, applied in one window")
stamps1 = ch(f"SELECT uniqExact(_apitap_lsn) FROM `{SHORT}` WHERE _apitap_op != 'B'")
rewind(SHORT)
drain(SHORT, budget=1 << 20)
stamps2 = ch(f"SELECT uniqExact(_apitap_lsn) FROM `{SHORT}` WHERE _apitap_op != 'B'")
print(f"   stamps: {stamps1} before the replay, {stamps2} after it (the replay took several windows)")
if int(stamps2) < 2:
    ok = False
    print("   ✗ (rig) the 1 MiB replay did not split the window — nothing shorter was replayed")
once_each(SHORT, 100, "updates after a shorter replay")
same_as_source(SHORT)

print("== T2b: a replay window that does not carry a table ==")
ma(f"CREATE TABLE bench.{GU} (id BIGINT PRIMARY KEY, v VARCHAR(40000)) DEFAULT CHARSET=latin1")
ma(f"CREATE TABLE bench.{GT} (id BIGINT PRIMARY KEY, v VARCHAR(64))")
ma(f"INSERT INTO bench.{GU} SELECT seq, 'b' FROM seq_1_to_10")
ma(f"INSERT INTO bench.{GT} VALUES (0, 'seed')")    # an empty table bootstraps no table at all
drain(tables=[GU, GT])
snapshot_watermark(GU, SNAPS[2])
snapshot_watermark(GT, SNAPS[3])
big_updates(GU, 100, 10)
ma(";".join(f"INSERT INTO bench.{GT} VALUES ({i}, 't{i}')" for i in range(1, 101)))
drain(tables=[GU, GT], budget=64 << 20)
once_each(GT, 100, "inserts, applied in one window")
rewind(GU, SNAPS[2])
rewind(GT, SNAPS[3])
drain(tables=[GU, GT], budget=1 << 20)
once_each(GU, 100, "updates after the replay")
once_each(GT, 100, "inserts after a replay whose first window did not carry the table")
same_as_source(GU)
gt_src = ma(f"SELECT CONCAT_WS('|', id, v) FROM bench.{GT} ORDER BY id")
gt_dst = ch(f"SELECT concatWithSeparator('|', toString(id), v) FROM `{GT}__current` ORDER BY id")
ok &= gt_src == gt_dst
print(f"   {'✓' if gt_src == gt_dst else '✗'} {GT}__current equals the MariaDB table")

print("== cleanup ==")
reset()

print("\n   ===== CHANGELOG REPLAY E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
