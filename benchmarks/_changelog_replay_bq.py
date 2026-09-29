"""T8: `e2e_changelog_replay.py bq` — the replay cases, from MariaDB into
BigQuery (claims `changelog.replay-not-doubled`, `changelog.group-replay`,
`changelog.seq-continuation` on bq).

BigQuery commits a table's rows and its watermark in ONE transaction, so a
window of one table is never half-applied. It is still applied twice: a group
goes out in several transactions, one member's commits, a sibling's fails, and
the next run re-drains every member from the group minimum. Rewinding a
table's watermark row to where its window started is exactly that state, and
it is how this leg makes it: the rows are in the table, the watermark is back.

Until 0.57.0 BigQuery recorded no attempt at all, so every replay appended its
window again (`e2e_changelog_replay.py`'s ClickHouse cases, where 0.56.0 at
least skipped a same-length replay, did not apply here).

  replay  eleven operations applied once, rewound, re-drained: nothing
          appended, every (lsn, seq) once, `__current` = source
  T3      three rows an older version wrote at this window's stamp, no marker:
          the new event numbers from seq 3, and `__current` shows it
  T2      100 updates of ~30 KB applied in one 64 MiB window, rewound,
          re-drained at 1 MiB (several windows): 100 updates, each once
  T2b     a group [U, T], U's 100 big updates then T's 100 inserts in one
          window; both rewound; the 1 MiB replay's first window holds U only:
          T's attempt is trimmed and its inserts land once

Every answer is asked of BigQuery. RED: the 0.56.0 wheel (`~/gate-0560-venv`)
appends the replays again. Rig: `apitap-bench-mariadb` :3309, BQ_SA.
"""
import os
import subprocess

import apitap

import _rig

MA = "mysql://root:bench@127.0.0.1:3309/bench"
T = "cl_replay_bq"
SHORT, GU, GT = f"{T}_short", f"{T}_gu", f"{T}_gt"
MINE = (T, SHORT, GU, GT)
DS = f"{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}"


def ma(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb",
         "-uroot", "-pbench", "-N", "-D", "bench", "-e", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def tbl(t):
    return f"`{DS}.{t}`"


STATE, PENDING = tbl("_apitap_state"), tbl("_apitap_cdc_pending")


def one(sql):
    rows = _rig.bq(sql)
    return rows[0][0] if rows else None


def snap_of(t):
    return f"_apitap_wm_snap_{t}"


def wm_of(t):
    # A MySQL-source table has TWO state rows: its watermark and a
    # `server-identity:` marker. Only the first is the position.
    return f"dest_table = '{t}' AND source_id NOT LIKE 'server-identity:%'"


def watermark(t):
    """The binlog position `t` is applied to: the newest row's first line
    (MySQL keeps `position\\nserver_id`)."""
    return one(f"SELECT SPLIT(watermark, '\\n')[OFFSET(0)] FROM {STATE} WHERE {wm_of(t)} "
               f"ORDER BY synced_at DESC LIMIT 1")


def snapshot(t):
    """Copy the newest watermark row aside, in SQL, so it goes back verbatim."""
    _rig.bq(f"CREATE OR REPLACE TABLE {tbl(snap_of(t))} AS "
            f"SELECT dest_table, source_id, cursor_col, watermark, mode, last_rows FROM {STATE} "
            f"WHERE {wm_of(t)} QUALIFY ROW_NUMBER() OVER (ORDER BY synced_at DESC) = 1")


def rewind(t):
    """The watermark back where the window started, newest by `synced_at`:
    what a sibling's failed transaction leaves for this table."""
    _rig.bq(f"INSERT INTO {STATE} (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) "
            f"SELECT dest_table, source_id, cursor_col, watermark, mode, last_rows, CURRENT_TIMESTAMP() "
            f"FROM {tbl(snap_of(t))}")
    want = one(f"SELECT SPLIT(watermark, '\\n')[OFFSET(0)] FROM {tbl(snap_of(t))}")
    if watermark(t) != want:
        _rig.rig_fail(f"rewind of {t} did not take: {watermark(t)} != {want}")


def reset():
    """Every source table, destination object and bookkeeping row of this leg."""
    for t in MINE:
        ma(f"DROP TABLE IF EXISTS bench.{t}")
    have = set(_rig.bq_tables())
    stmts = []
    for t in MINE:
        stmts += [f"DROP VIEW IF EXISTS {tbl(t + '__current')};", f"DROP TABLE IF EXISTS {tbl(t)};",
                  f"DROP TABLE IF EXISTS {tbl(snap_of(t))};"]
        # 0.56.0 (the RED control) leaves its staging untokenized.
        stmts += [f"DROP TABLE IF EXISTS {tbl(n)};" for n in (f"{t}__apitap_cdc", f"{t}__apitap_cl")]
    for s in ("_apitap_state", "_apitap_cdc_pending"):
        if s in have:
            stmts.append(f"DELETE FROM {tbl(s)} WHERE dest_table IN ({', '.join(repr(t) for t in MINE)});")
    _rig.bq("\n".join(stmts))


def drain(table=T, budget=None, **kw):
    """One log_based run, in this process; `budget` = APITAP_CDC_WINDOW_BYTES."""
    if budget:
        os.environ["APITAP_CDC_WINDOW_BYTES"] = str(budget)
    target = kw if "tables" in kw else dict(kw, table=table)
    try:
        return apitap.transfer(MA, _rig.bq_url(), mode="log_based", changelog=True, **target)
    finally:
        os.environ.pop("APITAP_CDC_WINDOW_BYTES", None)


def log_facts(t):
    """(events, distinct (lsn, seq), updates recorded twice, ids inserted
    twice) — one query job."""
    x = tbl(t)
    r = _rig.bq(
        f"SELECT (SELECT COUNT(*) FROM {x} WHERE _apitap_op != 'B'), "
        f"(SELECT COUNT(DISTINCT FORMAT('%d/%d', _apitap_lsn, _apitap_seq)) FROM {x} WHERE _apitap_op != 'B'), "
        f"(SELECT COUNT(*) FROM (SELECT v FROM {x} WHERE _apitap_op = 'U' GROUP BY v HAVING COUNT(*) > 1)), "
        f"(SELECT COUNT(*) FROM (SELECT id FROM {x} WHERE _apitap_op = 'I' GROUP BY id HAVING COUNT(*) > 1))")[0]
    return tuple(int(v) for v in r)


def same_as_source(t):
    src = ma(f"SELECT CONCAT_WS('|', id, LOWER(MD5(IFNULL(v, '<N>')))) FROM bench.{t} ORDER BY id")
    dst = "\n".join(r[0] for r in _rig.bq(
        f"SELECT CONCAT(CAST(id AS STRING), '|', TO_HEX(MD5(IFNULL(v, '<N>')))) "
        f"FROM {tbl(t + '__current')} ORDER BY id"))
    return src == dst


def main():
    ok = True

    def case(label, good, detail=""):
        nonlocal ok
        ok &= bool(good)
        print(f"   {'✓' if good else '✗'} {label}{': ' + detail if detail else ''}", flush=True)

    def once_each(t, n, what):
        got, pairs, twice, dup_ins = log_facts(t)
        case(f"{t}: {got} {what} recorded (want {n}), each once",
             got == n and pairs == got and twice == 0 and dup_ins == 0,
             f"count {got} vs distinct (lsn, seq) {pairs}; updates twice {twice}; ids inserted twice {dup_ins}")

    print("== reset ==", flush=True)
    reset()
    try:
        print("== bootstrap ==", flush=True)
        ma(f"CREATE TABLE bench.{T} (id BIGINT PRIMARY KEY, v VARCHAR(64))")
        ma(f"INSERT INTO bench.{T} VALUES (1,'a'),(2,'b'),(3,'c')")
        drain()

        print("== replay: eleven operations applied once, rewound, drained again ==", flush=True)
        snapshot(T)
        ma(";".join([f"UPDATE bench.{T} SET v = 'a1' WHERE id = 1", f"UPDATE bench.{T} SET v = 'a2' WHERE id = 1",
                     f"UPDATE bench.{T} SET v = 'a3' WHERE id = 1", f"DELETE FROM bench.{T} WHERE id = 3",
                     f"INSERT INTO bench.{T} VALUES (4,'d'),(5,'e')", f"UPDATE bench.{T} SET v = 'b1' WHERE id = 2"]))
        drain()
        n1 = log_facts(T)[0]
        case("the window applied once, __current equals the source", same_as_source(T), f"{n1} events")
        rewind(T)
        drain()
        n2, pairs, _, dup_ins = log_facts(T)
        case("the replayed window appended nothing", n2 == n1, f"{n1} -> {n2}")
        case("every (_apitap_lsn, _apitap_seq) is one event", pairs == n2 and dup_ins == 0,
             f"count {n2}, distinct {pairs}, ids inserted twice {dup_ins}")
        case("__current still equals the MariaDB table", same_as_source(T))

        print("== T3: an older version's rows at this window's stamp ==", flush=True)
        w = watermark(T)
        _rig.bq(f"INSERT INTO {tbl(T)} (id, v, _apitap_op, _apitap_lsn, _apitap_seq, _apitap_at) VALUES "
                f"(1,'old0','U',{w},0,CURRENT_TIMESTAMP()),(1,'old1','U',{w},1,CURRENT_TIMESTAMP()),"
                f"(1,'old2','U',{w},2,CURRENT_TIMESTAMP())")
        if "_apitap_cdc_pending" in _rig.bq_tables():
            _rig.bq(f"DELETE FROM {PENDING} WHERE dest_table = '{T}'")
        ma(f"UPDATE bench.{T} SET v = 'new' WHERE id = 1")
        drain()
        first = one(f"SELECT CAST(MIN(_apitap_seq) AS STRING) FROM {tbl(T)} WHERE _apitap_lsn = {w} AND v = 'new'")
        cur = one(f"SELECT v FROM {tbl(T + '__current')} WHERE id = 1")
        case("the new event numbers above the three rows at its stamp", first == "3", f"seq {first} at {w}")
        case("__current shows the new value", cur == "new", f"{cur!r}")

        print("== T2: a shorter replay appends nothing twice ==", flush=True)
        ma(f"CREATE TABLE bench.{SHORT} (id BIGINT PRIMARY KEY, v VARCHAR(40000)) DEFAULT CHARSET=latin1")
        ma(f"INSERT INTO bench.{SHORT} SELECT seq, 'b' FROM seq_1_to_10")
        drain(SHORT)
        snapshot(SHORT)
        big_updates(SHORT, 100, 10)
        drain(SHORT, budget=64 << 20)
        once_each(SHORT, 100, "updates, applied in one window")
        rewind(SHORT)
        drain(SHORT, budget=1 << 20)
        stamps = one(f"SELECT CAST(COUNT(DISTINCT _apitap_lsn) AS STRING) FROM {tbl(SHORT)} WHERE _apitap_op != 'B'")
        print(f"   stamps after the replay: {stamps}", flush=True)
        if int(stamps) < 2:
            case("(rig) the 1 MiB replay split the window", False, "nothing shorter was replayed")
        once_each(SHORT, 100, "updates after a shorter replay")
        case(f"{SHORT}__current equals the MariaDB table", same_as_source(SHORT))

        print("== T2b: a replay window that does not carry a table ==", flush=True)
        ma(f"CREATE TABLE bench.{GU} (id BIGINT PRIMARY KEY, v VARCHAR(40000)) DEFAULT CHARSET=latin1")
        ma(f"CREATE TABLE bench.{GT} (id BIGINT PRIMARY KEY, v VARCHAR(64))")
        ma(f"INSERT INTO bench.{GU} SELECT seq, 'b' FROM seq_1_to_10")
        ma(f"INSERT INTO bench.{GT} VALUES (0, 'seed')")   # an empty table bootstraps no table at all
        drain(tables=[GU, GT])
        snapshot(GU)
        snapshot(GT)
        big_updates(GU, 100, 10)
        ma(";".join(f"INSERT INTO bench.{GT} VALUES ({i}, 't{i}')" for i in range(1, 101)))
        drain(tables=[GU, GT], budget=64 << 20)
        once_each(GT, 100, "inserts, applied in one window")
        rewind(GU)
        rewind(GT)
        drain(tables=[GU, GT], budget=1 << 20)
        once_each(GU, 100, "updates after the replay")
        once_each(GT, 100, "inserts after a replay whose first window did not carry the table")
        case(f"{GU}__current equals the MariaDB table", same_as_source(GU))
        case(f"{GT}__current equals the MariaDB table", same_as_source(GT))
    finally:
        print("== cleanup ==", flush=True)
        reset()
    return ok


def big_updates(table, n, ids):
    """`n` autocommit updates of ~30 KB each, one statement (= one binlog
    transaction) apiece, each value distinct."""
    ma(";".join(f"UPDATE bench.{table} SET v = CONCAT('{i}:', REPEAT('x', 30000)) "
                f"WHERE id = {1 + i % ids}" for i in range(n)))
