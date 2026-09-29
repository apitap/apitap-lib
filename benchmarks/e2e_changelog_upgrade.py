"""A `changelog=True` destination upgraded straight from 0.55.x keeps its order
at the one stamp both versions write.

0.55.x stamped a changelog window with its END — the watermark it wrote, which
is exactly where the next window STARTS. From 0.56.0 on a window is stamped
with its start, so the last 0.55.x window and the first window after the
upgrade share `_apitap_lsn`. `<t>__current` orders by `(lsn, seq)`, and 0.56.0
numbered its window from seq 0 again: a key the old window touched last (a
high seq) outranked the new version's first event at seq 0, and `__current`
kept the OLD value for good — and two rows carried one `(lsn, seq)`. 0.57.0
counts what is already at the stamp and numbers above it (`replay_plan`, R1).

  1  OLD (0.55.1) bootstraps, then drains ONE window updating keys 2..10 and
     key 1 last (the highest seq at the boundary stamp)
  2  the collision exists: rows at the stamp the watermark names (the
     documented diagnostic)
  3  NEW drains a window updating key 1 FIRST — no idle drain in between
  4  `__current` shows NEW's value for key 1 and equals the source; no
     `(_apitap_lsn, _apitap_seq)` pair occurs twice; NEW's first seq at the
     boundary stamp is the old maximum + 1

    python benchmarks/e2e_changelog_upgrade.py ch     # into ClickHouse
    python benchmarks/e2e_changelog_upgrade.py bq     # into BigQuery (BQ_SA)

OLD is `APITAP_PY_0551` (prepared from PyPI); NEW is the interpreter running
this file. RED: run it with the 0.56.0 wheel as NEW (`~/gate-0560-venv`) —
`__current` keeps `old`, and NEW's event sits at seq 0.

Rig: `apitap-bench-mariadb` :3309 (the binlog source both versions read the
same way), `apitap-bench-ch` :8124, and for `bq` the gate's dataset.
"""
import os
import subprocess
import sys

import _rig

ENGINE = sys.argv[1] if len(sys.argv) > 1 else ""
if ENGINE not in ("ch", "bq"):
    sys.exit(f"usage: {sys.argv[0]} ch|bq")
OLD_PY = os.environ["APITAP_PY_0551"]
NEW_PY = sys.executable
MA = "mysql://root:bench@127.0.0.1:3309/bench"
T = f"cl_upgrade_{ENGINE}"
ok = True

if ENGINE == "ch":
    DST = "clickhouse://default:bench@127.0.0.1:8124/default"

    def dq(sql):
        return _rig.clickhouse(sql)

    def tb(t):
        return f"`{t}`"

    # The stamp the watermark names: MySQL keeps `position\nserver_id` (0.55.1
    # may keep the position alone), so the first line, as a number.
    AT_WM = (f"SELECT toString(toUInt64(splitByChar(char(10), watermark)[1])) FROM `_apitap_state` FINAL "
             f"WHERE dest_table = '{T}' AND source_id NOT LIKE 'server-identity:%'")
    COUNT = "count()"
    DIGEST = f"SELECT concatWithSeparator('|', toString(id), v) FROM `{T}__current` ORDER BY id"

    def clean():
        ma(f"DROP TABLE IF EXISTS bench.{T}")
        dq(f"DROP VIEW IF EXISTS `{T}__current`")
        dq(f"DROP TABLE IF EXISTS `{T}`")
        for t in ("_apitap_state", "_apitap_cdc_pending"):
            if dq(f"SELECT count() FROM system.tables WHERE name = '{t}'") != "0":
                dq(f"ALTER TABLE `{t}` DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")
else:
    DST = _rig.bq_url()
    DS = f"{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}"

    def dq(sql):
        rows = _rig.bq(sql)
        return "\n".join("\t".join(c or "" for c in r) for r in rows)

    def tb(t):
        return f"`{DS}.{t}`"

    AT_WM = (f"SELECT SPLIT(watermark, '\\n')[OFFSET(0)] FROM {tb('_apitap_state')} "
             f"WHERE dest_table = '{T}' AND source_id NOT LIKE 'server-identity:%' ORDER BY synced_at DESC LIMIT 1")
    COUNT = "COUNT(*)"
    DIGEST = f"SELECT CONCAT(CAST(id AS STRING), '|', v) FROM {tb(T + '__current')} ORDER BY id"

    def clean():
        ma(f"DROP TABLE IF EXISTS bench.{T}")
        have = set(_rig.bq_tables())
        # OLD leaves its scratch untokenized; NEW never bootstraps this table.
        stmts = [f"DROP VIEW IF EXISTS {tb(T + '__current')};", f"DROP TABLE IF EXISTS {tb(T)};"]
        stmts += [f"DROP TABLE IF EXISTS {tb(n)};" for n in (f"{T}__apitap_cdc", f"{T}__apitap_cl")]
        stmts += [f"DELETE FROM {tb(s)} WHERE dest_table = '{T}';"
                  for s in ("_apitap_state", "_apitap_cdc_pending") if s in have]
        _rig.bq("\n".join(stmts))


def ma(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench",
                        "-N", "-D", "bench", "-e", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def s_(expr):
    """`expr` as a string, in the destination's dialect."""
    return f"toString({expr})" if ENGINE == "ch" else f"CAST({expr} AS STRING)"


def version(py):
    return subprocess.run([py, "-c", "import apitap; print(apitap.__version__)"],
                          capture_output=True, text=True).stdout.strip()


def drain(py):
    code = ("import apitap, sys\n"
            "try:\n"
            f"    apitap.transfer({MA!r}, {DST!r}, table={T!r}, mode='log_based', changelog=True)\n"
            "except Exception as e:\n"
            "    print('RAISED', type(e).__name__, str(e).replace(chr(10), ' ')[:600], flush=True)\n"
            "    sys.exit(1)\n")
    r = subprocess.run([py, "-c", code], capture_output=True, text=True, timeout=900)
    if r.returncode:
        _rig.rig_fail(f"{version(py)} drain failed: {(r.stdout + r.stderr).strip()[-500:]}")


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'✓' if passed else '✗'} {name}: {detail}", flush=True)


print(f"== OLD {version(OLD_PY)} -> NEW {version(NEW_PY)}, changelog=True into {ENGINE} ==")
clean()
try:
    ma(f"CREATE TABLE bench.{T} (id BIGINT PRIMARY KEY, v VARCHAR(64))")
    ma(f"INSERT INTO bench.{T} SELECT seq, CONCAT('b', seq) FROM seq_1_to_10")
    drain(OLD_PY)

    print("== 1. OLD drains one window, key 1 last ==")
    ma(";".join([f"UPDATE bench.{T} SET v = 'o{i}' WHERE id = {i}" for i in range(2, 11)]
                + [f"UPDATE bench.{T} SET v = 'old' WHERE id = 1"]))
    drain(OLD_PY)

    print("== 2. the collision: rows at the stamp the watermark names ==")
    w = dq(AT_WM)
    if not w.isdigit():
        _rig.rig_fail(f"no single watermark for {T}: {w!r}")
    at_w = dq(f"SELECT {s_(COUNT)} FROM {tb(T)} WHERE _apitap_lsn = {w} AND _apitap_op != 'B'")
    old_max = dq(f"SELECT {s_('max(_apitap_seq)')} FROM {tb(T)} WHERE _apitap_lsn = {w} AND _apitap_op != 'B'")
    key1 = dq(f"SELECT {s_('_apitap_seq')} FROM {tb(T)} WHERE _apitap_lsn = {w} AND v = 'old'")
    print(f"   watermark {w}: {at_w} rows there, max seq {old_max}, key 1's 'old' at seq {key1}")
    if at_w == "0":
        _rig.rig_fail("the OLD window's rows are not at the watermark's stamp — no collision to test")

    print("== 3. NEW drains a window touching key 1 first, no idle drain ==")
    ma(f"UPDATE bench.{T} SET v = 'new' WHERE id = 1; UPDATE bench.{T} SET v = 'new2' WHERE id = 2")
    drain(NEW_PY)

    print(f"== 4. what {ENGINE} holds ==")
    cur = dq(f"SELECT v FROM {tb(T + '__current')} WHERE id = 1")
    case("__current shows NEW's value for key 1", cur == "new", f"{cur!r}")
    src = ma(f"SELECT CONCAT_WS('|', id, v) FROM bench.{T} ORDER BY id")
    dst = dq(DIGEST)
    case("__current equals the MariaDB table", src == dst, "" if src == dst else f"\n     src {src!r}\n     dst {dst!r}")
    twice = dq(f"SELECT {s_(COUNT)} FROM (SELECT _apitap_lsn, _apitap_seq FROM {tb(T)} WHERE _apitap_op != 'B' "
               f"GROUP BY 1, 2 HAVING {COUNT} > 1)")
    case("no (_apitap_lsn, _apitap_seq) pair occurs twice", twice == "0", f"{twice} repeated")
    first = dq(f"SELECT {s_('min(_apitap_seq)')} FROM {tb(T)} WHERE _apitap_lsn = {w} AND v IN ('new', 'new2')")
    case("NEW's first seq at the boundary stamp is the old maximum + 1",
         old_max.isdigit() and first == str(int(old_max) + 1), f"{first} (old max {old_max})")
finally:
    print("== cleanup ==")
    clean()

print("\n   ===== CHANGELOG UPGRADE E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
