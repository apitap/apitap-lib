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

    python benchmarks/e2e_changelog_upgrade.py ch

OLD is `APITAP_PY_0551` (prepared from PyPI); NEW is the interpreter running
this file. RED: run it with the 0.56.0 wheel as NEW (`~/gate-0560-venv`) —
`__current` keeps `old`, and NEW's event sits at seq 0.

Rig: `apitap-bench-mariadb` :3309 (the binlog source both versions read the
same way), `apitap-bench-ch` :8124.
"""
import os
import subprocess
import sys

import _rig

ENGINE = sys.argv[1]
if ENGINE != "ch":
    sys.exit(f"usage: {sys.argv[0]} ch")
OLD_PY = os.environ["APITAP_PY_0551"]
NEW_PY = sys.executable
MA = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cl_upgrade_ch"
ok = True


def ma(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench",
                        "-N", "-D", "bench", "-e", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return _rig.clickhouse(sql)


def version(py):
    return subprocess.run([py, "-c", "import apitap; print(apitap.__version__)"],
                          capture_output=True, text=True).stdout.strip()


def drain(py):
    code = ("import apitap, sys\n"
            "try:\n"
            f"    apitap.transfer({MA!r}, {CH!r}, table={T!r}, mode='log_based', changelog=True)\n"
            "except Exception as e:\n"
            "    print('RAISED', type(e).__name__, str(e).replace(chr(10), ' ')[:600], flush=True)\n"
            "    sys.exit(1)\n")
    r = subprocess.run([py, "-c", code], capture_output=True, text=True, timeout=600)
    if r.returncode:
        _rig.rig_fail(f"{version(py)} drain failed: {(r.stdout + r.stderr).strip()[-500:]}")


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'✓' if passed else '✗'} {name}: {detail}", flush=True)


def clean():
    ma(f"DROP TABLE IF EXISTS bench.{T}")
    ch(f"DROP VIEW IF EXISTS `{T}__current`")
    ch(f"DROP TABLE IF EXISTS `{T}`")
    for t in ("_apitap_state", "_apitap_cdc_pending"):
        if ch(f"SELECT count() FROM system.tables WHERE name = '{t}'") != "0":
            ch(f"ALTER TABLE `{t}` DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")


# The stamp the watermark names: MySQL keeps `position\nserver_id` (0.55.1
# may keep the position alone), so the first line, as a number.
AT_WM = (f"SELECT toString(toUInt64(splitByChar(char(10), watermark)[1])) FROM `_apitap_state` FINAL "
         f"WHERE dest_table = '{T}' AND source_id NOT LIKE 'server-identity:%'")

print(f"== OLD {version(OLD_PY)} -> NEW {version(NEW_PY)}, changelog=True into ClickHouse ==")
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
    w = ch(AT_WM)
    if not w.isdigit():
        _rig.rig_fail(f"no single watermark for {T}: {w!r}")
    at_w = ch(f"SELECT count() FROM `{T}` WHERE _apitap_lsn = {w} AND _apitap_op != 'B'")
    old_max = ch(f"SELECT toString(max(_apitap_seq)) FROM `{T}` WHERE _apitap_lsn = {w} AND _apitap_op != 'B'")
    key1 = ch(f"SELECT toString(_apitap_seq) FROM `{T}` WHERE _apitap_lsn = {w} AND v = 'old'")
    print(f"   watermark {w}: {at_w} rows there, max seq {old_max}, key 1's 'old' at seq {key1}")
    if at_w == "0":
        _rig.rig_fail("the OLD window's rows are not at the watermark's stamp — no collision to test")

    print("== 3. NEW drains a window touching key 1 first, no idle drain ==")
    ma(f"UPDATE bench.{T} SET v = 'new' WHERE id = 1; UPDATE bench.{T} SET v = 'new2' WHERE id = 2")
    drain(NEW_PY)

    print("== 4. what ClickHouse holds ==")
    cur = ch(f"SELECT v FROM `{T}__current` WHERE id = 1")
    case("__current shows NEW's value for key 1", cur == "new", f"{cur!r}")
    src = ma(f"SELECT CONCAT_WS('|', id, v) FROM bench.{T} ORDER BY id")
    dst = ch(f"SELECT concatWithSeparator('|', toString(id), v) FROM `{T}__current` ORDER BY id")
    case("__current equals the MariaDB table", src == dst, "" if src == dst else f"\n     src {src!r}\n     dst {dst!r}")
    twice = ch(f"SELECT count() FROM (SELECT _apitap_lsn, _apitap_seq FROM `{T}` WHERE _apitap_op != 'B' "
               f"GROUP BY 1, 2 HAVING count() > 1)")
    case("no (_apitap_lsn, _apitap_seq) pair occurs twice", twice == "0", f"{twice} repeated")
    first = ch(f"SELECT toString(min(_apitap_seq)) FROM `{T}` WHERE _apitap_lsn = {w} AND v IN ('new', 'new2')")
    case("NEW's first seq at the boundary stamp is the old maximum + 1",
         old_max.isdigit() and first == str(int(old_max) + 1), f"{first} (old max {old_max})")
finally:
    print("== cleanup ==")
    clean()

print("\n   ===== CHANGELOG UPGRADE E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")
sys.exit(0 if ok else 1)
