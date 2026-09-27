"""A rolling upgrade: an older apitap and this one, side by side, on one table.

0.56.0 shipped a drain whose announcement (a `__apitap_lock`) a 0.55.1 bulk run
cannot see — 0.55.1 scans for staging only — so in the most ordinary upgrade
there is, the two ran over each other. 0.57.0 declares what every supported
release writes and scans (`naming::compat`), and a drain now announces an empty
staging MARKER beside its lock so every 0.55.1+ reader refuses beside it. This
leg runs the OLD wheel (`APITAP_PY_0551` / `APITAP_PY_0560`, prepared from PyPI)
against the NEW one (the interpreter running this file) and asks the server what
happened:

  a  a NEW drain (200k-row backlog, 64 KiB windows) holds T: an OLD replace is refused, and OLD staging never
     appears beside T                       (compat.old-bulk-refused-by-new-drain)
  b  an OLD replace is loading T: a NEW drain is refused by type
                                            (compat.new-drain-refused-by-old-bulk)
  c  (0.55.1, Postgres) an OLD drain writes no artifact at all, for its whole
     life — the documented hole             (compat.old-drain-writes-nothing)
  d  (0.55.1, Postgres) rolling back replicates `_apitap_lease` — which keeps
     every collected victim's row for good — as user data; the documented
     DROPs stop it                            (compat.rollback-leaks)
  e  a NEW drain killed outright leaves its lock and marker; the OLD release is
     refused, naming the marker; a NEW run after the TTL collects both and
     resumes

    python benchmarks/e2e_rolling_upgrade.py <0551|0560> <pg|ch|bq>

Rig: `apitap-bench-pg-src` :5544; Postgres :5545 (its own database `ru_dst`),
ClickHouse :8124, or the gate's BigQuery dataset. RED: run it with the NEW
interpreter replaced by the 0.56.0 wheel — case (a) then lets the OLD 0.55.1
replace through and its staging appears.
"""
import os
import signal
import subprocess
import sys
import time

import _rig

OLD_V, ENGINE = sys.argv[1], sys.argv[2]
OLD_PY = os.environ[{"0551": "APITAP_PY_0551", "0560": "APITAP_PY_0560"}[OLD_V]]
NEW_PY = sys.executable
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = f"ru_{ENGINE}_{OLD_V}"
BIG = "ru_big"
TTL = 30
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} [{OLD_V}->{ENGINE}] {name}: {detail}", flush=True)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


class Pg:
    url = "postgres://postgres:bench@127.0.0.1:5545/ru_dst"
    where = ("apitap-bench-pg-dst", "ru_dst")

    def setup(self):
        if _rig.psql("SELECT count(*) FROM pg_database WHERE datname = 'ru_dst'",
                     ("apitap-bench-pg-dst", "postgres")) == "0":
            _rig.psql("CREATE DATABASE ru_dst", ("apitap-bench-pg-dst", "postgres"))

    def q(self, sql):
        return _rig.psql(sql, self.where)

    def names(self):
        return [n for n in self.q(f"SELECT relname FROM pg_class WHERE relkind = 'r' "
                                  f"AND relname LIKE '{T}%'").splitlines() if n]

    def count(self):
        return self.q(f"SELECT count(*) FROM {T}")

    def watermark(self):
        if self.q("SELECT to_regclass('_apitap_state') IS NULL") == "t":
            return ""
        return self.q(f"SELECT watermark FROM _apitap_state WHERE dest_table = '{T}' "
                      "AND source_id NOT LIKE 'server-identity:%'")

    def clean(self):
        for n in self.names():
            self.q(f'DROP TABLE IF EXISTS "{n}" CASCADE')
        for t, w in (("_apitap_state", f"dest_table IN ('{T}', 'public.{T}')"),
                     ("_apitap_lease", f"dest_key = 'public.{T}'")):
            if self.q(f"SELECT to_regclass('{t}') IS NOT NULL") == "t":
                self.q(f"DELETE FROM {t} WHERE {w}")


class Ch:
    url = "clickhouse://default:bench@127.0.0.1:8124/default"

    def setup(self):
        pass

    def names(self):
        return [n for n in _rig.clickhouse(
            "SELECT name FROM system.tables WHERE database = currentDatabase() "
            f"AND startsWith(name, '{T}')").splitlines() if n]

    def count(self):
        return _rig.clickhouse(f"SELECT count() FROM `{T}`")

    def watermark(self):
        return _rig.clickhouse(f"SELECT watermark FROM `_apitap_state` FINAL WHERE dest_table = '{T}' "
                               "AND source_id NOT LIKE 'server-identity:%'")

    def clean(self):
        for n in self.names():
            _rig.clickhouse(f"DROP TABLE IF EXISTS `{n}`")
        _rig.clickhouse(f"DROP VIEW IF EXISTS `{T}__current`")
        for t, w in (("_apitap_state", f"dest_table = '{T}'"),
                     ("_apitap_lease", f"dest_key = 'default.{T}'")):
            if _rig.clickhouse(f"SELECT count() FROM system.tables WHERE name = '{t}'") != "0":
                _rig.clickhouse(f"ALTER TABLE `{t}` DELETE WHERE {w} SETTINGS mutations_sync = 1")


class Bq:
    url = None

    def __init__(self):
        self.url = _rig.bq_url()
        self.fq = f"`{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}"

    def setup(self):
        pass

    def names(self):
        return sorted(n for n in _rig.bq_tables() if n.startswith(T))

    def count(self):
        r = _rig.bq(f"SELECT COUNT(*) FROM {self.fq}.{T}`")
        return r[0][0] if r else None

    def watermark(self):
        tabs = _rig.bq_tables()
        if "_apitap_state" not in tabs:
            return ""
        r = _rig.bq(f"SELECT watermark FROM {self.fq}._apitap_state` WHERE dest_table = '{T}' "
                    "ORDER BY synced_at DESC LIMIT 1")
        return r[0][0] if r else ""

    def clean(self):
        for n in self.names():
            _rig.bq_delete_table(n)
        tabs = _rig.bq_tables()
        if "_apitap_state" in tabs:
            _rig.bq(f"DELETE FROM {self.fq}._apitap_state` WHERE dest_table = '{T}'")
        if "_apitap_lease" in tabs:
            _rig.bq(f"DELETE FROM {self.fq}._apitap_lease` WHERE dest_key = '{_rig.BQ_DATASET}.{T}'")


E = {"pg": Pg, "ch": Ch, "bq": Bq}[ENGINE]()
_SLOTS = set(src("SELECT slot_name FROM pg_replication_slots").split())


def run(py, table, mode, dest_table=None, env=None, background=False):
    code = ("import apitap, sys\n"
            "try:\n"
            f"    r = apitap.transfer({PG!r}, {E.url!r}, table={table!r}, mode={mode!r}"
            + (f", dest_table={dest_table!r}" if dest_table else "") + ")\n"
            "    print('ROWS', r.rows, flush=True)\n"
            "except Exception as e:\n"
            "    print('RAISED', type(e).__name__, str(e).replace(chr(10), ' ')[:600], flush=True)\n"
            "    sys.exit(1)\n")
    e = dict(os.environ, APITAP_LEASE_TTL_SECS=str(TTL), **(env or {}))
    if background:
        return subprocess.Popen([py, "-c", code], env=e, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
    return subprocess.run([py, "-c", code], env=e, capture_output=True, text=True, timeout=1800)


def tail(r):
    return ((r.stdout or "") + " " + (r.stderr or "")).strip()[-400:]


def locks():
    return [n for n in E.names() if n.endswith("__apitap_lock")]


def markers():
    return [n for n in E.names() if n.endswith("__apitap_staging")
            and len(n) == len(T) + 16 + len("__apitap_staging") and n[len(T) + 8] == "l"]


def swap_staging():
    """Any REPLACE run's staging beside T, raw — on BigQuery a worker's `_N`."""
    out = []
    for n in E.names():
        base = n.rsplit("_", 1)[0] if n.rsplit("_", 1)[-1].isdigit() else n
        if base.endswith("__apitap_staging") and len(base) == len(T) + 16 + len("__apitap_staging") \
                and base[len(T) + 8] == "r":
            out.append(n)
    return out


def reset(rows=100):
    E.clean()
    for p in src(f"SELECT DISTINCT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split():
        src(f"DROP PUBLICATION IF EXISTS {p}")
    src(f"DROP TABLE IF EXISTS {T} CASCADE")
    for s in set(src("SELECT slot_name FROM pg_replication_slots").split()) - _SLOTS:
        src(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
            f"WHERE slot_name = '{s}' AND NOT active")
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    src(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1,{rows}) g")


def backlog(n, start):
    for lo in range(start, start + n, 10_000):
        src(f"INSERT INTO {T} SELECT g, 'w'||g FROM generate_series({lo},{min(lo + 9_999, start + n - 1)}) g")


def held_new_drain():
    """A NEW drain with a backlog, stopped the moment its announcement is on the
    server — a drain in flight, held there as long as the case needs. Waiting on
    the LOCK, which every drain since 0.56.0 writes, so that run against the
    0.56.0 wheel as NEW (the RED control) the case still happens and fails on
    what it asserts, not on its precondition."""
    boot = run(NEW_PY, T, "log_based")
    if boot.returncode:
        _rig.rig_fail(f"NEW bootstrap: {tail(boot)}")
    backlog(200_000, 1_000)
    d = run(NEW_PY, T, "log_based", env={"APITAP_CDC_WINDOW_BYTES": "65536"}, background=True)
    if not _rig.wait_for(lambda: d.poll() is not None or bool(locks()), 60, step=0.02) \
            or d.poll() is not None:
        d.kill()
        _rig.rig_fail(f"the NEW drain never showed its lock (rc={d.poll()}, names={E.names()})")
    os.kill(d.pid, signal.SIGSTOP)
    # The marker is created right after the lock by the same announcement.
    _rig.wait_for(lambda: bool(markers()), 2, step=0.05)
    return d


E.setup()
src(f"DROP TABLE IF EXISTS {BIG}")
src(f"CREATE TABLE {BIG} (id int PRIMARY KEY, v text)")
src(f"INSERT INTO {BIG} SELECT g, repeat('x', 200) FROM generate_series(1, 1000000) g")
try:
    print(f"== (a) a NEW drain holds {T}: an OLD {OLD_V} replace is refused ==", flush=True)
    reset()
    d = held_new_drain()
    case("the NEW drain announced a lock AND a staging marker", locks() and markers(),
         f"locks={locks()} markers={markers()}")
    old = run(OLD_PY, T, "replace", background=True)
    seen = []
    while old.poll() is None:
        seen += [n for n in swap_staging() if n not in seen]
        time.sleep(0.02)
    out = old.stdout.read() + old.stderr.read()
    case("the OLD replace is refused", old.returncode != 0 and "locked" in out.lower()
         and "draining changes into it" in out, out.strip()[-220:])
    case("and its staging never appeared beside the table", seen == [], f"{seen}")
    os.kill(d.pid, signal.SIGCONT)
    d.wait(600)
    case("the NEW drain then finishes", d.returncode == 0, (d.stderr.read() or "")[-200:])
    want = src(f"SELECT count(*) FROM {T}")
    case("and the destination equals the source", E.count() == want, f"{E.count()} vs {want}")

    print(f"== (b) an OLD {OLD_V} replace is loading {T}: a NEW drain is refused ==", flush=True)
    reset()
    old = run(OLD_PY, BIG, "replace", dest_table=T, background=True)
    if not _rig.wait_for(lambda: old.poll() is not None or bool(swap_staging()), 120, step=0.05) \
            or old.poll() is not None:
        old.kill()
        _rig.rig_fail(f"the OLD replace never showed staging while loading (rc={old.poll()})")
    os.kill(old.pid, signal.SIGSTOP)
    r = run(NEW_PY, T, "log_based")
    case("the NEW drain is refused BY TYPE", "RAISED LockedError" in r.stdout, tail(r)[-220:])
    os.kill(old.pid, signal.SIGCONT)
    old.wait(1800)
    case("and the OLD replace finishes", old.returncode == 0, (old.stderr.read() or "")[-200:])

    if OLD_V == "0551" and ENGINE == "pg":
        print("== (c) an OLD 0.55.1 drain writes no artifact at all — the documented hole ==",
              flush=True)
        reset()
        boot = run(OLD_PY, T, "log_based")
        backlog(40_000, 1_000)
        d = run(OLD_PY, T, "log_based", env={"APITAP_CDC_WINDOW_BYTES": "262144"}, background=True)
        seen = []
        while d.poll() is None:
            seen += [n for n in E.names() if n.endswith(("__apitap_lock", "__apitap_staging"))
                     and n not in seen]
            time.sleep(0.2)
        case("(rig) the OLD drain ran to completion", boot.returncode == 0 and d.returncode == 0,
             (d.stderr.read() or "")[-160:])
        case("and nothing was ever beside the table for a peer to see", seen == [], f"{seen}")

        print("== (d) rolling back to 0.55.1 replicates _apitap_lease as user data ==", flush=True)
        # The lease table is only copied while it holds rows (0.55.1 skips an
        # empty table), and it does hold one for good once any drain has been
        # collected: a collector never deletes the victim's row. So: a drain
        # killed outright, then collected by the next run — the ordinary way a
        # destination comes to carry a collected row.
        reset()
        d = held_new_drain()
        d.kill()
        d.wait()
        time.sleep(TTL + 3)
        r = run(NEW_PY, T, "log_based")
        case("(rig) the next NEW drain collected the killed one and ran", r.returncode == 0,
             tail(r)[-160:])
        case("(rig) the collected row is there", E.q("SELECT count(*) FROM _apitap_lease "
                                                    "WHERE collected") != "0")
        scratch = "ru_scratch"
        _rig.psql(f"DROP DATABASE IF EXISTS {scratch} WITH (FORCE)", ("apitap-bench-pg-dst", "postgres"))
        _rig.psql(f"CREATE DATABASE {scratch}", ("apitap-bench-pg-dst", "postgres"))
        # Everything in the schema, the way a 0.55.1 "copy this database" job
        # reads it: 0.55.1 hides `_apitap_state` from discovery and nothing else.
        roll = ("import apitap; apitap.transfer('postgres://postgres:bench@127.0.0.1:5545/ru_dst', "
                f"'postgres://postgres:bench@127.0.0.1:5545/{scratch}', schema='public', mode='replace')")
        r = subprocess.run([OLD_PY, "-c", roll], capture_output=True, text=True, timeout=600)
        case("(rig) the 0.55.1 schema copy ran", r.returncode == 0, tail(r)[-200:])
        got = _rig.psql("SELECT relname FROM pg_class WHERE relkind = 'r' AND relname = '_apitap_lease'",
                        ("apitap-bench-pg-dst", scratch))
        case("0.55.1 copied _apitap_lease as if a user had made it", got == "_apitap_lease", got or "absent")
        live = E.q("SELECT count(*) FROM _apitap_lease WHERE NOT collected AND expires_at > now()")
        case("(rig) the documented pre-flight finds no live lease", live == "0", live)
        E.q("DROP TABLE IF EXISTS _apitap_lease")
        E.q("DROP TABLE IF EXISTS _apitap_cdc_pending")
        _rig.psql(f"DROP DATABASE IF EXISTS {scratch} WITH (FORCE)", ("apitap-bench-pg-dst", "postgres"))
        _rig.psql(f"CREATE DATABASE {scratch}", ("apitap-bench-pg-dst", "postgres"))
        r = subprocess.run([OLD_PY, "-c", roll], capture_output=True, text=True, timeout=600)
        case("(rig) the second 0.55.1 schema copy ran", r.returncode == 0, tail(r)[-200:])
        got = _rig.psql("SELECT relname FROM pg_class WHERE relkind = 'r' AND relname = '_apitap_lease'",
                        ("apitap-bench-pg-dst", scratch))
        case("after the documented DROPs it is not copied", got == "", got or "absent")
        _rig.psql(f"DROP DATABASE IF EXISTS {scratch} WITH (FORCE)", ("apitap-bench-pg-dst", "postgres"))

    print(f"== (e) a NEW drain killed outright; the OLD {OLD_V} run beside its remains ==", flush=True)
    reset()
    d = held_new_drain()
    wm_before = E.watermark()
    d.kill()
    d.wait()
    left_l, left_m = locks(), markers()
    case("the killed drain left its lock and its marker", left_l and left_m,
         f"locks={left_l} markers={left_m}")
    time.sleep(TTL + 3)
    # 0.56.0 is the release a rollback goes to, and a drain is what it runs;
    # 0.55.1's drains see nothing at all (case c), so its replace is asked.
    r = run(OLD_PY, T, "log_based" if OLD_V == "0560" else "replace")
    case(f"the OLD {OLD_V} run is refused, naming the marker",
         r.returncode != 0 and "__apitap_staging" in tail(r), tail(r)[-240:])
    case("and the marker is still there for the next run to collect", markers() == left_m,
         f"{markers()}")
    r = run(NEW_PY, T, "log_based")
    case("a NEW run after the TTL collects what is left and resumes", r.returncode == 0,
         tail(r)[-200:])
    case("the lock and the marker are both gone", locks() == [] and markers() == [],
         f"{E.names()}")
    wm_after = E.watermark()
    case("and the watermark advanced", wm_after not in ("", wm_before), f"{wm_before} -> {wm_after}")
finally:
    print("== cleanup ==", flush=True)
    try:
        reset(rows=0)
        src(f"DROP TABLE IF EXISTS {T} CASCADE")
        E.clean()
    finally:
        src(f"DROP TABLE IF EXISTS {BIG}")

print(f"\nROLLING UPGRADE E2E ({OLD_V}->{ENGINE}): " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
