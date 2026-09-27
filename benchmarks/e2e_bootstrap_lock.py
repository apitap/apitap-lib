"""A CDC bootstrap keeps the drain's lock to its last statement.

The first run of a drain is a full load. 0.56.0 released the drain's lock and
lease before that load — the load is a bulk `replace`, whose own guard would
otherwise have refused the drain's lock — and let the bulk lock stand in for
it. So for the whole first run (the long part, and `bootstrap_finish` after
it) the table was held by nothing the drain owned: another drain of the same
table could start beside it, with the slot and the watermark still in play.

0.57.0 runs the full load as a NESTED run (`transfer_within`): its own token
names the drain as its parent, so the drain's lock and marker are
`Found::Parent` to it — never a peer, never deleted — and the drain keeps them,
renewed, until its exit arm. This leg watches the destination catalog through
the whole first run of a two-table group and asks it:

  F1  the moment the load's staging exists, the drain's lock is there too
  F2  while `bootstrap_finish` is held at its `_apitap_state` write, the lock
      is there, its lease is live, and a second drain of the group is refused
      by type — and the lock is still there after that refusal
  F3  afterwards: two log_based state rows, a primary key on the loaded table,
      no lock, no lease row for the run

    python benchmarks/e2e_bootstrap_lock.py <pg|my>

`pg`: sources on Postgres :5544. `my`: the same tables on MariaDB :3309 (a
MySQL-source drain bootstraps through its own path). Destination: Postgres
:5545 for both. RED: 0.56.0 — F1 sees 0 drain locks during the load.
"""
import os
import subprocess
import sys
import time

import psycopg2

import _rig

ENGINE = sys.argv[1]
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
SRC = {"pg": "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src",
       "my": "mysql://root:bench@127.0.0.1:3309/bench"}[ENGINE]
A, B = "bl_a", "bl_b"
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} [{ENGINE}] {name}: {detail}", flush=True)


def dst(sql):
    return _rig.psql(sql, _rig.PG_DST)


def src(sql):
    if ENGINE == "pg":
        return _rig.psql(sql, _rig.PG_SRC)
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench",
                        "-N", "-D", "bench", "-e", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def kind_count(table, suffix, letter):
    """Artifacts of `table` with `suffix` whose token is of kind `letter`."""
    return int(dst(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = 'public' AND c.relname LIKE '{table}\\_%{suffix.replace('_', chr(92) + '_')}' "
        f"AND length(c.relname) = {len(table) + 16 + len(suffix)} "
        f"AND substr(c.relname, {len(table) + 9}, 1) = '{letter}'"))


def drain_token():
    rows = dst("SELECT relname FROM pg_class WHERE relname LIKE 'bl\\_a\\_%\\_\\_apitap\\_lock' "
               f"AND substr(relname, {len(A) + 9}, 1) = 'l'").split()
    return rows[0][len(A):len(A) + 16] if rows else None


_SLOTS = set(_rig.psql("SELECT slot_name FROM pg_replication_slots", _rig.PG_SRC).split())


def clean():
    for t in (A, B, "bl_warm"):
        src(f"DROP TABLE IF EXISTS {t}")
        for n in dst(f"SELECT relname FROM pg_class WHERE relkind = 'r' AND relname LIKE '{t}%'").split():
            if n:
                dst(f'DROP TABLE IF EXISTS "{n}" CASCADE')
    for t, w in (("_apitap_state", "dest_table ~ '^(public\\.)?bl_(a|b|warm)$'"),
                 ("_apitap_lease", "dest_key IN ('public.bl_a', 'public.bl_b', 'public.bl_warm')")):
        if dst(f"SELECT to_regclass('public.{t}') IS NOT NULL") == "t":
            dst(f"DELETE FROM public.{t} WHERE {w}")
    if ENGINE == "pg":
        for p in _rig.psql("SELECT DISTINCT pubname FROM pg_publication_tables "
                           "WHERE tablename IN ('bl_a', 'bl_b', 'bl_warm')", _rig.PG_SRC).split():
            _rig.psql(f"DROP PUBLICATION IF EXISTS {p}", _rig.PG_SRC)
        for s in set(_rig.psql("SELECT slot_name FROM pg_replication_slots", _rig.PG_SRC).split()) - _SLOTS:
            _rig.psql(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
                      f"WHERE slot_name = '{s}' AND NOT active", _rig.PG_SRC)


def spawn():
    code = ("import apitap, sys\n"
            "try:\n"
            f"    apitap.transfer({SRC!r}, {PGD!r}, tables=[{A!r}, {B!r}], mode='log_based')\n"
            "    print('DONE', flush=True)\n"
            "except Exception as e:\n"
            "    print('RAISED', type(e).__name__, str(e).replace(chr(10), ' ')[:400], flush=True)\n"
            "    sys.exit(1)\n")
    return subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)


print(f"== reset ({ENGINE}) ==", flush=True)
clean()
if ENGINE == "pg":
    src(f"CREATE TABLE {A} (id int PRIMARY KEY, v text)")
    src(f"INSERT INTO {A} SELECT g, repeat('x', 100) FROM generate_series(1, 3000000) g")
    src(f"CREATE TABLE {B} (id int PRIMARY KEY, v text)")
    src(f"INSERT INTO {B} SELECT g, 'v'||g FROM generate_series(1, 1000) g")
    src("CREATE TABLE bl_warm (id int PRIMARY KEY)")
    src("INSERT INTO bl_warm VALUES (1)")
else:
    src(f"CREATE TABLE {A} (id INT PRIMARY KEY, v TEXT)")
    src(f"INSERT INTO {A} SELECT seq, REPEAT('x', 100) FROM seq_1_to_3000000")
    src(f"CREATE TABLE {B} (id INT PRIMARY KEY, v TEXT)")
    src(f"INSERT INTO {B} SELECT seq, CONCAT('v', seq) FROM seq_1_to_1000")
    src("CREATE TABLE bl_warm (id INT PRIMARY KEY)")
    src("INSERT INTO bl_warm VALUES (1)")
# Warm-up: `_apitap_state` must exist before F2 can lock it.
w = subprocess.run([sys.executable, "-c",
                    f"import apitap; apitap.transfer({SRC!r}, {PGD!r}, table='bl_warm', mode='log_based')"],
                   capture_output=True, text=True)
if w.returncode or dst("SELECT to_regclass('public._apitap_state') IS NULL") == "t":
    _rig.rig_fail(f"warm-up: {w.stderr[-300:]}")
dst("DELETE FROM public._apitap_state WHERE dest_table ~ '^(public\\.)?bl_warm$'")

holder = None
a = b = None
try:
    print("== F1: the moment the load's staging exists ==", flush=True)
    a = spawn()
    seen = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 300 and a.poll() is None:
        if kind_count(A, "__apitap_staging", "r"):
            seen = kind_count(A, "__apitap_lock", "l")
            break
        time.sleep(0.05)
    if seen is None:
        a.kill()
        _rig.rig_fail(f"the load's staging was never seen (rc={a.poll()}) — the rig is too fast")
    case("the drain's lock is there while its full load runs", seen == 1, f"{seen} drain lock(s)")

    print("== F2: bootstrap_finish held at its state write ==", flush=True)
    holder = psycopg2.connect(host="127.0.0.1", port=5545, user="postgres", password="bench",
                              dbname="apitap_bench_dst")
    holder.autocommit = False
    holder.cursor().execute("LOCK TABLE public._apitap_state IN SHARE MODE")
    blocked = _rig.wait_for(lambda: a.poll() is not None or dst(
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
        "AND query ILIKE '%_apitap_state%'") != "0", 120, step=0.1)
    if not blocked or a.poll() is not None:
        _rig.rig_fail(f"the drain never blocked on _apitap_state (rc={a.poll()})")
    stmt = dst("SELECT left(query, 90) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
               "AND query ILIKE '%_apitap_state%' LIMIT 1")
    print(f"      held at: {stmt}", flush=True)
    tok = drain_token()
    case("the drain's lock is still there", kind_count(A, "__apitap_lock", "l") == 1, f"token {tok}")
    live = dst(f"SELECT count(*) FROM public._apitap_lease WHERE dest_key = 'public.{A}' "
               f"AND token = '{tok}' AND NOT collected AND expires_at > now()") if tok else "0"
    case("and its lease is live", live == "1", f"{live} live lease row(s)")
    b = spawn()
    b_out = b.communicate(timeout=300)[0]
    case("a second drain of the group is refused BY TYPE", b.returncode != 0 and
         "RAISED LockedError" in b_out, b_out.strip()[-200:])
    case("and the lock is still there after the refusal", kind_count(A, "__apitap_lock", "l") == 1)
    holder.commit()
    holder.close()
    holder = None
    a_out = a.communicate(timeout=600)[0]
    case("the first run then finishes", a.returncode == 0, a_out.strip()[-200:])

    print("== F3: afterwards ==", flush=True)
    # A MySQL-source drain also keeps a `server-identity:` row per table; it is
    # bookkeeping about the source server, not a watermark.
    rows = dst("SELECT count(*) FROM public._apitap_state WHERE mode = 'log_based' "
               "AND dest_table IN ('bl_a', 'bl_b', 'public.bl_a', 'public.bl_b') "
               "AND source_id NOT LIKE 'server-identity:%'")
    case("two log_based state rows", rows == "2", rows)
    pk = dst(f"SELECT count(*) FROM pg_index WHERE indrelid = 'public.{A}'::regclass AND indisprimary")
    case(f"{A} has its primary key", pk == "1", pk)
    case("no drain lock is left", kind_count(A, "__apitap_lock", "l") == 0)
    left = dst(f"SELECT count(*) FROM public._apitap_lease WHERE token = '{tok}'") if tok else "?"
    case("and no lease row for the run", left == "0", left)
finally:
    print("== cleanup ==", flush=True)
    if holder is not None:
        holder.rollback()
        holder.close()
    for p in (a, b):
        if p is not None and p.poll() is None:
            p.kill()
            p.wait()
    clean()

print(f"\nBOOTSTRAP LOCK E2E ({ENGINE}): " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
