"""MySQL 8.4, the version a new install gets, exercised in both directions.

apitap once HUNG against MySQL 8.4 — no error, no timeout, no output. The fix
shipped in the profiling retune (`mysql_async` 0.37 plus a 30 s loud deadline on
every sink connection) but no gate leg ever ran against 8.4, so from that day to
this the fix has been believed rather than proven, and a regression would have
been invisible until a user hit it. The bench rig has 8.0 and MariaDB 10.6 and
nothing else.

A hang is the failure mode, so every leg here carries its own deadline and
reports a timeout as a FAILURE with the elapsed time. A leg that simply blocks
until the harness kills the script would look like an infrastructure problem
rather than the bug.

  leg 0  the rig is really 8.4     — asked of the SERVER, not of the image tag
  leg 1  8.4 as DESTINATION, bulk  — this is where the hang was: the sink connect
  leg 2  8.4 as DESTINATION, CDC   — the log_based apply path into 8.4
  leg 3  8.4 as SOURCE, binlog CDC — bootstrap, change, drain, exact compare

Leg 1 is the one that matters most and is listed first for that reason: the
original silence was a sink connection that never returned.

Rig: `apitap-bench-my84` on :3310 (MySQL 8.4), `apitap-bench-pg-src` on :5544,
ClickHouse on :8124.
"""
import os
import subprocess
import sys
import time

MY84 = os.environ.get("MY84_URL", "mysql://root:bench@127.0.0.1:3310/bench")
PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
MY_C = os.environ.get("MY84_CONTAINER", "apitap-bench-my84")
PG_C = "apitap-bench-pg-src"

# Generous, but finite. Every one of these runs in about a second when it works;
# the point of the number is that a hang ends the leg instead of the script.
DEADLINE = int(os.environ.get("MY84_DEADLINE", "120"))

SRC_T = "my84_src"      # on 8.4, read by CDC
DST_T = "my84_dst"      # on 8.4, written to
N = 5000

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def my(sql, check=True):
    o = sh(["docker", "exec", "-i", MY_C, "mysql", "-uroot", "-pbench",
            "-N", "-D", "bench", "-e", sql])
    if check and o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def pg(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
            "-d", PG.rsplit("/", 1)[-1], "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)
def _slots_now():
    return set(pg("SELECT slot_name FROM pg_replication_slots").split())


# Slots that existed BEFORE this leg started. Anything else is ours.
#
# The blanket `SELECT pg_drop_replication_slot(slot_name) ... WHERE NOT active`
# that used to be here is a live grenade on a shared rig: a CDC job that is
# merely BETWEEN drains has an inactive slot, and dropping it destroys its WAL
# continuity. It took out a running 24 h soak on 2026-08-20. Scope the cleanup
# to what this leg made.
_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for s in sorted(_slots_now() - _SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{s}')"
           f" FROM pg_replication_slots WHERE slot_name='{s}' AND NOT active")



def transfer(src, dst, table, mode="replace", dest_table=None):
    """Run one transfer under a deadline; a hang comes back as a timeout, loudly."""
    code = (
        "import apitap\n"
        f"r = apitap.transfer({src!r}, {dst!r}, table={table!r}, mode={mode!r}"
        + (f", dest_table={dest_table!r}" if dest_table else "")
        + ")\n"
        "print('ROWS', r.rows, flush=True)\n"
    )
    t0 = time.time()
    try:
        o = sh([sys.executable, "-c", code], timeout=DEADLINE)
        return dict(hung=False, rc=o.returncode, out=o.stdout, err=o.stderr,
                    secs=time.time() - t0)
    except subprocess.TimeoutExpired:
        return dict(hung=True, rc=-1, out="", err="", secs=time.time() - t0)


# ---------------------------------------------------------------------------
print("== leg 0: the rig is really MySQL 8.4 ==")
# Ask the server. An image tag is a label someone typed; VERSION() is the thing
# under test telling you what it is.
ver = my("SELECT VERSION()")
case("the server reports 8.4", ver.startswith("8.4"), ver)
case("binlog is on and ROW-formatted",
     my("SELECT @@log_bin") == "1" and my("SELECT @@binlog_format") == "ROW",
     f"log_bin={my('SELECT @@log_bin')} format={my('SELECT @@binlog_format')}")

# ---------------------------------------------------------------------------
print("== leg 1: 8.4 as DESTINATION, bulk — where the silence used to be ==")
pg("DROP TABLE IF EXISTS my84_feed")
pg("CREATE TABLE my84_feed (id int primary key, v text, n bigint)")
pg(f"INSERT INTO my84_feed SELECT g, 'v'||g, g*7 FROM generate_series(1,{N}) g")
my(f"DROP TABLE IF EXISTS {DST_T}")

r = transfer(PG, MY84, "my84_feed", "replace", dest_table=DST_T)
case("the sink connection returns at all", not r["hung"],
     f"HUNG — no answer in {DEADLINE}s, which is the original bug" if r["hung"]
     else f"{r['secs']:.1f}s")
if not r["hung"]:
    case("the bulk load succeeds", r["rc"] == 0, r["err"].strip()[-300:])
    landed = my(f"SELECT count(*) FROM {DST_T}", check=False)
    case("every row landed", landed == str(N), f"{landed} of {N}")
    s_sum = pg("SELECT coalesce(sum(n),0)::text FROM my84_feed")
    d_sum = my(f"SELECT coalesce(sum(n),0) FROM {DST_T}", check=False)
    case("and the values agree", s_sum == d_sum, f"pg={s_sum} my84={d_sum}")

# ---------------------------------------------------------------------------
print("== leg 2: 8.4 as DESTINATION, log_based apply ==")
drop_our_slots()
my("DROP TABLE IF EXISTS my84_feed", check=False)
# Dropping the destination TABLE is not enough: the watermark lives in the
# destination's _apitap_state, and a watermark whose slot no longer exists is
# exactly what apitap refuses (correctly) to drain against. The first version of
# this leg cleared only the table and failed on its own second run with
# "destination has a watermark but slot ... is GONE".
my("DELETE FROM _apitap_state WHERE dest_table='my84_feed'", check=False)
r = transfer(PG, MY84, "my84_feed", "log_based")
case("the CDC bootstrap into 8.4 returns", not r["hung"],
     f"HUNG after {DEADLINE}s" if r["hung"] else f"{r['secs']:.1f}s")
if not r["hung"]:
    case("the bootstrap succeeds", r["rc"] == 0, r["err"].strip()[-300:])
    pg(f"UPDATE my84_feed SET n = n + 1 WHERE id <= 100")
    pg(f"DELETE FROM my84_feed WHERE id > {N - 50}")
    r = transfer(PG, MY84, "my84_feed", "log_based")
    case("the CDC drain into 8.4 returns", not r["hung"],
         f"HUNG after {DEADLINE}s" if r["hung"] else f"{r['secs']:.1f}s")
    if not r["hung"]:
        case("the drain succeeds", r["rc"] == 0, r["err"].strip()[-300:])
        s_n = pg("SELECT count(*) FROM my84_feed")
        d_n = my("SELECT count(*) FROM my84_feed", check=False)
        s_sum = pg("SELECT coalesce(sum(n),0)::text FROM my84_feed")
        d_sum = my("SELECT coalesce(sum(n),0) FROM my84_feed", check=False)
        case("the update and the delete both arrived",
             s_n == d_n and s_sum == d_sum,
             f"n {s_n}/{d_n} sum {s_sum}/{d_sum}")

# ---------------------------------------------------------------------------
print("== leg 3: 8.4 as SOURCE, binlog CDC ==")
my(f"DROP TABLE IF EXISTS {SRC_T}")
my(f"CREATE TABLE {SRC_T} (id INT PRIMARY KEY, v TEXT, n BIGINT)")
my(f"INSERT INTO {SRC_T} (id,v,n) VALUES " +
   ",".join(f"({g},'v{g}',{g * 3})" for g in range(1, 1001)))
ch(f"DROP TABLE IF EXISTS {SRC_T}")
ch(f"DROP TABLE IF EXISTS `{SRC_T}__apitap_cdc_del`")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{SRC_T}' SETTINGS mutations_sync=1")

r = transfer(MY84, CH, SRC_T, "log_based")
case("the binlog bootstrap from 8.4 returns", not r["hung"],
     f"HUNG after {DEADLINE}s" if r["hung"] else f"{r['secs']:.1f}s")
if not r["hung"]:
    case("the bootstrap succeeds", r["rc"] == 0, r["err"].strip()[-300:])
    my(f"UPDATE {SRC_T} SET n = n + 1000 WHERE id <= 200")
    my(f"DELETE FROM {SRC_T} WHERE id > 950")
    my(f"INSERT INTO {SRC_T} (id,v,n) VALUES " +
       ",".join(f"({g},'new{g}',{g})" for g in range(2001, 2101)))
    r = transfer(MY84, CH, SRC_T, "log_based")
    case("the binlog drain from 8.4 returns", not r["hung"],
         f"HUNG after {DEADLINE}s" if r["hung"] else f"{r['secs']:.1f}s")
    if not r["hung"]:
        case("the drain succeeds", r["rc"] == 0, r["err"].strip()[-300:])
        s_n = my(f"SELECT count(*) FROM {SRC_T}", check=False)
        d_n = ch(f"SELECT count() FROM {SRC_T}")
        s_sum = my(f"SELECT coalesce(sum(n),0) FROM {SRC_T}", check=False)
        d_sum = ch(f"SELECT sum(n) FROM {SRC_T}")
        case("insert, update and delete all agree with the source",
             s_n == d_n and s_sum == d_sum, f"n {s_n}/{d_n} sum {s_sum}/{d_sum}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
pg("DROP TABLE IF EXISTS my84_feed")
drop_our_slots()
my(f"DROP TABLE IF EXISTS {SRC_T}", check=False)
my(f"DROP TABLE IF EXISTS {DST_T}", check=False)
my("DROP TABLE IF EXISTS my84_feed", check=False)
my("DELETE FROM _apitap_state WHERE dest_table IN ('my84_feed','" + DST_T + "')", check=False)
ch(f"DROP TABLE IF EXISTS {SRC_T}")
ch(f"DROP TABLE IF EXISTS `{SRC_T}__apitap_cdc_del`")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{SRC_T}' SETTINGS mutations_sync=1")

print("\nMYSQL 8.4 E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
