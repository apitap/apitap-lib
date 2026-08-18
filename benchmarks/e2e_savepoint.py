"""A savepoint rollback inside a STREAMED transaction must not lose the rest.

pgoutput's Stream Abort message carries TWO transaction ids: the top-level one
and the SUBtransaction that aborted. apitap read only the first and removed
that buffer — so a `ROLLBACK TO SAVEPOINT` anywhere inside a streamed
transaction discarded EVERY row the transaction had buffered, applied nothing,
and advanced the watermark anyway. Silent, permanent, and invisible in every
count the run reports.

Reproducing it needs the transaction to be STREAMED, which Postgres only does
once it exceeds `logical_decoding_work_mem` before committing. apitap raises
that GUC to 1 GB on purpose (it keeps big transactions off pg_replslot spill
files), so this leg turns it down via APITAP_DECODE_WORKMEM — without that the
transaction commits before it is ever streamed and the whole file passes while
testing nothing.

  leg 1  the stream really happens   — the run must see Stream messages at all
  leg 2  a savepoint rollback        — refused loudly, not applied silently
  leg 3  the recovery is real        — the message says re-run; re-running must
                                       land exactly the rows the source kept

Leg 3 is what makes leg 2 acceptable. A refusal that leaves no way forward is
just a different way to lose the data.

Rig: `apitap-bench-pg-src` on :5544, ClickHouse on :8124.
"""
import os
import subprocess
import sys

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
PG_C = os.environ.get("PG_CONTAINER", "apitap-bench-pg-src")
T = "sp_demo"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
            "-d", PG.rsplit("/", 1)[-1], "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg_script(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
            "-d", PG.rsplit("/", 1)[-1], "-v", "ON_ERROR_STOP=1"],
           input=sql)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def drain(workmem="64kB", debug=False):
    """One log_based run with streaming FORCED on.

    A small `logical_decoding_work_mem` is not enough on its own — whether a
    given transaction crosses the threshold depends on row widths and on when
    the decoder happens to read it, so a leg built on that alone passes or
    fails for reasons that have nothing to do with the code. Postgres 16 has
    `debug_logical_replication_streaming = immediate`, which streams EVERY
    transaction; apitap passes it through the same startup-options channel it
    uses for the decode budget, so the drain under test really does take the
    streamed path every time.
    """
    env = dict(os.environ, APITAP_DECODE_WORKMEM=workmem)
    if debug:
        env["APITAP_DEBUG"] = "1"
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={T!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code], env=env)


def case(label, good, detail=""):
    global ok
    print(f"   {'✓' if good else '✗'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


# ───────────────────────────────────────────────────────────────────────────
print("== setup: force streaming, then bootstrap ==")
# `user` context, so a plain SET on the server's default reaches every session
# that follows — including the walsender's.
pg("ALTER SYSTEM SET debug_logical_replication_streaming = immediate")
pg("SELECT pg_reload_conf()")
forced = pg("SELECT setting FROM pg_settings WHERE name = "
            "'debug_logical_replication_streaming'")
case("the server is set to stream every transaction", forced == "immediate", forced)

pg(f"DROP TABLE IF EXISTS {T}")
pg(f"CREATE TABLE {T} (id int primary key, v text)")
pg(f"INSERT INTO {T} SELECT g, 'seed'||g FROM generate_series(1,100) g")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")
pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active")

r = drain()
if r.returncode:
    case("bootstrap", False, r.stderr.strip()[-300:])
    print("\nSAVEPOINT E2E: FAILED")
    raise SystemExit(1)
case("bootstrap landed the seed", ch(f"SELECT count() FROM {T}") == "100")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 1: the transaction is really STREAMED, not just large ==")
# Without streaming there is no Stream Abort and nothing under test. The
# server-side proof is that Postgres spilled/streamed: with a 64 kB decode
# budget and 20k rows in one uncommitted transaction, it must.
pg_script(f"""
BEGIN;
INSERT INTO {T} SELECT g, repeat('x', 200) FROM generate_series(1000, 21000) g;
COMMIT;
""")
r = drain(debug=True)
case("a large uncommitted transaction drains", r.returncode == 0,
     r.stderr.strip()[-200:] if r.returncode else f"{ch(f'SELECT count() FROM {T}')} rows")
# The decisive witness is the SERVER's own counter, not the run's exit code.
# pg_stat_replication_slots.stream_txns counts transactions the server decided
# to stream before they committed — if it is zero, nothing below this line
# exercises the Stream Abort path at all and the whole file is theatre.
streamed = pg(
    "SELECT coalesce(max(stream_txns), 0) FROM pg_stat_replication_slots "
    "WHERE slot_name LIKE 'apitap%'"
)
case("the server says it STREAMED the transaction (stream_txns > 0)",
     streamed.isdigit() and int(streamed) > 0,
     f"stream_txns={streamed}")
if not (streamed.isdigit() and int(streamed) > 0):
    print("      ^ without streaming there is no Stream Abort, so legs 2 and 3")
    print("        would pass while proving nothing about the fix under test")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 2: a savepoint rollback inside a streamed transaction ==")
before = ch(f"SELECT count()||'|'||sum(id) FROM {T}")
pg_script(f"""
BEGIN;
INSERT INTO {T} SELECT g, repeat('a', 200) FROM generate_series(30000, 50000) g;
SAVEPOINT sp1;
INSERT INTO {T} SELECT g, repeat('b', 200) FROM generate_series(60000, 60100) g;
ROLLBACK TO SAVEPOINT sp1;
INSERT INTO {T} SELECT g, repeat('c', 200) FROM generate_series(70000, 70100) g;
COMMIT;
""")
src = pg(f"SELECT count(*)||'|'||sum(id) FROM {T}")
r = drain()
dst = ch(f"SELECT count()||'|'||sum(id) FROM {T}")

if r.returncode == 0:
    # Accepting it is only correct if the data actually matches — the old bug
    # accepted it AND lost every row of the transaction.
    case("if it applied, it applied the right rows", src == dst,
         f"src {src} / dst {dst}")
    if src != dst:
        print("      ^ this is the silent loss the leg exists to catch")
else:
    # The whole message, not its last line — it is deliberately multi-line
    # (what happened, then the lever, then what re-running alone will do). The
    # phrase searched for is one only this error produces, and the child
    # process's source contains no SQL, so a traceback cannot supply it.
    named = "rolled back to a savepoint" in r.stderr
    lever = "logical_decoding_work_mem" in r.stderr
    first = [l for l in r.stderr.strip().splitlines() if "rolled back" in l]
    case("refused, and the message names the savepoint", named,
         (first[0].strip()[:150] if first else r.stderr.strip()[-150:]))
    case("…and names a recovery that is not just 'try again'", lever)
    case("nothing was applied behind the refusal", dst == before,
         f"{dst} (was {before})")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 3: the recovery the message promises actually works ==")
# This is the leg that decides whether the refusal above is acceptable. A
# refusal whose recovery does not recover is just a slower way to lose the
# data — and the first version of this message said "re-run", which replays
# the same WAL, streams the same transaction, and stops in the same place
# forever. The lever that actually works is the one that stops the streaming,
# so that is what the message names and what this leg exercises.
pg("ALTER SYSTEM RESET debug_logical_replication_streaming")
pg("SELECT pg_reload_conf()")
r2 = drain(workmem="1GB")
src = pg(f"SELECT count(*)||'|'||sum(id) FROM {T}")
dst = ch(f"SELECT count()||'|'||sum(id) FROM {T}")
case("with room to buffer it, the same window applies cleanly",
     r2.returncode == 0 and src == dst,
     f"src {src} / dst {dst}"
     + ("" if r2.returncode == 0 else " / " + r2.stderr.strip()[-200:]))
rolled_back = ch(f"SELECT count() FROM {T} WHERE id BETWEEN 60000 AND 60100")
case("the rolled-back rows are NOT there", rolled_back == "0", f"{rolled_back} found")

pg(f"DROP TABLE IF EXISTS {T}")
pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active")
ch(f"DROP TABLE IF EXISTS {T}")
# Leave the rig as it was found: every other CDC leg on this server would
# otherwise stream every transaction, which is not what they are measuring.
pg("ALTER SYSTEM RESET debug_logical_replication_streaming")
pg("SELECT pg_reload_conf()")
print("   (server setting restored)")
print("\n" + ("SAVEPOINT E2E: ALL GREEN" if ok else "SAVEPOINT E2E: FAILED"))
raise SystemExit(0 if ok else 1)
