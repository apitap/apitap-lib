"""The MySQL replication plane's liveness contract.

Three findings, one theme: a binlog stream that has gone wrong must SAY so.

  leg 1  retention gauge     — every run reports how many binlog files stand
                               between its resume position and the purge edge.
                               The Postgres lane has printed its slot's WAL
                               forever; the MySQL lane printed nothing until the
                               position was already gone, at which point the
                               only answer is a full re-bootstrap.
  leg 2  the purge-edge warn — when the file we resume from IS the oldest the
                               server has, that is one rotation from data loss
                               and must be a warning, not a number to notice.
  leg 3  overlapping runs    — two drains from one source must both make
                               progress. With a server_id derived only from the
                               pipeline, MySQL evicts whichever replica
                               registered first, so two runs knocked each other
                               off the socket in a loop.

Leg 3 is the one that needed the design to change rather than a value to be
tuned: the old id was deliberately STABLE (its test said so), and stability is
exactly what makes two connections collide.

Rig: `apitap-bench-my` on :3307 (MySQL 8.0), ClickHouse on :8124.
"""
import os
import re
import subprocess
import sys
import threading

MY = os.environ.get("MY_URL", "mysql://root:bench@127.0.0.1:3307/bench")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
MY_C = "apitap-bench-my"
T = "my_liveness"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def my(sql, check=True):
    o = sh(["docker", "exec", "-i", MY_C, "mysql", "-uroot", "-pbench",
            "-N", "-D", "bench", "-e", sql])
    if check and o.returncode and "Unknown table" not in o.stderr:
        raise RuntimeError(f"{sql[:90]} -> {o.stdout.strip()[-160:]} {o.stderr.strip()[-200:]}")
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def drain(env_extra=None):
    code = ("import apitap\n"
            f"r = apitap.transfer({MY!r}, {CH!r}, table={T!r}, mode='log_based')\n"
            "print('ROWS', r.rows, flush=True)\n")
    env = dict(os.environ, APITAP_PROGRESS="1")
    if env_extra:
        env.update(env_extra)
    return sh([sys.executable, "-c", code], env=env)


def reset():
    my(f"DROP TABLE IF EXISTS {T}", check=False)
    my(f"CREATE TABLE {T} (id INT PRIMARY KEY, v TEXT)")
    my(f"INSERT INTO {T} (id,v) VALUES " + ",".join(f"({g},'v{g}')" for g in range(1, 201)))
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS `{T}__apitap_cdc_del`")
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


# ---------------------------------------------------------------------------
print("== setup ==")
reset()
r = drain()
case("bootstrap", r.returncode == 0 and ch(f"SELECT count() FROM {T}") == "200",
     r.stderr.strip()[-200:] if r.returncode else ch(f"SELECT count() FROM {T}"))

# ---------------------------------------------------------------------------
print("== leg 1: every run reports its binlog retention headroom ==")
my(f"INSERT INTO {T} (id,v) VALUES (900,'x')")
r = drain()
case("the drain runs", r.returncode == 0, r.stderr.strip()[-200:])
gauge = [ln for ln in r.stderr.splitlines() if "binlog.retention" in ln]
case("a binlog.retention gauge is emitted", bool(gauge),
     "" if gauge else "no gauge line — an operator has no number to alert on")
if gauge:
    line = gauge[-1]
    print(f"      {line.strip()[:150]}")
    for field in ("resume_file", "files_retained", "files_before_ours",
                  "retained_bytes", "at_purge_edge"):
        case(f"  gauge carries {field}", field in line)
    m = re.search(r"files_retained=(\d+)", line)
    server_files = my("SHOW BINARY LOGS").count("\n") + 1
    case("  files_retained agrees with SHOW BINARY LOGS",
         m and int(m.group(1)) == server_files,
         f"gauge={m.group(1) if m else '?'} server={server_files}")

# ---------------------------------------------------------------------------
print("== leg 2: sitting on the oldest binlog must WARN, not just measure ==")
# Rotate so several files exist, then purge everything except the newest — the
# resume position then lands in the oldest surviving file, which is exactly the
# one-rotation-from-loss shape.
before = my("SHOW BINARY LOGS")
case("(rig) the server has more than one binlog to work with",
     before.count("\n") >= 1, f"{before.count(chr(10)) + 1} files")
my(f"INSERT INTO {T} (id,v) VALUES (901,'y')")
my("FLUSH BINARY LOGS")
my(f"INSERT INTO {T} (id,v) VALUES (902,'z')")
r = drain()
case("the drain after a rotation runs", r.returncode == 0, r.stderr.strip()[-200:])
edge = [ln for ln in r.stderr.splitlines() if "binlog.retention" in ln]
if edge:
    at_edge = "at_purge_edge=true" in edge[-1]
    print(f"      {edge[-1].strip()[:150]}")
    # Not asserting WHICH way it lands — that depends on where the drain
    # resumed — but the two must agree: the warning appears exactly when the
    # gauge says we are at the edge.
    warned = any("OLDEST binlog" in ln for ln in r.stderr.splitlines())
    case("the warning fires exactly when the gauge says purge-edge",
         warned == at_edge, f"at_purge_edge={at_edge} warned={warned}")

# ---------------------------------------------------------------------------
print("== leg 3: two pipelines off one source must not evict each other ==")
# The scenario the finding names: ONE MySQL source, TWO pipelines. Each drains
# the same table into its OWN destination table, which is the shape a fan-out
# has (one source feeding two warehouses) and the shape an overrunning schedule
# approximates.
#
# Deliberately NOT two drains into the SAME destination table: that contends on
# the destination's own state objects, which nothing has ever promised to
# tolerate — docs/usage.md says so — and a first version of this leg failed on
# exactly that, blaming the socket for a ClickHouse mutation conflict. The
# server_id property under test is about the SOURCE's replica registry, so the
# experiment has to isolate it.
my(f"INSERT INTO {T} (id,v) SELECT n, 'p' FROM ("
   + " UNION ALL ".join(f"SELECT {g} AS n" for g in range(1000, 1100)) + ") s")

DESTS = (f"{T}_p1", f"{T}_p2")
for d in DESTS:
    ch(f"DROP TABLE IF EXISTS {d}")
    ch(f"DROP TABLE IF EXISTS `{d}__apitap_cdc_del`")
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{d}' SETTINGS mutations_sync=1")

results = {}


def worker(dest):
    code = ("import apitap\n"
            f"r = apitap.transfer({MY!r}, {CH!r}, table={T!r}, "
            f"dest_table={dest!r}, mode='log_based')\n"
            "print('ROWS', r.rows, flush=True)\n")
    results[dest] = sh([sys.executable, "-c", code],
                       env=dict(os.environ, APITAP_PROGRESS="1"))


threads = [threading.Thread(target=worker, args=(d,)) for d in DESTS]
for t in threads:
    t.start()
for t in threads:
    t.join(300)

for d in DESTS:
    r = results.get(d)
    if r is None:
        case(f"{d}: finished", False, "still running after 300s")
        continue
    evicted = any(sig in (r.stderr or "") for sig in
                  ("Connection reset", "connection reset", "closed mid-packet",
                   "binlog stream closed"))
    case(f"{d}: kept its binlog socket (no server_id eviction)", not evicted,
         (r.stderr.strip().splitlines() or [""])[-1][:150] if evicted else "")
    last = [ln for ln in (r.stderr or "").strip().splitlines() if ln.strip()]
    case(f"{d}: the drain succeeded", r.returncode == 0,
         last[-1][:200] if r.returncode and last else "")

src_n = my(f"SELECT count(*) FROM {T}")
for d in DESTS:
    got = ch(f"SELECT count() FROM {d}")
    case(f"{d}: landed every row the source has", got == src_n,
         f"my={src_n} ch={got}")

for d in DESTS:
    ch(f"DROP TABLE IF EXISTS {d}")
    ch(f"DROP TABLE IF EXISTS `{d}__apitap_cdc_del`")
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{d}' SETTINGS mutations_sync=1")

# ---------------------------------------------------------------------------
print("== cleanup ==")
my(f"DROP TABLE IF EXISTS {T}", check=False)
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"DROP TABLE IF EXISTS `{T}__apitap_cdc_del`")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")

print("\nMYSQL LIVENESS E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
