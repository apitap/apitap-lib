"""A drain whose replication connection dies while its apply is still landing
windows: the run must JOIN that apply before it gives the table back.

The drain decodes window N+1 while a spawned task applies window N. 0.56.0
joined that task only at the end of the loop, and a `?` between the spawn and
the join — the standby status write that confirms a window to the slot —
returned past it: the run raised, released its lock and lease, and the apply
task went on committing windows, beside whatever run collected the table next.
0.57.0 runs the loop through `run_overlapped`, which joins the task on every
path, and the tenure's release waits for every open unit besides.

Two ways the connection dies, one case each (pg -> pg):
  A  the walsender is terminated on the source (`pg_terminate_backend`);
  B  the drain's own TCP connections to the source are reset under it
     (`ss -K`) — what a NAT or a load balancer dropping the flow looks like,
     and the path that makes the next standby status write fail at once.

In each: a destination trigger makes every inserted row sleep, so the apply is
far behind the drain; the connection dies once the first window's watermark is
visible. The server is then asked, every 50 ms and for 20 s after the drain
raised:
  - the drain raised;
  - no watermark became visible after it raised;
  - its lock disappeared only after the last watermark appeared;
  - the next run is not refused ("BEHIND") and converges on the source.

The victim runs in a 1-core cgroup namespace when `sudo -n` allows it: the
wheel picks the multi-thread runtime only above 0.6 core of quota (`run_cdc`),
and only there does a detached task outlive the call that raised. Without it the
second assertion is weaker (the runtime dies with the call), and the leg says so.
The victim prints when the call raised and then stays alive 25 s, so a task it
left running keeps its runtime while the watch looks for its commits.

RED on 0.56.0 (APITAP_PY_0560): case B — the standby write failed while window
k was applying, the run returned past the join and dropped its lock, and the
orphan committed window k 12.3 s later. On a Postgres destination 0.56.0's
lease close then queued behind the orphan's row lock, so the call raised only
once that commit landed: the orphan never committed AFTER the raise here, and
"the lock went before the last watermark" is the assertion that separates.
Case A did not reproduce on 0.56.0 — the drain had already read the whole
backlog into its pump when the backend died, so the write that failed was the
final confirmation, after the last window had landed — and stays as a guard.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545.
"""
import os
import subprocess
import sys
import threading
import time

import psycopg2

import _rig

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
T = "orphan_demo"
MARK = "apitap-orphan-victim"
ROWS_PER_TX, TXS, PAD = 100, 30, 2000
SLEEP_MS = 20

ok = True


def case(name, passed, detail=""):
    global ok
    ok &= passed
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def dst(sql):
    return _rig.psql(sql, _rig.PG_DST)


def clean():
    # Only the slot and publication that carry THIS table (a single-table
    # group's publication is its slot's name plus `_pub`).
    for p in src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split():
        if not p:
            continue
        slot = p[: -len("_pub")]
        src(f"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
            f"WHERE slot_name = '{slot}' AND active")
        _rig.wait_for(lambda: src(f"SELECT active FROM pg_replication_slots WHERE slot_name = '{slot}'") != "t", 10)
        src(f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name = '{slot}'")
        src(f"DROP PUBLICATION IF EXISTS {p}")
    src(f"DROP TABLE IF EXISTS {T} CASCADE")
    dst(f"DROP TABLE IF EXISTS {T} CASCADE")
    dst("DROP FUNCTION IF EXISTS orphan_slow() CASCADE")
    for n in dst(f"SELECT relname FROM pg_class WHERE relkind = 'r' AND relname LIKE '{T}\\_%\\_\\_apitap\\_%'").split():
        if n:
            dst(f'DROP TABLE IF EXISTS "{n}"')
    if dst("SELECT to_regclass('_apitap_state') IS NOT NULL") == "t":
        dst(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")
    if dst("SELECT to_regclass('_apitap_lease') IS NOT NULL") == "t":
        dst(f"DELETE FROM _apitap_lease WHERE dest_key LIKE '%.{T}'")


def quota_prefix():
    """argv that runs a command in a 1-core cgroup namespace, or None."""
    if os.path.exists("/sys/fs/cgroup/cpu.max"):
        q = open("/sys/fs/cgroup/cpu.max").read().split()
        if q[0] != "max" and int(q[0]) / int(q[1]) > 0.6:
            return []  # already inside one
    if subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode != 0:
        return None
    return ["sudo", "-n", "systemd-run", "--scope", "-q", "-p", "CPUQuota=100%",
            "unshare", "-Cm", "--propagation", "private", "sh", "-c",
            'umount -l /sys/fs/cgroup; mount -t cgroup2 cgroup2 /sys/fs/cgroup && '
            f'exec setpriv --reuid={os.getuid()} --regid={os.getgid()} --init-groups "$@"', "sh"]


def run_py(code, env, prefix):
    """The victim's command line: its env spelled out (sudo resets it)."""
    envs = [f"{k}={v}" for k, v in env.items() if k.startswith(("APITAP_", "HOME", "PATH"))]
    return (prefix or []) + ["env"] + envs + [sys.executable, "-c", code, MARK]


class Sampler(threading.Thread):
    """(db_clock, visible watermark, lock present) every 50 ms, from the server."""

    def __init__(self):
        super().__init__(daemon=True)
        self.rows, self.stop = [], False
        self.c = psycopg2.connect(DST)
        self.c.autocommit = True

    def run(self):
        cur = self.c.cursor()
        while not self.stop:
            cur.execute(
                "SELECT extract(epoch FROM clock_timestamp())::float8, "
                f"(SELECT max(synced_at)::text FROM _apitap_state WHERE dest_table = '{T}' "
                "AND source_id NOT LIKE 'server-identity:%%'), "
                "(SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relname LIKE "
                f"'{T}\\_%%\\_\\_apitap\\_lock')")
            self.rows.append(cur.fetchone())
            time.sleep(0.05)


def clock():
    """The destination's clock — the one the samples are stamped with."""
    c = psycopg2.connect(DST)
    try:
        cur = c.cursor()
        cur.execute("SELECT extract(epoch FROM clock_timestamp())::float8")
        return cur.fetchone()[0]
    finally:
        c.close()


def source_sockets():
    """The victim's established TCP connections to the source port, by local port."""
    out = subprocess.run(["ss", "-tnpH", "state", "established", "dst", "127.0.0.1:5544"],
                         capture_output=True, text=True).stdout
    ports = []
    for line in out.splitlines():
        if "pid=" not in line:
            continue
        pid = line.split("pid=")[1].split(",")[0]
        try:
            argv = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")
        except OSError:
            continue
        if MARK.encode() in argv:
            ports.append(line.split()[2].rsplit(":", 1)[1])
    return ports


def one_case(label, kill, prefix):
    print(f"\n── case {label}", flush=True)
    clean()
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, pad text)")
    src(f"INSERT INTO {T} SELECT g, 'seed' FROM generate_series(1, 10) g")
    env = dict(os.environ, APITAP_LEASE_TTL_SECS="30", APITAP_CDC_WINDOW_BYTES="1048576")
    code = f"import apitap; apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')"
    # The victim says when the call raised, then stays alive: a task the call
    # left running keeps its runtime — and its commits — only while the
    # process lives, and the watch after the raise is what would see them.
    victim_code = (
        "import sys, time, apitap\n"
        "try:\n"
        f"    apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode='log_based')\n"
        "except Exception as e:\n"
        "    print('RAISED', flush=True)\n"
        "    print(f'{type(e).__name__}: {e}', file=sys.stderr, flush=True)\n"
        "    time.sleep(25)\n"
        "    sys.exit(1)\n")
    boot = subprocess.run(run_py(code, env, prefix), capture_output=True, text=True, env=env)
    if boot.returncode:
        _rig.rig_fail(f"bootstrap: {boot.stderr.strip()[-300:]}")
    dst(f"CREATE FUNCTION orphan_slow() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN PERFORM pg_sleep({SLEEP_MS / 1000}); RETURN NEW; END $$")
    dst(f"CREATE TRIGGER orphan_slow BEFORE INSERT ON {T} FOR EACH ROW EXECUTE FUNCTION orphan_slow()")
    # One transaction per 100 rows: a window never splits a source
    # transaction, so a backlog in one statement would be one window.
    for i in range(TXS):
        lo = 1000 + i * ROWS_PER_TX
        src(f"INSERT INTO {T} SELECT g, repeat('x', {PAD}) FROM generate_series({lo + 1}, {lo + ROWS_PER_TX}) g")
    total = 10 + TXS * ROWS_PER_TX

    s = Sampler()
    s.start()
    time.sleep(0.3)
    base = s.rows[-1][1]
    victim = subprocess.Popen(run_py(victim_code, env, prefix), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, env=env)
    raised, errs = [], []

    def watch_raise():
        for line in victim.stdout:
            if line.strip() == "RAISED":
                raised.append(clock())

    def drain_stderr():
        errs.append(victim.stderr.read())

    readers = [threading.Thread(target=f, daemon=True) for f in (watch_raise, drain_stderr)]
    for r in readers:
        r.start()
    if not _rig.wait_for(lambda: s.rows[-1][1] != base, 90, 0.05):
        victim.kill()
        s.stop = True
        _rig.rig_fail(f"no window landed in 90 s: {''.join(errs)[-300:]}")
    if not s.rows[-1][2]:
        _rig.rig_fail("the drain holds no lock while it applies")
    kill()
    if not _rig.wait_for(lambda: raised or victim.poll() is not None, 300, 0.05):
        victim.kill()
        _rig.rig_fail("the drain neither raised nor exited in 300 s")
    t_raise = raised[0] if raised else clock()
    time.sleep(20)
    s.stop = True
    s.join()
    rows = s.rows
    try:
        victim.wait(timeout=60)
    except subprocess.TimeoutExpired:
        victim.kill()
        victim.wait()
    for r in readers:
        r.join(timeout=10)
    err = "".join(errs)

    changes = [cur[0] for prev, cur in zip(rows, rows[1:]) if cur[1] != prev[1]]
    last_change = max(changes) if changes else None
    held = [i for i, r in enumerate(rows) if r[2]]
    gone = rows[held[-1] + 1][0] if held and held[-1] + 1 < len(rows) else None
    applied = int(dst(f"SELECT count(*) FROM {T}"))
    case("the drain raised", bool(raised),
         f"rc={victim.returncode} {err.strip().splitlines()[-1][:160] if err.strip() else ''}")
    late = [round(t - t_raise, 2) for t in changes if t > t_raise + 0.2]
    case("no watermark became visible after it raised (20 s watched)", not late,
         f"late advances at +{late} s" if late else f"{len(changes)} advances, all before the raise; "
         f"{applied} of {total} rows applied")
    case("its lock went only after the last watermark appeared",
         gone is not None and last_change is not None and gone >= last_change,
         f"lock gone at {gone - t_raise:+.2f} s, last advance at {last_change - t_raise:+.2f} s"
         if gone and last_change else f"gone={gone} last={last_change}")

    dst(f"DROP TRIGGER orphan_slow ON {T}")
    again = subprocess.run(run_py(code, env, prefix), capture_output=True, text=True, env=env)
    d, n = int(dst(f"SELECT count(*) FROM {T}")), int(src(f"SELECT count(*) FROM {T}"))
    case("the next run is not refused and converges", again.returncode == 0 and d == n,
         f"rc={again.returncode} dest {d} / source {n} {again.stderr.strip()[-200:]}")
    s.c.close()


def main():
    prefix = quota_prefix()
    if prefix is None:
        print("   (note) no sudo -n: the victim runs on the current-thread runtime, where a "
              "detached task dies with the call — the late-advance check is weaker", flush=True)
    try:
        def terminate():
            src("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                "WHERE slot_name LIKE 'apitap%' AND active")

        one_case("A: the walsender is terminated", terminate, prefix)

        def reset():
            ports = source_sockets()
            if not ports:
                _rig.rig_fail("found no source connection of the victim to reset")
            for p in ports:
                subprocess.run(["sudo", "-n", "ss", "-K", "state", "established", "dst", "127.0.0.1:5544",
                                "src", f"127.0.0.1:{p}"], capture_output=True)

        if prefix is None:
            print("   (skip) case B needs sudo -n for ss -K", flush=True)
        else:
            one_case("B: the connection is reset under the drain", reset, prefix)
    finally:
        clean()
        dst("DROP FUNCTION IF EXISTS orphan_slow() CASCADE")
        print("   cleaned up", flush=True)
    print(f"\n   ===== CDC APPLY ORPHAN E2E: {'PASSED' if ok else 'FAILED'} =====", flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
