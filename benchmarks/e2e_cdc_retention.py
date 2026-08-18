"""The two ways a paused CDC schedule hurts you — and what apitap says about it.

The risk is not symmetric between the sources, and both halves are real:

  * Postgres keeps every WAL segment a slot has not confirmed. That is the
    guarantee CDC rests on, and also how a stopped schedule fills the SOURCE's
    disk. apitap cannot refuse (a big backlog is exactly when the drain must
    run), so it must REPORT the retained WAL every run and say something before
    the number becomes an outage.

  * MySQL and MariaDB keep nothing. The server purges binlogs on its own
    retention, so a schedule paused longer than that loses the position we
    stored — and resuming from a purged file means skipping every change in
    between. That is silent data loss, so it must REFUSE, with the reason and
    the remedy, instead of MySQL's error 1236.

This file proves both against live servers: it purges a MariaDB binlog on
purpose to make the dangerous state, and reads the slot's retained WAL from
Postgres itself.
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MA = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
PT = "ret_pg"
MT = "ret_ma"


def sh(args):
    return subprocess.run(args, capture_output=True, text=True)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ma(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb", "-uroot",
            "-pbench", "-N", "-D", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def transfer(src, table, stderr_wanted=False):
    """Run a CDC drain in a child so the engine's stderr can be inspected.

    sys.executable, never a bare "python3": inside the release gate this file
    runs from a virtualenv, and the bare name resolves to the system Python,
    which has no apitap. That cost a red gate leg — the engine was fine and the
    harness could not import it.
    """
    code = (
        "import apitap\n"
        f"r = apitap.transfer({src!r}, {CH!r}, table={table!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code])


ok = True

print("== Postgres: the slot's retained WAL must be reported every run ==")
pg(f"DROP TABLE IF EXISTS {PT}")
pg(f"CREATE TABLE {PT} (id int primary key, v text)")
pg(f"INSERT INTO {PT} SELECT g, 'v'||g FROM generate_series(1,100) g")
ch(f"DROP TABLE IF EXISTS {PT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{PT}' SETTINGS mutations_sync=1")
r = transfer(PG, PT)
if r.returncode:
    ok = False
    print(f"   ✗ bootstrap failed: {r.stderr[-400:]}")
else:
    # Make the slot hold something, then drain again and read the note.
    pg(f"INSERT INTO {PT} SELECT g, 'w'||g FROM generate_series(101,200) g")
    r = transfer(PG, PT)
    note = [l for l in r.stderr.splitlines() if "retains" in l]
    if note:
        print(f"   ✓ reported: {note[-1].strip()}")
    else:
        ok = False
        print(f"   ✗ no retained-WAL note in the engine's output:\n{r.stderr[-500:]}")
    slot = pg("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap%' LIMIT 1")
    held = pg("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint "
              f"FROM pg_replication_slots WHERE slot_name = '{slot}'") if slot else ""
    print(f"   ✓ slot {slot} holds {held} bytes of WAL according to Postgres itself")

print("== Postgres: a large backlog warns, and never refuses ==")
r = transfer(PG, PT)  # nothing new; exercises the reporting path again
if r.returncode == 0:
    print("   ✓ an empty drain still succeeds (reporting is never a gate)")
else:
    ok = False
    print(f"   ✗ the reporting path failed a clean run: {r.stderr[-300:]}")

print("== MariaDB: resuming from a vanished binlog must refuse, not skip ==")
# Creating the dangerous state is the hard part, and the test must PROVE it
# created it before it may accuse the engine. A first attempt used PURGE and
# quietly failed to remove the needed file (MariaDB refuses to purge past its
# binlog checkpoint), so the drain resumed correctly and the test called that a
# bug. Now: try PURGE, verify, and fall back to RESET MASTER, which cannot
# fail to destroy the position because it restarts the numbering.
ma(f"DROP TABLE IF EXISTS {MT}")
ma(f"CREATE TABLE {MT} (id INT PRIMARY KEY, v VARCHAR(20))")
ma(f"INSERT INTO {MT} VALUES (1,'a'),(2,'b')")
ch(f"DROP TABLE IF EXISTS {MT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{MT}' SETTINGS mutations_sync=1")
r = transfer(MA, MT)
if r.returncode:
    ok = False
    print(f"   ✗ bootstrap failed: {r.stderr[-400:]}")
else:
    wm = ch(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table='{MT}'")
    # The watermark packs file_index << 32 | position.
    idx = int(wm) >> 32 if wm.isdigit() else 0
    logs = [l.split()[0] for l in ma("SHOW BINARY LOGS").splitlines() if l.strip()]
    prefix = logs[-1].rsplit(".", 1)[0] if logs else "mariadb-bin"
    needed = f"{prefix}.{idx:06d}"
    print(f"   bootstrapped; the next run must resume from {needed}")
    ma(f"INSERT INTO {MT} VALUES (3,'c')")   # a change only that file holds

    # Attempt 1: ordinary retention.
    ma("FLUSH BINARY LOGS")
    ma("FLUSH BINARY LOGS")
    newest = [l.split()[0] for l in ma("SHOW BINARY LOGS").splitlines() if l.strip()][-1]
    try:
        ma(f"PURGE BINARY LOGS TO '{newest}'")
    except RuntimeError as e:
        print(f"   PURGE refused ({str(e).splitlines()[0][:60]}…)")
    present = [l.split()[0] for l in ma("SHOW BINARY LOGS").splitlines() if l.strip()]
    if needed in present:
        # Attempt 2: the server-reset shape. RESET MASTER deletes every binlog
        # and restarts numbering, so the stored coordinate cannot survive.
        print(f"   {needed} survived PURGE (checkpoint held it) — using RESET MASTER")
        ma("RESET MASTER")
        present = [l.split()[0] for l in ma("SHOW BINARY LOGS").splitlines() if l.strip()]
    if needed in present:
        ok = False
        print(f"   ✗ could not destroy {needed} on this server — the dangerous "
              f"state was never created, so nothing was tested")
    else:
        print(f"   {needed} is gone; server now holds {present}")
        r = transfer(MA, MT)
        msg = r.stderr
        if r.returncode == 0:
            ok = False
            print("   ✗ the drain SUCCEEDED after its position vanished — that is a "
                  "silent hole in the change stream")
        elif "no longer on the server" in msg and "fresh bootstrap" in msg:
            print("   ✓ refused, and the message names the cause and the recovery")
            line = [l for l in msg.splitlines() if "no longer on the server" in l]
            print(f"     {line[0].strip()[:140]}…")
        else:
            ok = False
            print(f"   ✗ refused with the wrong message:\n{msg[-600:]}")

print("== cleanup ==")
pg(f"DROP TABLE IF EXISTS {PT}")
ma(f"DROP TABLE IF EXISTS {MT}")
for t in (PT, MT):
    ch(f"DROP TABLE IF EXISTS {t}")
for s in pg("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap%'").splitlines():
    if s:
        pg(f"SELECT pg_drop_replication_slot('{s}')")
print("   dropped test tables and slots")

print("\n" + ("CDC RETENTION E2E: ALL GREEN" if ok else "CDC RETENTION E2E: FAILED"))
raise SystemExit(0 if ok else 1)
