"""A slots=N retry's bootstrap shares the cage with the draining siblings.

The first-run shape is safe by construction: the process-wide bootstrap turn
serializes the groups' full loads, and a group waiting on the turn holds
~nothing. A RETRY is not that shape — one group's bootstrap failed and rolled
back to no state while its siblings' state stands, so the next slots=N run
bootstraps that one group at the FULL single-table plan while the other N-1
groups drain beside it in the same process, and the cage ceiling must hold
for the sum (deep review 3, D18 finding 1).

The bootstrap's nested load now divides its memory plan by 1 + however many
groups of this process are on the drain side, read per table after the turn —
a first run reads 0 (the siblings are parked on the turn, not draining), so
nothing is divided away there and no measured number regresses.

Proof (pg -> ch, APITAP_MEM_BUDGET=256M, APITAP_DEBUG=1): run 1 names a
table that does not exist yet, so its group fails at admission while the
other group bootstraps alone at the FULL plan (the no-regression half of the
contract, read off the load's own `[plan]` line — the engine's numbers, not
a proxy). Then the missing table is created and run 2 retries: the landed
group drains beside the re-bootstrap, whose `[plan]` line must show the
SHARE — mem_limit halved, fewer pipes — and both tables must land exact.
Every verdict asks the servers, never the exit code alone.
"""
import os
import re
import subprocess
import sys

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
A, B = "slots_retry_budget_a", "slots_retry_budget_b"
ROWS = 200

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def run(tables):
    """One slots=2 call in ONE process: the draining count is per process."""
    code = ("import apitap\n"
            f"apitap.transfer({PG!r}, {CH!r}, tables={tables!r}, mode='log_based', slots=2)\n")
    env = {**os.environ, "APITAP_MEM_BUDGET": "256M", "APITAP_DEBUG": "1"}
    return sh([sys.executable, "-c", code], timeout=600, env=env)


def plans(stderr):
    """The load's own `[plan]` lines: chunk/pipes/model_peak/mem_limit."""
    out = []
    for line in stderr.splitlines():
        if line.startswith("[plan]"):
            out.append({k: v for k, v in re.findall(r"(\w+)=(\d+)", line)})
    return out


def wipe():
    for t in (A, B):
        for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{t}'").split()):
            pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
               f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
            pg(f"DROP PUBLICATION IF EXISTS {pub}")
        pg(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table IN ('{A}', '{B}') "
           f"SETTINGS mutations_sync = 1")


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {A} (id int PRIMARY KEY, v int)")
        pg(f"INSERT INTO {A} SELECT g, g FROM generate_series(1, {ROWS}) g")

        # Run 1: B does not exist, so its group fails at admission while A's
        # group bootstraps — alone, nothing draining beside it.
        r = run([A, B])
        b_absent = ch(f"SELECT count() FROM system.tables WHERE database = 'default' "
                      f"AND name = '{B}'") == "0"
        a_landed = ch(f"SELECT count() FROM {A}") == str(ROWS)
        case("run 1: the missing group failed, the other bootstrapped",
             r.returncode != 0 and b_absent and a_landed,
             f"rc={r.returncode} b_absent={b_absent} a_landed={a_landed}")
        p1 = plans(r.stderr)
        case("run 1: the bootstrap planned the FULL cage (nothing drains beside it)",
             len(p1) == 1 and p1[0]["mem_limit"] == "256" and int(p1[0]["pipes"]) >= 2,
             str(p1))

        # The missing table appears; the retry is the run that matters.
        pg(f"CREATE TABLE {B} (id int PRIMARY KEY, v int)")
        pg(f"INSERT INTO {B} SELECT g, -g FROM generate_series(1, {ROWS}) g")
        r2 = run([A, B])
        case("run 2: the retry succeeds", r2.returncode == 0,
             (r2.stderr or "").strip()[-250:] if r2.returncode else "")
        p2 = plans(r2.stderr)
        case("run 2: the re-bootstrap planned its SHARE (mem_limit halved beside the draining sibling)",
             len(p2) == 1 and p2[0]["mem_limit"] == "128", str(p2))
        case("run 2: the share runs fewer pipes than the full plan",
             len(p2) == 1 and len(p1) == 1 and int(p2[0]["pipes"]) < int(p1[0]["pipes"]),
             f"{p1[0]['pipes'] if p1 else '?'} -> {p2[0]['pipes'] if p2 else '?'}")
        case("run 2: both tables are exact",
             ch(f"SELECT count() FROM {A}") == str(ROWS)
             and ch(f"SELECT count() FROM {B}") == str(ROWS)
             and ch(f"SELECT sum(v) FROM {B}") == str(-ROWS * (ROWS + 1) // 2))
    finally:
        wipe()
    print("\nSLOTS RETRY BUDGET E2E: " + ("PASSED" if ok else "FAILED"))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
