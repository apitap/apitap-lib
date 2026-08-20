"""A destination table name at the dialect's identifier limit.

`<bare>__apitap_staging` is 16 characters longer than the table it stages, and
databases do not refuse an over-long identifier — they TRUNCATE it. Postgres
cuts at 63 bytes, so a 63-character destination produced a staging name
identical to the destination itself. Measured against 0.53.0:

    run 1 exit=1   dest exists=True   rows=100
    run 2 exit=1   dest exists=True   rows=150
    error: rename staging: relation "txxx…" does not exist

Read that carefully, because the interesting part is not the failure. `prepare`
dropped the destination (thinking it was staging) and recreated it, the load
landed 100 rows into it, and `finalize`'s transaction — DROP the destination,
RENAME staging onto it — rolled back when the rename found nothing. So the run
FAILED while the destination table had been replaced. `docs/stability.md` lists
"a failed transfer never leaves the destination table changed" as committed
surface; this was a run that failed and changed it.

Two tables sharing a long prefix collided the same way: past 48 characters the
suffix starts getting cut, and past the limit two different tables simply have
the same staging name.

  leg 1  the short control  — an ordinary name, twice, must work
  leg 2  the limit          — a name at exactly the identifier limit, twice
  leg 3  the shared prefix  — two tables differing only past the cut, each
                              landing its OWN rows

Leg 1 is what makes the others readable: if the control fails too, the rig is
broken, not the naming.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545,
`apitap-bench-my-dst` on :3308.
"""
import os
import subprocess
import sys

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-300:])
    return o.stdout.strip()


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def pg_dest():
    def q(sql):
        o = sh(["docker", "exec", "-i", "apitap-bench-pg-dst", "psql", "-U", "postgres",
                "-d", "apitap_bench_dst", "-Atc", sql])
        if o.returncode:
            raise RuntimeError(o.stderr[-300:])
        return o.stdout.strip()
    return dict(
        name="postgres", limit=63,
        url=os.environ.get("PGD_URL",
                           "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"),
        exists=lambda t: q(f"SELECT count(*) FROM information_schema.tables "
                           f"WHERE table_name='{t}'") == "1",
        rows=lambda t: q(f'SELECT count(*) FROM "{t}"'),
        reset=lambda t: (q(f'DROP TABLE IF EXISTS "{t}"'),
                         q(f"DELETE FROM _apitap_state WHERE dest_table = '{t}'")),
    )


def my_dest():
    def q(sql):
        o = sh(["docker", "exec", "-i", "apitap-bench-my-dst", "mysql", "-uroot",
                "-pbench", "-N", "-D", "bench", "-e", sql])
        if o.returncode and "Unknown table" not in o.stderr:
            raise RuntimeError(f"{sql[:100]} -> {o.stdout.strip()[-160:]} "
                               f"{o.stderr.strip()[-200:]}")
        return o.stdout.strip()
    return dict(
        name="mysql", limit=64,
        url=os.environ.get("MYD_URL", "mysql://root:bench@127.0.0.1:3308/bench"),
        exists=lambda t: q(f"SELECT count(*) FROM information_schema.tables "
                           f"WHERE table_schema='bench' AND table_name='{t}'") == "1",
        rows=lambda t: q(f"SELECT count(*) FROM `{t}`"),
        reset=lambda t: (q(f"DROP TABLE IF EXISTS `{t}`"),
                         q(f"DELETE FROM _apitap_state WHERE dest_table = '{t}'")),
    )


def transfer(d, t):
    code = (f"import apitap\n"
            f"r = apitap.transfer({SRC!r}, {d['url']!r}, table={t!r}, mode='replace')\n"
            f"print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code])


def seed(t, lo, hi):
    src(f'CREATE TABLE IF NOT EXISTS "{t}" (id int primary key, v text)')
    src(f"INSERT INTO \"{t}\" SELECT g, 'v'||g FROM generate_series({lo},{hi}) g")


def twice(d, t, label):
    """Load, add rows, load again — the second run is where a self-colliding
    staging name does its damage, because the first one has left a destination
    table for `prepare` to drop."""
    src(f'DROP TABLE IF EXISTS "{t}"')
    d["reset"](t)
    seed(t, 1, 100)
    r1 = transfer(d, t)
    case(f"{label}: run 1 succeeds", r1.returncode == 0,
         (r1.stderr.strip().splitlines() or [""])[-1][:160])
    case(f"{label}: run 1 landed 100 rows",
         d["exists"](t) and d["rows"](t) == "100",
         d["rows"](t) if d["exists"](t) else "table missing")
    seed(t, 101, 150)
    r2 = transfer(d, t)
    case(f"{label}: run 2 succeeds", r2.returncode == 0,
         (r2.stderr.strip().splitlines() or [""])[-1][:160])
    case(f"{label}: run 2 landed 150 rows",
         d["exists"](t) and d["rows"](t) == "150",
         d["rows"](t) if d["exists"](t) else "table missing")
    src(f'DROP TABLE IF EXISTS "{t}"')
    d["reset"](t)


for make in (pg_dest, my_dest):
    d = make()
    lim = d["limit"]
    print(f"\n════════ destination: {d['name']} (identifier limit {lim}) ════════")
    twice(d, "ln_" + "s" * 30, "control, 33 chars")
    twice(d, "l" + "n" * (lim - 1), f"at the limit, {lim} chars")

    # Two tables that differ only past the point where the suffix gets cut.
    print(f"   ── two names differing only past the cut ──")
    a = "p" * (lim - 6) + "alpha"
    b = "p" * (lim - 6) + "beta1"
    for t in (a, b):
        src(f'DROP TABLE IF EXISTS "{t}"')
        d["reset"](t)
    seed(a, 1, 10)
    seed(b, 1, 20)
    ra, rb = transfer(d, a), transfer(d, b)
    case("both long-prefix tables transfer", ra.returncode == 0 and rb.returncode == 0,
         (rb.stderr.strip().splitlines() or [""])[-1][:160])
    if d["exists"](a) and d["exists"](b):
        case("and each landed its OWN row count, not the other's",
             d["rows"](a) == "10" and d["rows"](b) == "20",
             f"{a[-6:]}={d['rows'](a)} {b[-6:]}={d['rows'](b)} (want 10 and 20)")
    else:
        case("and each landed its OWN row count, not the other's", False,
             "one of the two tables is missing")
    for t in (a, b):
        src(f'DROP TABLE IF EXISTS "{t}"')
        d["reset"](t)

print("\nLONG NAMES E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
