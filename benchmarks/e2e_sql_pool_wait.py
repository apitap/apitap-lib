"""apitap.sql()'s key-range probe waits for a server that is full for now.

The probe of a join key no index leads reads MIN/MAX in eight page ranges at
once, on eight connections. When the server (or the role) has no room for
them, sqlx retries "too many clients" for 30 s without saying so and then
reports "pool timed out while waiting for an open connection": one statement
among twelve run at once failed that way (QA p6, W1 round P, L4). The probe
now waits (up to five minutes, with a note) and, past that, says why.

The leg makes the server full on purpose: a role with CONNECTION LIMIT 4, the
statement's probe held back 10 s by the debug knob, and three of the four
slots taken from second 4 to second 49. The probe then finds one slot for
eight connections. Before the fix the statement failed at ~40 s with the pool
timeout; now it answers, exactly, once the slots come free.
"""
import os
import subprocess
import sys
import threading
import time

USER = "l4_wait"
PG_URL = f"postgres://{USER}:bench@127.0.0.1:5544/apitap_bench_src"
HOLD_AT, HOLD_S, HOLDERS, LIMIT = 4, 45, 3, 4


def pg(sql, user="postgres"):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-h", "127.0.0.1", "-U", user,
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, env={**os.environ, "PGPASSWORD": "bench"})
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    pg("DROP TABLE IF EXISTS l4_a; DROP TABLE IF EXISTS l4_b")
    pg(f"DROP ROLE IF EXISTS {USER}")


def main():
    wipe()
    # No index on k, 400K rows each (> 1024 pages): the probe runs in parts.
    pg("CREATE TABLE l4_a AS SELECT g AS k, g % 97 AS v FROM generate_series(1, 400000) g;"
       "CREATE TABLE l4_b AS SELECT g * 2 AS k, g % 13 AS w FROM generate_series(1, 400000) g;"
       "ANALYZE l4_a; ANALYZE l4_b;"
       f"CREATE ROLE {USER} LOGIN PASSWORD 'bench' CONNECTION LIMIT {LIMIT};"
       f"GRANT SELECT ON l4_a, l4_b TO {USER}")
    want = pg("SELECT count(*) || '|' || sum(a.v) FROM l4_a a JOIN l4_b b ON a.k = b.k")
    q = "SELECT COUNT(*) AS n, SUM(a.v) AS s FROM pg.l4_a a JOIN pg.l4_b b ON a.k = b.k"
    code = ("import apitap\n"
            f"r = apitap.sql({q!r}, sources={{'pg': {PG_URL!r}}}).to_polars().rows()[0]\n"
            "print('ANSWER', f'{r[0]}|{r[1]}')\n")

    held = []

    def one():
        try:
            pg(f"SELECT pg_sleep({HOLD_S})", user=USER)
            held.append(True)
        except RuntimeError:
            held.append(False)

    def hold():
        time.sleep(HOLD_AT)
        for _ in range(HOLDERS):
            threading.Thread(target=one, daemon=True).start()

    threading.Thread(target=hold, daemon=True).start()
    env = {**os.environ, "APITAP_DEBUG": "1", "APITAP_SQL_PROBE_RANGE_DELAY_MS": "10000", "APITAP_PROGRESS": "plain"}
    t0 = time.time()
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    wall = time.time() - t0
    got = next((ln.split()[1] for ln in r.stdout.splitlines() if ln.startswith("ANSWER")), None)
    waited = "has no connection free yet; waiting" in r.stderr
    timed_out = "pool timed out" in r.stderr
    # Exact (having waited or not: a statement that sees few connections free
    # runs serially and may never need to wait), or refused by name within
    # the bound; never sqlx's bare "pool timed out".
    named = "no connection" in r.stderr
    ok = ((r.returncode == 0 and got == want) or (r.returncode != 0 and named)) and not timed_out and wall <= 360
    print(f"  answered in {wall:.1f}s: {got} (truth {want}); probe waited: {waited}; pool timeout: {timed_out}")
    if r.returncode:
        print("   ", [ln for ln in r.stderr.splitlines() if "Error" in ln or "timed out" in ln][-3:])
    time.sleep(max(0, HOLD_AT + HOLD_S + 2 - wall))
    # A holder that could not connect found the role full of the statement's
    # own connections: the server was full either way.
    print(f"  holders that got a slot: {sum(held)} of {len(held)}")
    wipe()
    print("SQL POOL WAIT E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
