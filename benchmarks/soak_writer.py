"""Keep changes flowing at a bounded rate while the soak drains them.

Separate process on purpose: a soak where the source is idle during the drain
tests nothing that a gate leg does not already test. The interesting state —
a slot that keeps growing, a watermark that drifts — only appears when writes
land WHILE a drain is reading.

Stops when the stop file appears; pauses while the pause file exists, so the
soak can take a quiet reading without racing the writer.
"""
import os
import random
import subprocess
import sys
import time

PG_C = "apitap-bench-pg-src"
DB = "apitap_bench_src"
T = os.environ.get("SOAK_TABLE", "soak_cdc")
STOP = "/tmp/soak.stop"
PAUSE = "/tmp/soak.pause"
BURST = int(os.environ.get("SOAK_BURST", "400"))     # rows per burst
EVERY = float(os.environ.get("SOAK_BURST_EVERY", "2"))  # seconds between bursts


def pg_script(sql):
    return subprocess.run(
        ["docker", "exec", "-i", PG_C, "psql", "-U", "postgres", "-d", DB,
         "-q", "-v", "ON_ERROR_STOP=1"],
        input=sql, capture_output=True, text=True)


# Deterministic id stream so a restart cannot collide with what is already there.
nxt = int(subprocess.run(
    ["docker", "exec", "-i", PG_C, "psql", "-U", "postgres", "-d", DB, "-Atc",
     f"SELECT coalesce(max(id),0)+1 FROM {T}"],
    capture_output=True, text=True).stdout.strip() or 1)

rnd = random.Random(20260820)
bursts = 0
while not os.path.exists(STOP):
    while os.path.exists(PAUSE) and not os.path.exists(STOP):
        time.sleep(0.5)
    if os.path.exists(STOP):
        break
    lo, hi = nxt, nxt + BURST - 1
    # A real workload is not inserts only: updates and deletes exercise the
    # collapser and the delete-marker sidecar, which inserts alone never touch.
    sql = (
        f"INSERT INTO {T} SELECT g, repeat('x',120), 0 "
        f"FROM generate_series({lo},{hi}) g;\n"
    )
    if nxt > 1:
        u_lo = max(1, nxt - rnd.randint(BURST, BURST * 5))
        sql += (f"UPDATE {T} SET touched = touched + 1 "
                f"WHERE id BETWEEN {u_lo} AND {u_lo + BURST // 4};\n")
        d_lo = max(1, nxt - rnd.randint(BURST * 5, BURST * 20))
        sql += f"DELETE FROM {T} WHERE id BETWEEN {d_lo} AND {d_lo + BURST // 20};\n"
    r = pg_script(sql)
    if r.returncode:
        print(f"writer error: {r.stderr.strip()[-300:]}", flush=True)
    nxt = hi + 1
    bursts += 1
    if bursts % 300 == 0:
        print(f"writer: {bursts} bursts, next id {nxt}", flush=True)
    time.sleep(EVERY)
print(f"writer: stopping after {bursts} bursts, next id {nxt}", flush=True)
