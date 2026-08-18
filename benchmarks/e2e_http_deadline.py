"""A transfer that takes longer than a deadline must not die of the deadline.

v0.44.0 gave every HTTP client a 120-second `read_timeout` under the belief
that it bounds the gap BETWEEN bytes. It does not: reqwest's read_timeout
covers the whole span from sending the request to receiving the response
HEADERS, and apitap's ClickHouse loader holds one request open for a worker's
whole share of a table while asking for `wait_end_of_query=1` — so the server
sends nothing until the INSERT is finished. Every load over two minutes died
while streaming perfectly.

Both halves are checked here, because either one alone can be satisfied by
doing nothing:

  leg 1  no env var   — a transfer longer than the old default must SUCCEED
  leg 2  env var set  — the same transfer must FAIL, proving the knob is real
                        and that leg 1 is not passing by accident

The table is chosen so the run comfortably exceeds the deadline leg 2 sets.
"""
import os
import subprocess
import sys
import time

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
SRC = os.environ.get("SLOW_TABLE", "bench_data_10m")
T = "http_deadline_probe"

ok = True


def ch(sql):
    return subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True).stdout.strip()


def run(env_extra):
    env = dict(os.environ, **env_extra)
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG!r}, {CH!r}, table={SRC!r}, dest_table={T!r}, "
        "mode='replace')\n"
        "print('ROWS', r.rows)\n"
    )
    t0 = time.time()
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    return p, time.time() - t0


def case(label, good, detail=""):
    global ok
    print(f"   {'✓' if good else '✗'} {label}{': ' + detail if detail else ''}")
    ok = ok and good


print("== leg 1: the default must not cut a healthy transfer ==")
ch(f"DROP TABLE IF EXISTS {T}")
env = {k: v for k, v in os.environ.items() if k != "APITAP_HTTP_READ_TIMEOUT"}
os.environ.pop("APITAP_HTTP_READ_TIMEOUT", None)
p, el = run({})
if p.returncode:
    tail = [l for l in p.stderr.strip().splitlines() if l.strip()][-1]
    case(f"transfer survives ({el:.0f}s)", False, tail[:200])
else:
    landed = ch(f"SELECT count() FROM {T}")
    case(f"transfer survives and lands every row ({el:.0f}s)",
         landed.isdigit() and int(landed) > 0, f"{landed} rows")
    # A run that finished in a couple of seconds would prove nothing about a
    # deadline measured in minutes.
    case("the run was long enough to have been cut by the old default",
         el > 6, f"{el:.0f}s")

print("== leg 2: the opt-in knob is real ==")
ch(f"DROP TABLE IF EXISTS {T}")
p2, el2 = run({"APITAP_HTTP_READ_TIMEOUT": "5"})
if p2.returncode == 0:
    case("a 5-second total deadline stops this transfer", False,
         f"it finished in {el2:.0f}s — the knob does nothing")
else:
    tail = [l for l in p2.stderr.strip().splitlines() if l.strip()][-1]
    case(f"a 5-second total deadline stops this transfer ({el2:.1f}s)", True,
         tail[:120])

ch(f"DROP TABLE IF EXISTS {T}")
print("\n" + ("HTTP DEADLINE E2E: ALL GREEN" if ok else "HTTP DEADLINE E2E: FAILED"))
raise SystemExit(0 if ok else 1)
