"""A tuning knob set to something it does not take is refused by name before the run.

Through the W1 wheels `APITAP_RECV_BUF_BYTES=junk` was read as unset: the run
went on with the default and said nothing (QA p13). Now every entry point
(transfer, read, sql) refuses it up front with the knob, the value, and what
it takes; `0`/`off` and a byte count still run.

Usage: e2e_env_knobs.py
"""
import os
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "env_knob_probe"


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


CALLS = {
    "sql": f"apitap.sql('SELECT COUNT(*) AS n FROM pg.{T}', sources={{'pg': {PG!r}}}).to_polars()",
    "read": f"apitap.read({PG!r}, {T!r}).to_polars()",
}


def run(call, value):
    env = dict(os.environ)
    env.pop("APITAP_RECV_BUF_BYTES", None)
    if value is not None:
        env["APITAP_RECV_BUF_BYTES"] = value
    r = subprocess.run([sys.executable, "-c", f"import apitap\n{CALLS[call]}\nprint('RAN')"],
                       capture_output=True, text=True, env=env)
    return r.returncode == 0 and "RAN" in r.stdout, r.stderr.strip().splitlines()[-1:] or [""]


def main():
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} AS SELECT g AS id FROM generate_series(1, 1000) g")
    ok = True
    for call in CALLS:
        for value, want_run in [("junk", False), ("12MB", False), (None, True), ("0", True), ("off", True), ("4194304", True)]:
            ran, last = run(call, value)
            named = f"APITAP_RECV_BUF_BYTES='{value}' is not a byte count" in last[0]
            good = ran if want_run else (not ran and named)
            ok &= good
            print(f"  {'ok ' if good else 'BAD'} {call} with {value!r}: {'ran' if ran else last[0][:160]}", flush=True)
    pg(f"DROP TABLE IF EXISTS {T}")
    print("ENV KNOBS E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
