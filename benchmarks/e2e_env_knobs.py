"""A tuning knob set to something it does not take is refused by name before the run.

Through the W1 wheels `APITAP_RECV_BUF_BYTES=junk` was read as unset: the run
went on with the default and said nothing (QA p13). Now every entry point
(transfer, read, sql) refuses it up front with the knob, the value, and what
it takes; `0`/`off` and a byte count still run.

Round P review (p9): the same for every numeric APITAP_* knob (workers,
lanes, timeouts, byte budgets, MEM_BUDGET, PROGRESS_INTERVAL, …) at every
entry including read_schema: junk is refused by name, a good value runs.

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


# Every numeric knob (round P review, p9): a junk value is refused by name at
# each entry (transfer, read, read_schema, sql), and a good one runs.
KNOBS = {
    "APITAP_RUNTIME_WORKERS": "2", "APITAP_BATCH_BYTES": "1048576", "APITAP_BQ_APPLY_LANES": "2",
    "APITAP_CDC_APPLY_LANES": "2", "APITAP_CDC_WINDOW_BYTES": "8388608", "APITAP_FOLLOW_SECS": "5",
    "APITAP_GSHEETS_PAGE_ROWS": "500", "APITAP_S3_COPY_PART_BYTES": "6000000", "APITAP_WINDOW_DEPTH": "2",
    "APITAP_WINDOW_MAX_SECS": "60", "APITAP_SQL_PROBE_PIPES": "2", "APITAP_REPLICATION_SILENCE_SECS": "60",
    "APITAP_CH_SYNC_TIMEOUT_S": "60", "APITAP_GITHUB_MAX_PAGES": "10", "APITAP_HTTP_POOL_IDLE_MS": "500",
    "APITAP_HTTP_READ_TIMEOUT": "30", "APITAP_HTTP_CONNECT_TIMEOUT": "10", "APITAP_HTTP_REQUEST_TIMEOUT": "60",
    "APITAP_LEASE_TTL_SECS": "60", "APITAP_MY_WINDOW_SECS": "10", "APITAP_PG_CONNECT_TIMEOUT_SECS": "10",
    "APITAP_SLOT_WAL_WARN": "1G", "APITAP_SQL_BUILD_BYTES": "64M", "APITAP_SQL_GROUP_BYTES": "32M",
    "APITAP_SQL_OVERLAP_CHUNK": "1M", "APITAP_SQL_STAGE_BYTES": "32M", "APITAP_SQL_VIEW_BUCKET_BYTES": "32M",
    "APITAP_TX_BUF_BYTES": "8M", "APITAP_CH_MAX_BODY": "64M", "APITAP_MEM_BUDGET": "512M",
    "APITAP_PROGRESS_INTERVAL": "2.5", "APITAP_READ_COALESCE_US": "off",
}
CALLS["read_schema"] = f"apitap._apitap.read_schema({PG!r}, {T!r})"


def run(call, value, knob="APITAP_RECV_BUF_BYTES"):
    env = dict(os.environ)
    for k in [knob, "APITAP_RECV_BUF_BYTES", *KNOBS]:
        env.pop(k, None)
    if value is not None:
        env[knob] = value
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
    for knob, good_value in KNOBS.items():
        for call in ("sql", "read_schema"):
            for value, want_run in [("junk", False), (good_value, True)]:
                ran, last = run(call, value, knob)
                named = f"{knob}='{value}' is not" in last[0]
                good = ran if want_run else (not ran and named)
                ok &= good
                if not good or call == "sql":
                    print(f"  {'ok ' if good else 'BAD'} {call} with {knob}={value!r}: {'ran' if ran else last[0][:140]}", flush=True)
    pg(f"DROP TABLE IF EXISTS {T}")
    print("ENV KNOBS E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
