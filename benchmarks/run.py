#!/usr/bin/env python3
"""Head-to-head PG→PG benchmark: apitap vs ingestr, on ingestr's own benchmark table.

The table schema and value generators are ingestr's (see seed.sql — ported 1:1 from
their benchmarks/sql/*), and ingestr is invoked exactly as their own benchmark runner
does (`uv tool run --python 3.13 ingestr@0.14.141 ingest … --yes --full-refresh`), so
the comparison is apples-to-apples. Both tools move the same table between the same two
databases; both results are checksum-validated against the source before timing counts.

    python benchmarks/run.py                     # 1M rows, apitap only
    python benchmarks/run.py --rows 10000000     # the 10M headline run
    python benchmarks/run.py --with-ingestr      # also run ingestr (installs via uv)
    python benchmarks/run.py --keep              # keep the postgres container

Requires: docker, uv, and `apitap` importable (uv pip install -e py-apitap).
"""

import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path

CONTAINER = "apitap-bench-pg"
PORT = 5545
PASSWORD = "bench"
# The latest ingestr release (their own benchmark runner pins the older 0.14.141 —
# comparing against the newest is the fairer fight).
INGESTR_SPEC = "ingestr@1.0.75"
# ingestr's runner uses --python 3.13, but the pendulum dependency ships no 3.13 arm64
# wheel and its sdist fails to build (pyo3 link error) — 3.12 has wheels and runs the
# identical ingestr version.
INGESTR_PYTHON = "3.12"

# One checksum per column of the ingestr schema — a transfer only counts if every one
# matches the source (order-independent, format-independent).
VALIDATE_SQL = """
SELECT count(*),
       sum(id), sum(tiny_int), sum(regular_int), sum(big_int),
       md5(string_agg(small_str, '' ORDER BY id)),
       md5(string_agg(medium_str, '' ORDER BY id)),
       sum(length(large_str)), sum(length(extra_text)),
       round(sum(float_val)::numeric, 0), sum(decimal_val),
       count(*) FILTER (WHERE bool_val),
       min(date_val), max(date_val),
       min(ts_val), max(ts_val), min(ts_tz_val), max(ts_tz_val),
       md5(string_agg(json_val->>'key', '' ORDER BY id))
FROM {table}
"""


def sh(cmd: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True, **kw)


def psql(db: str, sql: str) -> str:
    r = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres", "-d", db, "-tAq"],
        input=sql, check=True, capture_output=True, text=True,
    )
    return r.stdout.strip()


def uri(db: str) -> str:
    return f"postgresql://postgres:{PASSWORD}@localhost:{PORT}/{db}"


def ensure_pg():
    up = subprocess.run(["docker", "inspect", CONTAINER], capture_output=True)
    if up.returncode != 0:
        print(f"==> starting {CONTAINER} (postgres:16-alpine on :{PORT})")
        sh(
            f"docker run --rm -d --name {CONTAINER} -e POSTGRES_PASSWORD={PASSWORD} "
            f"-p {PORT}:5432 --shm-size=1g postgres:16-alpine"
        )
    # postgres images boot TWICE (initdb's temp server, then the real one) — pg_isready can
    # answer during the temp phase and the next statement lands mid-restart. Wait for a real
    # query to succeed, then settle one extra second.
    for _ in range(60):
        if subprocess.run(
            ["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-qc", "SELECT 1"],
            capture_output=True,
        ).returncode == 0:
            break
        time.sleep(1)
    time.sleep(1)
    for db in ("src", "dst"):
        subprocess.run(
            ["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-qc", f"CREATE DATABASE {db}"],
            capture_output=True,
        )


def seed(table: str, rows: int):
    have = psql("src", f"SELECT count(*) FROM public.{table}") if psql(
        "src", f"SELECT to_regclass('public.{table}') IS NOT NULL"
    ) == "t" else "0"
    if have == str(rows):
        print(f"==> {table}: already seeded ({rows:,} rows)")
        return
    print(f"==> seeding {table} ({rows:,} rows)…")
    sql = (Path(__file__).parent / "seed.sql").read_text()
    sql = sql.replace("BENCH_TABLE_PLACEHOLDER", table).replace("BENCH_ROWS_PLACEHOLDER", str(rows))
    t0 = time.time()
    psql("src", sql)
    print(f"    seeded in {time.time() - t0:.1f}s")


def validate(table: str, src_sum: str) -> bool:
    got = psql("dst", VALIDATE_SQL.format(table=f"public.{table}"))
    return got == src_sum


def run_apitap(table: str) -> float:
    import apitap

    t0 = time.time()
    r = apitap.transfer(uri("src"), uri("dst"), table=f"public.{table}")
    dt = time.time() - t0
    print(f"    apitap: {r.rows:,} rows over {r.parallel} pipes")
    return dt


def run_ingestr(table: str) -> float:
    # Exactly ingestr's own benchmark invocation (benchmarks/scripts/runner.py).
    cmd = (
        f"uv tool run --python {INGESTR_PYTHON} {INGESTR_SPEC} ingest"
        f" --source-uri {shlex.quote(uri('src'))}"
        f" --source-table public.{table}"
        f" --dest-uri {shlex.quote(uri('dst'))}"
        f" --dest-table public.{table}_ingestr"
        f" --yes --full-refresh"
    )
    t0 = time.time()
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    dt = time.time() - t0
    if r.returncode != 0:
        print(r.stdout[-2000:], r.stderr[-2000:], sep="\n", file=sys.stderr)
        raise RuntimeError("ingestr failed")
    return dt


# ---------- capped mode: each tool inside a --cpus/--memory-limited container ----------

INGESTR_VERSION = INGESTR_SPEC.split("@")[1]
APITAP_IMG = "apitap-bench-apitap:local"
INGESTR_IMG = f"apitap-bench-ingestr:{INGESTR_VERSION}"
# Inside a tool container the bench Postgres is reachable via Docker Desktop's host alias.
CAP_URI = f"postgresql://postgres:{PASSWORD}@host.docker.internal:{PORT}/{{db}}"


def image_exists(tag: str) -> bool:
    return subprocess.run(["docker", "image", "inspect", tag], capture_output=True).returncode == 0


def build_capped_images():
    repo = Path(__file__).parents[1]
    if not image_exists(APITAP_IMG):
        print("==> building the apitap manylinux wheel (one-time, a few minutes)…")
        sh(
            "docker run --rm -v {repo}:/io -v apitap-bench-cargo:/root/.cargo/registry "
            "ghcr.io/pyo3/maturin build --release -m py-apitap/Cargo.toml -o benchmarks/wheels".format(repo=repo)
        )
        print("==> building the apitap runner image…")
        df = "FROM python:3.12-slim\nCOPY *.whl /tmp/\nRUN pip install --no-cache-dir /tmp/*.whl\n"
        subprocess.run(
            ["docker", "build", "-t", APITAP_IMG, "-f-", str(repo / "benchmarks" / "wheels")],
            input=df, check=True, capture_output=True, text=True,
        )
    if not image_exists(INGESTR_IMG):
        print(f"==> building the ingestr {INGESTR_VERSION} image (one-time, a few minutes)…")
        df = f"FROM python:3.12-slim\nRUN pip install --no-cache-dir ingestr=={INGESTR_VERSION}\n"
        subprocess.run(
            ["docker", "build", "-t", INGESTR_IMG, "-"],
            input=df, check=True, capture_output=True, text=True,
        )


def capped_run(img: str, py_body: str, cpus: str, mem: str) -> float:
    """Run `py_body` inside a resource-capped container; it must print ELAPSED <secs>
    (timed inside, so container startup doesn't skew either tool)."""
    r = subprocess.run(
        ["docker", "run", "--rm", f"--cpus={cpus}", f"--memory={mem}",
         "--add-host=host.docker.internal:host-gateway", img, "python", "-c", py_body],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        oom = "137" if r.returncode == 137 else ""
        print(r.stdout[-2000:], r.stderr[-2000:], sep="\n", file=sys.stderr)
        raise RuntimeError(f"capped run failed (exit {r.returncode}{' — likely OOM-killed' if oom else ''})")
    for line in r.stdout.splitlines():
        if line.startswith("ELAPSED "):
            return float(line.split()[1])
    raise RuntimeError(f"no ELAPSED in output:\n{r.stdout[-1000:]}")


def run_apitap_capped(table: str, cpus: str, mem: str) -> float:
    body = (
        "import apitap, time\n"
        f"t0 = time.time()\n"
        f"r = apitap.transfer({CAP_URI.format(db='src')!r}, {CAP_URI.format(db='dst')!r}, table='public.{table}')\n"
        "print(f'    apitap: {r.rows:,} rows over {r.parallel} pipes')\n"
        "print('ELAPSED', time.time() - t0)\n"
    )
    return capped_run(APITAP_IMG, body, cpus, mem)


def run_ingestr_capped(table: str, cpus: str, mem: str) -> float:
    src, dst = CAP_URI.format(db="src"), CAP_URI.format(db="dst")
    body = (
        "import subprocess, sys, time\n"
        "cmd = ['ingestr', 'ingest',\n"
        f"    '--source-uri', {src!r}, '--source-table', 'public.{table}',\n"
        f"    '--dest-uri', {dst!r}, '--dest-table', 'public.{table}_ingestr',\n"
        "    '--yes', '--full-refresh']\n"
        "t0 = time.time()\n"
        "r = subprocess.run(cmd, capture_output=True, text=True)\n"
        "if r.returncode != 0:\n"
        "    print(r.stdout[-2000:], r.stderr[-2000:], sep='\\n', file=sys.stderr); sys.exit(1)\n"
        "print('ELAPSED', time.time() - t0)\n"
    )
    return capped_run(INGESTR_IMG, body, cpus, mem)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--with-ingestr", action="store_true")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--capped", action="store_true",
                    help="run each tool inside a resource-capped container (see --cap-cpus/--cap-mem)")
    ap.add_argument("--cap-cpus", default="2")
    ap.add_argument("--cap-mem", default="2g")
    args = ap.parse_args()

    suffix = f"{args.rows // 1_000_000}m" if args.rows >= 1_000_000 else f"{args.rows // 1000}k"
    table = f"bench_data_{suffix}"

    ensure_pg()
    try:
        seed(table, args.rows)
        src_sum = psql("src", VALIDATE_SQL.format(table=f"public.{table}"))

        results = []
        if args.capped:
            build_capped_images()

        print("==> apitap" + (f" (capped {args.cap_cpus} cpu / {args.cap_mem})" if args.capped else ""))
        dt = run_apitap_capped(table, args.cap_cpus, args.cap_mem) if args.capped else run_apitap(table)
        ok = validate(table, src_sum)
        results.append(("apitap", dt, ok))

        if args.with_ingestr:
            print(f"==> ingestr ({INGESTR_SPEC})" + (f" (capped {args.cap_cpus} cpu / {args.cap_mem})" if args.capped else ""))
            dt = (
                run_ingestr_capped(table, args.cap_cpus, args.cap_mem)
                if args.capped
                else run_ingestr(table)
            )
            got = psql("dst", VALIDATE_SQL.format(table=f"public.{table}_ingestr"))
            results.append(("ingestr", dt, got == src_sum))

        cap_note = f" · each tool capped at {args.cap_cpus} cpu / {args.cap_mem}" if args.capped else ""
        print(f"\n{args.rows:,} rows · PG → PG · ingestr benchmark schema (15 cols){cap_note}")
        print(f"{'tool':<10} {'time':>10} {'validated':>10}")
        for name, dt, ok in results:
            print(f"{name:<10} {dt:>9.1f}s {'✅ match' if ok else '❌ MISMATCH':>10}")
        if len(results) == 2 and all(ok for _, _, ok in results):
            print(f"\napitap is {results[1][1] / results[0][1]:.1f}× faster")
        if any(not ok for _, _, ok in results):
            sys.exit(1)
    finally:
        if not args.keep:
            subprocess.run(["docker", "stop", "-t", "2", CONTAINER], capture_output=True)


if __name__ == "__main__":
    main()
