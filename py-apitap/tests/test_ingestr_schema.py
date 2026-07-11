"""End-to-end transfer test on ingestr's benchmark schema (all 15 column types).

Spins an ephemeral postgres:16 container, seeds benchmarks/seed.sql (the exact table
ingestr's own benchmark uses — every distinct type: varchars, small/int/bigint, float,
numeric, bool, date, timestamp, timestamptz, jsonb, text), transfers it with
apitap.transfer, and asserts a per-column checksum matches the source exactly.

Skipped automatically when docker isn't available.
"""

import subprocess
import time
from pathlib import Path

import pytest

CONTAINER = "apitap-test-pg"
PORT = 5546
ROWS = 50_000

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
FROM public.bench_data
"""


def docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=15).returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not docker_available(), reason="docker not available")


def psql(db: str, sql: str) -> str:
    r = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres", "-d", db, "-tAq"],
        input=sql, check=True, capture_output=True, text=True,
    )
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pg():
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    subprocess.run(
        ["docker", "run", "--rm", "-d", "--name", CONTAINER,
         "-e", "POSTGRES_PASSWORD=test", "-p", f"{PORT}:5432", "postgres:16-alpine"],
        check=True, capture_output=True,
    )
    for _ in range(30):
        if subprocess.run(
            ["docker", "exec", CONTAINER, "pg_isready", "-U", "postgres", "-q"],
            capture_output=True,
        ).returncode == 0:
            break
        time.sleep(1)
    for db in ("src", "dst"):
        subprocess.run(
            ["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-qc", f"CREATE DATABASE {db}"],
            check=True, capture_output=True,
        )
    seed = (Path(__file__).parents[2] / "benchmarks" / "seed.sql").read_text()
    seed = seed.replace("BENCH_TABLE_PLACEHOLDER", "bench_data").replace(
        "BENCH_ROWS_PLACEHOLDER", str(ROWS)
    )
    psql("src", seed)
    yield
    subprocess.run(["docker", "stop", "-t", "2", CONTAINER], capture_output=True)


def uri(db: str) -> str:
    return f"postgresql://postgres:test@localhost:{PORT}/{db}"


def test_transfer_preserves_every_column_type(pg):
    import apitap

    report = apitap.transfer(uri("src"), uri("dst"), table="public.bench_data")
    assert report.rows == ROWS
    assert report.parallel >= 1  # integer PK → parallel pipes auto-detected

    assert psql("dst", VALIDATE_SQL) == psql("src", VALIDATE_SQL)

    # Binary passthrough must also preserve the exact column types, not just values.
    types_sql = (
        "SELECT column_name || ':' || data_type FROM information_schema.columns "
        "WHERE table_name = 'bench_data' ORDER BY ordinal_position"
    )
    assert psql("dst", types_sql) == psql("src", types_sql)


def test_zero_row_source_never_wipes_destination(pg):
    import apitap

    psql("src", "CREATE TABLE IF NOT EXISTS public.empty_t (id int primary key)")
    report = apitap.transfer(
        uri("src"), uri("dst"), table="public.empty_t", dest_table="public.bench_data"
    )
    assert report.rows == 0
    assert psql("dst", "SELECT count(*) FROM public.bench_data") == str(ROWS)
