"""A Postgres time column never lands in parquet as raw binary bytes.

The parquet lane admits a column when its type decodes exactly from the
typed binary stream. The admitted names mixed in MySQL's vocabulary, and a
Postgres `time` matched MySQL's "time": time_send's 8-byte microsecond count
was stored verbatim as a STRING, so 00:00:01 became the bytes
"\\0\\0\\0\\0\\0\\x0fB@" on a green run (deep review 2, N18; S3/GCS parquet,
Iceberg and BigQuery's parquet lane).

Proof (pg -> s3 format=parquet): a time column. The run must refuse the
column and say how to cast it; a run that succeeds must have written the
times as the server spells them.
"""
import subprocess
import sys

sys.path.insert(0, "benchmarks")
import _rig  # noqa: E402

import apitap  # noqa: E402

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PREFIX = "e2e_parquet_pg_time"
T = "pg_time_parquet"


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    for key in _rig.s3_list(f"{PREFIX}/"):
        _rig.s3_delete(key)
    pg(f"DROP TABLE IF EXISTS {T}")


def read_back():
    import duckdb
    d = duckdb.connect()
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    return d.execute(f"SELECT id, CAST(t AS VARCHAR) FROM read_parquet('s3://apitap-bench/{PREFIX}/{T}/*.parquet') "
                     f"ORDER BY id").fetchall()


def main():
    wipe()
    try:
        pg(f"CREATE TABLE {T} (id int8 PRIMARY KEY, t time); "
           f"INSERT INTO {T} VALUES (1, '00:00:01'), (2, '12:00:00'), (3, '23:59:59.5')")
        want = [tuple(r.split("|")) for r in pg(f"SELECT id || '|' || t FROM {T} ORDER BY id").split("\n")]
        try:
            apitap.transfer(PG, _rig.s3_url(PREFIX), table=T, mode="replace")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "column t" in msg and "::text" in msg, f"refused for the wrong reason: {msg}"
            print(f"  a Postgres time column is refused on the parquet lane: {msg[:90]}...")
        else:
            got = [(str(i), v) for i, v in read_back()]
            assert got == want, f"the parquet file holds {got!r}, the source {want!r}"
            print(f"  the times landed as the server spells them: {got}")
    finally:
        wipe()
    print("✓ a Postgres time column never lands in parquet as raw binary")


if __name__ == "__main__":
    main()
