"""An S3 publish sweeps only its own table directory, never one nested under it.

A parquet publish deletes the previous generation's part files under
`<prefix>/<table>/`. It listed that prefix recursively and kept any key whose
BASENAME looked like a part, so a second destination written UNDER the table
dir (prefix `<prefix>/events`, table `clicks` -> `<prefix>/events/clicks/`)
lost every published part to the first table's next publish: table clicks
silently empty, both runs green (deep review D25). The same listing reached
another run's in-flight staging parts.

Proof: publish `clicks` under `<prefix>/events`, then publish `events` under
`<prefix>` twice (the second publish sweeps the first's generation). Every
part of clicks must survive, its rows readable, and events must hold only
its newest generation.
"""
import subprocess
import sys

sys.path.insert(0, "benchmarks")
import _rig  # noqa: E402

import apitap  # noqa: E402

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PREFIX = "e2e_nested_sweep"


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def parts(prefix):
    return sorted(k for k in _rig.s3_list(prefix) if k.endswith(".parquet"))


def rows(prefix):
    import duckdb
    d = duckdb.connect()
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    return d.execute(f"SELECT count(*) FROM read_parquet('s3://apitap-bench/{prefix}/*.parquet')").fetchone()[0]


def wipe():
    for key in _rig.s3_list(f"{PREFIX}/"):
        _rig.s3_delete(key)
    pg("DROP TABLE IF EXISTS clicks; DROP TABLE IF EXISTS events")


def main():
    wipe()
    try:
        pg("CREATE TABLE clicks (id int PRIMARY KEY, v text); "
           "INSERT INTO clicks SELECT g, 'c' FROM generate_series(1, 300) g; "
           "CREATE TABLE events (id int PRIMARY KEY, v text); "
           "INSERT INTO events SELECT g, 'e' FROM generate_series(1, 200) g")
        apitap.transfer(PG, _rig.s3_url(f"{PREFIX}/events"), table="clicks", mode="replace")
        nested = parts(f"{PREFIX}/events/clicks/")
        assert nested and rows(f"{PREFIX}/events/clicks") == 300, f"(rig) clicks published: {nested}"
        print(f"  clicks published under events/: {len(nested)} part(s), 300 rows")

        for _ in range(2):
            apitap.transfer(PG, _rig.s3_url(PREFIX), table="events", mode="replace")

        left = parts(f"{PREFIX}/events/clicks/")
        assert left == nested, f"the events publish deleted clicks' parts: {nested} -> {left}"
        got = rows(f"{PREFIX}/events/clicks")
        assert got == 300, f"clicks holds {got} rows after the events publish, want 300"
        own = [k for k in parts(f"{PREFIX}/events/") if k.count("/") == 2]
        gens = {k.rsplit("/", 1)[1].split("-")[1] for k in own}
        assert len(gens) == 1, f"events must hold one generation of parts, has {sorted(own)}"
        assert rows(f"{PREFIX}/events") == 200, "events must read 200 rows"
        print(f"  two events publishes later: clicks keeps {len(left)} part(s) and 300 rows, "
              f"events holds one generation ({len(own)} part(s))")
    finally:
        wipe()
    print("✓ an S3 publish sweeps only its own table directory")


if __name__ == "__main__":
    main()
