"""S3 hardening: a key holding an XML-escaped character (MinIO :9100).

P2 (docs/review/2026-10-18-p2-triage.md), s3-xml-entities: S3 listings
entity-escape `<Key>` text (`&` comes back as `&amp;`), and the sink used
it raw. Under a prefix holding `&`, the publish copied a key that does not
exist and the stale sweep deleted nothing. Keys are entity-decoded now.

Proof: Postgres -> s3://apitap-bench/p2-a&b/, mode=replace twice. Both runs
succeed, the prefix holds only the live part files, and they hold the table.
Usage: e2e_s3_hardening.py amp
"""
import html
import re
import subprocess
import sys

import apitap

sys.path.insert(0, "benchmarks")
import _rig  # noqa: E402

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PREFIX = "p2-a&b"
URL = (f"s3://apitap-bench/{PREFIX}?format=parquet&endpoint=http://127.0.0.1:9100"
       "&region=us-east-1&access_key_id=bench&secret_access_key=benchbench")


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def keys():
    # The rig helper reads <Key> raw, as the sink did: decode here.
    return [html.unescape(k) for k in _rig.s3_list(f"{PREFIX}/")]


def wipe():
    for k in keys():
        _rig.s3_delete(k)
    pg("DROP TABLE IF EXISTS s3_amp")


def amp():
    t = "s3_amp"
    wipe()
    try:
        pg(f"CREATE TABLE {t} (id int PRIMARY KEY, v text); "
           f"INSERT INTO {t} SELECT g, md5(g::text) FROM generate_series(1, 1000) g")
        apitap.transfer(PG, URL, table=t, mode="replace")
        pg(f"INSERT INTO {t} SELECT g, md5(g::text) FROM generate_series(1001, 1500) g")
        apitap.transfer(PG, URL, table=t, mode="replace")
        got = sorted(keys())
        live = re.compile(rf"{re.escape(PREFIX)}/{t}/part-([a-z0-9]+)-\d+\.parquet")
        stray = [k for k in got if not live.fullmatch(k)]
        gens = {live.fullmatch(k).group(1) for k in got if live.fullmatch(k)}
        assert got and not stray and len(gens) == 1, \
            f"only one generation of part files may remain under {PREFIX}/: {got}"
        import duckdb
        d = duckdb.connect()
        d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
                  "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
        files = [f"s3://apitap-bench/{k}" for k in got]
        n = d.execute("SELECT count(*), count(DISTINCT id) FROM read_parquet(?)", [files]).fetchone()
        assert n == (1500, 1500), f"the second replace's 1500 rows, each once: {n}"
        print(f"  replace twice under '{PREFIX}/': {len(got)} part file(s), {n[0]} rows, nothing stale")
    finally:
        wipe()


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"amp": amp}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_s3_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ S3 hardening: {which[0]}")
