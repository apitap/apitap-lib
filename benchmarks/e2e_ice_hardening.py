"""Iceberg hardening (MinIO :9100 + REST :8181).

P2 findings (docs/review/2026-10-18-p2-triage.md):
  v3        only format-version 1 was refused: a v3 table got v2 manifest lists
            and snapshots without row lineage. Now anything but v2 is refused.
  numeric   numeric(2,4) (PG15+, values like 0.0012) became decimal(2,2):
            0.0012 landed as 0.00.
Usage: e2e_ice_hardening.py v3|numeric
"""
import json
import subprocess
import sys
import urllib.request

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
NS = "cdc_e2e"
REST = "http://127.0.0.1:8181/v1"
ICE = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
       "&access_key_id=bench&secret_access_key=benchbench")


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def rest(method, path, body=None):
    req = urllib.request.Request(REST + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def drop(t):
    try:
        rest("DELETE", f"/namespaces/{NS}/tables/{t}?purgeRequested=true")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def ice_rows(t, sql):
    import duckdb
    d = duckdb.connect()
    d.execute("INSTALL iceberg; LOAD iceberg;")
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    loc = rest("GET", f"/namespaces/{NS}/tables/{t}")["metadata-location"]
    return d.execute(sql.replace("{T}", f"iceberg_scan('{loc}')")).fetchall()


def v3():
    t = "ice_v3_refused"
    pg(f"DROP TABLE IF EXISTS {t}")
    drop(t)
    try:
        pg(f"CREATE TABLE {t} (id bigint PRIMARY KEY); INSERT INTO {t} SELECT generate_series(1, 5)")
        schema = {"type": "struct", "schema-id": 0,
                  "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}]}
        try:
            made = rest("POST", f"/namespaces/{NS}/tables",
                        {"name": t, "schema": schema, "properties": {"format-version": "3"}})
        except urllib.error.HTTPError as e:
            print(f"  (rig) the REST catalog will not create a v3 table ({e.code}); nothing to refuse")
            return
        fv = made["metadata"]["format-version"]
        assert fv == 3, f"the fixture made format-version {fv}, not 3"
        try:
            apitap.transfer(PG, ICE, table=t, mode="append", cursor="id")
            m = None
        except Exception as e:  # noqa: BLE001
            m = str(e)
        assert m and "format-version 3" in m, f"a v3 table must be refused, got {m!r}"
        print(f"  append into a format-version 3 table: refused ({m[:70]}...)")
    finally:
        pg(f"DROP TABLE IF EXISTS {t}")
        drop(t)


def numeric():
    t = "ice_numeric_24"
    pg(f"DROP TABLE IF EXISTS {t}")
    drop(t)
    try:
        pg(f"CREATE TABLE {t} (id int PRIMARY KEY, n numeric(2,4)); INSERT INTO {t} VALUES (1, 0.0012), (2, -0.0099)")
        apitap.transfer(PG, ICE, table=t, mode="replace")
        from decimal import Decimal
        got = [Decimal(r[0]) for r in ice_rows(t, "SELECT CAST(n AS VARCHAR) FROM {T} ORDER BY id")]
        assert got == [Decimal("0.0012"), Decimal("-0.0099")], f"numeric(2,4) values exact, got {got}"
        print(f"  numeric(2,4) into Iceberg: {[str(g) for g in got]}")
    finally:
        pg(f"DROP TABLE IF EXISTS {t}")
        drop(t)


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"v3": v3, "numeric": numeric}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_ice_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ Iceberg hardening: {which[0]}")
