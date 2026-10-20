"""An Iceberg append bootstraps its watermark past an all-NULL cursor row group.

With no watermark property (after a mode='replace', or on a table another
engine wrote), an append derives the watermark from the data files' footer
statistics. A row group whose cursor is NULL in every row has statistics
with no min/max, and the bootstrap refused it with "run once with
mode='replace'": a loop, since a replace seeds no watermark and the next
append read the same footers again (deep review D22). The delta never
selects a NULL cursor, so such a group says nothing about the watermark.

Proof (mysql -> iceberg): mode='replace' of 500 rows whose ts is NULL (one
data file, its ts all NULL); 500 rows with ts set, appended on cursor='id'
(a second file). With the watermark properties removed, an append on
cursor='ts' derives its watermark from both footers; then 10 newer rows and one more
append. The table must end with 1010 rows, none twice.
"""
import json
import subprocess
import urllib.request

import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
NS = "cdc_e2e"
ICE = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
       "&access_key_id=bench&secret_access_key=benchbench")
T = "ice_null_cursor_group"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def my(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def drop_ice():
    req = urllib.request.Request(
        f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{T}?purgeRequested=true", method="DELETE")
    try:
        urllib.request.urlopen(req)
    except Exception:  # noqa: BLE001 — absent is fine
        pass


def clear_watermarks():
    """Drop the table's apitap.watermark* properties: the state a replace, or
    another engine's writes, leave behind, where an append derives its
    watermark from the data files' footers."""
    url = f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{T}"
    with urllib.request.urlopen(url) as r:
        props = json.load(r)["metadata"].get("properties", {})
    keys = [k for k in props if k.startswith("apitap.watermark")]
    assert keys, f"(rig) the cursor='id' append must have left a watermark property: {sorted(props)}"
    body = json.dumps({"requirements": [], "updates": [{"action": "remove-properties", "removals": keys}]})
    req = urllib.request.Request(url, data=body.encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req).read()


def scan_count():
    import duckdb
    d = duckdb.connect()
    d.execute("INSTALL iceberg; LOAD iceberg;")
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{T}") as r:
        loc = json.load(r)["metadata-location"]
    return d.execute(f"SELECT count(*) FROM iceberg_scan('{loc}')").fetchone()[0]


def main():
    drop_ice()
    my(f"DROP TABLE IF EXISTS {T}")
    try:
        my(f"CREATE TABLE {T} (id int PRIMARY KEY, ts datetime(6) NULL, v varchar(8)); "
           f"INSERT INTO {T} WITH RECURSIVE g(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM g WHERE i < 500) "
           f"SELECT i, NULL, 'old' FROM g")
        apitap.transfer(MY, ICE, table=T, mode="replace")
        my(f"INSERT INTO {T} WITH RECURSIVE g(i) AS (SELECT 501 UNION ALL SELECT i + 1 FROM g WHERE i < 1000) "
           f"SELECT i, TIMESTAMP('2026-10-01') + INTERVAL i SECOND, 'v' FROM g")
        apitap.transfer(MY, ICE, table=T, mode="append", cursor="id")
        assert scan_count() == 1000, "(rig) 1000 rows in two files"
        clear_watermarks()
        apitap.transfer(MY, ICE, table=T, mode="append", cursor="ts")
        my(f"INSERT INTO {T} WITH RECURSIVE g(i) AS (SELECT 1001 UNION ALL SELECT i + 1 FROM g WHERE i < 1010) "
           f"SELECT i, TIMESTAMP('2026-10-02') + INTERVAL i SECOND, 'new' FROM g")
        apitap.transfer(MY, ICE, table=T, mode="append", cursor="ts")
        n = scan_count()
        assert n == 1010, f"the table holds {n} rows after the appends, want 1010"
        print(f"  an all-NULL ts file next to one with values, then appends on ts: {n} rows")
    finally:
        drop_ice()
        my(f"DROP TABLE IF EXISTS {T}")
    print("✓ an Iceberg append bootstraps its watermark past an all-NULL cursor row group")


if __name__ == "__main__":
    main()
