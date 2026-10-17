"""An Iceberg CDC drain refetches TOAST holes through the source's primary key.

Iceberg cannot read a TOAST cell back from its own files, so a masked update
is filled from the source's current row. The lookup compared
`"pk"::text = $1`, which no index serves: one sequential scan of the source
table per masked key — hours on a large table (MASTER review M-59). The key
now compares in its own type.

Proof: 1000 rows with an out-of-line body, bootstrapped into Iceberg; 200
updates that leave the body untouched (masked). Draining them must not
sequentially scan the source table (pg_stat_user_tables.seq_scan), must use
its index, and every body must survive.
"""
import json
import subprocess
import time
import urllib.request

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
NS = "cdc_e2e"
ICE = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
       "&access_key_id=bench&secret_access_key=benchbench")
T = "ice_toast_refetch"
TABLE_URL = f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{T}"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def src(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ice_drop():
    req = urllib.request.Request(TABLE_URL + "?purgeRequested=true", method="DELETE")
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def ice_rows():
    import duckdb
    d = duckdb.connect()
    d.execute("INSTALL iceberg; LOAD iceberg;")
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    with urllib.request.urlopen(TABLE_URL) as r:
        loc = json.load(r)["metadata-location"]
    return d.execute(f"SELECT count(*), sum(length(body)), sum(CASE WHEN v = 'x' THEN 1 ELSE 0 END) "
                     f"FROM iceberg_scan('{loc}')").fetchone()


def scans():
    time.sleep(1.5)  # cumulative stats flush at most once a second
    return [int(x) for x in src(f"SELECT seq_scan, coalesce(idx_scan, 0) FROM pg_stat_user_tables "
                                f"WHERE relname = '{T}'").split("|")]


def wipe():
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    ice_drop()


def main():
    wipe()
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v text, body text)")
    src(f"ALTER TABLE {T} ALTER body SET STORAGE EXTERNAL")
    src(f"INSERT INTO {T} SELECT g, 'a', repeat(md5(g::text), 100) FROM generate_series(1, 1000) g")
    apitap.transfer(SRC, ICE, table=T, mode="log_based")
    src(f"UPDATE {T} SET v = 'x' WHERE id % 5 = 0")            # 200 masked updates
    seq0, idx0 = scans()
    apitap.transfer(SRC, ICE, table=T, mode="log_based")
    seq1, idx1 = scans()
    n, total, updated = ice_rows()
    assert (n, total, updated) == (1000, 1000 * 3200, 200), f"Iceberg holds {n} rows, {total} body bytes, {updated} updated"
    print(f"  200 masked updates drained: seq_scan +{seq1 - seq0}, idx_scan +{idx1 - idx0}, every body intact")
    assert seq1 - seq0 == 0, f"the refetch must not scan the source table sequentially (+{seq1 - seq0})"
    assert idx1 - idx0 >= 200, f"the refetch must go through the key's index (+{idx1 - idx0})"
    wipe()
    print("✓ an Iceberg CDC drain refetches TOAST holes through the primary key")


if __name__ == "__main__":
    main()
