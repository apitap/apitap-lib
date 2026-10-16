"""An Iceberg append on a MySQL BIGINT UNSIGNED cursor keeps working after run 1.

A first append into a missing Iceberg table lands as an overwrite, and an
overwrite cleared every apitap watermark and wrote none; run 2 then had to
rebuild the watermark from parquet footer statistics, which could not read
the decimal(20,0) an UNSIGNED BIGINT lands as — so every run after the first
failed, and its advice ("run once with mode='replace'") looped back to the
same state (MASTER review M-34, reproduced). The flipped bootstrap now seeds
its own watermark, and whole-number decimal statistics are read.

Proof: three appends, the deltas crossing 2^63; each lands exactly its new
rows and the catalog's snapshot total matches the source.
"""
import json
import subprocess
import sys
import urllib.request

MY = "mysql://root:bench@127.0.0.1:3307/bench"
NS = "cdc_e2e"
ICE = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
       "&access_key_id=bench&secret_access_key=benchbench")
T = "ice_u64_cursor"
BIG = 2 ** 63


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
    except Exception:
        pass


def ice_total():
    with urllib.request.urlopen(f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{T}") as r:
        meta = json.load(r)["metadata"]
    # The live rows: added-records along the parent chain back to (and
    # including) the last overwrite. Appends only add here.
    by_id = {s["snapshot-id"]: s for s in meta["snapshots"]}
    sid, total = meta["current-snapshot-id"], 0
    while sid is not None and sid in by_id:
        snap = by_id[sid]
        total += int(snap["summary"].get("added-records", 0))
        if snap["summary"].get("operation") == "overwrite":
            break
        sid = snap.get("parent-snapshot-id")
    return total


def insert(lo, n, base):
    rows = ",".join(f"({lo + i}, {base + lo + i}, 'r')" for i in range(n))
    my(f"INSERT INTO {T} VALUES {rows}")


def run():
    code = (f"import apitap\nr = apitap.transfer({MY!r}, {ICE!r}, table={T!r}, mode='append', cursor='c')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def main():
    drop_ice()
    my(f"DROP TABLE IF EXISTS {T}")
    my(f"CREATE TABLE {T} (id int PRIMARY KEY, c bigint unsigned NOT NULL, v text)")
    insert(1, 100, 0)
    want = 0
    for rnd, (n, base) in enumerate(((100, 0), (50, BIG - 120), (50, BIG))):
        if rnd:
            insert(1000 * rnd, n, base)
        r = run()
        want += n
        assert r.returncode == 0, f"run {rnd + 1} failed:\n{r.stderr[-600:]}"
        got = r.stdout.strip().splitlines()[0]
        assert got == f"ROWS {n}", f"run {rnd + 1} landed {got!r}, expected {n} new rows"
        assert ice_total() == want, f"run {rnd + 1}: catalog total {ice_total()} vs {want}"
        print(f"  run {rnd + 1}: +{n} rows, total {want}")
    drop_ice()
    my(f"DROP TABLE IF EXISTS {T}")
    print("✓ Iceberg append on an UNSIGNED BIGINT cursor: every run after the first resumes, past 2^63")


if __name__ == "__main__":
    main()
