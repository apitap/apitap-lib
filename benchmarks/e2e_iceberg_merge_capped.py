#!/usr/bin/env python3
"""E2 (0.57.0 §2.D): an Iceberg merge delta does not grow memory.

0.56.0 materialised the delta's merge keys in a `KeyCap` until commit. The run
below moves 2.1M uuid keys, so that cap is ~150 MB beside the pipes — a
256 MB / 0.5 CPU cage is OOM-killed — and even a survivor commits exactly ONE
equality-delete file. This release streams one delete file per data file, in
lockstep with the data file's row groups, so memory is flat in the delta and
the snapshot summary shows `added-delete-files == added-data-files >= 2`.

The leg seeds a kept source table, runs an uncapped bootstrap, mutates the
source, then runs the delta inside `docker run --memory=256m --cpus=0.5`. It
asks DOCKER for OOMKilled/exit, the KERNEL for cgroup memory.peak, the CATALOG
for the current snapshot's summary, and DUCKDB for the readback (4.1M rows
would mean the equality deletes did not apply). The readback is a
`read_parquet` anti-join over the snapshot's own files, not `iceberg_scan`:
the iceberg extension's equality-delete scan allocated without bound on this
rig (13+ GB and climbing on a 164 MB table), while the anti-join is the same
merge-on-read semantics and stays under half a GB.

Requires: iceberg (REST catalog at 127.0.0.1:8181 + MinIO at 127.0.0.1:9100).
Usage: e2e_iceberg_merge_capped.py
"""
import json
import os
import re
import shlex
import subprocess
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _rig

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
NS = "e2e_cap"
TABLE = "ice_merge"
DEST = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
        "&access_key_id=bench&secret_access_key=benchbench")
CATALOG_URL = f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{TABLE}"
SEED_ROWS = 2_000_000
DELTA_ROWS = 100_000
CAP = 256 << 20
PEAK_FRACTION = 0.90


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def seed():
    """Reset the source to its pristine 2M-row state; kept between runs."""
    have = int(src(f"SELECT count(*) FROM pg_class WHERE relname='{TABLE}'"))
    rows = int(src(f"SELECT count(*) FROM {TABLE}")) if have else -1
    if rows == SEED_ROWS:
        return
    if have:
        src(f"DROP TABLE {TABLE}")
    print(f"seeding {TABLE}: {SEED_ROWS:,} rows (kept for the next run)")
    src(f"CREATE TABLE {TABLE} (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), "
        f"updated_at timestamptz DEFAULT now(), v text, n bigint)")
    src(f"INSERT INTO {TABLE} (v, n) SELECT md5(g::text), g*7 "
        f"FROM generate_series(1, {SEED_ROWS}) g")
    src(f"ANALYZE {TABLE}")


def cleanup():
    """Drop the Iceberg table and everything it wrote (the seed stays).

    A run killed before its release leaves its announcement under
    `metadata/apitap-runs/`, and nothing collects it on its own — a
    timestamp cannot tell a crashed run from a slow one — so the next run
    of this table refuses `locked`. This leg's scratch table is the one
    place that may collect it, and it must do so BEFORE the drop: the
    catalog metadata that names the base path goes with the table."""
    try:
        with urllib.request.urlopen(CATALOG_URL) as r:
            loc = json.load(r).get("metadata-location", "")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        loc = ""
    if loc.startswith("s3://apitap-bench/"):
        base = loc.split("s3://apitap-bench/", 1)[1].rsplit("/metadata/", 1)[0]
        for k in _rig.s3_list(base + "/metadata/apitap-runs/"):
            _rig.s3_delete(k)
            print(f"   swept a crashed run's marker: {k}")
    req = urllib.request.Request(CATALOG_URL + "?purgeRequested=true", method="DELETE")
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def current_snapshot():
    with urllib.request.urlopen(CATALOG_URL) as r:
        meta = json.load(r)
    md = meta["metadata"]
    cur = md.get("current-snapshot-id")
    for s in md.get("snapshots", []):
        if s["snapshot-id"] == cur:
            return meta, s["summary"]
    raise RuntimeError(f"no snapshot {cur} in {md.get('snapshots')}")


_duck = None


def duck():
    global _duck
    if _duck is None:
        import duckdb
        _duck = duckdb.connect()
        # DuckDB's default memory limit is 80% of host RAM; on the 62 GB bench
        # box an unbounded scan takes the whole host down with it. 2 GB is far
        # above this readback's measured ~430 MB and keeps the leg a guest.
        _duck.execute("SET memory_limit='2GB'; SET threads=2; "
                      "SET preserve_insertion_order=false;")
        _duck.execute("INSTALL httpfs; LOAD httpfs;")
        _duck.execute(
            "SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; "
            "SET s3_url_style='path'; SET s3_access_key_id='bench'; "
            "SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    return _duck


def readback(meta):
    """DuckDB reads the current snapshot's own files: every carried-over data
    file (the bootstrap's included), minus the equality-delete keys of the
    current merge run, plus that run's own same-sequence data. Returns
    (count|sum, why-not) — 4.1M rows means the deletes were not applied."""
    key = meta["metadata-location"].split("s3://apitap-bench/", 1)[1]
    base = key.rsplit("/metadata/", 1)[0]
    cur = meta["metadata"]["current-snapshot-id"]
    snaps = [k for k in _rig.s3_list(base + "/metadata/") if f"/snap-{cur}-" in k]
    if not snaps:
        return None, f"no manifest list for the current snapshot {cur}"
    run = snaps[0].rsplit("-", 1)[-1].removesuffix(".avro")
    data = _rig.s3_list(base + "/data/")
    new = [k for k in data if f"/{run}-" in k and not k.endswith("-deletes.parquet")]
    dele = [k for k in data if f"/{run}-" in k and k.endswith("-deletes.parquet")]
    old = [k for k in data if f"/{run}-" not in k and not k.endswith("-deletes.parquet")]
    if not dele:
        return None, "the current snapshot has no delete files"

    def uri(keys):
        return "[" + ",".join(f"'s3://apitap-bench/{k}'" for k in keys) + "]"

    parts = [f"SELECT id, n FROM read_parquet({uri(new)})"]
    if old:
        parts.append(f"SELECT id, n FROM read_parquet({uri(old)}) "
                     f"WHERE id NOT IN (SELECT id FROM read_parquet({uri(dele)}))")
    q = f"SELECT count(*)||'|'||sum(n) FROM ({' UNION ALL '.join(parts)})"
    return "|".join(str(x) for x in duck().execute(q).fetchone()), None


SITE_PACKAGES = None
IMAGE = None


def site_packages():
    global SITE_PACKAGES
    if SITE_PACKAGES is None:
        import apitap
        SITE_PACKAGES = os.path.dirname(os.path.dirname(os.path.abspath(apitap.__file__)))
    return SITE_PACKAGES


def run_capped():
    name = "apitap-bench-e2eim"
    sh(["docker", "rm", "-f", name])
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {DEST!r}, table={TABLE!r}, "
            "mode='merge', cursor='updated_at')\n"
            "print('rows', r.rows)")
    script = (f"python -c {shlex.quote(code)}; "
              "echo MEMPEAK=$(cat /sys/fs/cgroup/memory.peak)")
    r = sh(["docker", "run", "--name", name, "--network", "host",
            f"--memory={CAP >> 20}m", f"--memory-swap={CAP >> 20}m", "--cpus=0.5",
            "-v", f"{site_packages()}:/py:ro", "-e", "PYTHONPATH=/py",
            "-e", "APITAP_DEBUG=1", IMAGE, "sh", "-c", script])
    state = sh(["docker", "inspect", "-f",
                "{{.State.OOMKilled}} {{.State.ExitCode}}", name]).stdout.strip()
    sh(["docker", "rm", "-f", name])
    return r, state


def main():
    global IMAGE
    IMAGE = f"python:{sys.version_info.major}.{sys.version_info.minor}-slim"
    if sh(["docker", "container", "inspect", "apitap-bench-icecat"]).returncode != 0:
        _rig.rig_fail("the Iceberg REST catalog (apitap-bench-icecat) is not running")
    if sh(["docker", "image", "inspect", IMAGE]).returncode != 0:
        print(f"pulling {IMAGE} …")
        if sh(["docker", "pull", IMAGE]).returncode != 0:
            _rig.rig_fail(f"cannot get {IMAGE}")

    import apitap
    seed()
    cleanup()  # a hard-killed previous run may have left its table behind
    bad = []
    try:
        print("\n-- run 1: bootstrap 2M rows (uncapped)")
        r = apitap.transfer(SRC, DEST, table=TABLE, mode="merge", cursor="updated_at")
        print(f"   rows={r.rows:,}")
        if r.rows != SEED_ROWS:
            bad.append(f"run 1 loaded {r.rows}, want {SEED_ROWS}")

        print(f"-- delta: {SEED_ROWS:,} updates + {DELTA_ROWS:,} inserts")
        src(f"UPDATE {TABLE} SET n = n + 1, updated_at = now()")
        src(f"INSERT INTO {TABLE} (v, n) SELECT md5(g::text), g*7 "
            f"FROM generate_series(1, {DELTA_ROWS}) g")

        print("-- run 2: the delta in a 256 MB / 0.5 CPU cage")
        r, state = run_capped()
        out = (r.stdout or "") + (r.stderr or "")
        st = state.split()
        oom, rc = st if len(st) == 2 else (state, "?")
        plan = re.search(r"\[plan\] chunk=(\d+)KiB row_group=(\d+)MiB pipes=(\d+)", out)
        if plan:
            print(f"   plan chunk={plan.group(1)}KiB row_group={plan.group(2)}MiB "
                  f"pipes={plan.group(3)}")
        peak_m = re.search(r"MEMPEAK=(\d+)", r.stdout or "")
        peak = int(peak_m.group(1)) if peak_m else None
        if oom != "false" or rc != "0":
            if peak is not None:
                print(f"   memory.peak={peak / 1048576:.0f}MB (cage {CAP >> 20}MB)")
            print(out[-1500:])
            bad.append(f"docker says OOMKilled={oom} exit={rc} — the delta was "
                       "materialised or the cage was breached")
        elif peak is None:
            print(out[-1500:])
            bad.append(f"the run printed no MEMPEAK (docker state {state})")
        elif peak > PEAK_FRACTION * CAP:
            bad.append(f"memory.peak {peak / 1048576:.0f}MB > {PEAK_FRACTION:.0%} of "
                       f"{CAP >> 20}MB")

        meta, summary = current_snapshot()
        print(f"   snapshot summary: operation={summary.get('operation')} "
              f"added-records={summary.get('added-records')} "
              f"added-delete-files={summary.get('added-delete-files')} "
              f"added-data-files={summary.get('added-data-files')} "
              f"added-equality-deletes={summary.get('added-equality-deletes')}")
        if summary.get("operation") != "overwrite":
            bad.append(f"operation {summary.get('operation')!r} != 'overwrite'")
        if summary.get("added-records") != str(SEED_ROWS + DELTA_ROWS):
            bad.append(f"added-records {summary.get('added-records')!r} != "
                       f"{SEED_ROWS + DELTA_ROWS}")
        if summary.get("added-equality-deletes") != str(SEED_ROWS + DELTA_ROWS):
            bad.append(f"added-equality-deletes {summary.get('added-equality-deletes')!r} "
                       f"!= {SEED_ROWS + DELTA_ROWS} (the delete set was not complete)")
        n_df = int(summary.get("added-data-files", 0))
        n_del = int(summary.get("added-delete-files", 0))
        if n_del != n_df or n_del < 2:
            bad.append(f"added-delete-files {n_del} != added-data-files {n_df} "
                       "(or < 2): the delta still commits one delete file")
        if peak is not None:
            print(f"   memory.peak={peak / 1048576:.0f}MB "
                  f"({peak * 100 // CAP}% of {CAP >> 20}MB)")

        got, why = readback(meta)
        if why:
            bad.append(f"readback: {why}")
        else:
            want = src(f"SELECT count(*)||'|'||sum(n) FROM {TABLE}")
            if got != want:
                bad.append(f"readback {got} != source {want} "
                           "(4.1M rows means the equality deletes did not apply)")
            else:
                print(f"   readback == source ({got})")
    finally:
        cleanup()

    if bad:
        print("\nE2 ICEBERG MERGE CAPPED: FAILED")
        for b in bad:
            print(f"  !! {b}")
        return 1
    print("\nE2 ICEBERG MERGE CAPPED: ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
