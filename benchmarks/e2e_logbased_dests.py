"""log_based E2E for non-Postgres destinations (ch | my | ice), on the rig.

Same op mix as e2e_logbased.py — bootstrap, insert/update/delete, PK-change,
empty-string vs NULL, unchanged-TOAST masked update, net-delete tx, empty
drain, TRUNCATE — plus bool and bytea columns to exercise the type-OID
translation (t/f, \\x hex) that non-Postgres apply paths do.

ice reads back through DuckDB's iceberg extension (equality-delete
merge-on-read is proven at smoke scale, see iceberg-showdown.md).

Usage: e2e_logbased_dests.py ch|my|ice
"""
import json, subprocess, sys, time, urllib.request

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
ICE_NS = "cdc_e2e"
DESTS = {
    "ch": "clickhouse://default:bench@127.0.0.1:8124/default",
    "my": "mysql://root:bench@127.0.0.1:3307/bench",
    "ice": f"iceberg://127.0.0.1:8181/{ICE_NS}?endpoint=http://127.0.0.1:9100"
           "&access_key_id=bench&secret_access_key=benchbench",
}
T = "cdc_demo"
which = sys.argv[1]
DST = DESTS[which]
# The parquet lane refuses bytea outright (pre-existing bulk-path limit,
# "cast it in a source view") — the ice leg runs without the blob column.
HAS_BLOB = which != "ice"



def sh(args):
    out = subprocess.run(args, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr or out.stdout)
    return out.stdout.strip()


def src(sql):
    return sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
               "-d", "apitap_bench_src", "-Atc", sql])


def ch(sql):
    return sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client",
               "-u", "default", "--password", "bench", "-q", sql])


def my(sql):
    return sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot",
               "-pbench", "bench", "-N", "-B", "-e", sql])


ICE_TABLE_URL = f"http://127.0.0.1:8181/v1/namespaces/{ICE_NS}/tables/{T}"


def ice_meta():
    with urllib.request.urlopen(ICE_TABLE_URL) as r:
        return json.load(r)


def ice_drop():
    req = urllib.request.Request(ICE_TABLE_URL + "?purgeRequested=true", method="DELETE")
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


_duck = None


def duck():
    global _duck
    if _duck is None:
        import duckdb
        _duck = duckdb.connect()
        _duck.execute("INSTALL iceberg; LOAD iceberg;")
        _duck.execute(
            "SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; "
            "SET s3_url_style='path'; SET s3_access_key_id='bench'; "
            "SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    return _duck


def run(label):
    t0 = time.time()
    r = apitap.transfer(SRC, DST, table=T, mode="log_based")
    print(f"{label}: rows={r.rows:,} in {time.time()-t0:.1f}s")
    return r


# Normalized row form on both sides: id|v|big|flag(1/0)|blob, NULL='<N>'.
# blob normalization follows each bulk lane's convention: ch stores the pg
# \x-text form verbatim (compare bytea::text), mysql stores raw bytes
# (compare upper hex).
def rows_src():
    # ch: compare the stored \x-text form via base64 (clickhouse-client
    # TSV-escapes backslashes on output, psql doesn't — base64 sidesteps it).
    blob = ("coalesce(encode(convert_to(blob::text,'UTF8'),'base64'),'<N>')"
            if which == "ch"
            else "coalesce(upper(encode(blob,'hex')),'<N>')")
    tail = f"||'|'||{blob}" if HAS_BLOB else ""
    return src(
        f"SELECT id||'|'||coalesce(v,'<N>')||'|'||coalesce(big,'<N>')||'|'"
        f"||coalesce((flag::int)::text,'<N>')"
        f"{tail} FROM {T} ORDER BY id")


def rows_dst():
    if which == "ch":
        return ch(
            f"SELECT concat(toString(id),'|',ifNull(v,'<N>'),'|',ifNull(big,'<N>'),'|',"
            f"ifNull(toString(toUInt8(flag)),'<N>'),'|',ifNull(base64Encode(blob),'<N>')) "
            f"FROM {T} ORDER BY id")
    if which == "my":
        return my(
            f"SELECT CONCAT(id,'|',COALESCE(v,'<N>'),'|',COALESCE(big,'<N>'),'|',"
            f"COALESCE(flag,'<N>'),'|',COALESCE(HEX(`blob`),'<N>')) FROM {T} ORDER BY id")
    q = (f"SELECT id || '|' || coalesce(v,'<N>') || '|' || coalesce(big,'<N>') || '|' "
         f"|| coalesce(cast(cast(flag as int) as varchar),'<N>') "
         f"FROM iceberg_scan('{ice_meta()['metadata-location']}') ORDER BY id")
    return "\n".join(r[0] for r in duck().execute(q).fetchall())


def check(label):
    a, b = rows_src(), rows_dst()
    ok = a == b
    print(f"CHECK {label}: {'MATCH' if ok else 'MISMATCH'} "
          f"(src {len(a.splitlines())} rows / dst {len(b.splitlines())} rows)")
    if not ok:
        sa, sb = a.splitlines(), b.splitlines()
        for i, (x, y) in enumerate(zip(sa, sb)):
            if x != y:
                print(f"  first diff at line {i}:\n  src: {x[:200]}\n  dst: {y[:200]}")
                break
        if len(sa) != len(sb):
            print(f"  src has {len(sa)} lines, dst has {len(sb)}")
        raise SystemExit(1)


def state_mode():
    if which == "ice":
        props = ice_meta()["metadata"].get("properties", {})
        cur = [v for k, v in props.items() if k.startswith("apitap.watermark-cursor.")]
        return "log_based" if cur == ["_lsn"] else f"bad-props:{props}"
    if which == "ch":
        return ch(f"SELECT mode FROM `_apitap_state` FINAL WHERE dest_table = '{T}' AND source_id NOT LIKE 'server-identity:%'")
    return my(f"SELECT mode FROM _apitap_state WHERE dest_table = '{T}' AND source_id NOT LIKE 'server-identity:%'")


# ── fresh start ─────────────────────────────────────────────────────────────
src(f"DROP TABLE IF EXISTS {T} CASCADE")
if which == "ch":
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}__apitap_cdc_del")
    ch(f"DELETE FROM `_apitap_state` WHERE dest_table = '{T}'") if ch(
        f"SELECT count() FROM system.tables WHERE name = '_apitap_state'") != "0" else None
elif which == "my":
    my(f"DROP TABLE IF EXISTS {T}")
    my(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'") if my(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='bench' AND table_name='_apitap_state'") != "0" else None
else:
    ice_drop()
for s in src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'").splitlines():
    if s:
        src(f"SELECT pg_drop_replication_slot('{s}')")
for p in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'").splitlines():
    if p:
        src(f"DROP PUBLICATION {p}")

blob_col = "blob bytea," if HAS_BLOB else ""
blob_ins = ", decode(lpad(to_hex(g), 8, '0'), 'hex')" if HAS_BLOB else ""
blob_names = ", blob" if HAS_BLOB else ""
src(f"""CREATE TABLE {T} (
      id int PRIMARY KEY, v text, big text,
      flag boolean, {blob_col}
      ts timestamp DEFAULT now())""")
src(f"INSERT INTO {T}(id, v, big, flag{blob_names}) "
    f"SELECT g, 'v'||g, NULL, g % 2 = 0{blob_ins} "
    f"FROM generate_series(1, 100000) g")
src(f"UPDATE {T} SET big = repeat('x', 200000) WHERE id = 42")  # real TOAST

# ── run 1: bootstrap ────────────────────────────────────────────────────────
r = run("run1 bootstrap")
assert r.rows == 100000, r.rows
check("after bootstrap")
assert state_mode() == "log_based", state_mode()

# ── window 1: the full op mix across several transactions ───────────────────
if HAS_BLOB:
    src(f"INSERT INTO {T}(id, v, flag, blob) VALUES (100001, 'new', true, '\\x00ff'), (100002, '', NULL, NULL)")
else:
    src(f"INSERT INTO {T}(id, v, flag) VALUES (100001, 'new', true), (100002, '', NULL)")
src(f"UPDATE {T} SET v = 'updated', flag = NOT flag WHERE id <= 5")
src(f"DELETE FROM {T} WHERE id BETWEEN 10 AND 19")
src(f"UPDATE {T} SET id = 999999 WHERE id = 7")                        # PK change
src(f"UPDATE {T} SET v = 'toast-kept' WHERE id = 42")                  # unchanged TOAST
src(f"INSERT INTO {T}(id, v) VALUES (100003, 'gone'); DELETE FROM {T} WHERE id = 100003")
if HAS_BLOB:
    src(f"UPDATE {T} SET blob = '\\xdeadbeef' WHERE id = 100")         # bytea update
r = run("run2 drain")
assert r.rows > 0
check("after mixed window")

# ── empty drain is a no-op ──────────────────────────────────────────────────
r = run("run3 empty")
assert r.rows == 0, r.rows
check("after empty drain")

# ── heavier window: set-based path with real volume ─────────────────────────
src(f"UPDATE {T} SET v = v || '+' WHERE id % 3 = 0")
src(f"DELETE FROM {T} WHERE id % 97 = 0")
src(f"INSERT INTO {T}(id, v, flag) SELECT g, 'late'||g, false FROM generate_series(200001, 250000) g")
r = run("run4 heavy")
check("after heavy window")

# ── TRUNCATE capture ────────────────────────────────────────────────────────
src(f"TRUNCATE {T}")
if HAS_BLOB:
    src(f"INSERT INTO {T}(id, v, flag, blob) VALUES (1, 'post-trunc', true, '\\x01')")
else:
    src(f"INSERT INTO {T}(id, v, flag) VALUES (1, 'post-trunc', true)")
r = run("run5 truncate")
check("after truncate")

print("ALL GREEN")

# ── cleanup (standing rule: dest tables dropped, seeds kept) ────────────────
src(f"DROP TABLE IF EXISTS {T} CASCADE")
if which == "ch":
    ch(f"DROP TABLE IF EXISTS {T}")
    ch(f"DELETE FROM `_apitap_state` WHERE dest_table = '{T}'")
elif which == "my":
    my(f"DROP TABLE IF EXISTS {T}")
    my(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")
else:
    ice_drop()
for s in src("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%'").splitlines():
    if s:
        src(f"SELECT pg_drop_replication_slot('{s}')")
for p in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'").splitlines():
    if p:
        src(f"DROP PUBLICATION {p}")
print("cleaned up")
