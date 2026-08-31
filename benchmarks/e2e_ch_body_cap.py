"""ClickHouse behind a body-limiting proxy — the 413 and the cap that fixes it.

A corporate ClickHouse is usually reached through nginx/an ingress, and those
cap the request body (nginx's `client_max_body_size` defaults to 1 MB). apitap
streams each worker's data as ONE chunked request, so the whole table part
counts as one body: no chunk setting can get under the limit, and the user
sees `413 Request Entity Too Large` from the proxy, not from ClickHouse.

`APITAP_CH_MAX_BODY` (bytes, or K/M/G) makes the loader end the current INSERT and open
another once the cap is reached. This file proves both halves against a real
nginx with a 1 MB limit: without the cap the transfer must FAIL with a 413
that names the fix, and with it the same transfer must land every row.

Rig: `apitap-bench-chproxy` (nginx :18125, client_max_body_size 1m) in front of
`apitap-bench-ch` (:8124); source is the Postgres bench box.
"""
import os
import subprocess
import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PROXY = "clickhouse://default:bench@127.0.0.1:18125/default"
SRC_TABLE = "bench_data_1m"
T = "body_cap_demo"


def ch(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-Atc", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def transfer(chunk=None):
    # chunk_bytes must be <= the body cap: a request can only end between
    # whole rows, so a buffer bigger than the cap has nowhere to go.
    return apitap.transfer(PG, CH_PROXY, table=SRC_TABLE, dest_table=T,
                           mode="replace", chunk_bytes=chunk)


ok = True
def drop_artifacts():
    """Drop the destination AND every artifact of it, tokenized or not.

    The control run below is MEANT to fail, and a failed run leaves its staging
    table behind — deliberately, since 0.55.0: nothing collects a crashed run's
    workspace on a timer, because the token records when the run STARTED and so
    cannot prove the object is dead. The next run therefore refuses while it is
    there, which is correct behaviour and was this leg failing for the right
    reason. Dropping by name is not enough any more: the name carries a run
    token, so the artifacts have to be discovered.
    """
    ch(f"DROP TABLE IF EXISTS {T}")
    rows = ch("SELECT name FROM system.tables WHERE database = currentDatabase() "
              f"AND name LIKE '{T}%__apitap_%'")
    for name in (rows or "").split():
        ch(f"DROP TABLE IF EXISTS {name}")


drop_artifacts()

print("== without the cap: the proxy must refuse, and say how to fix it ==")
os.environ.pop("APITAP_CH_MAX_BODY", None)
try:
    transfer()
    ok = False
    print("   ✗ expected a 413 from the proxy, transfer succeeded instead")
except Exception as e:  # noqa: BLE001 — any failure type, we assert on the text
    msg = str(e)
    need = ["413", "APITAP_CH_MAX_BODY", "client_max_body_size"]
    missing = [n for n in need if n not in msg]
    if missing:
        ok = False
        print(f"   ✗ 413 error does not mention {missing}:\n      {msg[:400]}")
    else:
        print("   ✓ refused with a 413 that names the proxy limit AND the fix")

print("== with the cap: every row lands, through the same 1 MB proxy ==")
os.environ["APITAP_CH_MAX_BODY"] = "512K"
drop_artifacts()
r = transfer(chunk=256 * 1024)
print(f"   transfer: {r}")
src_n, src_s = pg(f"SELECT count(*) FROM {SRC_TABLE}"), pg(f"SELECT sum(id::bigint) FROM {SRC_TABLE}")
dst_n, dst_s = ch(f"SELECT count() FROM {T}"), ch(f"SELECT sum(toInt64(id)) FROM {T}")
if (src_n, src_s) == (dst_n, dst_s):
    print(f"   ✓ {dst_n} rows, sum(id)={dst_s} — identical to the source")
else:
    ok = False
    print(f"   ✗ mismatch  src=({src_n},{src_s})  dst=({dst_n},{dst_s})")

print("== the cap must not change what a DIRECT connection does ==")
os.environ["APITAP_CH_MAX_BODY"] = "512K"
ch(f"DROP TABLE IF EXISTS {T}")
apitap.transfer(PG, "clickhouse://default:bench@127.0.0.1:8124/default",
                table=SRC_TABLE, dest_table=T, mode="replace", chunk_bytes=256 * 1024)
dst_n2 = ch(f"SELECT count() FROM {T}")
if dst_n2 == src_n:
    print(f"   ✓ direct connection with the cap set: {dst_n2} rows")
else:
    ok = False
    print(f"   ✗ direct+cap mismatch: {dst_n2} vs {src_n}")

ch(f"DROP TABLE IF EXISTS {T}")
os.environ.pop("APITAP_CH_MAX_BODY", None)
print("\n" + ("CH BODY-CAP E2E: ALL GREEN" if ok else "CH BODY-CAP E2E: FAILED"))
raise SystemExit(0 if ok else 1)
