"""ClickHouse cluster behind a round-robin balancer — the full corporate sim.

The topology that produced three different production errors in one afternoon
(seen live on a real 2-node deployment): every HTTP request may reach a
DIFFERENT node, the database engine is Atomic (DDL does NOT replicate by
itself), and the balancer caps request bodies at 1 MB. Node-local staging then
fails three ways: DROP on node A + CREATE on node B ("already exists"),
CREATE on A + INSERT on B ("does not exist"), and big bodies 413 at the proxy.

With `on_cluster=` set, staging is now created ON CLUSTER with the user's
Replicated engine, finalize is a pure ON CLUSTER exchange, and the state table
replicates — so ANY request may land on ANY node. This file proves it on a
real 1-shard × 2-replica cluster (keeper + two 24.8 nodes) behind a
round-robin nginx with client_max_body_size 1m:

  1. transfer through the balancer lands every row, verified on BOTH nodes
     directly (count + sum(id) against the Postgres source);
  2. a second replace over the existing table stays green (the exchange path);
  3. mode="append" with on_cluster refuses loudly;
  4. without on_cluster the same rig fails (the refusal proves the balancer
     really does round-robin — if this leg ever passes, the rig is broken).

Rig: apitap-bench-ch-{a,b} + apitap-bench-keeper on docker net `chnet`,
balancer apitap-bench-chlb on :18200 (default/bench), cluster `benchcluster`.
"""
import os
import subprocess
import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
LB = "clickhouse://default:bench@127.0.0.1:18200/default"
T = "cluster_demo"


def node(name, sql):
    o = subprocess.run(
        ["docker", "exec", "-i", f"apitap-bench-ch-{name}", "clickhouse-client",
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


def clean():
    for t in (T, f"{T}_append", f"{T}__apitap_staging", f"{T}_append__apitap_staging",
              f"{T}__apitap_new", "_apitap_state"):
        node("a", f"DROP TABLE IF EXISTS {t} ON CLUSTER benchcluster SYNC")


def transfer(mode="replace", cluster=True):
    kw = dict(table="bench_data_1m", dest_table=T, mode=mode,
              chunk_bytes=256 * 1024)
    if cluster:
        kw.update(engine="ReplicatedMergeTree", on_cluster="benchcluster")
    return apitap.transfer(PG, LB, **kw)


ok = True
os.environ["APITAP_CH_MAX_BODY"] = "512K"        # the balancer caps at 1 MB
truth_n = pg("SELECT count(*) FROM bench_data_1m")
truth_s = pg("SELECT sum(id::bigint) FROM bench_data_1m")
clean()

print("== control: WITHOUT on_cluster the round-robin rig must fail ==")
try:
    transfer(cluster=False)
    ok = False
    print("   ✗ transfer without on_cluster SUCCEEDED — the balancer is not "
          "round-robining and every other leg here is meaningless")
except Exception as e:  # noqa: BLE001
    print(f"   ✓ failed as the topology dictates: {str(e).splitlines()[0][:110]}")
clean()

print("== the fix: on_cluster through the same balancer, 1M rows ==")
r = transfer()
print(f"   transfer: {r}")
for n in ("a", "b"):
    got_n = node(n, f"SELECT count() FROM {T}")
    got_s = node(n, f"SELECT sum(toInt64(id)) FROM {T}")
    if (got_n, got_s) == (truth_n, truth_s):
        print(f"   ✓ node {n}: {got_n} rows, sum(id)={got_s} — matches Postgres")
    else:
        ok = False
        print(f"   ✗ node {n}: rows={got_n} sum={got_s}, want {truth_n}/{truth_s}")
orphans = node("a", "SELECT count() FROM system.tables WHERE name LIKE '%apitap_staging%' OR name LIKE '%apitap_new%'")
orphans_b = node("b", "SELECT count() FROM system.tables WHERE name LIKE '%apitap_staging%' OR name LIKE '%apitap_new%'")
if orphans == orphans_b == "0":
    print("   ✓ no staging/shadow orphans on either node")
else:
    ok = False
    print(f"   ✗ orphans left behind: node-a={orphans} node-b={orphans_b}")

print("== replace over the EXISTING table (the exchange path) ==")
r = transfer()
print(f"   transfer: {r}")
# The report itself is a claim about the data and must not be short: it is read
# from whichever replica the balancer picked, and an unsynced one under-counts
# (measured 999,338 of 1,000,000 before sequential consistency was required).
if str(r.rows) == truth_n:
    print(f"   ✓ report says {r.rows} rows — agrees with the source")
else:
    ok = False
    print(f"   ✗ report says {r.rows}, source has {truth_n} — a lagging replica "
          f"answered the staged-row count")
for n in ("a", "b"):
    got_n = node(n, f"SELECT count() FROM {T}")
    if got_n == truth_n:
        print(f"   ✓ node {n}: still {got_n} rows after the swap")
    else:
        ok = False
        print(f"   ✗ node {n}: {got_n} rows after re-replace, want {truth_n}")

print("== the state table must be cluster-wide, not node-local ==")
# mode='replace' writes no watermark row (it rewrote everything; a later
# incremental run derives its own start) — verified identical on a single
# node, so this leg checks the property that IS cluster-specific: the table
# exists on every node with a Replicated engine. A node-local state table
# would let run N write its watermark on one node and run N+1 read a stale
# copy from the other: a silent incremental skip.
engines = {n: node(n, "SELECT engine FROM system.tables WHERE name='_apitap_state'")
           for n in ("a", "b")}
if all(e.startswith("Replicated") for e in engines.values()) and len(set(engines.values())) == 1:
    print(f"   ✓ _apitap_state is {engines['a']} on both nodes")
else:
    ok = False
    print(f"   ✗ state table is not cluster-wide: {engines}")

print("== mode='append' with on_cluster must refuse BEFORE moving data ==")
try:
    # Deliberately a run that WOULD have rows to move (fresh dest name), so a
    # refusal cannot be confused with 'nothing to append'.
    apitap.transfer(PG, LB, table="bench_data_1m", dest_table=f"{T}_append",
                    mode="append", chunk_bytes=256 * 1024,
                    engine="ReplicatedMergeTree", on_cluster="benchcluster")
    ok = False
    print("   ✗ append+on_cluster went through — the node-local attach is a "
          "silent-subset risk and must refuse")
except Exception as e:  # noqa: BLE001
    msg = str(e)
    if "append" in msg and "on_cluster" in msg:
        print("   ✓ refused, and the message names the reason")
    else:
        ok = False
        print(f"   ✗ refused with the wrong message: {msg[:160]}")
staged = node("a", f"SELECT count() FROM system.tables WHERE name LIKE '{T}_append%'")
if staged == "0":
    print("   ✓ nothing was created before the refusal")
else:
    ok = False
    print(f"   ✗ the refusal left {staged} table(s) behind")

clean()
print("\n" + ("CH CLUSTER E2E: ALL GREEN" if ok else "CH CLUSTER E2E: FAILED"))
raise SystemExit(0 if ok else 1)
