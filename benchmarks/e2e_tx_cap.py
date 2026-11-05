"""A Postgres transaction past APITAP_TX_BUF_BYTES spills to disk and lands whole (A2).

The drain applies a transaction whole after its commit. Through 0.65.0, one
larger than `APITAP_TX_BUF_BYTES` was refused, watermark kept: a single UPDATE
over a big table stopped the lane until the write was split on the source or
the cap raised past what the container holds. From 0.66 the transaction's ops
move to a local file at the cap. After the commit they replay in window-sized
chunks, and every chunk but the last holds the watermark at the previous
commit, so memory is bounded by the window budget, not by the transaction.

Postgres hands a transaction over in one of two ways, and both are covered:

  after      the server buffers it and ships it at commit (the transaction
             fits the server's logical_decoding_work_mem)
  streamed   the server ships it in progress (logical_decoding_work_mem set
             to 64kB here). It carries a savepoint that rolls back after its
             rows were streamed, and the drain spills them, so the replay must
             skip them

Each case is one transaction: an UPDATE of every row, 2,000 INSERTs and 1,000
DELETEs. It runs under a 4 MiB cap into three destinations:

  ClickHouse MergeTree (the default), ClickHouse
  ReplacingMergeTree(_apitap_ver, _apitap_deleted) (rows versioned by the
  window, so chunks must carry their own versions), and Postgres.

Each run is checked for:

  - more than one chunk replayed, the last one marked (APITAP_DEBUG)
  - the destination's rows equal the source's, digest for digest
  - a changelog=True lane still refuses with the cap and names the reason
    (its replay rule cannot tell chunks from re-drains yet)
"""
import os
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "tx_cap"
ROWS = 30000  # ~15 MiB of row frames at 400 bytes/row inside one transaction
ENGINE = "ReplacingMergeTree(_apitap_ver, _apitap_deleted)"
DESTS = {
    "ch": (CH, T + "_ch", {}),
    "ch_tomb": (CH, T + "_cht", {"engine": ENGINE}),
    "pg": (PGD, T + "_pg", {}),
}
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def psql(sql, box="apitap-bench-pg-src", db="apitap_bench_src"):
    o = sh(["docker", "exec", "-i", box, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1",
            "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pgd(sql):
    return psql(sql, "apitap-bench-pg-dst", "apitap_bench_dst")


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


SRC_DIGEST = f"SELECT md5(string_agg(id || ':' || v, ',' ORDER BY id)) FROM {T}"


def dest_digest(name):
    url, dt, kw = DESTS[name]
    if name == "pg":
        return pgd(f"SELECT md5(string_agg(id || ':' || v, ',' ORDER BY id)) FROM {dt}")
    final = " FINAL WHERE _apitap_deleted = 0" if "engine" in kw else ""
    return ch(f"SELECT lower(hex(MD5(arrayStringConcat(groupArray(s), ',')))) FROM "
              f"(SELECT concat(toString(id), ':', v) AS s FROM {dt}{final} ORDER BY id)")


def wipe():
    for name, (url, dt, kw) in DESTS.items():
        if name == "pg":
            pgd(f"DROP TABLE IF EXISTS {dt}")
            if pgd("SELECT to_regclass('_apitap_state') IS NOT NULL") == "t":
                pgd(f"DELETE FROM _apitap_state WHERE dest_table = '{dt}'")
        else:
            ch(f"DROP TABLE IF EXISTS {dt}")
            ch(f"DROP TABLE IF EXISTS {dt}__apitap_cl")
            ch(f"DROP TABLE IF EXISTS {dt}__current")
            if ch("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
                ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{dt}' SETTINGS mutations_sync=1")


SLOTS_BEFORE = set(psql("SELECT slot_name FROM pg_replication_slots").split())


def drop_our_slots():
    for s in sorted(set(psql("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS_BEFORE):
        psql(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
             f"WHERE slot_name = '{s}' AND NOT active")
        psql(f'DROP PUBLICATION IF EXISTS "{s}_pub"')


def drain(name, cap=None, changelog=False):
    url, dt, kw = DESTS[name]
    extra = dict(kw, changelog=True) if changelog else kw
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {url!r}, table={T!r}, dest_table={dt!r}, mode='log_based', "
            f"**{extra!r})\n"
            "print('ROWS', r.rows)\n")
    env = dict(os.environ, APITAP_DEBUG="1")
    if cap:
        # A 1 MiB window makes the ~13 MiB transaction several chunks.
        env.update(APITAP_TX_BUF_BYTES=cap, APITAP_CDC_WINDOW_BYTES=str(1 << 20))
    return sh([sys.executable, "-c", code], env=env, timeout=600)


def big_tx(streamed, base):
    # One transaction: every row rewritten, 2,000 new (ids from `base`), some
    # gone; the streamed one also rolls a savepoint back after its rows went out.
    sp = (f"SAVEPOINT s; INSERT INTO {T} SELECT g, 'rolled-back' FROM generate_series({base + 5001}, "
          f"{base + 9000}) g; ROLLBACK TO s; ") if streamed else ""
    psql(f"BEGIN; UPDATE {T} SET v = v || 'u'; {sp}"
         f"INSERT INTO {T} SELECT g, repeat('n', 400) FROM generate_series({base + 1}, {base + 2000}) g; "
         f"DELETE FROM {T} WHERE id % 30 = 0; COMMIT;")


def set_work_mem(v):
    psql(f"ALTER SYSTEM SET logical_decoding_work_mem = '{v}'")
    psql("SELECT pg_reload_conf()")


def main():
    wipe()
    drop_our_slots()
    psql(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v text); "
         f"INSERT INTO {T} SELECT g, repeat('x', 400) FROM generate_series(1, {ROWS}) g")
    try:
        for name in DESTS:
            r = drain(name)
            case(f"{name}: bootstrap", r.returncode == 0, r.stderr.strip()[-300:])
        for mode, streamed in (("after", False), ("streamed", True)):
            print(f"════════ {mode}: one transaction past a 4 MiB cap ════════", flush=True)
            set_work_mem("64kB" if streamed else "64MB")
            big_tx(streamed, ROWS + (20000 if streamed else 0))
            want = psql(SRC_DIGEST)
            for name in DESTS:
                r = drain(name, cap="4M")
                chunks = [l for l in r.stderr.splitlines() if "spilled transaction: chunk" in l]
                case(f"{mode}/{name}: the run lands", r.returncode == 0, r.stderr.strip()[-300:])
                case(f"{mode}/{name}: replayed in chunks, the last one marked",
                     len(chunks) >= 2 and "(last)" in chunks[-1], f"{len(chunks)} chunk(s)")
                got = dest_digest(name)
                case(f"{mode}/{name}: the destination equals the source", got == want,
                     f"dest {got} source {want}")
        print("════════ changelog=True keeps the refusal ════════", flush=True)
        set_work_mem("64MB")
        ch(f"DROP TABLE IF EXISTS {T}_chl")
        DESTS["chl"] = (CH, T + "_chl", {})
        r = drain("chl", changelog=True)
        case("changelog: bootstrap", r.returncode == 0, r.stderr.strip()[-300:])
        big_tx(False, ROWS + 40000)
        r = drain("chl", cap="4M", changelog=True)
        out = r.stderr + r.stdout
        case("changelog: past the cap is refused, naming the cap and why",
             r.returncode != 0 and "APITAP_TX_BUF_BYTES" in out and "changelog=True" in out, out.strip()[-300:])
    finally:
        psql("ALTER SYSTEM RESET logical_decoding_work_mem")
        psql("SELECT pg_reload_conf()")
        wipe()
        psql(f"DROP TABLE IF EXISTS {T}")
        drop_our_slots()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 — the verdict line is the gate's contract
        print(f"   XX {e}")
        ok = False
    print("TX SPILL E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
