"""`slots=2`: two groups, two tenures — evicting one group stops only it.

Four tables, `slots=2`: {a, b} and {c, d} drain concurrently, each group over
its own slot, and each under its own tenure — its own run token, its own lease
rows, its own keeper. A peer that claims {a, b} must stop that group at its
next write and leave {c, d} to finish; the caller must be told exactly which
tables failed, the way every multi-table run reports a partial failure.

The leg (pg -> pg, TTL 30):
  1. bootstrap all four; queue a backlog on every table;
  2. drain with slots=2, and the moment {a, b}'s lock appears, collect its
     lease rows (`UPDATE _apitap_lease SET collected = true`), then count a, b;
  3. ask the servers:
     - the run raised `MultiTransferError`, whose report names a and b as
       failed and c and d as done;
     - c and d equal their source, and their watermarks advanced;
     - a and b hold exactly what they held when their claim was collected.

RED on 0.56.0: a failed slot group raised one plain error for the whole call,
so the caller could not tell that c and d had landed. Two-sided: a single
run-wide lease key would stop c and d with a and b.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545.
"""
import os
import sys
import threading

import psycopg2

import _rig

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
TABLES = ["slots_a", "slots_b", "slots_c", "slots_d"]
EVICT, KEEP = TABLES[:2], TABLES[2:]
os.environ["APITAP_LEASE_TTL_SECS"] = "30"
os.environ["APITAP_CDC_WINDOW_BYTES"] = "1048576"

import apitap  # noqa: E402  (after the env it reads)

ok = True


def case(name, passed, detail=""):
    global ok
    ok &= passed
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def dst(sql):
    return _rig.psql(sql, _rig.PG_DST)


def clean():
    for p in src("SELECT DISTINCT pubname FROM pg_publication_tables "
                 f"WHERE tablename IN ({', '.join(repr(t) for t in TABLES)})").split():
        if not p:
            continue
        slot = p[: -len("_pub")]
        src(f"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
            f"WHERE slot_name = '{slot}' AND active")
        _rig.wait_for(lambda: src(f"SELECT active FROM pg_replication_slots WHERE slot_name = '{slot}'") != "t", 10)
        src(f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name = '{slot}'")
        src(f"DROP PUBLICATION IF EXISTS {p}")
    for t in TABLES:
        src(f"DROP TABLE IF EXISTS {t} CASCADE")
        dst(f"DROP TABLE IF EXISTS {t} CASCADE")
        for n in dst(f"SELECT relname FROM pg_class WHERE relkind = 'r' "
                     f"AND relname LIKE '{t}\\_%\\_\\_apitap\\_%'").split():
            if n:
                dst(f'DROP TABLE IF EXISTS "{n}"')
    if dst("SELECT to_regclass('_apitap_state') IS NOT NULL") == "t":
        dst(f"DELETE FROM _apitap_state WHERE dest_table IN ({', '.join(repr(t) for t in TABLES)})")
    if dst("SELECT to_regclass('_apitap_lease') IS NOT NULL") == "t":
        dst("DELETE FROM _apitap_lease WHERE " + " OR ".join(f"dest_key LIKE '%.{t}'" for t in TABLES))


def watermark(t):
    return dst(f"SELECT watermark FROM _apitap_state WHERE dest_table = '{t}' "
               "AND source_id NOT LIKE 'server-identity:%'")


def count(sql_fn, t):
    return int(sql_fn(f"SELECT count(*) FROM {t}"))


def main():
    clean()
    try:
        for t in TABLES:
            src(f"CREATE TABLE {t} (id int PRIMARY KEY, pad text)")
            src(f"INSERT INTO {t} SELECT g, 'seed' FROM generate_series(1, 100) g")
        apitap.transfer(SRC, DST, tables=TABLES, mode="log_based", slots=2)
        before = {t: watermark(t) for t in TABLES}
        if not all(before.values()):
            _rig.rig_fail(f"the bootstrap left no watermark: {before}")
        # A backlog of many transactions on every table: several windows each.
        for t in TABLES:
            for i in range(40):
                lo = 1000 + i * 500
                src(f"INSERT INTO {t} SELECT g, repeat('x', 1500) FROM generate_series({lo}, {lo + 499}) g")

        # The evictor: once {a, b}'s lock exists, collect both lease rows of
        # that token, then count a and b — anything committed before the claim
        # (a unit already holding the row finishes first) is in that count.
        at_eviction, evicted_tok = {}, []
        conn = psycopg2.connect(DST)
        conn.autocommit = True

        def evictor():
            cur = conn.cursor()
            lock = f"{EVICT[0]}\\_%\\_\\_apitap\\_lock"

            def found():
                cur.execute(f"SELECT relname FROM pg_class WHERE relkind = 'r' AND relname LIKE '{lock}'")
                r = cur.fetchone()
                return r[0] if r else None

            if not _rig.wait_for(lambda: found() is not None, 60, 0.005):
                return
            tok = _rig.token_of(found())
            cur.execute("UPDATE _apitap_lease SET collected = true WHERE token = %s AND ("
                        + " OR ".join(f"dest_key LIKE '%%.{t}'" for t in EVICT) + ") RETURNING dest_key",
                        (tok,))
            evicted_tok.append((tok, [r[0] for r in cur.fetchall()]))
            for t in EVICT:
                cur.execute(f"SELECT count(*) FROM {t}")
                at_eviction[t] = cur.fetchone()[0]

        th = threading.Thread(target=evictor, daemon=True)
        th.start()
        err = None
        try:
            apitap.transfer(SRC, DST, tables=TABLES, mode="log_based", slots=2)
        except Exception as e:  # noqa: BLE001 — the class is what is asserted
            err = e
        th.join(timeout=5)
        conn.close()
        if not evicted_tok or len(evicted_tok[0][1]) != 2:
            _rig.rig_fail(f"could not collect both of {EVICT}'s lease rows: {evicted_tok}")

        rep = getattr(err, "report", None)
        failed = sorted(r.table for r in rep.tables if r.error) if rep else []
        done = sorted(r.table for r in rep.tables if not r.error) if rep else []
        case("the run raised MultiTransferError", type(err).__name__ == "MultiTransferError",
             f"{type(err).__name__}: {str(err)[:200]}" if err else "it returned")
        case("its report names a and b as failed, c and d as done",
             failed == sorted(EVICT) and done == sorted(KEEP), f"failed {failed}, done {done}")
        for t in KEEP:
            s, d = count(src, t), count(dst, t)
            case(f"{t} equals its source and its watermark advanced",
                 s == d and watermark(t) != before[t], f"src {s} dst {d}, watermark {before[t]} -> {watermark(t)}")
        for t in EVICT:
            d = count(dst, t)
            case(f"{t} holds what it held when its claim was collected",
                 d == at_eviction.get(t), f"at eviction {at_eviction.get(t)}, now {d}")
    finally:
        clean()
        print("   cleaned up", flush=True)
    print(f"\n   ===== SLOTS E2E: {'PASSED' if ok else 'FAILED'} =====", flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
