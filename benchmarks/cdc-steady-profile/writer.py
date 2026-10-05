#!/usr/bin/env python3
"""Paced multi-connection CDC change writer for the steady-profile rig.

Offers an exact, counted stream of changes:

    BEGIN
      UPDATE  t SET regular_int = regular_int + 1 WHERE id BETWEEN u0 AND u0+NU-1
      INSERT  t (cols) SELECT id+IB, <other cols> FROM t WHERE id BETWEEN s0 AND s0+NI-1
      DELETE  t WHERE id BETWEEN s0+IB AND s0+IB+NI-1
    COMMIT

Per transaction: NU UPDATE + NI INSERT + NI DELETE changes; the inserted rows
are deleted again in the same transaction, so the table's row count is constant
("INSERT then DELETE the same rows", the shape benchmarks/cdc-stress.md proved
net-zero). Every thread owns disjoint bands: (update band, insert source band,
clone ids) never collide between threads, and clone ids never collide with the
seed range.

The rate is exact and server-witnessed:
  * local ledger: committed transactions x changes-per-tx, wall from first
    commit to last;
  * pg: pg_current_wal_lsn() bracket + pg_stat_user_tables counter deltas;
  * mysql: SHOW BINARY LOG STATUS bracket + Handler_write/update/delete deltas.

Usage (host, connecting to the bench containers' loopback ports):
  writer.py --dialect pg --url postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src \
      --tables public.prof_pg_m --threads 4 --rate 60000 --duration 60
  writer.py --dialect mysql --url mysql://root:bench@127.0.0.1:3307/bench \
      --tables prof_my_m --threads 4 --rate 60000 --duration 60

--rate 0 = unpaced (maximum).
"""
import argparse
import signal
import sys
import threading
import time

STOP = threading.Event()
LOCK = threading.Lock()

# clone ids live above every seed id (<= 2,000,000) and below 2^31.
# The update band and the insert source band are INSIDE the seed range
# (1..1,000,000): a band outside it makes the transaction a no-op, which
# writes no WAL, commits nothing, and still ticks the ledger — the writer
# would report millions of phantom changes. Every transaction asserts its
# own rowcounts; see worker().
IBASE = 40_000_000
U0 = 50_001             # first update band (inside 1..1M)
S0 = 900_001            # first insert source band (inside 1..1M)


def cols_of(cur, table):
    cur.execute(f"SELECT * FROM {table} LIMIT 0")
    return [d[0] for d in cur.description]


def pg_connect(url):
    import psycopg2
    return psycopg2.connect(url, application_name="apitap-prof-writer")


def my_connect(url):
    import pymysql
    from urllib.parse import urlparse
    u = urlparse(url)
    return pymysql.connect(
        host=u.hostname, port=u.port or 3306, user=u.username,
        password=u.password, database=u.path.lstrip("/"),
        autocommit=False, program_name="apitap-prof-writer",
        charset="utf8mb4")


def witness(dialect, conn, tables=()):
    with conn.cursor() as cur:
        if dialect == "pg":
            cur.execute("SELECT pg_current_wal_lsn()::text")
            lsn = cur.fetchone()[0]
            names = [t.split(".")[-1] for t in tables]
            cur.execute(
                "SELECT coalesce(sum(n_tup_ins+n_tup_upd+n_tup_del),0) "
                "FROM pg_stat_user_tables WHERE relname = ANY(%s)",
                (names,))
            counters = int(cur.fetchone()[0])
            return f"lsn={lsn} counters={counters}"
        conn.commit()
        try:
            cur.execute("SHOW BINARY LOG STATUS")
            row = cur.fetchone()
        except Exception:
            cur.execute("SHOW MASTER STATUS")
            row = cur.fetchone()
        pos = f"{row[0]}:{row[1]}"
        cur.execute("SHOW GLOBAL STATUS WHERE Variable_name IN "
                    "('Handler_write','Handler_update','Handler_delete','Com_commit')")
        st = {r[0]: int(r[1]) for r in cur.fetchall()}
        counters = st.get("Handler_write", 0) + st.get("Handler_update", 0) + st.get("Handler_delete", 0)
        cur.execute(
            "SELECT COALESCE(SUM(SUM_ROWS_AFFECTED),0) "
            "FROM performance_schema.events_statements_summary_by_digest "
            "WHERE DIGEST_TEXT LIKE 'INSERT INTO `prof_%' "
            "   OR DIGEST_TEXT LIKE 'UPDATE `prof_%' "
            "   OR DIGEST_TEXT LIKE 'DELETE FROM `prof_%'")
        affected = int(cur.fetchone()[0])
        return (f"binlog={pos} counters={counters} com_commit={st.get('Com_commit', 0)} "
                f"affected={affected}")


def lsn_bytes(dialect, lsn_a, lsn_b):
    # pg lsn "X/Y" -> byte distance
    def to_int(s):
        hi, lo = s.split("/")
        return (int(hi, 16) << 32) + int(lo, 16)
    return to_int(lsn_b) - to_int(lsn_a)


def binlog_bytes(pos_a, pos_b):
    fa, pa = pos_a.split(":")
    fb, pb = pos_b.split(":")
    if fa == fb:
        return int(pb) - int(pa)
    # cross-file: count files ordinal difference x 1 GiB lower bound (only files
    # split by binlog_expire are rotated); this rig keeps files under rotation
    return (int(fb.split(".")[-1]) - int(fa.split(".")[-1])) * (1 << 30) + int(pb) - int(pa)


def worker(dialect, url, tables, g, args, stats):
    connect = pg_connect if dialect == "pg" else my_connect
    conn = connect(url)
    cur = conn.cursor()
    # One SQL triple per table, built once; the thread rotates tables per
    # transaction so a 30-table group is fed evenly by any thread count.
    u0 = U0 + g * (args.update + args.ins + 10)
    s0 = S0 + g * (args.ins + 10)
    c0 = s0 + IBASE
    per_table = []
    for t in tables:
        cols = cols_of(cur, t)
        sel = ", ".join(
            [f"id+{IBASE}" if c == "id" else c for c in cols])
        collist = ", ".join(cols)
        upd = (f"UPDATE {t} SET regular_int = regular_int + 1 "
               f"WHERE id BETWEEN {u0} AND {u0 + args.update - 1}")
        ins = (f"INSERT INTO {t} ({collist}) SELECT {sel} FROM {t} "
               f"WHERE id BETWEEN {s0} AND {s0 + args.ins - 1}")
        dele = f"DELETE FROM {t} WHERE id BETWEEN {c0} AND {c0 + args.ins - 1}"
        per_table.append((upd, ins, dele))
    tx_changes = args.update + 2 * args.ins
    period = 0.0 if args.rate == 0 else tx_changes / (args.rate / args.threads)
    local = 0
    k = 0
    t_prev = time.monotonic()
    try:
        while not STOP.is_set():
            table = tables[(g + k) % len(tables)]
            upd, ins, dele = per_table[(g + k) % len(tables)]
            cur.execute(upd)
            if cur.rowcount != args.update:
                raise RuntimeError(
                    f"UPDATE touched {cur.rowcount} rows, expected {args.update} "
                    f"(band {u0}..{u0 + args.update - 1}) — refusing to report "
                    f"phantom changes")
            if args.ins:
                cur.execute(ins)
                if cur.rowcount != args.ins:
                    raise RuntimeError(
                        f"INSERT produced {cur.rowcount} rows, expected {args.ins} "
                        f"(source band {s0}..{s0 + args.ins - 1}) — refusing to "
                        f"report phantom changes")
                cur.execute(dele)
                if cur.rowcount != args.ins:
                    raise RuntimeError(
                        f"DELETE touched {cur.rowcount} rows, expected {args.ins} "
                        f"(clone band {c0}..{c0 + args.ins - 1}) — refusing to "
                        f"report phantom changes")
            conn.commit()
            k += 1
            local += tx_changes
            # live counter: a plain dict store is atomic under the GIL, and the
            # ticker sums the per-thread slots, so progress is visible while
            # the threads run (the pre-fix ticker only saw end-of-thread totals).
            stats["threads"][g] = local
            if args.max_changes:
                with LOCK:
                    done = sum(stats["threads"].values())
                if done >= args.max_changes:
                    break
            if period:
                now = time.monotonic()
                slack = period - (now - t_prev)
                if slack > 0:
                    STOP.wait(slack)
                t_prev = time.monotonic()
    except Exception as exc:                                  # noqa: BLE001
        with LOCK:
            stats.setdefault("errors", []).append(f"thread {g} ({tables[g % len(tables)]}): {exc}")
        STOP.set()
    with LOCK:
        stats["total"] += local
        stats["threads"][g] = local
    try:
        conn.close()
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dialect", choices=["pg", "mysql"], required=True)
    ap.add_argument("--url", required=True)
    ap.add_argument("--tables", default="", help="comma-separated")
    ap.add_argument("--table", default="")
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--update", type=int, default=800)
    ap.add_argument("--ins", type=int, default=100)
    ap.add_argument("--del", dest="del_", type=int, default=0,
                    help="ignored: DELETE count always equals --ins")
    ap.add_argument("--rate", type=int, default=0, help="total changes/s, 0=max")
    ap.add_argument("--duration", type=float, default=0)
    ap.add_argument("--max-changes", type=int, default=0)
    args = ap.parse_args()
    tables = [t for t in args.tables.split(",") if t] or [args.table]
    stats = {"total": 0, "threads": {}}
    signal.signal(signal.SIGTERM, lambda *a: STOP.set())
    signal.signal(signal.SIGINT, lambda *a: STOP.set())

    conn = pg_connect(args.url) if args.dialect == "pg" else my_connect(args.url)
    w0 = witness(args.dialect, conn, tables)
    print(f"WRITER_START dialect={args.dialect} tables={len(tables)} threads={args.threads} "
          f"tx_changes={args.update + 2 * args.ins} rate={args.rate} {w0}", flush=True)
    conn.close()

    ths = []
    t0 = None
    for g in range(args.threads):
        th = threading.Thread(
            target=worker, daemon=True,
            args=(args.dialect, args.url, tables, g, args, stats))
        th.start()
        ths.append(th)
        if t0 is None:
            t0 = time.monotonic()
    t0 = t0 or time.monotonic()

    next_tick = t0 + 5.0
    while any(t.is_alive() for t in ths):
        now = time.monotonic()
        if args.duration and now - t0 >= args.duration:
            STOP.set()
        if now >= next_tick:
            with LOCK:
                done = sum(stats["threads"].values())
            print(f"WRITER_TICK t={now - t0:.1f}s changes={done} "
                  f"rate={done / max(now - t0, 1e-9):.0f}/s", flush=True)
            next_tick += 5.0
        time.sleep(0.05)
    wall = time.monotonic() - t0
    for th in ths:
        th.join(timeout=10)
    if stats.get("errors"):
        for e in stats["errors"]:
            print(f"WRITER_ERROR {e}", flush=True)
        print(f"WRITER_TOTAL changes={stats['total']} wall_s={wall:.3f} "
              f"rate={stats['total'] / wall:.1f}/s VERDICT INVALID", flush=True)
        sys.exit(2)
    conn = pg_connect(args.url) if args.dialect == "pg" else my_connect(args.url)
    w1 = witness(args.dialect, conn, tables)
    conn.close()
    total = stats["total"]
    print(f"WRITER_TOTAL changes={total} wall_s={wall:.3f} rate={total / wall:.1f}/s "
          f"per_thread={stats['threads']}", flush=True)
    print(f"WRITER_WITNESS start={w0} end={w1}", flush=True)
    if args.dialect == "pg":
        a, b = w0.split("lsn=")[1].split(" ")[0], w1.split("lsn=")[1].split(" ")[0]
        nb = lsn_bytes("pg", a, b)
        print(f"WRITER_WAL_BYTES {nb} bytes_per_change={nb / max(total, 1):.1f}", flush=True)
    else:
        a = w0.split("binlog=")[1].split(" ")[0]
        b = w1.split("binlog=")[1].split(" ")[0]
        nb = binlog_bytes(a, b)
        print(f"WRITER_BINLOG_BYTES {nb} bytes_per_change={nb / max(total, 1):.1f}", flush=True)


if __name__ == "__main__":
    main()
