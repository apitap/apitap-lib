"""CDC of a declaratively partitioned Postgres table.

A partitioned parent emits no WAL of its own — every change is logged against
the leaf partition that holds the row. Without `publish_via_partition_root`,
pgoutput's Relation messages therefore carry the LEAF's name, the drain's
tracking map (keyed by the parent the user asked for) does not recognise it,
and every change is silently discarded while the watermark advances past it.

The shape of the failure is the worst one this project knows: the BOOTSTRAP is
correct (a SELECT on the parent reads all partitions), so the destination looks
right on day one — and then freezes forever while every run reports success
with changes=0. The consumed WAL is confirmed and gone, so fixing the bug later
cannot recover the gap; only a re-bootstrap can.

  leg 0  control        — a plain table, same shape, same statements: it must
                          track. If the control freezes too, the rig is broken,
                          not partitioning.
  leg 1  the bug        — insert + update + delete against the partitioned
                          parent must reach the destination.
  leg 2  the mechanism  — asked of the SERVER: pg_publication.pubviaroot must
                          be true for apitap's publication. This is the witness
                          that changes travel under the ROOT's name.
  leg 3  a new partition— attached mid-pipeline, rows inserted into it must
                          flow on the next drain with no re-bootstrap. Monthly
                          partitions are the whole point of partitioning; a fix
                          that only covers the partitions present at bootstrap
                          would rot in 30 days.

Rig: `apitap-bench-pg-src` on :5544 (PG 16), `apitap-bench-pg-dst` on :5545.
"""
import os
import subprocess
import sys

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
PT = "part_orders"
CT = "plain_orders"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def dst(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-dst", "psql", "-U", "postgres",
            "-d", "apitap_bench_dst", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def _slots_now():
    return set(src("SELECT slot_name FROM pg_replication_slots").split())


_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for s in sorted(_slots_now() - _SLOTS_BEFORE):
        src(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
            f"WHERE slot_name='{s}' AND NOT active")


def drop_our_pubs():
    for p in src("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%'").split():
        src(f"DROP PUBLICATION IF EXISTS {p}")


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def drain(table):
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {DST!r}, table={table!r}, mode='log_based')\n"
            "print('ROWS', r.rows, flush=True)\n")
    return sh([sys.executable, "-c", code])


def fingerprint(side, table):
    q = (f"SELECT count(*) || '|' || coalesce(sum(id),0) || '|' || "
         f"coalesce(sum(amount),0) FROM {table}")
    return src(q) if side == "src" else dst(q)


# ---------------------------------------------------------------------------
print("== setup ==")
src(f"DROP TABLE IF EXISTS {PT}")
src(f"DROP TABLE IF EXISTS {CT}")
src(f"""CREATE TABLE {PT} (
        id bigint NOT NULL, created date NOT NULL, amount bigint,
        PRIMARY KEY (id, created)
    ) PARTITION BY RANGE (created)""")
src(f"CREATE TABLE {PT}_aug PARTITION OF {PT} "
    f"FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')")
src(f"CREATE TABLE {PT}_sep PARTITION OF {PT} "
    f"FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')")
src(f"INSERT INTO {PT} SELECT g, ('2026-08-0' || (1 + g % 9))::date, g * 10 "
    f"FROM generate_series(1, 200) g")
src(f"CREATE TABLE {CT} (id bigint NOT NULL, created date NOT NULL, amount bigint, "
    f"PRIMARY KEY (id, created))")
src(f"INSERT INTO {CT} SELECT g, ('2026-08-0' || (1 + g % 9))::date, g * 10 "
    f"FROM generate_series(1, 200) g")
for t in (PT, CT):
    dst(f"DROP TABLE IF EXISTS {t}")
    dst(f"DELETE FROM _apitap_state WHERE dest_table = '{t}'")

r = drain(PT)
case("bootstrap of the partitioned table succeeds", r.returncode == 0,
     r.stderr.strip()[-300:])
case("and it landed every partition's rows",
     fingerprint("dst", PT) == fingerprint("src", PT), fingerprint("dst", PT))
r = drain(CT)
case("bootstrap of the control succeeds", r.returncode == 0)

# ---------------------------------------------------------------------------
print("== leg 2 first: the mechanism, asked of the server ==")
viaroot = src("SELECT bool_and(pubviaroot::text = 'true') FROM pg_publication "
              "WHERE pubname LIKE 'apitap_%'")
case("apitap's publications publish via the partition root", viaroot == "t",
     f"pubviaroot={viaroot!r} — without this, changes travel under the LEAF's "
     f"name and the drain discards them")

# ---------------------------------------------------------------------------
print("== leg 0 + 1: the same statements against both tables ==")
for t in (PT, CT):
    src(f"INSERT INTO {t} VALUES (1001, '2026-08-15', 111), (1002, '2026-09-15', 222)")
    src(f"UPDATE {t} SET amount = amount + 5 WHERE id <= 50")
    src(f"DELETE FROM {t} WHERE id BETWEEN 190 AND 200")

r = drain(CT)
case("control: the drain runs", r.returncode == 0, r.stderr.strip()[-200:])
case("control: a plain table tracks its changes",
     fingerprint("dst", CT) == fingerprint("src", CT),
     f"src {fingerprint('src', CT)} vs dst {fingerprint('dst', CT)}")

r = drain(PT)
case("partitioned: the drain runs", r.returncode == 0, r.stderr.strip()[-200:])
case("partitioned: insert, update and delete all arrive",
     fingerprint("dst", PT) == fingerprint("src", PT),
     f"src {fingerprint('src', PT)} vs dst {fingerprint('dst', PT)}")

# ---------------------------------------------------------------------------
print("== leg 3: a partition attached AFTER the pipeline started ==")
src(f"CREATE TABLE {PT}_oct PARTITION OF {PT} "
    f"FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')")
src(f"INSERT INTO {PT} VALUES (2001, '2026-10-05', 555), (2002, '2026-10-06', 666)")
r = drain(PT)
case("the drain after attaching a new partition runs", r.returncode == 0,
     r.stderr.strip()[-200:])
case("rows in the NEW partition flow without a re-bootstrap",
     fingerprint("dst", PT) == fingerprint("src", PT),
     f"src {fingerprint('src', PT)} vs dst {fingerprint('dst', PT)}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
for t in (PT, CT):
    src(f"DROP TABLE IF EXISTS {t}")
    dst(f"DROP TABLE IF EXISTS {t}")
    dst(f"DELETE FROM _apitap_state WHERE dest_table = '{t}'")
drop_our_slots()
drop_our_pubs()

print("\nPARTITIONED E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
