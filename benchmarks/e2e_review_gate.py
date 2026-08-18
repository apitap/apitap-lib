"""Three findings from the 0.42.0 review, each turned into a live-server proof.

Written to run exactly where the other e2e legs run: the bench VPS, against the
same containers, from the gate venv. Nothing here needs credentials the other
legs don't already have.

The framing that makes leg 1 airtight: it never asserts what the "correct"
value is. It asserts only that BOOTSTRAP AND CDC AGREE. A tool may legitimately
choose to render an ENUM as its label or as its index — but it may not render
the same column one way during the initial load and the other way during a
drain, because then the destination holds two spellings of one value and every
GROUP BY silently splits. Self-consistency needs no oracle, so this leg cannot
be argued with.

  leg 1  MariaDB/MySQL CDC type fidelity   — ENUM, SET, BIT, TIME(neg), JSON
  leg 2  Postgres idle-table WAL retention — does an idle publication confirm?
  leg 3  One wide row                      — is peak RSS bounded by the budget?

A NOTE ON leg 1 AND MariaDB: MariaDB implements JSON as an alias for LONGTEXT,
so its binlog carries JSON as ordinary text and the JSON sub-leg will PASS on
MariaDB while proving nothing. MySQL 8 stores JSON in its own binary format and
is the only server that exercises that path. Point MY_URL/MY_CONTAINER at a
MySQL 8 container to test it for real; the script says which server it used and
downgrades the JSON verdict to SKIPPED on MariaDB rather than claiming a pass.
"""
import os
import re
import resource
import subprocess
import sys

PG = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
MY = os.environ.get("MY_URL", "mysql://root:bench@127.0.0.1:3309/bench")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")

PG_C = os.environ.get("PG_CONTAINER", "apitap-bench-pg-src")
MY_C = os.environ.get("MY_CONTAINER", "apitap-bench-mariadb")
CH_C = os.environ.get("CH_CONTAINER", "apitap-bench-ch")
MY_CLI = "mariadb" if "maria" in MY_C else "mysql"

TT = "rev_types"      # leg 1
IT = "rev_idle"       # leg 2
NT = "rev_noise"      # leg 2, published by nobody — just a WAL generator
WT = "rev_wide"       # leg 3


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
            "-d", PG.rsplit("/", 1)[-1], "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "-i", MY_C, MY_CLI, "-uroot", "-pbench",
            "-N", "-D", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", CH_C, "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def transfer(src, table, mode="log_based", extra=""):
    """Run one transfer in a child, like the other legs, so stderr is inspectable."""
    code = (
        "import apitap\n"
        f"r = apitap.transfer({src!r}, {CH!r}, table={table!r}, mode={mode!r}{extra})\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code])


def transfer_peak_rss(src, table, mode="replace"):
    """Same, but report the child's peak RSS in MB (ru_maxrss, KiB on Linux)."""
    code = (
        "import apitap\n"
        f"r = apitap.transfer({src!r}, {CH!r}, table={table!r}, mode={mode!r})\n"
        "print('ROWS', r.rows)\n"
    )
    before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    out = sh([sys.executable, "-c", code])
    after = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return out, max(after, before) / 1024.0


ok = True
server = my("SELECT VERSION()")
is_mariadb = "maria" in server.lower()

# ───────────────────────────────────────────────────────────────────────────
print(f"== leg 1: MySQL CDC must render a column the same way the bootstrap did ==")
print(f"   server: {server}")

my(f"DROP TABLE IF EXISTS {TT}")
my(f"""CREATE TABLE {TT} (
        id      INT PRIMARY KEY,
        status  ENUM('new','paid','shipped'),
        perms   SET('read','write','admin'),
        flags   BIT(8),
        span    TIME,
        doc     JSON,
        touched INT
      )""")
# Row 1 is loaded by the bootstrap and never touched again — the reference.
# Row 2 is loaded identically, then UPDATEd so the SAME values ride the binlog.
my(f"""INSERT INTO {TT} VALUES
        (1,'shipped','read,write',b'00000101','-01:00:00','{{"a":1}}',0),
        (2,'shipped','read,write',b'00000101','-01:00:00','{{"a":1}}',0)""")

ch(f"DROP TABLE IF EXISTS {TT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{TT}' SETTINGS mutations_sync=1")

r = transfer(MY, TT)
if r.returncode:
    ok = False
    print(f"   ✗ bootstrap failed: {r.stderr[-400:]}")
else:
    # Touch row 2 only. Its type-bearing columns keep the SAME values, so any
    # difference that appears is the decoder's, not the data's.
    my(f"UPDATE {TT} SET touched = 1 WHERE id = 2")
    r = transfer(MY, TT)
    if r.returncode:
        ok = False
        print(f"   ✗ drain failed: {r.stderr[-400:]}")
    else:
        cols = ["status", "perms", "flags", "span", "doc"]
        for c in cols:
            boot = ch(f"SELECT toString({c}) FROM {TT} WHERE id=1")
            cdc = ch(f"SELECT toString({c}) FROM {TT} WHERE id=2")
            if c == "doc" and is_mariadb:
                print(f"   ⊘ {c:7s} SKIPPED — MariaDB stores JSON as LONGTEXT; "
                      f"only MySQL 8 exercises the binary-JSON path")
                continue
            if boot == cdc:
                print(f"   ✓ {c:7s} agrees: bootstrap={boot!r} cdc={cdc!r}")
            else:
                ok = False
                print(f"   ✗ {c:7s} DIVERGED: bootstrap={boot!r} but cdc={cdc!r} "
                      f"— same source value, two spellings in one table")

my(f"DROP TABLE IF EXISTS {TT}")
ch(f"DROP TABLE IF EXISTS {TT}")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 2: an idle published table must still let the slot advance ==")
# The claim under test: when the drain finds nothing to apply it never sends a
# standby status, so confirmed_flush_lsn stays where the last window left it —
# while the rest of the database keeps generating WAL the slot must retain.

pg(f"DROP TABLE IF EXISTS {IT}")
pg(f"DROP TABLE IF EXISTS {NT}")
pg(f"CREATE TABLE {IT} (id int primary key, v text)")
pg(f"CREATE TABLE {NT} (id serial primary key, pad text)")
pg(f"INSERT INTO {IT} SELECT g, 'v'||g FROM generate_series(1,100) g")
ch(f"DROP TABLE IF EXISTS {IT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{IT}' SETTINGS mutations_sync=1")

r = transfer(PG, IT)
if r.returncode:
    ok = False
    print(f"   ✗ bootstrap failed: {r.stderr[-400:]}")
else:
    slot = pg("SELECT slot_name FROM pg_replication_slots "
              "WHERE slot_name LIKE 'apitap%' ORDER BY slot_name DESC LIMIT 1")
    lag0 = int(pg("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
                  f"FROM pg_replication_slots WHERE slot_name='{slot}'"))
    # Generate WAL that the publication does NOT carry. pgoutput emits nothing
    # for it, so the drain sees keepalives and an empty window — the exact shape
    # of an idle table on a busy database.
    for _ in range(4):
        pg(f"INSERT INTO {NT} (pad) SELECT repeat('x', 4000) FROM generate_series(1,20000)")
        pg("CHECKPOINT")
    # …and drain repeatedly, which is what a schedule does.
    for _ in range(3):
        transfer(PG, IT)
    lag1 = int(pg("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint "
                  f"FROM pg_replication_slots WHERE slot_name='{slot}'"))
    grew = pg("SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
              f"FROM pg_replication_slots WHERE slot_name='{slot}'")
    print(f"   slot {slot}: unconfirmed WAL {lag0} B → {lag1} B after ~320 MB of "
          f"unrelated writes and 3 drains (retained: {grew})")
    if lag1 > lag0 + (64 << 20):
        ok = False
        print("   ✗ the slot did NOT advance — an idle table pins WAL and running "
              "the drain does not release it")
    else:
        print("   ✓ the slot advanced; an idle publication still confirms")

pg(f"DROP TABLE IF EXISTS {IT}")
pg(f"DROP TABLE IF EXISTS {NT}")
ch(f"DROP TABLE IF EXISTS {IT}")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 3: one wide row must not cost more than the budget ==")
# Postgres sends one CopyData message per ROW, so a single fat value is a single
# fat protocol message. If nothing caps a value, peak RSS tracks the widest row
# rather than the chunk size — which is the difference between a memory model
# that holds at 170 MB for 100 GB and one that dies on a table with a PDF in it.

pg(f"DROP TABLE IF EXISTS {WT}")
pg(f"CREATE TABLE {WT} (id int primary key, blob bytea)")
pg(f"INSERT INTO {WT} SELECT g, decode(repeat('ab', 32*1024*1024), 'hex') "
   f"FROM generate_series(1,3) g")   # 3 rows × 32 MB
ch(f"DROP TABLE IF EXISTS {WT}")

r, rss = transfer_peak_rss(PG, WT)
if r.returncode:
    print(f"   ! transfer failed (that is a legitimate outcome if a cap refused "
          f"it): {r.stderr.strip()[-300:]}")
    if "budget" in r.stderr or "exceeds" in r.stderr:
        print("   ✓ refused loudly with a named limit — better than an OOM kill")
    else:
        ok = False
        print("   ✗ failed without naming a per-value limit")
else:
    print(f"   transferred 3 × 32 MB rows; child peak RSS {rss:.0f} MB")
    if rss > 300:
        ok = False
        print("   ✗ peak RSS tracks the row width, not the chunk budget — a table "
              "with a large value will OOM a small container")
    else:
        print("   ✓ peak RSS stayed bounded")

pg(f"DROP TABLE IF EXISTS {WT}")
ch(f"DROP TABLE IF EXISTS {WT}")

print("== cleanup ==")
print("   dropped test tables")
print("REVIEW GATE: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
