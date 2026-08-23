"""The _apitap_state contract: a watermark is only usable in its own vocabulary.

One state table, two writers. The bulk lane stores cursor-column watermarks
("last id I shipped"); the CDC lane stores LSNs. Before the contract, the bulk
read used the row's VALUE and ignored its vocabulary — so a table that had been
CDC'd and was later run with mode="append" adopted an LSN as a cursor value and
resumed from a position that meant nothing, skipping or repeating rows while
reporting success. And the two lanes spelled the key differently (schema.bare
vs bare), so a replace's state clear could miss the CDC row entirely and a
later log_based run resumed from a watermark that predated the replace.

  leg 1  CDC then append          — must REFUSE, naming the LSN problem
  leg 2  CDC then replace then CDC— the replace must clear the CDC watermark:
                                    the following log_based run re-bootstraps
                                    and lands EXACTLY the source, instead of
                                    resuming from a pre-replace position
  leg 3  append(a) then append(b) — cursor mismatch must REFUSE, naming both
  leg 4  the other lane's row     — the bulk lane writes schema.bare and the
                                    CDC lane writes bare; a CDC read must find
                                    a bulk-written row anyway and refuse it,
                                    instead of seeing nothing and quietly
                                    re-bootstrapping

Neither spelling was changed. Rewriting them to one canonical key was the first
attempt and it broke the recovery instructions apitap itself prints — every
"clear the state row" message, runbook and fixture names the bare one. What
closes the gap is that every READ and every DELETE covers both.

Rig: `apitap-bench-pg-src` on :5544, `apitap-bench-pg-dst` on :5545.
"""
import os
import subprocess
import sys

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
DST = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
T = "state_contract"

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


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def run(mode, cursor=None):
    kw = f", cursor={cursor!r}" if cursor else ""
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {DST!r}, table={T!r}, mode={mode!r}{kw})\n"
            "print('ROWS', r.rows, flush=True)\n")
    return sh([sys.executable, "-c", code])


def fp(side):
    q = f"SELECT count(*) || '|' || coalesce(sum(id),0) || '|' || coalesce(sum(n),0) FROM {T}"
    return (src if side == "src" else dst)(q)


def reset():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")
    drop_our_slots()
    src(f"CREATE TABLE {T} (id bigint PRIMARY KEY, n bigint)")
    src(f"INSERT INTO {T} SELECT g, g * 3 FROM generate_series(1, 500) g")


# ---------------------------------------------------------------------------
print("== leg 1: a CDC-managed table run with mode='append' must refuse ==")
reset()
r = run("log_based")
case("CDC bootstrap", r.returncode == 0 and fp("dst") == fp("src"))
r = run("append", cursor="id")
case("append against the CDC watermark is REFUSED", r.returncode != 0,
     "it ran and 'succeeded' — the LSN was adopted as a cursor value"
     if r.returncode == 0 else "")
if r.returncode != 0:
    case("and the message names the vocabulary problem",
         "CDC-managed" in r.stderr or "LSN" in r.stderr,
         r.stderr.strip().splitlines()[-1][:140] if r.stderr.strip() else "(empty)")

# ---------------------------------------------------------------------------
print("== leg 2: replace must clear the CDC watermark, not strand it ==")
# Same table, still CDC-managed. A replace rebuilds the table from scratch;
# the CDC watermark that pointed into the OLD table's history must go with it.
src(f"UPDATE {T} SET n = n + 1 WHERE id <= 100")   # changes the CDC lane never saw
r = run("replace")
case("the replace runs", r.returncode == 0, r.stderr.strip()[-200:])
state_rows = dst(f"SELECT count(*) FROM _apitap_state "
                 f"WHERE dest_table IN ('{T}', 'public.{T}')")
case("no state row survives the replace (either spelling)", state_rows == "0",
     f"{state_rows} rows left — a log_based run would resume from a "
     f"pre-replace LSN")
src(f"INSERT INTO {T} SELECT g, g * 3 FROM generate_series(1000, 1100) g")
r = run("log_based")
case("the following log_based run re-bootstraps cleanly", r.returncode == 0,
     r.stderr.strip()[-200:])
case("and the destination is exactly the source", fp("dst") == fp("src"),
     f"src {fp('src')} vs dst {fp('dst')}")

# ---------------------------------------------------------------------------
print("== leg 3: two appends with different cursors must refuse, naming both ==")
reset()
r = run("append", cursor="id")
case("first append (cursor=id) bootstraps", r.returncode == 0)
src(f"INSERT INTO {T} VALUES (2000, 6000)")
r = run("append", cursor="n")
case("append with a DIFFERENT cursor is refused", r.returncode != 0,
     "resumed an id-watermark as an n-watermark" if r.returncode == 0 else "")
if r.returncode != 0:
    err = r.stderr
    case("and the message names both cursors", "'id'" in err and "'n'" in err,
         err.strip().splitlines()[-1][:140] if err.strip() else "(empty)")

# ---------------------------------------------------------------------------
print("== leg 4: the CDC lane must SEE a row the bulk lane wrote ==")
# The mirror of leg 1, and the half that needs the dual-spelling read.
#
# The two lanes key the same table differently and always have: the CDC lane
# writes the bare name, the bulk lane writes schema.bare. Neither spelling was
# changed — rewriting them would have broken every "clear the state row"
# instruction apitap itself prints — so what closes the gap is that every READ
# and every DELETE covers both.
#
# Here the bulk lane goes first. Its row is qualified; the CDC lane's read must
# find it anyway and refuse, because a cursor value is not an LSN. Without the
# fallback the CDC run would see no row at all and quietly re-bootstrap: a full
# reload that looks like success and silently discards the incremental history.
reset()
r = run("append", cursor="id")
case("an append bootstraps the table", r.returncode == 0, r.stderr.strip()[-200:])
keys = dst(f"SELECT string_agg(dest_table, ',' ORDER BY dest_table) "
           f"FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")
case("(rig) the bulk lane wrote the QUALIFIED spelling", keys == f"public.{T}", keys)

src(f"INSERT INTO {T} VALUES (4000, 12000)")
r = run("log_based")
case("log_based against a cursor-managed table is REFUSED", r.returncode != 0,
     "it re-bootstrapped instead — the other lane's row was invisible"
     if r.returncode == 0 else "")
if r.returncode != 0:
    err = r.stderr
    case("and the refusal explains the vocabulary, not just a mismatch",
         "cursor" in err or "LSN" in err,
         err.strip().splitlines()[-1][:150] if err.strip() else "(empty)")

# And the reverse spelling still reaches it: clearing by the name apitap tells
# operators to use must actually clear the row, whichever lane wrote it.
dst(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")
left = dst(f"SELECT count(*) FROM _apitap_state "
           f"WHERE dest_table IN ('{T}', 'public.{T}')")
case("clearing by the bare name is NOT enough on its own (both spellings exist)",
     left in ("0", "1"), f"{left} rows left")
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")
r = run("log_based")
case("with the state cleared, log_based bootstraps cleanly", r.returncode == 0,
     r.stderr.strip()[-200:])
case("and lands exactly the source", fp("dst") == fp("src"),
     f"src {fp('src')} vs dst {fp('dst')}")

# ---------------------------------------------------------------------------
print("== cleanup ==")
src(f"DROP TABLE IF EXISTS {T}")
dst(f"DROP TABLE IF EXISTS {T}")
dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}')")
drop_our_slots()

print("\nSTATE CONTRACT E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
