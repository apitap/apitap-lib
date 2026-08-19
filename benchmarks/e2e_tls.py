"""Does the replication connection actually encrypt, and refuse when it cannot?

Until v0.45.0 the walsender client spoke plain TCP and REFUSED `sslmode=require`
outright — but it ACCEPTED `sslmode=prefer`, which is libpq's default, and then
connected in cleartext. A URL that looks encrypted and is not is worse than one
that fails, because nothing anywhere says so.

The rig is a Postgres that will not accept a cleartext connection at all
(`hostssl`-only in pg_hba.conf, ssl=on with a self-signed certificate). That
distinction matters: a server that merely TOLERATES TLS cannot tell a client
that negotiated it from one that skipped it, so it proves nothing. Here, a run
that succeeds is a run that encrypted.

Every leg runs `mode="log_based"`, and that is not incidental. A Postgres
SOURCE in a bulk mode rides the sqlx lane, which has always had TLS — so a
`mode="replace"` leg would pass without touching the walsender client at all
and prove nothing about the code under test. `log_based` is the only mode that
opens the hand-rolled replication socket.

  leg 1  sslmode=require       — the drain connects, and the SERVER logs an SSL walsender
  leg 2  sslmode=disable       — the run refuses; the SERVER is the one saying no
  leg 3  sslmode=verify-full   — must FAIL on a self-signed certificate
  leg 4  sslmode=verify-ca     — refused by apitap ITSELF, by name, before any socket
  leg 4b ssl-mode=… (hyphen)   — the other spelling sqlx accepts must mean the same
  leg 5  a full CDC round trip — bootstrap + drain, digest-checked, over TLS

Legs 2 and 3 are product-level: a `log_based` run opens sqlx's control pool
before the replication socket, so whichever layer refuses first is the one
that speaks. That is the right behaviour to assert — the run must not proceed
— but it is not evidence about the walsender specifically. Leg 1 is.

Leg 3 is the one worth reading twice: a verification mode that passes against a
certificate no public root signed would mean the verification is not happening.
Leg 4b is the one that would have caught a real hole: the parser knew only one
of the two spellings, so the hyphen form ran the replication socket in
cleartext while the pool beside it was encrypted.

Rig: `apitap-tls-pg` on :5546 (built by ~/tls_rig.sh), ClickHouse on :8124.
"""
import os
import subprocess
import sys

PG_TLS = os.environ.get("PG_TLS_URL", "postgres://postgres:bench@127.0.0.1:5546/tlsdb")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
PG_C = os.environ.get("PG_TLS_CONTAINER", "apitap-tls-pg")
T = "tls_demo"

ok = True


def sh(args):
    return subprocess.run(args, capture_output=True, text=True)


def pg(sql):
    o = sh(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres", "-d", "tlsdb",
            "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def transfer(url, mode="replace", table=T, dest=None):
    """`url` is passed whole — some legs need a spelling, not a mode name."""
    code = (
        "import apitap\n"
        f"r = apitap.transfer({url!r}, {CH!r}, table={table!r}, "
        f"mode={mode!r}{'' if dest is None else f', dest_table={dest!r}'})\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code])


def case(label, good, detail=""):
    global ok
    print(f"   {'✓' if good else '✗'} {label}{': ' + detail if detail else ''}")
    ok = ok and good
def _slots_now():
    return set(pg("SELECT slot_name FROM pg_replication_slots").split())


# Slots that existed BEFORE this leg started. Anything else is ours.
#
# The blanket `SELECT pg_drop_replication_slot(slot_name) ... WHERE NOT active`
# that used to be here is a live grenade on a shared rig: a CDC job that is
# merely BETWEEN drains has an inactive slot, and dropping it destroys its WAL
# continuity. It took out a running 24 h soak on 2026-08-20. Scope the cleanup
# to what this leg made.
_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for s in sorted(_slots_now() - _SLOTS_BEFORE):
        pg(f"SELECT pg_drop_replication_slot('{s}')"
           f" FROM pg_replication_slots WHERE slot_name='{s}' AND NOT active")



def url(mode):
    return f"{PG_TLS}?sslmode={mode}"


# ───────────────────────────────────────────────────────────────────────────
print("== leg 0: the rig really does refuse cleartext ==")
# Without this the whole file proves nothing: every later pass could be a
# cleartext connection to a permissive server.
hba = sh(["docker", "exec", PG_C, "cat", "/var/lib/postgresql/data/pg_hba.conf"]).stdout
strict = "hostssl all all 0.0.0.0/0" in hba and "\nhost    all all 0.0.0.0/0" not in hba
case("pg_hba.conf is hostssl-only for remote connections", strict)
case("the server has ssl on", pg("SHOW ssl") == "on", pg("SHOW ssl"))

# ───────────────────────────────────────────────────────────────────────────
print("== leg 1: sslmode=require opens the replication socket over TLS ==")
ch(f"DROP TABLE IF EXISTS {T}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")
drop_our_slots()
# The server's log is the witness: it records the walsender's connection and,
# with log_connections on, whether it was SSL. A client claiming encryption is
# not evidence of encryption.
pg("ALTER SYSTEM SET log_connections = on")
pg("SELECT pg_reload_conf()")
mark = sh(["docker", "logs", "--tail", "1", PG_C]).stderr[-80:]
r = transfer(url("require"), mode="log_based")
if r.returncode:
    case("require connects", False, r.stderr.strip()[-300:])
else:
    landed = ch(f"SELECT count() FROM {T}")
    src = pg(f"SELECT count(*) FROM {T}")
    case("the drain connects and moves the rows", landed == src, f"{landed} == {src}")
    logs = sh(["docker", "logs", "--since", "2m", PG_C]).stderr
    ssl_lines = [l for l in logs.splitlines()
                 if "connection authorized" in l and "SSL" in l and "replication" in l]
    case("the SERVER logged an SSL replication connection",
         bool(ssl_lines), ssl_lines[-1].split("SSL")[1][:70] if ssl_lines else "none found")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 2: the run refuses when the server will not take cleartext ==")
r = transfer(url("disable"), mode="log_based", table=T, dest=T + "_x")
refused = r.returncode != 0
case("a cleartext attempt fails", refused,
     (r.stderr.strip()[-160:] if refused else "it SUCCEEDED — the rig is not strict"))
ch(f"DROP TABLE IF EXISTS {T}_x")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 3: sslmode=verify-full must REJECT a self-signed certificate ==")
r = transfer(url("verify-full"), mode="log_based", table=T, dest=T + "_v")
msg = r.stderr
if r.returncode == 0:
    case("verify-full rejects an unsigned chain", False,
         "it CONNECTED — verification is not actually happening")
else:
    named = "verify-full" in msg or "tls handshake" in msg or "certificate" in msg.lower()
    case("verify-full refuses, and the message explains why", named,
         msg.strip().splitlines()[-1][:190] if msg.strip() else "(no message)")
ch(f"DROP TABLE IF EXISTS {T}_v")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 4: sslmode=verify-ca is refused by name, before any socket ==")
r = transfer(url("verify-ca"), mode="log_based", table=T, dest=T + "_c")
# Only the ERROR LINE counts. Searching the whole stderr would find
# `sslmode=verify-ca` in the traceback's echo of the source line and pass on
# its own input — which is exactly what it did before this comment existed.
last = r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ""
case("verify-ca refused by apitap, naming the mode and the alternatives",
     r.returncode != 0 and "verify-ca is not implemented" in last,
     last[:190] or "(no message)")

# ───────────────────────────────────────────────────────────────────────────
print("== leg 4b: the hyphen spelling means the same thing ==")
# sqlx accepts `sslmode` AND `ssl-mode`; so does this project's own MySQL
# parser. The walsender accepted only the first — so a URL written with the
# hyphen encrypted its sqlx pool and ran the replication socket in CLEARTEXT,
# with the "you asked for TLS and did not get it" note suppressed, because as
# far as that parser could tell nobody had asked.
#
# `verify-full` is the discriminator: against this self-signed certificate it
# MUST fail. A parser that ignores the hyphen falls back to `prefer`, which
# connects happily — so here a passing connection is the bug.
r = transfer(f"{PG_TLS}?ssl-mode=verify-full", mode="log_based", table=T,
             dest=T + "_h")
case("ssl-mode=verify-full (hyphen) is honoured, not ignored",
     r.returncode != 0,
     (r.stderr.strip().splitlines()[-1][:150] if r.returncode
      else "it CONNECTED — the hyphen spelling fell back to prefer"))
r = transfer(f"{PG_TLS}?ssl-mode=verify-ca", mode="log_based", table=T,
             dest=T + "_h2")
last = r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ""
case("ssl-mode=verify-ca (hyphen) reaches apitap's own refusal",
     "verify-ca is not implemented" in last, last[:150])
ch(f"DROP TABLE IF EXISTS {T}_h")
ch(f"DROP TABLE IF EXISTS {T}_h2")


print("== leg 5: a real CDC drain over the encrypted socket ==")
CT = "tls_cdc"
pg(f"DROP TABLE IF EXISTS {CT}")
pg(f"CREATE TABLE {CT} (id int primary key, v text)")
pg(f"INSERT INTO {CT} SELECT g, 'v'||g FROM generate_series(1,2000) g")
ch(f"DROP TABLE IF EXISTS {CT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{CT}' SETTINGS mutations_sync=1")

r = transfer(url("require"), mode="log_based", table=CT)
if r.returncode:
    case("bootstrap over TLS", False, r.stderr.strip()[-300:])
else:
    pg(f"UPDATE {CT} SET v = 'changed' WHERE id <= 50")
    pg(f"DELETE FROM {CT} WHERE id > 1900")
    r2 = transfer(url("require"), mode="log_based", table=CT)
    if r2.returncode:
        case("drain over TLS", False, r2.stderr.strip()[-300:])
    else:
        src = pg(f"SELECT count(*)||'|'||coalesce(sum(id),0) FROM {CT}")
        dst = ch(f"SELECT count()||'|'||sum(id) FROM {CT}")
        case("every change arrived over the encrypted socket", src == dst,
             f"src {src} / dst {dst}")

pg(f"DROP TABLE IF EXISTS {CT}")
drop_our_slots()
ch(f"DROP TABLE IF EXISTS {CT}")
ch(f"DROP TABLE IF EXISTS {T}")

print("\n" + ("TLS E2E: ALL GREEN" if ok else "TLS E2E: FAILED"))
raise SystemExit(0 if ok else 1)
