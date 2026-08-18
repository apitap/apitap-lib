"""Does the fast MySQL plane verify anything, and does it protect the password?

`mywire.rs` is the hand-rolled MySQL protocol client apitap uses for reads and
for binlog CDC. It has always encrypted — and never verified: every TLS mode it
supported used an accept-anything certificate verifier, so an active
man-in-the-middle was indistinguishable from the server. That matters most at
one specific moment: caching_sha2_password FULL authentication sends the
PASSWORD ITSELF, and it did so over that unverified channel.

The rig is a MySQL with `require_secure_transport=ON`, which refuses cleartext
connections outright, and the self-signed certificate MySQL generates for
itself at initialisation — which is exactly the shape `verify_identity` must
reject. A server that merely tolerates TLS could not tell these cases apart.

  leg 0  the rig really refuses cleartext
  leg 1  ssl-mode=required        — connects (encrypt, do not verify: MySQL's own meaning)
  leg 2  ssl-mode=disabled        — refused, because the SERVER will not take it
  leg 3  ssl-mode=verify_identity — must FAIL against a self-signed certificate
  leg 4  ssl-mode=verify_ca       — refused by apitap, by name
  leg 5  binlog CDC over TLS      — the encrypted socket carries a real drain

Leg 3 is the one that decides whether any of this is real: a verification mode
that PASSES against a certificate no public root signed is not verifying.

Rig: `apitap-tls-my` on :3312 (built by ~/tls_my_rig.sh), ClickHouse on :8124.
"""
import os
import subprocess
import sys

MY = os.environ.get("MY_TLS_URL", "mysql://root:bench@127.0.0.1:3312/tlsdb")
CH = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")
MY_C = os.environ.get("MY_TLS_CONTAINER", "apitap-tls-my")
T = "tls_my"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def my(sql, check=True):
    o = sh(["docker", "exec", "-i", MY_C, "mysql", "-uroot", "-pbench", "-N",
            "-D", "tlsdb", "-e", sql])
    if check and o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
               "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()


def transfer(mode_q, table=T, dest=None, mode="replace"):
    url = f"{MY}?ssl-mode={mode_q}"
    code = (
        "import apitap\n"
        f"r = apitap.transfer({url!r}, {CH!r}, table={table!r}, mode={mode!r}"
        f"{'' if dest is None else f', dest_table={dest!r}'})\n"
        "print('ROWS', r.rows)\n"
    )
    return sh([sys.executable, "-c", code])


def case(label, good, detail=""):
    global ok
    print(f"   {'✓' if good else '✗'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


print("== leg 0: the rig refuses cleartext ==")
case("require_secure_transport is ON", my("SELECT @@require_secure_transport") == "1")

print("== leg 1: ssl-mode=required connects (encrypt, do not verify) ==")
ch(f"DROP TABLE IF EXISTS {T}")
r = transfer("required")
if r.returncode:
    case("required connects", False, r.stderr.strip()[-260:])
else:
    landed, src = ch(f"SELECT count() FROM {T}"), my(f"SELECT count(*) FROM {T}")
    case("required connects and moves the rows", landed == src, f"{landed} == {src}")

print("== leg 2: ssl-mode=disabled is refused ==")
r = transfer("disabled", dest=T + "_x")
case("a cleartext attempt fails", r.returncode != 0,
     r.stderr.strip().splitlines()[-1][:170] if r.returncode
     else "it SUCCEEDED — the rig is not strict")
ch(f"DROP TABLE IF EXISTS {T}_x")

print("== leg 3: ssl-mode=verify_identity must REJECT a self-signed certificate ==")
r = transfer("verify_identity", dest=T + "_v")
if r.returncode == 0:
    case("verify_identity rejects an unsigned chain", False,
         "it CONNECTED — verification is not actually happening")
else:
    last = r.stderr.strip().splitlines()[-1]
    case("verify_identity refuses, and the message is about the certificate",
         "certificate" in last.lower() or "tls" in last.lower(), last[:170])
ch(f"DROP TABLE IF EXISTS {T}_v")

print("== leg 4: ssl-mode=verify_ca is refused by name ==")
r = transfer("verify_ca", dest=T + "_c")
last = r.stderr.strip().splitlines()[-1] if r.stderr.strip() else ""
case("verify_ca refused by apitap, naming the alternatives",
     r.returncode != 0 and "verify_ca is not implemented" in last, last[:170])

print("== leg 5: binlog CDC over the encrypted socket ==")
CT = "tls_my_cdc"
my(f"DROP TABLE IF EXISTS {CT}")
my(f"CREATE TABLE {CT} (id INT PRIMARY KEY, v VARCHAR(64))")
my(f"INSERT INTO {CT} VALUES (1,'a'),(2,'b'),(3,'c')")
ch(f"DROP TABLE IF EXISTS {CT}")
ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{CT}' SETTINGS mutations_sync=1")

r = transfer("required", table=CT, mode="log_based")
if r.returncode:
    case("bootstrap over TLS", False, r.stderr.strip()[-260:])
else:
    my(f"UPDATE {CT} SET v='changed' WHERE id=2")
    my(f"DELETE FROM {CT} WHERE id=3")
    r2 = transfer("required", table=CT, mode="log_based")
    if r2.returncode:
        case("drain over TLS", False, r2.stderr.strip()[-260:])
    else:
        src = my(f"SELECT count(*), sum(id) FROM {CT}").replace("\t", "|")
        dst = ch(f"SELECT count()||'|'||sum(id) FROM {CT}")
        case("every change arrived over the encrypted socket", src == dst,
             f"src {src} / dst {dst}")

my(f"DROP TABLE IF EXISTS {CT}", check=False)
ch(f"DROP TABLE IF EXISTS {CT}")
ch(f"DROP TABLE IF EXISTS {T}")
print("\n" + ("MYSQL TLS E2E: ALL GREEN" if ok else "MYSQL TLS E2E: FAILED"))
raise SystemExit(0 if ok else 1)
