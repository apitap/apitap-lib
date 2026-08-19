"""A URL that will not parse must name the fault and never the password.

Almost every unparseable URL apitap is handed has the same cause — a password
with a reserved character in it — and the parser reports the *consequence*
several characters later ("invalid port number"), which names neither the
password nor the fact that one is involved. The obvious next move, printing the
URL, is the one thing that must never happen: the URL IS the credential, and it
lands in a scheduler's log where a lot of people can read it.

So this leg asserts both halves at once, through the real Python surface rather
than against the Rust helper:

  leg 1  nothing leaks     — for a spread of broken URLs, no part of the
                             password appears anywhere in the exception text,
                             its repr, or the traceback
  leg 2  the fault is named — the offending character and the percent-encoding
                             fix appear, so the message is actionable
  leg 3  the host survives  — an unencoded '@' or '/' in the password must not
                             swallow the host from the diagnostic, which is the
                             bug the first version of the splitter had
  leg 4  no false advice    — a URL with no credentials must not be told to
                             encode a password it does not have
  leg 5  a correct URL still works — percent-encoded, the same password
                             connects, so the advice the message gives is
                             advice that actually works

Leg 5 is what makes the other four worth anything: a diagnostic that tells you
to do something that does not fix your problem is worse than a terse one.

Rig: `apitap-bench-pg-src` on :5544.
"""
import subprocess
import traceback
from urllib.parse import quote

import apitap
import polars as pl

PG_C = "apitap-bench-pg-src"
DB = "apitap_bench_src"
# Deliberately every character that ends or redirects an authority.
NASTY = "p@ss/w:o?r#d[]"

ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


def fails_with(url):
    """Return everything a user could see when this URL is rejected."""
    try:
        apitap.transfer(url, "clickhouse://default:bench@127.0.0.1:8124/default",
                        table="whatever")
    except BaseException as e:
        return f"{e}\n{e!r}\n{''.join(traceback.format_exception(e))}"
    return ""


def pg(sql, user="postgres"):
    o = subprocess.run(["docker", "exec", "-i", PG_C, "psql", "-U", "postgres",
                        "-d", DB, "-Atc", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


# ---------------------------------------------------------------------------
print("== leg 1: the password never appears in what the user sees ==")
BROKEN = [
    f"postgres://alice:{NASTY}@db.internal:5432/app",
    f"mysql://root:{NASTY}@my.internal:3306/bench",
    f"clickhouse://default:{NASTY}@ch.internal:8123/default",
    "postgres://alice:hunter2:extra@@@:99999/app",
]
# Fragments long enough to be meaningful — a single '@' would match the URL's
# own separator and make this assertion impossible to satisfy or to fail.
LEAKS = ["p@ss", "w:o?r#d", "ss/w", "hunter2"]
for url in BROKEN:
    seen = fails_with(url)
    leaked = [f for f in LEAKS if f in seen]
    case(f"nothing leaks from {url.split('://')[0]}://…", not leaked,
         f"leaked {leaked}" if leaked else "")
    if not seen:
        case("  …and it was actually rejected", False, "the URL was accepted")

# ---------------------------------------------------------------------------
print("== leg 2: the message names the fault and the fix ==")
seen = fails_with(f"postgres://alice:{NASTY}@db.internal:5432/app")
case("the offending character is named", "'@'" in seen or "'/'" in seen,
     seen.strip().splitlines()[-1][:120] if seen else "no error at all")
case("the percent-encoding fix is spelled out", "quote(password, safe='')" in seen)
case("the length stands in for the secret", "chars, hidden" in seen)

# ---------------------------------------------------------------------------
print("== leg 3: an unencoded '@' or '/' does not swallow the host ==")
for pw, why in [("p@ssw0rd", "an unencoded '@'"), ("a/b", "an unencoded '/'")]:
    seen = fails_with(f"postgres://alice:{pw}@db.internal:5432/app")
    case(f"the host survives {why}", "db.internal" in seen,
         seen.strip().splitlines()[-1][:120] if seen else "no error at all")

# ---------------------------------------------------------------------------
print("== leg 4: no password advice for a URL with no password ==")
seen = fails_with("postgres://db.internal:99999/app")
case("a credential-free URL is still rejected", bool(seen))
case("and is not told to encode a password it does not have",
     "quote(password" not in seen, seen.strip().splitlines()[-1][:120])

# ---------------------------------------------------------------------------
print("== leg 5: the advice the message gives actually works ==")
# The whole point of leg 2 is that a user who follows it gets a working URL.
# Proven against a live server with the same reserved characters in the real
# password, not asserted.
pg("REVOKE ALL ON SCHEMA public FROM urltest" if
   pg("SELECT count(*) FROM pg_roles WHERE rolname='urltest'") == "1" else "SELECT 1")
pg("DROP ROLE IF EXISTS urltest")
pg(f"CREATE ROLE urltest LOGIN PASSWORD '{NASTY}'")
pg("GRANT USAGE ON SCHEMA public TO urltest")
pg("DROP TABLE IF EXISTS url_probe")
pg("CREATE TABLE url_probe (id int primary key, v text)")
pg("INSERT INTO url_probe VALUES (1,'a'),(2,'b'),(3,'c')")
pg("GRANT SELECT ON url_probe TO urltest")

raw = f"postgres://urltest:{NASTY}@127.0.0.1:5544/{DB}"
case("the same URL unencoded is rejected", bool(fails_with(raw)))

encoded = f"postgres://urltest:{quote(NASTY, safe='')}@127.0.0.1:5544/{DB}"
try:
    # read() hands back a Reader, not a frame — collect through polars, the way
    # the manual shows it.
    n = int(
        apitap.read(encoded, table="url_probe")
        .lazy()
        .select(pl.len().alias("n"))
        .collect(engine="streaming")["n"][0]
    )
    err = ""
except BaseException as e:
    n, err = -1, f"{e}"
case("percent-encoded, it connects and reads", n == 3, err or f"{n} rows")

# Revoke before dropping: Postgres refuses to drop a role that still owns
# privileges, and the first version of this leg died in its own cleanup.
pg("DROP TABLE IF EXISTS url_probe")
pg("REVOKE ALL ON SCHEMA public FROM urltest")
pg("DROP ROLE IF EXISTS urltest")

print("\nURL ERROR E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
