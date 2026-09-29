"""changelog=True e2e → BigQuery.

The ClickHouse changelog e2e, run against the other analytical destination, so
the two engines are held to the SAME contract:
  1. EVERY operation is captured (a key updated 3x lands 3 rows, not 1),
  2. `<table>__current` equals the source table, and
  3. an empty drain appends nothing.

Plus the things that are BigQuery-specific:
  4. the rebuilt table is partitioned MONTHLY on `_apitap_at`,
  5. `partition_by` on a non-time column is refused with a useful message, and
  6. T7 (audit §3.12): a window's script runs under apitap's own job id, and a
     script whose poll is cut off is adopted — asked about again under that
     id — not run a second time.

T7, the second half, needs a poll to fail after the script started. The drain
runs as a child whose HTTPS goes through a CONNECT proxy in this process (the
engine's HTTP client honours HTTPS_PROXY); the leg watches the dataset's
running jobs, and the moment the window's script shows up it cuts every
BigQuery tunnel and refuses new ones for a few seconds. The drain's poll then
fails with no answer — the lost-poll case. Asked of BigQuery afterwards
(INFORMATION_SCHEMA.JOBS_BY_USER): exactly one job ran that script's text, it
finished without error under an `apitap_cdc_` id, and the log holds the
window's events once. 0.56.0 minted no id and did not retry a lost answer, so
the drain fails; a build that re-submits under a NEW id (0.56.0's rule for a
503) runs the committed script twice and appends the window twice.

BigQuery is a destination, not a read source, so the readback goes through
google-auth + REST exactly like the replica-mode e2e does. The leg drops every
BigQuery object it made when it exits.

ENV: BQ_SA (service-account JSON path), BQ_PROJECT, BQ_DATASET.
"""
import atexit
import os
import select
import socket
import subprocess
import sys
import threading
import time
import apitap
import requests
from google.oauth2 import service_account
import google.auth.transport.requests as gt

PROJECT = os.environ.get("BQ_PROJECT", "apitap")
DATASET = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = f"bigquery://{PROJECT}/{DATASET}?credentials={os.environ['BQ_SA']}"
T = os.environ.get("T", "cl_bq_demo")
DS = DATASET

_creds = service_account.Credentials.from_service_account_file(
    os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])


def pg(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
         "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def bq(sql):
    """Rows as lists of strings. The SA key stays inside google-auth."""
    _creds.refresh(gt.Request())
    r = requests.post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries",
        headers={"Authorization": f"Bearer {_creds.token}"},
        json={"query": sql, "useLegacySql": False, "timeoutMs": 120000, "location": "US"})
    j = r.json()
    if r.status_code >= 400 or j.get("jobComplete") is False:
        raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
    return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]


def drain(**kw):
    return apitap.transfer(PG, BQ, table=T, mode="log_based", changelog=True, **kw)


def current_matches(stage):
    p = pg(f"SELECT id||'|'||COALESCE(v,'<N>') FROM {T} ORDER BY id")
    rows = bq(f"SELECT CONCAT(CAST(id AS STRING),'|',IFNULL(v,'<N>')) "
              f"FROM `{DS}.{T}__current` ORDER BY id")
    c = "\n".join(r[0] for r in rows)
    if p == c:
        print(f"   ✓ {stage}: __current matches pg ({len(p.splitlines())} rows)")
        return True
    print(f"   ✗ {stage}: MISMATCH\n   pg:\n{p}\n   bq:\n{c}")
    return False


def count(sql):
    return int(bq(f"SELECT COUNT(*) FROM `{DS}.{T}` WHERE {sql}")[0][0])


def drop_bq_objects():
    """Every BigQuery object this leg makes, and its bookkeeping rows."""
    for t in (f"{T}__current",):
        try:
            bq(f"DROP VIEW IF EXISTS `{DS}.{t}`")
        except Exception:
            pass
    for t in (T, f"{T}__apitap_cl", f"{T}__apitap_cdc", f"{T}_p"):
        try:
            bq(f"DROP TABLE IF EXISTS `{DS}.{t}`")
        except Exception:
            pass
    for t in ("_apitap_state", "_apitap_cdc_pending"):
        try:
            bq(f"DELETE FROM `{DS}.{t}` WHERE dest_table IN ('{T}', '{T}_p')")
        except Exception:
            pass


# ── T7: job identity ───────────────────────────────────────────────────────
BQ_API = f"https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}"
SCRIPT_OF_T = f"%INSERT INTO `{PROJECT}.{DS}.{T}`%"


def server_now():
    """BigQuery's clock, so a JOBS_BY_USER bound never depends on this box's."""
    return bq("SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', CURRENT_TIMESTAMP())")[0][0]


def window_scripts(since):
    """Every top-level script job since `since` that appends into T — the
    window's transaction, whichever version wrote it. (job_id, state, error,
    query text, end time as epoch seconds). JOBS_BY_USER, not JOBS_BY_PROJECT:
    listing the project's jobs is not granted to the gate's account."""
    return bq(
        "SELECT job_id, state, IFNULL(error_result.reason, ''), query, "
        "  CAST(UNIX_MICROS(end_time) / 1e6 AS STRING) "
        "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER "
        f"WHERE creation_time > TIMESTAMP '{since}' AND parent_job_id IS NULL "
        f"AND job_type = 'QUERY' AND STARTS_WITH(query, 'BEGIN TRANSACTION') "
        f"AND query LIKE '{SCRIPT_OF_T}' ORDER BY creation_time")


def settled_scripts(since, want_at_least):
    """`window_scripts`, waited for: JOBS_BY_USER shows a job seconds late."""
    rows = []
    for _ in range(20):
        rows = window_scripts(since)
        if len(rows) >= want_at_least and all(r[1] == "DONE" for r in rows):
            return rows
        time.sleep(3)
    return rows


class Blackout:
    """A CONNECT proxy for the drain's HTTPS that can cut BigQuery off.

    It sees only which host a tunnel is for — the TLS inside is the engine's
    and stays end to end. `cut(secs)` closes every open BigQuery tunnel and
    refuses new ones until `secs` have passed; OAuth (another host) is never
    touched, so the drain keeps its token."""
    HOST = "bigquery.googleapis.com"

    def __init__(self):
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(64)
        self.port = self.srv.getsockname()[1]
        self.until = 0.0
        self.lock = threading.Lock()
        self.live = set()
        self.closed = 0      # BigQuery tunnels closed by a cut
        self.refused = 0     # BigQuery CONNECTs refused during one
        threading.Thread(target=self._accept, daemon=True).start()

    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def _accept(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(c,), daemon=True).start()

    def _serve(self, c):
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = c.recv(4096)
            if not d:
                c.close()
                return
            buf += d
        head, rest = buf.split(b"\r\n\r\n", 1)
        target = head.split(b"\r\n")[0].decode().split(" ")[1]
        host, port = target.rsplit(":", 1)
        to_bq = host == self.HOST
        if to_bq and time.time() < self.until:
            with self.lock:
                self.refused += 1
            c.close()
            return
        try:
            u = socket.create_connection((host, int(port)), timeout=15)
        except OSError:
            c.close()
            return
        u.settimeout(None)
        c.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        if rest:
            u.sendall(rest)
        pair = (c, u)
        if to_bq:
            with self.lock:
                self.live.add(pair)
        try:
            while True:
                r, _, _ = select.select([c, u], [], [], 1.0)
                done = False
                for s in r:
                    try:
                        d = s.recv(65536)
                    except OSError:
                        d = b""
                    if not d:
                        done = True
                        break
                    (u if s is c else c).sendall(d)
                if done:
                    break
        except OSError:
            pass
        finally:
            with self.lock:
                self.live.discard(pair)
            for s in pair:
                try:
                    s.close()
                except OSError:
                    pass

    def cut(self, secs):
        with self.lock:
            self.until = time.time() + secs
            for pair in list(self.live):
                for s in pair:
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                self.closed += 1
            self.live.clear()


def running_window_script(since_ms, token):
    """The window's script while it is still pending or running (jobs.list:
    free, and current — INFORMATION_SCHEMA lags). (job_id, query) or None."""
    r = requests.get(f"{BQ_API}/jobs", headers={"Authorization": f"Bearer {token}"},
                     params={"stateFilter": ["pending", "running"], "projection": "full",
                             "maxResults": 50, "minCreationTime": str(since_ms)}, timeout=20)
    for j in r.json().get("jobs", []):
        q = j.get("configuration", {}).get("query", {}).get("query", "")
        if q.startswith("BEGIN TRANSACTION") and f"INSERT INTO `{PROJECT}.{DS}.{T}`" in q:
            return j["jobReference"]["jobId"], q
    return None


ok = True
print("== reset ==")
pg(f"DROP TABLE IF EXISTS {T} CASCADE")
pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{T}")
pg("SELECT pg_drop_replication_slot(s) FROM (SELECT slot_name s FROM "
   "pg_replication_slots WHERE slot_name LIKE 'apitap_%') x")
drop_bq_objects()
atexit.register(drop_bq_objects)

pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text, body text)")  # body TOASTs later
# Out of line and uncompressed, or the unchanged-TOAST case below tests
# nothing: 40 KB of one letter compresses to a few hundred bytes, stays in the
# tuple, and every UPDATE re-sends it whole.
pg(f"ALTER TABLE {T} ALTER COLUMN body SET STORAGE EXTERNAL")
pg(f"INSERT INTO {T} VALUES (1,'a'),(2,'b'),(3,'c')")

print("== bootstrap (baseline rows get op B) ==")
r = drain()
print(f"   bootstrap rows={r.rows}")
base = count("_apitap_op='B'")
print(f"   baseline rows tagged B: {base}")
ok &= base == 3
ok &= current_matches("bootstrap")

print("== partitioning: MONTHLY on _apitap_at ==")
part = bq(
    f"SELECT ddl FROM `{DS}.INFORMATION_SCHEMA.TABLES` WHERE table_name='{T}'")[0][0]
monthly = "TIMESTAMP_TRUNC(_apitap_at, MONTH)" in part.replace("`", "")
print(f"   partitioned monthly on _apitap_at: {monthly}")
ok &= monthly

print("== window 1: 3 updates on ONE key + insert + delete ==")
pg(f"UPDATE {T} SET v='a1' WHERE id=1")
pg(f"UPDATE {T} SET v='a2' WHERE id=1")
pg(f"UPDATE {T} SET v='a3' WHERE id=1")
pg(f"INSERT INTO {T} VALUES (4,'d')")
pg(f"DELETE FROM {T} WHERE id=2")
t0 = server_now()
r = drain()
print(f"   window1 events={r.rows}")
u1 = count("id=1 AND _apitap_op='U'")
print(f"   'U' records for id=1: {u1}  (collapse would have left 1)")
ok &= u1 == 3
d2 = count("_apitap_op='D'")
print(f"   'D' records: {d2}")
ok &= d2 == 1
ok &= current_matches("window1")

print("== T7: the window's script ran under apitap's own job id ==")
jobs = settled_scripts(t0, 1)
ours = [j for j in jobs if j[0].startswith("apitap_cdc_")]
print(f"   window scripts since {t0}: {len(jobs)}; under an apitap_cdc_ id: {len(ours)}; "
      f"ids {[j[0] for j in jobs]}")
good = len(jobs) >= 1 and len(ours) == len(jobs) and all(j[1] == "DONE" and not j[2] for j in ours)
print(f"   {'✓' if good else '✗'} every window script carries a client id and finished without error")
ok &= good

print("== T7: a script whose poll is cut off is adopted, not run twice ==")
pg(f"UPDATE {T} SET v='c-retry' WHERE id=3")
pg(f"INSERT INTO {T} VALUES (5,'e')")
t1 = server_now()
since_ms = int(time.time() * 1000) - 60_000
proxy = Blackout()
code = ("import apitap\n"
        f"r = apitap.transfer({PG!r}, {BQ!r}, table={T!r}, mode='log_based', changelog=True)\n"
        "print('ROWS', r.rows, flush=True)\n")
child = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True,
                         env=dict(os.environ, HTTPS_PROXY=proxy.url(), https_proxy=proxy.url()))
_creds.refresh(gt.Request())
seen, t_arm = None, None
deadline = time.time() + 240
while child.poll() is None and time.time() < deadline:
    seen = running_window_script(since_ms, _creds.token)
    if seen:
        t_arm = time.time()
        proxy.cut(2.5)
        break
    time.sleep(0.3)
try:
    out, _ = child.communicate(timeout=max(10, deadline - time.time()))
except subprocess.TimeoutExpired:
    child.kill()
    out, _ = child.communicate()
print(f"   drain rc={child.returncode}; {out.strip()[-300:]}")
if not seen:
    print("   ✗ (rig) the window's script was never seen running — nothing was cut")
    ok = False
else:
    print(f"   cut at the script {seen[0]}: {proxy.closed} tunnel(s) closed, "
          f"{proxy.refused} CONNECT(s) refused during the cut")
    jobs = settled_scripts(t1, 1)
    same = [j for j in jobs if j[3] == seen[1]]
    ended = [float(j[4]) for j in same if j[4]]
    hit = proxy.closed + proxy.refused > 0 and bool(ended) and max(ended) > t_arm
    print(f"   {'✓' if hit else '✗'} (rig) the cut hit the drain while the script ran "
          f"(job ended {max(ended) - t_arm if ended else float('nan'):+.1f}s after the cut)")
    ok &= hit
    good = child.returncode == 0
    print(f"   {'✓' if good else '✗'} the drain survives a poll that got no answer")
    ok &= good
    good = len(same) == 1 and same[0][1] == "DONE" and not same[0][2] and same[0][0].startswith("apitap_cdc_")
    print(f"   {'✓' if good else '✗'} JOBS_BY_USER: that script's text ran as exactly one job, "
          f"DONE without error under an apitap_cdc_ id: "
          f"{[(j[0], j[1], j[2]) for j in same]}")
    ok &= good
    u3 = count("id=3 AND v='c-retry' AND _apitap_op='U'")
    i5 = count("id=5 AND _apitap_op='I'")
    pairs = bq(f"SELECT COUNT(*), COUNT(DISTINCT FORMAT('%d/%d', _apitap_lsn, _apitap_seq)) "
               f"FROM `{DS}.{T}` WHERE _apitap_op != 'B'")[0]
    good = u3 == 1 and i5 == 1 and pairs[0] == pairs[1]
    print(f"   {'✓' if good else '✗'} the window's events are in the log once: U(id=3) {u3}, "
          f"I(id=5) {i5}; log {pairs[0]} rows, {pairs[1]} distinct (lsn, seq)")
    ok &= good
    ok &= current_matches("after the cut")

print("== window 2: empty drain appends nothing ==")
before = count("TRUE")
drain()
after = count("TRUE")
print(f"   log rows {before} -> {after}")
ok &= before == after
ok &= current_matches("window2-empty")

print("== unchanged-TOAST: an UPDATE that skips a big column must not blank it ==")
BIG = 40000
pg(f"UPDATE {T} SET body = repeat('x', {BIG}) WHERE id = 1")
drain()                                                 # window carrying body
stored = int(pg(f"SELECT pg_column_size(body) FROM {T} WHERE id = 1"))
print(f"   body stored out of line, uncompressed: {stored} bytes")
ok &= stored >= BIG
pg(f"UPDATE {T} SET v = 'toast-probe' WHERE id = 1")    # body NOT touched
drain()                                                 # the WAL omits body
got = int(bq(f"SELECT LENGTH(IFNULL(body,'')) FROM `{DS}.{T}__current` WHERE id=1")[0][0])
print(f"   body length in __current after a body-less UPDATE: {got} (want {BIG})")
ok &= got == BIG
newest = int(bq(f"SELECT LENGTH(IFNULL(body,'')) FROM `{DS}.{T}` WHERE id=1 AND "
                f"_apitap_op='U' ORDER BY _apitap_lsn DESC, _apitap_seq DESC LIMIT 1")[0][0])
print(f"   …and the newest U record itself carries it: {newest}")
ok &= newest == BIG
ok &= current_matches("after-toast")

print("== partition_by on a non-time column is refused ==")
pg(f"DROP TABLE IF EXISTS {T}_p CASCADE")
pg(f"CREATE TABLE {T}_p (id int PRIMARY KEY, v text)")
pg(f"INSERT INTO {T}_p VALUES (1,'a')")
try:
    apitap.transfer(PG, BQ, table=f"{T}_p", mode="log_based", changelog=True,
                    partition_by="v")
    print("   ✗ a STRING partition column was accepted")
    ok = False
except Exception as e:
    good = "partition" in str(e).lower() and "time" in str(e).lower()
    print(f"   {'✓' if good else '✗'} refused: {str(e)[:150]}")
    ok &= good

print("\n   ===== BQ CHANGELOG E2E: " + ("ALL GREEN" if ok else "FAILED") + " =====")

# The verdict above was printed and then thrown away: this file used to end
# here, so `python e2e_changelog_*.py` exited 0 whatever `ok` held, and
# gate.py — which keys PASS on returncode — recorded a green leg over a
# printed FAILED. Four legs shared the bug; e2e_changelog_ch.py was the only
# one that ever exited on its verdict.
raise SystemExit(0 if ok else 1)
