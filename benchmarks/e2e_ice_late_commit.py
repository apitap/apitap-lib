"""A failure after an ambiguous Iceberg commit never sweeps files a late commit lands.

The commit loop keeps an `ambiguous` flag: once an attempt answered with a
5xx, the catalog may still land it, so the run must not delete the data
files that attempt references. But every `?` inside the loop (a manifest
GET, a PUT) returned a plain error past that flag, and the caller swept the
files — then the late commit landed a snapshot pointing at deleted objects
(MASTER review M-14).

Fault injection, two proxies: the REST catalog answers the run's first
commit with a 504 and forwards it a few seconds LATER (it lands); MinIO
answers every manifest-list GET with a 503 meanwhile, so the retry fails
with a plain error. After the late commit lands, the table must be
readable and exact — no snapshot may reference a swept file.
"""
import json
import socket
import subprocess
import sys
import threading
import time
import urllib.request

MY = "mysql://root:bench@127.0.0.1:3307/bench"
NS = "cdc_e2e"
CAT, S3 = 8181, 9100
T = "ice_late_commit"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def my(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def read_request(c):
    """One HTTP request (head + Content-Length body) off a client socket."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        b = c.recv(65536)
        if not b:
            return None
        buf += b
    head, _, body = buf.partition(b"\r\n\r\n")
    n = 0
    for line in head.split(b"\r\n")[1:]:
        k, _, v = line.partition(b":")
        if k.strip().lower() == b"content-length":
            n = int(v.strip())
    while len(body) < n:
        b = c.recv(65536)
        if not b:
            break
        body += b
    return head + b"\r\n\r\n" + body


def relay_one(raw, port):
    """Send one request upstream on a fresh connection; return the raw answer."""
    up = socket.create_connection(("127.0.0.1", port))
    up.sendall(raw)
    out = b""
    while True:
        b = up.recv(65536)
        if not b:
            break
        out += b
    up.close()
    return out


def answer(c, status, text):
    body = text.encode()
    c.sendall(f"HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)


class Proxy:
    """One request per connection (Connection: close forced upstream)."""

    def __init__(self, port, decide):
        self.port_up, self.decide = port, decide
        self.srv = socket.create_server(("127.0.0.1", 0))
        self.port = self.srv.getsockname()[1]
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(c,), daemon=True).start()

    def serve(self, c):
        try:
            while True:
                raw = read_request(c)
                if raw is None:
                    break
                raw = raw.replace(b"\r\nConnection: keep-alive", b"\r\nConnection: close")
                if b"\r\nconnection:" not in raw.split(b"\r\n\r\n")[0].lower():
                    raw = raw.replace(b"\r\n\r\n", b"\r\nConnection: close\r\n\r\n", 1)
                if self.decide(c, raw):
                    break
                c.sendall(relay_one(raw, self.port_up))
                break
        except OSError:
            pass
        finally:
            c.close()


class Faults:
    def __init__(self):
        self.armed = False
        self.held = threading.Event()
        self.landed = threading.Event()
        self.s3_refused = 0

    def catalog(self, c, raw):
        line = raw.split(b"\r\n", 1)[0]
        if self.armed and not self.held.is_set() and line.startswith(b"POST") \
                and f"/tables/{T}".encode() in line:
            self.held.set()
            answer(c, "504 Gateway Time-out", "<html>504</html>")

            def late():
                time.sleep(6)
                relay_one(raw, CAT)  # the commit lands, late
                self.landed.set()
            threading.Thread(target=late, daemon=True).start()
            return True
        return False

    def s3(self, c, raw):
        line = raw.split(b"\r\n", 1)[0]
        if self.held.is_set() and not self.landed.is_set() and line.startswith(b"GET") \
                and b"/metadata/" in line and b".avro" in line:
            self.s3_refused += 1
            answer(c, "503 Slow Down", "<Error><Code>SlowDown</Code></Error>")
            return True
        return False


def drop_ice():
    req = urllib.request.Request(
        f"http://127.0.0.1:{CAT}/v1/namespaces/{NS}/tables/{T}?purgeRequested=true", method="DELETE")
    try:
        urllib.request.urlopen(req)
    except Exception:
        pass


def scan():
    import duckdb
    d = duckdb.connect()
    d.execute("INSTALL iceberg; LOAD iceberg;")
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    with urllib.request.urlopen(f"http://127.0.0.1:{CAT}/v1/namespaces/{NS}/tables/{T}") as r:
        loc = json.load(r)["metadata-location"]
    return d.execute(f"SELECT count(*), sum(id) FROM iceberg_scan('{loc}')").fetchone()


def run(cat, s3):
    ice = (f"iceberg://127.0.0.1:{cat}/{NS}?endpoint=http://127.0.0.1:{s3}"
           "&access_key_id=bench&secret_access_key=benchbench")
    code = (f"import apitap\nr = apitap.transfer({MY!r}, {ice!r}, table={T!r}, mode='append', cursor='id')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def main():
    drop_ice()
    my(f"DROP TABLE IF EXISTS {T}")
    my(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    my(f"INSERT INTO {T} VALUES " + ",".join(f"({i}, 'a')" for i in range(1, 101)))
    f = Faults()
    cat, s3 = Proxy(CAT, f.catalog), Proxy(S3, f.s3)
    r = run(cat.port, s3.port)
    assert r.returncode == 0, r.stderr[-800:]

    my(f"INSERT INTO {T} VALUES " + ",".join(f"({i}, 'b')" for i in range(101, 151)))
    f.armed = True
    r = run(cat.port, s3.port)
    f.armed = False
    assert f.held.is_set(), "the first commit must have been held"
    assert f.landed.wait(30), "the late commit must have been forwarded"
    print(f"  faulted run rc={r.returncode}, manifest GETs refused={f.s3_refused}; the held commit landed late")
    try:
        got = scan()
    except Exception as e:
        raise AssertionError(f"the table is unreadable after the late commit — its files were swept: {e}")
    assert got == (150, sum(range(1, 151))), f"table after the late commit: {got}"
    print(f"  table readable and exact: {got[0]} rows")
    drop_ice()
    my(f"DROP TABLE IF EXISTS {T}")
    print("✓ an ambiguous Iceberg commit that lands late finds every file it references")


if __name__ == "__main__":
    main()
