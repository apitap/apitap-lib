"""An Iceberg commit answered with a redirect never counts as committed.

The catalog client followed redirects. A 301/302 to a POST makes the client
re-send it as a GET without a body: the commit `POST /v1/.../tables/<t>`
became a LoadTable GET, answered 200, and a 2xx commit answer counts as
done. Behind an ingress that redirects (a trailing slash, a scheme
upgrade), the run went green with nothing committed; on CDC the drain then
acked a window that was never written (deep review D23).

Fault injection: a proxy in front of the REST catalog answers the second
run's commit with `302 Found`, Location = the same URL. The run must either
fail or have committed every row: a green run whose table still holds the
first run's 100 rows fails the leg.
"""
import json
import socket
import subprocess
import sys
import threading
import urllib.request

MY = "mysql://root:bench@127.0.0.1:3307/bench"
NS = "cdc_e2e"
CAT, S3 = 8181, 9100
T = "ice_commit_redirect"


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


class Redirect:
    def __init__(self):
        self.armed = False
        self.sent = 0
        self.port = None

    def catalog(self, c, raw):
        line = raw.split(b"\r\n", 1)[0]
        if self.armed and line.startswith(b"POST") and f"/tables/{T}".encode() in line \
                and not line.split(b" ")[1].endswith(b"/metrics"):
            self.sent += 1
            path = line.split(b" ")[1].decode()
            c.sendall(f"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{self.port}{path}\r\n"
                      "Content-Length: 0\r\nConnection: close\r\n\r\n".encode())
            return True
        return False


def main():
    drop_ice()
    my(f"DROP TABLE IF EXISTS {T}")
    my(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)")
    my(f"INSERT INTO {T} VALUES " + ",".join(f"({i}, 'a')" for i in range(1, 101)))
    f = Redirect()
    cat = Proxy(CAT, f.catalog)
    f.port = cat.port
    try:
        r = run(cat.port, S3)
        assert r.returncode == 0, r.stderr[-800:]
        assert scan() == (100, sum(range(1, 101))), "(rig) the first run commits 100 rows"

        my(f"INSERT INTO {T} VALUES " + ",".join(f"({i}, 'b')" for i in range(101, 151)))
        f.armed = True
        r = run(cat.port, S3)
        f.armed = False
        assert f.sent > 0, "(rig) the commit must have been answered with a redirect"
        got = scan()
        if r.returncode == 0:
            assert got == (150, sum(range(1, 151))), \
                f"a green run after a redirected commit: the table holds {got}, want 150 rows"
            print("  the redirected commit was retried and landed: 150 rows")
        else:
            assert got == (100, sum(range(1, 101))), f"a failed run changed the table: {got}"
            tail_ = r.stderr.strip().splitlines()[-1][:140]
            print(f"  the redirected commit failed the run ({tail_}); the table keeps 100 rows")
    finally:
        drop_ice()
        my(f"DROP TABLE IF EXISTS {T}")
    print("✓ an Iceberg commit answered with a redirect never counts as committed")


if __name__ == "__main__":
    main()
