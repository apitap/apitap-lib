"""A ClickHouse 200 whose body never arrives fails the run; it is not an empty answer.

`exec` read a 2xx body with `unwrap_or_default`: a connection cut mid-body
gave "" and the caller parsed it as 0 — rows staged 0 (staging dropped,
"success"), an empty guard scan, an absent lease, an empty watermark
(MASTER review M-31).

Proof: a TCP proxy forwards every request; for the staged-row `count()` it
passes ClickHouse's status and headers, then promises a longer body than it
sends and closes. The replace must fail naming the unreadable body, the
destination must still hold the previous load, and a direct rerun is exact.
"""
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT = 8124
T = "ch_body_cut"


def ch_url(port):
    return f"clickhouse://default:bench@127.0.0.1:{port}/default"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


def pg(sql):
    o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


class CutBodyProxy:
    """Forwards everything; once armed, the first `SELECT count() FROM` of a
    table named after T gets ClickHouse's headers and a truncated body."""

    def __init__(self):
        self.armed = False
        self.cut = 0
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
        up = socket.create_connection(("127.0.0.1", CH_PORT))
        cut = threading.Event()

        def back():
            try:
                buf = b""
                while (b := up.recv(65536)):
                    if not cut.is_set():
                        c.sendall(b)
                        continue
                    buf += b
                    if b"\r\n\r\n" not in buf:
                        continue
                    head = buf.split(b"\r\n\r\n", 1)[0].split(b"\r\n")
                    keep = [h for h in head[1:] if not h.lower().startswith((b"content-length",
                            b"transfer-encoding", b"connection"))]
                    c.sendall(b"\r\n".join([head[0], *keep, b"Content-Length: 64", b"Connection: close"])
                              + b"\r\n\r\n" + b"1")
                    # shutdown, not just close: the request thread still holds
                    # this socket in recv, and a bare close sends no FIN.
                    c.shutdown(socket.SHUT_RDWR)
                    break
            except OSError:
                pass
            finally:
                c.close()
        threading.Thread(target=back, daemon=True).start()
        try:
            while (b := c.recv(65536)):
                text = urllib.parse.unquote_plus(b[:16384].decode("latin-1"))
                if self.armed and self.cut == 0 and "SELECT count() FROM" in text \
                        and T in text and "system." not in text:
                    self.cut += 1
                    cut.set()
                up.sendall(b)
        except OSError:
            pass

    def close(self):
        self.srv.close()


def run(port):
    code = (f"import apitap\nr = apitap.transfer({PG!r}, {ch_url(port)!r}, table={T!r}, mode='replace')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def digest(side):
    if side == "src":
        return pg(f"SELECT count(*) || ':' || sum(id) || ':' || max(v) FROM {T}")
    return ch(f"SELECT concat(toString(count()), ':', toString(sum(id)), ':', max(v)) FROM {T}")


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    for n in ch(f"SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '{T}%'").split():
        ch(f"DROP TABLE IF EXISTS `{n}`")


def main():
    wipe()
    proxy = CutBodyProxy()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} SELECT g, 'old' FROM generate_series(1, 300) g")
    r = run(proxy.port)
    assert r.returncode == 0, r.stderr[-800:]
    before = digest("dst")
    assert before == digest("src"), "first replace exact"

    pg(f"UPDATE {T} SET v = 'new'; INSERT INTO {T} SELECT g, 'new' FROM generate_series(301, 500) g")
    proxy.armed = True
    r = run(proxy.port)
    proxy.armed = False
    assert proxy.cut == 1, "the proxy must have cut one count() body"
    assert r.returncode != 0, f"a replace whose staged count never arrived must fail:\n{r.stdout[-400:]}"
    assert "could not be read" in r.stderr, f"the failure must name the unreadable body:\n{r.stderr[-600:]}"
    assert digest("dst") == before, f"the destination must still hold the previous load, got {digest('dst')}"
    print("  a cut 200 body on the staged count: refused, previous table intact")

    r = run(CH_PORT)
    assert r.returncode == 0, r.stderr[-800:]
    assert digest("dst") == digest("src"), "a direct rerun lands exactly"
    left = ch(f"SELECT count() FROM system.tables WHERE database = 'default' AND name LIKE '%{T}%' AND name != '{T}'")
    assert left == "0", f"nothing may be left behind after the rerun, found {left}"
    print("  direct rerun: exact, nothing left behind")
    proxy.close()
    wipe()
    print("✓ a ClickHouse 200 whose body never arrives fails the run")


if __name__ == "__main__":
    main()
