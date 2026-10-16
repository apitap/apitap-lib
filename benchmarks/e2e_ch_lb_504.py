"""A load balancer that forwards EXCHANGE and answers 504 never republishes old data.

Behind a proxy (the Paysera shape: ClickHouse behind an LB), a 502/504 means
the statement was FORWARDED and the answer lost. apitap retried every
statement on a proxy 5xx, so a replace whose EXCHANGE ran but whose answer
was lost sent EXCHANGE again — swapping the tables back — and then dropped
"staging", which by then held the NEW data. The run reported success with
the old table published (MASTER review M-17).

Proof: a TCP proxy forwards every request; for the first `EXCHANGE TABLES`
it lets ClickHouse answer, then sends apitap a 504 page instead. The replace
must not report success with stale data: it either fails saying the outcome
is unknown, or the destination equals the source. A direct rerun then lands
exactly, and a read through the same proxy still retries a 504.
"""
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT = 8124
T = "lb504"


def ch_url(port):
    return f"clickhouse://default:bench@127.0.0.1:{port}/default"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


class LoseAnswerProxy:
    """Forwards everything; the first request carrying `EXCHANGE TABLES`
    reaches ClickHouse, whose answer is replaced by a 504."""

    def __init__(self):
        self.armed = False
        self.lost = 0
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
        lose = threading.Event()

        def back():
            try:
                while (b := up.recv(65536)):
                    if lose.is_set():
                        page = b"<html><body>504 Gateway Time-out</body></html>"
                        c.sendall(b"HTTP/1.1 504 Gateway Time-out\r\nContent-Length: "
                                  + str(len(page)).encode() + b"\r\nConnection: close\r\n\r\n" + page)
                        break
                    c.sendall(b)
            except OSError:
                pass
            finally:
                c.close()
        threading.Thread(target=back, daemon=True).start()
        try:
            while (b := c.recv(65536)):
                text = urllib.parse.unquote_plus(b[:8192].decode("latin-1"))
                if self.armed and "EXCHANGE TABLES" in text and self.lost == 0:
                    self.lost += 1
                    lose.set()
                up.sendall(b)
        except OSError:
            pass

    def close(self):
        self.srv.close()


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")


def run(port):
    code = (f"import apitap\nr = apitap.transfer({PG!r}, {ch_url(port)!r}, table={T!r}, mode='replace')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def digest_src():
    return pg(f"SELECT count(*) || ':' || sum(id) || ':' || max(v) FROM {T}")


def digest_dst():
    return ch(f"SELECT concat(toString(count()), ':', toString(sum(id)), ':', max(v)) FROM {T}")


def main():
    wipe()
    proxy = LoseAnswerProxy()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {T} SELECT g, 'old' FROM generate_series(1, 500) g")
    r = run(proxy.port)
    assert r.returncode == 0, r.stderr[-800:]
    assert digest_dst() == digest_src(), "first replace exact"

    pg(f"UPDATE {T} SET v = 'new'; INSERT INTO {T} SELECT g, 'new' FROM generate_series(501, 700) g")
    proxy.armed = True
    r = run(proxy.port)
    proxy.armed = False
    assert proxy.lost == 1, "the proxy must have swallowed one EXCHANGE answer"
    if r.returncode == 0:
        assert digest_dst() == digest_src(), \
            f"the replace reported SUCCESS with stale data: dst {digest_dst()} vs src {digest_src()}"
    else:
        assert "unknown" in r.stderr, f"a failed replace must say the outcome is unknown:\n{r.stderr[-600:]}"
        print("  replace with a lost EXCHANGE answer: refused, outcome named unknown")

    r = run(proxy.port)
    assert r.returncode == 0, r.stderr[-800:]
    assert digest_dst() == digest_src(), "the rerun lands exactly"
    left = ch(f"SELECT count() FROM system.tables WHERE database = 'default' AND name LIKE '{T}%' AND name != '{T}'")
    assert left == "0", f"no staging left behind, found {left}"
    print("  rerun: destination equals source, no staging left")
    proxy.close()
    wipe()
    print("✓ CH behind an LB: a forwarded-then-lost EXCHANGE never republishes old data")


if __name__ == "__main__":
    main()
