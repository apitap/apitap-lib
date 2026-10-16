"""A run refused after its announcement leaves no staging behind.

After `prepare` creates staging (the run's announcement), the driver re-reads
the state row; if a peer committed in between it refuses, rightly. But that
exit — and an error in the re-read itself — returned without `discard`: the
staging stayed, read as a live peer, and every later run of the table was
refused as "locked" over a run that had ended (MASTER review M-33).

Proof (ClickHouse): an incremental append; a proxy lets the run's first state
read through, then commits a newer state row for the same source (the peer).
The run must refuse naming the peer, leave no staging, and the next run
must not be refused as locked.
"""
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT = 8124
T = "rfl33"


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
            "--user", "default", "--password", "bench", "-q", sql], stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


class PeerCommitsAfterRead:
    def __init__(self, on_read):
        self.armed = False
        self.fired = 0
        self.on_read = on_read
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
        hit = threading.Event()

        def back():
            try:
                while (b := up.recv(65536)):
                    c.sendall(b)
                    if hit.is_set() and self.fired == 0:
                        self.fired += 1
                        self.on_read()  # the peer commits right after our read
            except OSError:
                pass
            finally:
                c.close()
        threading.Thread(target=back, daemon=True).start()
        try:
            while (b := c.recv(65536)):
                text = urllib.parse.unquote_plus(b[:8192].decode("latin-1"))
                if self.armed and "_apitap_state" in text and "SELECT" in text and self.fired == 0:
                    hit.set()
                up.sendall(b)
        except OSError:
            pass

    def close(self):
        self.srv.close()


def run(port):
    code = (f"import apitap\nr = apitap.transfer({PG!r}, 'clickhouse://default:bench@127.0.0.1:{port}/default', "
            f"table={T!r}, mode='append', cursor='id')\nprint('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def leftovers():
    return ch(f"SELECT count() FROM system.tables WHERE database = 'default' "
              f"AND name LIKE '{T}%' AND name != '{T}'")


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    for name in ch(f"SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '{T}%'").split():
        ch(f"DROP TABLE IF EXISTS `{name}`")
    if ch("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {T} SELECT g, 'a' FROM generate_series(1, 300) g")

    def peer_commits():
        sid = ch(f"SELECT any(source_id) FROM _apitap_state WHERE dest_table = '{T}'")
        ch("INSERT INTO _apitap_state (dest_table, source_id, cursor_col, watermark, mode, last_rows) "
           f"VALUES ('{T}', '{sid}', 'id', '350', 'append', 50)")

    proxy = PeerCommitsAfterRead(peer_commits)
    r = run(proxy.port)
    assert r.returncode == 0, r.stderr[-800:]
    pg(f"INSERT INTO {T} SELECT g, 'b' FROM generate_series(301, 400) g")

    proxy.armed = True
    r = run(proxy.port)
    proxy.armed = False
    assert proxy.fired == 1, "the peer must have committed after the run's state read"
    assert r.returncode != 0 and "another run committed" in r.stderr, \
        f"the run must refuse, naming the peer:\n{r.stderr[-600:]}"
    left = leftovers()
    assert left == "0", f"the refused run left {left} object(s) behind"
    print("  refused after its announcement, nothing left behind")

    r = run(proxy.port)
    assert r.returncode == 0 and "locked" not in r.stderr, \
        f"the next run must not be refused over the ended one:\n{r.stderr[-600:]}"
    print("  next run proceeds")
    proxy.close()
    wipe()
    print("✓ a run refused after its announcement leaves no staging; the table stays usable")


if __name__ == "__main__":
    main()
