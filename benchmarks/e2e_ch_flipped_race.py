"""A first run that lost the race to a peer refuses instead of destroying it.

A first append into a missing ClickHouse table lands as a replace (a
"flipped bootstrap"). Its guard was check-then-act: probe system.tables,
then CREATE IF NOT EXISTS + EXCHANGE + DROP staging. A peer that published
between the probe and the EXCHANGE was swapped into staging and dropped —
its rows gone, its watermark deleted with the blanket state DELETE (MASTER
review M-11). The publish is now an atomic claim (RENAME / a plain CREATE)
that fails where the table exists, and only this source's state row is
cleared.

Proof: a proxy lets the run's publish-time probe (the first one after the
data reached staging) answer "absent", then creates the
peer's table with a sentinel row before relaying that answer. The run must
not succeed over the peer: the sentinel survives, and a rerun appends
beside it.
"""
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT = 8124
T = "flip_race"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    # stdin closed: an INSERT ... VALUES under `docker exec -i` would wait
    # for more data on stdin until EOF.
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql], stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def create_peer():
    ch(f"CREATE TABLE {T} (id Int32, v Nullable(String)) ENGINE = MergeTree ORDER BY id")
    ch(f"INSERT INTO {T} VALUES (-1, 'peer')")


class PeerAfterProbe:
    def __init__(self):
        self.armed = False
        self.loaded = False
        self.injected = 0
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
        probe = threading.Event()

        def back():
            try:
                while (b := up.recv(65536)):
                    if probe.is_set() and self.injected == 0:
                        self.injected += 1
                        create_peer()  # the peer publishes right after our probe
                    c.sendall(b)
            except OSError:
                pass
            finally:
                c.close()
        threading.Thread(target=back, daemon=True).start()
        try:
            while (b := c.recv(65536)):
                text = urllib.parse.unquote_plus(b[:8192].decode("latin-1"))
                # The publish probe is the one AFTER the data reached
                # staging; earlier probes belong to the run's state read,
                # which an existing guard already re-checks.
                if self.armed and "INSERT INTO" in text and T in text:
                    self.loaded = True
                if self.armed and self.loaded and "system.tables" in text and f"name = '{T}'" in text:
                    probe.set()
                up.sendall(b)
        except OSError:
            pass

    def close(self):
        self.srv.close()


def ch_url(port):
    return f"clickhouse://default:bench@127.0.0.1:{port}/default"


def run(port):
    code = (f"import apitap\nr = apitap.transfer({PG!r}, {ch_url(port)!r}, table={T!r}, "
            "mode='append', cursor='id')\nprint('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    for name in ch(f"SELECT name FROM system.tables WHERE database = 'default' "
                   f"AND name LIKE '{T}%' AND name != '{T}'").split():
        ch(f"DROP TABLE IF EXISTS `{name}`")
    if ch("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def main():
    wipe()
    proxy = PeerAfterProbe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {T} SELECT g, 'src' FROM generate_series(1, 300) g")
    proxy.armed = True
    r = run(proxy.port)
    proxy.armed = False
    assert proxy.injected == 1, "the peer must have been created right after the probe"
    sentinel = ch(f"SELECT count() FROM {T} WHERE id = -1 AND v = 'peer'")
    assert sentinel == "1", (f"the peer's row is GONE (run rc={r.returncode}) — "
                             "the first run replaced a table it never created")
    assert r.returncode != 0 and "another run published" in r.stderr, \
        f"the losing first run must refuse, naming the peer:\n{r.stderr[-600:]}"
    print("  first run lost the race: refused, the peer's table intact")

    r = run(proxy.port)
    assert r.returncode == 0, r.stderr[-800:]
    got = ch(f"SELECT countIf(v = 'src'), countIf(v = 'peer') FROM {T} FORMAT TSV")
    assert got == "300\t1", f"rerun must append beside the peer, got {got!r}"
    left = ch(f"SELECT count() FROM system.tables WHERE database = 'default' "
              f"AND name LIKE '{T}%' AND name != '{T}'")
    assert left == "0", f"no staging left behind, found {left}"
    print("  rerun: appended beside the peer, no staging left")
    proxy.close()
    wipe()
    print("✓ a flipped bootstrap that loses the race refuses; the peer keeps its table")


if __name__ == "__main__":
    main()
