"""A staging DROP that fails after the publish neither fails the run nor repeats it.

A ClickHouse append ATTACHes the staging parts into the destination, then
dropped staging with `?`, then wrote the watermark. A DROP refused after the
ATTACH (a table over max_table_size_to_drop) failed a run whose rows were
already published, and left its watermark behind them (MASTER review M-62).
The state row is now written right after the publish and the drop is
best-effort (a warning; the next run names the leftover).

Proof (fan-in shape: a sibling source's state row makes this source's state
authoritative, so a watermark left behind is the one the next run uses): a
proxy answers the run's staging `DROP TABLE` with a ClickHouse exception
(the statement never reaches the server). The faulted append must
succeed and leave the destination equal to the source; once the leftover
staging is dropped (as the next run's refusal instructs), a re-run must not
add a single duplicate.
"""
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT = 8124
T = "dap62"


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


class RefuseDrop:
    """Answers a `DROP TABLE` of this table's staging with a ClickHouse
    exception instead of forwarding it."""

    def __init__(self):
        self.refusing = False
        self.refused = 0
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

        def back():
            try:
                while (b := up.recv(65536)):
                    c.sendall(b)
            except OSError:
                pass
            finally:
                c.close()
        threading.Thread(target=back, daemon=True).start()
        try:
            while (b := c.recv(65536)):
                text = urllib.parse.unquote_plus(b[:8192].decode("latin-1"))
                if self.refusing and "DROP TABLE" in text and T in text and "staging" in text:
                    self.refused += 1
                    err = (b"Code: 359. DB::Exception: Table or Partition in default." + T.encode()
                           + b" was not dropped. Reason: size exceeds max_table_size_to_drop. (TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT)")
                    c.sendall(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: "
                              + str(len(err)).encode() + b"\r\nConnection: close\r\n\r\n" + err)
                    break
                up.sendall(b)
        except OSError:
            pass
        c.close()
        up.close()

    def close(self):
        self.srv.close()


def run(port):
    code = (f"import apitap\nr = apitap.transfer({PG!r}, 'clickhouse://default:bench@127.0.0.1:{port}/default', "
            f"table={T!r}, mode='append', cursor='id')\nprint('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}")
    for name in ch(f"SELECT name FROM system.tables WHERE database = 'default' AND name LIKE '{T}%'").split():
        ch(f"DROP TABLE IF EXISTS `{name}`")
    if ch("SELECT count() FROM system.tables WHERE database='default' AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def counts():
    return pg(f"SELECT count(*) || ':' || sum(id) FROM {T}"), \
        ch(f"SELECT concat(toString(count()), ':', toString(sum(id))) FROM {T}")


def main():
    wipe()
    proxy = RefuseDrop()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text);"
       f"INSERT INTO {T} SELECT g, 'a' FROM generate_series(1, 300) g")
    r = run(proxy.port)
    assert r.returncode == 0, r.stderr[-800:]

    # Fan-in: a sibling source's state row makes this source's state row
    # authoritative (M-5) — a watermark left behind is then the one used.
    ch("INSERT INTO _apitap_state (dest_table, source_id, cursor_col, watermark, mode, last_rows) "
       f"VALUES ('{T}', 'postgres://sibling:5432/other::public.{T}', 'id', '0', 'append', 0)")
    pg(f"INSERT INTO {T} SELECT g, 'b' FROM generate_series(301, 400) g")
    proxy.refusing = True
    r = run(proxy.port)
    proxy.refusing = False
    assert proxy.refused >= 1, "the staging DROP must have been refused"
    src, dst = counts()
    print(f"  faulted append: rc={r.returncode}, src {src}, dst {dst}")
    assert dst == src, f"the faulted append left dst {dst} vs src {src}"

    wm = ch(f"SELECT argMax(watermark, synced_at) FROM _apitap_state FINAL "
            f"WHERE dest_table = '{T}' AND source_id NOT LIKE '%sibling%'")
    assert wm == "400", f"the rows up to 400 are published, so the watermark must say 400, got {wm!r}"
    assert r.returncode == 0, f"the append landed, so it must succeed (warning only):\n{r.stderr[-500:]}"

    # The leftover staging is named by the next run; the operator drops it as
    # the message says, and re-runs. That re-run must not land the delta twice.
    for name in ch(f"SELECT name FROM system.tables WHERE database = 'default' "
                   f"AND name LIKE '{T}%staging%'").split():
        ch(f"DROP TABLE `{name}`")
    r = run(proxy.port)
    src, dst = counts()
    assert r.returncode == 0, r.stderr[-500:]
    assert dst == src, f"the re-run duplicated the delta: dst {dst} vs src {src}"
    print(f"  after the leftover is dropped, the re-run adds nothing: dst {dst}")
    proxy.close()
    wipe()
    print("✓ a staging DROP refused after the publish never re-lands the delta")


if __name__ == "__main__":
    main()
