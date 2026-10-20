"""A ClickHouse residue journal entry is only reused by its own window.

A masked update on ClickHouse is readback -> DELETE -> INSERT, and the image
is journalled first so a run that dies mid-op can re-land it (M-13). Journal
rows were told apart only by `ver > window start`, with ver = the dead
attempt's window END. A replay that drains a SHORTER window (a smaller
budget, a deadline, a SIGTERM) commits before that end, so the dead
attempt's rows survive it, and the NEXT window, seeing them as live, reused
an image computed for a different op on the same key: a lost update (deep
review D10).

Proof (fault injection): T1 masks id=1 to 'a', T2 is a >1 MiB filler, T3
masks id=1 to 'b'. Attempt A drains all three in one window and is cut after
its first residue op journalled 'a' (ver = A's end), between that op's DELETE
and INSERT; the journal row is checked on the server. The replay runs with
APITAP_CDC_WINDOW_BYTES=1 MiB, so its first window stops after T2 and its
second carries T3, both in one run. The second window's first op on id=1 is
T3: if it reuses A's entry, id=1 lands as 'a'. The destination must equal
the source.
"""
import os
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT, PROXY_PORT = 8124, 18237
T = "ch_journal_window"


def ch_url(port):
    return f"clickhouse://default:bench@127.0.0.1:{port}/default"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


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


class CutProxy:
    """Forwards to ClickHouse; while `cutting`, once a residue op has written
    its journal row, drops every connection whose request is an INSERT into
    `T`: the op dies between its DELETE and its INSERT."""

    def __init__(self):
        self.cutting = False
        self.armed = False
        self.cuts = 0
        self.srv = socket.create_server(("127.0.0.1", PROXY_PORT), reuse_port=True)
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
                if self.cutting and "INSERT INTO `_apitap_residue_journal`" in text:
                    self.armed = True
                if self.armed and f"INSERT INTO `{T}`" in text:
                    self.cuts += 1
                    break
                up.sendall(b)
        except OSError:
            pass
        c.close()
        up.close()

    def close(self):
        self.srv.close()


def wipe():
    for pub in set(pg(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
           f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    for meta in ("_apitap_state", "_apitap_residue_journal"):
        if ch(f"SELECT count() FROM system.tables WHERE database='default' AND name='{meta}'") != "0":
            ch(f"ALTER TABLE {meta} DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def run(env=None):
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {ch_url(PROXY_PORT)!r}, table={T!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300, env=dict(os.environ, **(env or {})))


def digest_src():
    return pg(f"SELECT id || ':' || small || ':' || md5(big) FROM {T} ORDER BY id")


def digest_dst():
    return ch(f"SELECT concat(toString(id), ':', small, ':', lower(hex(MD5(big)))) "
              f"FROM {T} ORDER BY id FORMAT TSV")


def main():
    wipe()
    proxy = CutProxy()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, small text, big text);"
           f"ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL;"
           f"INSERT INTO {T} SELECT g, 's' || g, repeat(md5(g::text), 300) FROM generate_series(1, 5) g")
        r = run()
        assert r.returncode == 0, r.stderr[-1500:]
        assert digest_dst() == digest_src(), "bootstrap exact"

        pg(f"UPDATE {T} SET small = 'a' WHERE id = 1")
        # >1 MiB of buffered changes, so a 1 MiB budget seals the window here.
        pg(f"INSERT INTO {T} SELECT g, repeat('f', 1500), 'x' FROM generate_series(1000, 2999) g")
        pg(f"UPDATE {T} SET small = 'b' WHERE id = 1")

        proxy.cutting = True
        r = run()
        proxy.cutting = proxy.armed = False
        assert r.returncode != 0 and proxy.cuts > 0, \
            f"(rig) attempt A must die mid-residue (rc={r.returncode}, cuts={proxy.cuts})"
        held = ch(f"SELECT count() FROM _apitap_residue_journal WHERE dest_table = '{T}'")
        assert held != "0", "(rig) attempt A must die after journalling its first residue op"
        print(f"  attempt A died mid-residue, {held} journal row(s) left")

        r = run({"APITAP_CDC_WINDOW_BYTES": str(1 << 20), "APITAP_DEBUG": "1"})
        assert r.returncode == 0, f"the replay failed:\n{r.stderr[-700:]}"
        got = ch(f"SELECT small FROM {T} WHERE id = 1")
        assert got == "b", f"lost update: id=1 is {got!r} at the destination, 'b' at the source"
        assert digest_dst() == digest_src(), "the destination must equal the source"
        windows = [ln for ln in r.stderr.splitlines() if ln.startswith(f"[residue] {T} window")]
        assert len(windows) >= 2, f"(rig) the replay must split T1 and T3 into two windows: {windows}"
        print(f"  the replay split T1 and T3 over {len(windows)} residue windows")
        print("  the replay applied T3 in its next window: id=1 is 'b', destination equals source")
    finally:
        proxy.close()
        wipe()
    print("✓ a residue journal entry is only reused by its own window")


if __name__ == "__main__":
    main()
