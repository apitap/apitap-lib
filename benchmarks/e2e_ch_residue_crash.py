"""ClickHouse residue ops replay exactly: after a cut mid-op, and after a
cut once the whole window landed.

On ClickHouse a TOAST-masked update (and a re-key) is readback → DELETE →
INSERT with no transaction. A run that died between the DELETE and the
INSERT lost the row; the replay then found it missing and wedged the lane
("masked update for a row missing at the destination") — or, for a re-key,
skipped the move silently (MASTER review M-13 / M-12). The patched image is
now journalled before the DELETE and the replay re-lands it.

Fault injection drives the real apply: a TCP proxy in front of ClickHouse
cuts every connection that carries an INSERT into the table once a DELETE
on it has passed. The proxied drain must fail mid-residue (a row really is
missing), and the next, direct drain must leave the destination equal to
the source: id, small value and md5 of the TOASTed value. Every run goes
through the proxy (the slot is named after the destination's host:port);
only the faulted run has cutting switched on.

Second phase (M-12): the review's re-key chains — `Rekey 1→2; Rekey 3→1`,
`DELETE 2; UPDATE id 1→2`, `Rekey 1→2; INSERT 1(X)` — in one transaction,
TOAST-masked, and the cut placed on the state write, so every residue op
already ran and the replay re-applies them over the window's own later
images. A readback then returns the wrong row or none; the replay must
reuse the first attempt's journalled images and land exactly.
"""
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT, PROXY_PORT = 8124, 18124
T = "rc13"


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
    """Forwards to ClickHouse; after a DELETE on `T` passes, drops every
    connection whose request is an INSERT into `T`."""

    def __init__(self):
        self.cutting = False
        self.cut_state = False
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
                if self.cutting and f"DELETE FROM `{T}`" in text:
                    self.armed = True
                if (self.armed and f"INSERT INTO `{T}`" in text) or \
                        (self.cut_state and "INSERT INTO `_apitap_state`" in text):
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
    # Our slot is the one whose publication covers our tables — asked of the
    # server BEFORE the tables go (a dropped table leaves its publication
    # empty and the slot unfindable, holding WAL for ever).
    for pub in set(pg("SELECT pubname FROM pg_publication_tables "
                       f"WHERE tablename IN ({', '.join(repr(t) for t in (T,))})").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    pg(f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database='default' "
          "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def run(port):
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {ch_url(port)!r}, table={T!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def digest_src():
    return pg(f"SELECT id || ':' || small || ':' || md5(big) FROM {T} ORDER BY id")


def digest_dst():
    return ch(f"SELECT concat(toString(id), ':', small, ':', lower(hex(MD5(big)))) "
              f"FROM {T} ORDER BY id FORMAT TSV")


def main():
    wipe()
    proxy = CutProxy()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, small text, big text);"
       f"ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL;"
       f"INSERT INTO {T} SELECT g, 's' || g, repeat(md5(g::text), 300) "
       f"FROM generate_series(1, 20) g")
    r = run(PROXY_PORT)
    assert r.returncode == 0, r.stderr[-1500:]
    assert digest_dst() == digest_src(), "bootstrap exact"

    # Masked updates (big unchanged) and a TOAST-masked re-key.
    pg(f"UPDATE {T} SET small = 'masked' WHERE id <= 3")
    pg(f"UPDATE {T} SET id = 100, small = 'moved' WHERE id = 10")

    proxy.cutting = True
    r = run(PROXY_PORT)
    proxy.cutting = proxy.armed = False
    assert r.returncode != 0 and proxy.cuts > 0, \
        f"the proxied drain must die mid-residue (rc={r.returncode}, cuts={proxy.cuts})"
    rows = ch(f"SELECT count() FROM {T}")
    assert rows == "19", f"the fault must leave a row deleted but not re-inserted, got {rows}"
    print(f"  proxied drain cut mid-residue: {rows} rows at the destination")

    r = run(PROXY_PORT)
    assert r.returncode == 0, f"the replay after the fault failed:\n{r.stderr[-700:]}"
    assert digest_dst() == digest_src(), "replay must leave the destination equal to the source"
    print("  replay recovered: destination equals source")

    # Phase 2: the re-key chains, cut at the state write.
    pg(f"BEGIN;"
       f"UPDATE {T} SET id = 201 WHERE id = 11;"
       f"UPDATE {T} SET id = 11, small = 'was13' WHERE id = 13;"
       f"DELETE FROM {T} WHERE id = 15;"
       f"UPDATE {T} SET id = 15, small = 'was16' WHERE id = 16;"
       f"UPDATE {T} SET id = 202 WHERE id = 17;"
       f"INSERT INTO {T} VALUES (17, 'fresh', repeat('X', 9000));"
       f"UPDATE {T} SET small = 'masked2' WHERE id IN (1, 2);"
       f"COMMIT")
    proxy.cut_state = True
    r = run(PROXY_PORT)
    proxy.cut_state = False
    assert r.returncode != 0 and proxy.cuts > 1, \
        f"the state write must be cut (rc={r.returncode}, cuts={proxy.cuts})"
    r = run(PROXY_PORT)
    assert r.returncode == 0, f"the chain replay failed:\n{r.stderr[-700:]}"
    assert digest_dst() == digest_src(), "chain replay must leave the destination equal to the source"
    print("  re-key chains replayed over their own images: destination equals source")
    proxy.close()
    wipe()
    print("✓ CH residue: a fault between DELETE and INSERT replays exact")


if __name__ == "__main__":
    main()
