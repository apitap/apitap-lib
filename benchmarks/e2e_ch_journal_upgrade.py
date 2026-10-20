"""A ClickHouse residue journal row that 0.62.0 wrote still recovers its op.

0.62.1 keys the residue journal by commit position (`pos|n|pred`); 0.62.0
keyed it `n|pred`. A 0.62.0 run cut between a TOAST re-key's DELETE and its
INSERT leaves the row at neither key and its image only in the journal,
under the old key. The upgraded replay missed it, read the old key back
(empty) and skipped the op: the row was gone, the window committed, no
error (deep review 2, N1).

Proof (pg -> ch, needs APITAP_PY_0620 = a 0.62.0 interpreter): bootstrap,
then UPDATE id 1 -> 9 leaving the TOASTed column alone. Attempt A runs on
0.62.0 and is cut after journalling that re-key, between its DELETE and
INSERT (the journal row is checked on the server). The replay runs on this
build. The destination must equal the source: id 9 present with its TOASTed
value.
"""
import os
import socket
import subprocess
import sys
import threading
import urllib.parse

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH_PORT, PROXY_PORT = 8124, 18238
T = "ch_journal_upgrade"
OLD_PY = os.environ.get("APITAP_PY_0620", "")


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


def run(env=None, py=None):
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {ch_url(PROXY_PORT)!r}, table={T!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    return sh([py or sys.executable, "-c", code], timeout=300, env=dict(os.environ, **(env or {})))


def digest_src():
    return pg(f"SELECT id || ':' || small || ':' || md5(big) FROM {T} ORDER BY id")


def digest_dst():
    return ch(f"SELECT concat(toString(id), ':', small, ':', lower(hex(MD5(big)))) "
              f"FROM {T} ORDER BY id FORMAT TSV")


def main():
    if not OLD_PY:
        print("SKIP: APITAP_PY_0620 is not set")
        return
    wipe()
    proxy = CutProxy()
    try:
        pg(f"CREATE TABLE {T} (id int PRIMARY KEY, small text, big text);"
           f"ALTER TABLE {T} ALTER COLUMN big SET STORAGE EXTERNAL;"
           f"INSERT INTO {T} SELECT g, 's' || g, repeat(md5(g::text), 300) FROM generate_series(1, 5) g")
        r = run()
        assert r.returncode == 0, r.stderr[-1500:]
        assert digest_dst() == digest_src(), "(rig) bootstrap exact"

        pg(f"UPDATE {T} SET id = 9 WHERE id = 1")
        proxy.cutting = True
        r = run(py=OLD_PY)
        proxy.cutting = proxy.armed = False
        assert r.returncode != 0 and proxy.cuts > 0, \
            f"(rig) the 0.62.0 attempt must die mid-residue (rc={r.returncode}, cuts={proxy.cuts})"
        held = ch(f"SELECT count() FROM _apitap_residue_journal WHERE dest_table = '{T}'")
        assert held != "0", "(rig) the 0.62.0 attempt must die after journalling the re-key"
        assert ch(f"SELECT count() FROM {T} WHERE id IN (1, 9)") == "0", \
            "(rig) the cut must leave the row at neither key"
        print(f"  0.62.0 attempt died between the re-key's DELETE and INSERT, {held} journal row(s)")

        r = run()
        assert r.returncode == 0, f"the upgraded replay failed:\n{r.stderr[-700:]}"
        assert digest_dst() == digest_src(), \
            f"the destination differs from the source after the replay: {digest_dst()!r}"
        print("  the upgraded replay re-landed the re-key from the 0.62.0 journal: destination equals source")
    finally:
        proxy.close()
        wipe()
    print("✓ a residue journal row that 0.62.0 wrote still recovers its op")


if __name__ == "__main__":
    main()
