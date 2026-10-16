"""An S3 (parquet directory) publish that fails leaves a readable table and no lock.

The S3 sink published by copying staging over the live `part-NNNNN.parquet`
names one by one, then deleted leftovers with `let _` (MASTER review M-32):
- a copy failing midway left a MIXED directory (some parts new, some old);
- a stale part whose delete failed stayed beside the new ones (duplicate
  rows for every reader) on a run reported green;
- `discard` returned at a failed `list()` before releasing the run's lock,
  so every later run of the table was refused.

Fault injection: a proxy in front of MinIO. Each case checks what a reader
of `s3://.../<table>/*.parquet` sees, and that the next run is not locked.
"""
import socket
import subprocess
import sys
import threading

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
S3 = 9100
PREFIX, T = "m32e2e", "s3pub"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def read_request(c):
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
        b = c.recv(1 << 20)
        if not b:
            break
        body += b
    return head, body


def relay(head, body):
    lines = [l for l in head.split(b"\r\n") if not l.lower().startswith(b"connection:")]
    raw = b"\r\n".join(lines) + b"\r\nConnection: close\r\n\r\n" + body
    up = socket.create_connection(("127.0.0.1", S3))
    up.sendall(raw)
    out = b""
    while (b := up.recv(1 << 20)):
        out += b
    up.close()
    return out


class S3Faults:
    def __init__(self):
        self.mode = None      # "copy2" | "stale_delete" | "list_after_copy"
        self.copies = 0
        self.hits = 0
        self.copy_failed = False
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

    def refuse(self, c):
        self.hits += 1
        body = b"<Error><Code>InternalError</Code><Message>injected</Message></Error>"
        c.sendall(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: "
                  + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)

    def serve(self, c):
        try:
            req = read_request(c)
            if req is None:
                return
            head, body = req
            line = head.split(b"\r\n", 1)[0]
            low = head.lower()
            is_copy = b"\r\nx-amz-copy-source:" in low
            published = f"/{PREFIX}/{T}/part-".encode() in line
            if self.mode == "copy2" and is_copy:
                self.copies += 1
                if self.copies >= 2:  # persistent: the client retries a 5xx
                    return self.refuse(c)
            if self.mode == "stale_delete" and line.startswith(b"DELETE") and published:
                return self.refuse(c)
            if self.mode == "list_after_copy":
                if is_copy:
                    self.copy_failed = True
                    return self.refuse(c)
                if self.copy_failed and line.startswith(b"GET") and b"list-type=2" in line:
                    return self.refuse(c)
            c.sendall(relay(head, body))
        except OSError:
            pass
        finally:
            c.close()


def url(port):
    return (f"s3://apitap-bench/{PREFIX}?format=parquet&endpoint=http://127.0.0.1:{port}"
            "&region=us-east-1&access_key_id=bench&secret_access_key=benchbench")


def run(port, mode="replace", parallel=4, env=None):
    import os
    code = (f"import apitap\nr = apitap.transfer({PG!r}, {url(port)!r}, table={T!r}, mode={mode!r}, "
            f"parallel={parallel})\nprint('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300, env=dict(os.environ, **(env or {})))


def read_dir():
    import duckdb
    d = duckdb.connect()
    d.execute("SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
              "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; SET s3_region='us-east-1';")
    return d.execute(f"SELECT count(*), sum(id), min(v), max(v) "
                     f"FROM read_parquet('s3://apitap-bench/{PREFIX}/{T}/*.parquet')").fetchone()


def wipe_s3():
    sys.path.insert(0, "benchmarks")
    import _rig
    for key in _rig.s3_list(f"{PREFIX}/"):
        _rig.s3_delete(key)


def load(tag, n):
    # ~1.5 KB of padding per row (under the TOAST threshold) spreads even a
    # few hundred rows over enough pages for several spans, i.e. several parts.
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v text, pad text);"
       f"INSERT INTO {T} SELECT g, '{tag}', repeat('x', 1500) FROM generate_series(1, {n}) g; ANALYZE {T}")


def main():
    wipe_s3()
    p = S3Faults()

    # A: the second copy of the publish fails.
    load("old", 1000)
    r = run(p.port)
    assert r.returncode == 0, r.stderr[-600:]
    load("new", 900)
    p.mode = "copy2"
    r = run(p.port)
    p.mode = None
    assert p.copies >= 2, f"the publish must copy several parts to stage this fault (copies={p.copies})"
    assert r.returncode != 0, "the faulted publish must fail"
    got = read_dir()
    assert got == (1000, sum(range(1, 1001)), "old", "old"), \
        f"A: a failed publish must leave the previous table intact, readers see {got}"
    print(f"  A copy fault: run failed, readers still see the previous table {got[:2]}")

    # B: a run with fewer parts; deleting a previous part fails.
    r = run(p.port)  # settle on the 900-row table
    assert r.returncode == 0, r.stderr[-600:]
    load("fewer", 300)
    p.mode = "stale_delete"
    r = run(p.port, parallel=1)
    p.mode = None
    got = read_dir()
    if r.returncode == 0:
        assert got == (300, sum(range(1, 301)), "fewer", "fewer"), \
            f"B: the run reported SUCCESS while readers see {got} (stale parts beside the new)"
    else:
        assert "could not be deleted" in r.stderr, f"B: the failure must name the leftovers:\n{r.stderr[-500:]}"
        r = run(p.port, parallel=1)
        assert r.returncode == 0 and read_dir() == (300, sum(range(1, 301)), "fewer", "fewer"), \
            f"B: the next run must sweep the leftovers, readers see {read_dir()}"
    print("  B stale-delete fault: never a green run over duplicate rows; the next run sweeps")

    # C: a failed publish whose staging LIST then fails too.
    load("c", 200)
    p.mode = "list_after_copy"
    r = run(p.port)
    p.mode = None
    assert r.returncode != 0 and p.copy_failed, "C: the faulted run must fail"
    r = run(p.port)
    assert r.returncode == 0 and "locked" not in r.stderr, \
        f"C: the next run must not be refused over the failed one:\n{r.stderr[-500:]}"
    assert read_dir()[0] == 200, f"C: readers see {read_dir()}"
    print("  C list fault in discard: the lock was released, the next run proceeds")

    # D: a part over the single-copy limit is copied in ranges (UploadPartCopy).
    # The limit is lowered to the 5 MiB multipart minimum; ~13 KB of hex per
    # row, stored uncompressed, makes one ~7 MB parquet part.
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (id int PRIMARY KEY, v text, pad text);"
       f"ALTER TABLE {T} ALTER COLUMN pad SET STORAGE EXTERNAL;"
       f"INSERT INTO {T} SELECT g, 'big', (SELECT string_agg(md5(g::text || i::text), '') "
       f"FROM generate_series(1, 400) i) FROM generate_series(1, 900) g; ANALYZE {T}")
    r = run(p.port, parallel=1, env={"APITAP_S3_COPY_PART_BYTES": str(5 << 20)})
    assert r.returncode == 0, f"D: the multipart copy failed:\n{r.stderr[-500:]}"
    assert read_dir() == (900, sum(range(1, 901)), "big", "big"), f"D: readers see {read_dir()}"
    print("  D part over the copy limit: copied in ranges, readers see it whole")

    p.srv.close()
    wipe_s3()
    pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ S3 publish faults: no mixed directory, no green run over duplicates, no leaked lock, big parts copy")


if __name__ == "__main__":
    main()
