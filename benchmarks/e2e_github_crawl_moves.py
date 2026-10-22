"""A github+api crawl survives an issue updated while it is being read.

/issues and /issues/comments were paged `sort=updated&direction=asc` by page
number. An item updated mid-crawl moves to the end of that order: every later
item shifts left by one, the first item of the next page is never read, and
the moved one is read twice. The watermark then passes the skipped row, which
is lost until it changes again, and a merge fails on the duplicate key (deep
review, D28). An incremental crawl now pages by keyset on `since=` and leaves
items updated after the crawl began to the next run; a full crawl pages in
creation order.

Proof: a mock GitHub API (APITAP_GITHUB_API) serving 250 issues, 100 a page,
with page-number Link headers. Right after the first page of the first
crawl, issue 5 (already read) is updated. merge with cursor=updated_at into
Postgres must land all 250 issues once; the next run must bring issue 5's
new version. The destination then equals the mock.
"""
import datetime as dt
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import apitap

DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "github_crawl_moves"
N = 250
MOVED = 5


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
base = now - dt.timedelta(days=1)
ISSUES = {i: {"id": i, "number": i, "title": f"t{i}", "state": "open", "user": {"login": "u"},
              "labels": [], "comments": 0, "created_at": iso(base + dt.timedelta(seconds=i)),
              "updated_at": iso(base + dt.timedelta(seconds=i)), "closed_at": None, "body": "b"}
          for i in range(1, N + 1)}
STATE = {"mutate": False, "served": 0}
LOCK = threading.Lock()


class Api(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send_json(self, body, link=None):
        raw = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        if link:
            self.send_header("link", link)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/repos/o/r":
            return self.send_json({"id": 1, "full_name": "o/r", "private": False, "default_branch": "main"})
        if u.path != "/repos/o/r/issues":
            self.send_response(404)
            self.end_headers()
            return
        with LOCK:
            items = [dict(v) for v in ISSUES.values()]
            if "since" in q:
                items = [i for i in items if i["updated_at"] >= q["since"]]
            key = "updated_at" if q.get("sort") == "updated" else "created_at"
            items.sort(key=lambda i: (i[key], i["id"]))
            per, page = int(q.get("per_page", 30)), int(q.get("page", 1))
            chunk = items[(page - 1) * per: page * per]
            link = None
            if page * per < len(items):
                q2 = dict(q, page=str(page + 1))
                link = f'<http://127.0.0.1:{PORT}/repos/o/r/issues?' + \
                       "&".join(f"{k}={v}" for k, v in q2.items()) + '>; rel="next"'
            STATE["served"] += 1
            if STATE["mutate"]:
                # Issue 5 gets a comment the moment the first page is out.
                STATE["mutate"] = False
                ISSUES[MOVED]["updated_at"] = iso(dt.datetime.now(dt.timezone.utc))
                ISSUES[MOVED]["comments"] = 1
        self.send_json(chunk, link)


srv = ThreadingHTTPServer(("127.0.0.1", 0), Api)
PORT = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()


def psql(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d",
                        "apitap_bench_dst", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    psql(f"DROP TABLE IF EXISTS {T}")
    psql(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
         f"DELETE FROM public._apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def dest_state():
    return psql(f"SELECT count(*) || '/' || count(DISTINCT id) || '/' || "
                f"COALESCE(string_agg(id || '=' || to_char(updated_at AT TIME ZONE 'UTC', "
                f"'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), ',' ORDER BY id), '') FROM {T}")


def mock_state():
    rows = sorted(ISSUES.values(), key=lambda i: i["id"])
    return f"{N}/{N}/" + ",".join(f"{i['id']}={i['updated_at']}" for i in rows)


def run():
    apitap.transfer("github+api://o/r", DST, table="issues", dest_table=T, mode="merge", cursor="updated_at")


def main():
    os.environ["APITAP_GITHUB_API"] = f"http://127.0.0.1:{PORT}"
    os.environ.pop("GITHUB_TOKEN", None)
    os.environ.pop("GH_TOKEN", None)
    wipe()
    try:
        STATE["mutate"] = True
        run()
        n, distinct = map(int, dest_state().split("/")[:2])
        assert (n, distinct) == (N, N), f"first crawl landed {n} rows, {distinct} distinct ids, want {N}"
        time.sleep(1.2)  # the next crawl starts in a later second than the update
        run()
        got, want = dest_state(), mock_state()
        assert got == want, f"after the second run the destination differs from the mock:\n{got[:300]}\n{want[:300]}"
        moved = psql(f"SELECT comments FROM {T} WHERE id = {MOVED}")
        assert moved == "1", f"issue {MOVED}'s update never arrived (comments={moved})"
        print(f"  issue {MOVED} updated mid-crawl: {N} issues once, then its new version ({STATE['served']} requests)")
    finally:
        wipe()
        srv.shutdown()
    print("✓ a github+api crawl survives an issue updated while it is being read")


if __name__ == "__main__":
    main()
