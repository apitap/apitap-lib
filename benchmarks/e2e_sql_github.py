"""apitap.sql() joins GitHub issues (github+api://) with a Postgres table, in Rust (B7).

Through 0.65.0, apitap.sql() did not exist, and GitHub data could only be copied
into a database before anything joined it. github+api:// is now a read() lane,
so apitap.sql() joins it straight away:

- the WHERE on the GitHub side runs per row in Rust, because the API has no
  server to filter;
- the key range comes from the Postgres side alone;
- the answer equals the one computed here from the mock's own data.

The GitHub API is a local mock (APITAP_GITHUB_API) with 230 issues: open and
closed states, comment counts, and one issue with a NULL author.

  1  SELECT t.team, COUNT(*), SUM(i.comments) FROM gh.issues i JOIN pg.triage t
     ON i.number = t.number WHERE i.state = 'open' GROUP BY 1: equals the mock
  2  the same with an IN list, OR and IS NOT NULL on the GitHub side
  3  one GitHub table alone: COUNT(*) GROUP BY state
  4  read(where=) on github+api is refused, naming the cause (no SQL server)
"""
import json
import os
import subprocess
import sys
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "sql_triage"
N = 230
ISSUES = [{"id": 1000 + i, "number": i, "title": f"t{i}", "state": "open" if i % 3 else "closed",
           "user": {"login": f"u{i % 7}"} if i != 17 else None, "labels": [], "comments": i % 5,
           "created_at": "2026-01-01T00:00:00Z", "updated_at": f"2026-01-01T00:{i // 60:02d}:{i % 60:02d}Z",
           "closed_at": None, "body": "b"} for i in range(1, N + 1)]
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


class Api(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/repos/o/r":
            body, link = {"id": 1, "full_name": "o/r", "private": False, "default_branch": "main",
                          "open_issues_count": N}, None
        elif u.path == "/repos/o/r/issues":
            per, page = int(q.get("per_page", 30)), int(q.get("page", 1))
            body = ISSUES[(page - 1) * per: page * per]
            link = None
            if page * per < len(ISSUES):
                q2 = dict(q, page=str(page + 1))
                link = f'<http://127.0.0.1:{PORT}/repos/o/r/issues?' + "&".join(f"{k}={v}" for k, v in q2.items()) \
                       + '>; rel="next"'
        else:
            self.send_response(404)
            self.end_headers()
            return
        raw = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        if link:
            self.send_header("link", link)
        self.end_headers()
        self.wfile.write(raw)


srv = ThreadingHTTPServer(("127.0.0.1", 0), Api)
PORT = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                        "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def rows(res):
    return [tuple(r) for r in res.to_polars().iter_rows()]


def main():
    os.environ["APITAP_GITHUB_API"] = f"http://127.0.0.1:{PORT}"
    os.environ.pop("GITHUB_TOKEN", None)
    os.environ.pop("GH_TOKEN", None)
    src = {"gh": "github+api://o/r", "pg": PG}
    # Triage covers issues 1..200 (30 issues have no team row).
    pg(f"DROP TABLE IF EXISTS {T}; CREATE TABLE {T} (number int PRIMARY KEY, team text); "
       f"INSERT INTO {T} SELECT g, 'team-' || (g % 4) FROM generate_series(1, 200) g")
    team = {i: f"team-{i % 4}" for i in range(1, 201)}
    try:
        q = (f"SELECT t.team, COUNT(*) AS n, SUM(i.comments) AS c FROM gh.issues i "
             f"JOIN pg.{T} t ON i.number = t.number WHERE i.state = 'open' GROUP BY 1 ORDER BY 1")
        agg = defaultdict(lambda: [0, 0])
        for it in ISSUES:
            if it["state"] == "open" and it["number"] in team:
                a = agg[team[it["number"]]]
                a[0] += 1
                a[1] += it["comments"]
        want = [(k, v[0], v[1]) for k, v in sorted(agg.items())]
        got = rows(apitap.sql(q, sources=src))
        case("1: github issues x Postgres, WHERE on the GitHub side, equals the mock", got == want, f"got {got} want {want}")

        q2 = (f"SELECT t.team, COUNT(*) FROM gh.issues i JOIN pg.{T} t ON i.number = t.number "
              f"WHERE (i.comments IN (1, 3) OR i.state = 'closed') AND i.author IS NOT NULL GROUP BY 1 ORDER BY 1")
        agg = defaultdict(int)
        for it in ISSUES:
            if it["number"] in team and it["user"] is not None and (it["comments"] in (1, 3) or it["state"] == "closed"):
                agg[team[it["number"]]] += 1
        want = sorted(agg.items())
        got = rows(apitap.sql(q2, sources=src))
        case("2: IN, OR and IS NOT NULL on the GitHub side", got == want, f"got {got} want {want}")

        got = rows(apitap.sql("SELECT state, COUNT(*) FROM gh.issues GROUP BY 1 ORDER BY 1", sources=src))
        want = sorted((s, sum(1 for i in ISSUES if i["state"] == s)) for s in {"open", "closed"})
        case("3: one GitHub table, GROUP BY", got == want, f"got {got} want {want}")

        try:
            apitap.read("github+api://o/r", table="issues", where="state = 'open'").to_polars()
            case("4: read(where=) on github+api is refused", False, "it read rows")
        except Exception as e:  # noqa: BLE001
            case("4: read(where=) on github+api is refused, naming the cause", "SQL server" in str(e), str(e)[:140])
    finally:
        pg(f"DROP TABLE IF EXISTS {T}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"   XX {type(e).__name__}: {e}")
        ok = False
    print("SQL GITHUB E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)
