"""MIN/MAX of text: answered under a byte collation, refused by name under any other.

Through p7 every MIN/MAX of a text column was refused ("MIN and MAX run on
numbers here"), even where the source orders the column by its bytes
(Postgres COLLATE "C"/"POSIX", MySQL _bin/binary), where apitap's byte order
is the server's. Round P p8 answers those: the accumulator keeps the value,
priced in the group budget. Under a linguistic collation (Postgres en_US,
MySQL _0900_ai_ci) the order is the collation's, which apitap does not
apply: refused by name, the collation named (L12).

Cases, each against the server's own answer: GROUP BY and global, NULLs
skipped and a group of NULLs NULL, PAD SPACE (bpchar), ORDER BY the MIN …
LIMIT, the probe side of a join on one server and across two, a MySQL _ci
GROUP BY whose groups merge (their accumulators combine), the parts path
(600K groups past the 32 MiB budget in the 256m/0.5 cage), a CTE's stage column and pass-through column, and the two refusals.

Usage: e2e_sql_minmax_text.py
"""
import os
import subprocess
import tempfile
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l for l in o.stdout.splitlines() if l]


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "--default-character-set=utf8mb4", "-uroot", "-pbench",
                        "-N", "-B", "bench", "-e", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return ["|".join("" if c == "NULL" else c for c in l.split("\t")) for l in o.stdout.splitlines() if l]


WORDS = ["apple", "Banana", "banana", "Zed", "zed", "éclair", "eclair", "a-b", "ab", "a b", "10", "9", "Ärger", "ärger", "_x"]


def rows():
    out = []
    for i in range(1, 61):
        g = i % 5 + 1
        s = None if g == 5 or i % 7 == 0 else WORDS[(i * 7) % len(WORDS)]
        gc = ["abc", "ABC", "Abc", "xyz", "XYZ"][i % 5]
        out.append((i, g, i % 4 + 1, s, gc))
    return out


def lit(v):
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def setup():
    vals = ", ".join(f"({i}, {g}, {j}, {lit(s)}, {lit(s and s[:6])}, {lit(s)}, {lit(gc)})" for i, g, j, s, gc in rows())
    pg("DROP TABLE IF EXISTS mm_t, mm_d, mm_big; "
       "CREATE TABLE mm_t (id int PRIMARY KEY, g int, j int, s text COLLATE \"C\", b char(8) COLLATE \"C\", l text, gc text); "
       f"INSERT INTO mm_t VALUES {vals}; "
       "CREATE TABLE mm_d (id int PRIMARY KEY, grp text COLLATE \"C\"); INSERT INTO mm_d VALUES (1,'x'),(2,'y'),(3,'x'),(4,'z'); "
       "CREATE TABLE mm_big (id int PRIMARY KEY, g int, s text COLLATE \"C\"); "
       "INSERT INTO mm_big SELECT i, i % 600000, md5(i::text) FROM generate_series(1, 1200000) i")
    my("DROP TABLE IF EXISTS mm_t, mm_d; "
       "CREATE TABLE mm_t (id int PRIMARY KEY, g int, j int, s varchar(20) COLLATE utf8mb4_bin, b char(8) COLLATE utf8mb4_bin, "
       "l varchar(20) COLLATE utf8mb4_0900_ai_ci, gc varchar(8) COLLATE utf8mb4_0900_ai_ci) CHARACTER SET utf8mb4; "
       f"INSERT INTO mm_t VALUES {vals}; "
       "CREATE TABLE mm_d (id int PRIMARY KEY, grp varchar(4) COLLATE utf8mb4_bin) CHARACTER SET utf8mb4; "
       "INSERT INTO mm_d VALUES (1,'x'),(2,'y'),(3,'x'),(4,'z')")


def got_rows(q, env=None):
    return ["|".join("" if x is None else str(x) for x in r) for r in apitap.sql(q, sources=SRC).to_polars().rows()]


def parts_case():
    """The parts path in the 256m/0.5 cage: 600K groups of text MIN/MAX outgrow
    the 32 MiB group budget on their own (no budget lever), so the statement is
    answered in parts; every group equal to the server's."""
    q = "SELECT g, MIN(s) AS lo, MAX(s) AS hi FROM pg.mm_big GROUP BY g ORDER BY g"
    # The answer leaves the cage as a CSV written from Arrow (600K rows as
    # Python tuples would not fit beside it in 256 MiB: the harness, not apitap).
    out_dir = tempfile.mkdtemp(prefix="mm-parts-", dir=os.path.expanduser("~/w1"))
    code = ("import apitap,sys\n"
            f"df = apitap.sql({q!r}, sources={SRC!r}).to_polars()\n"
            "df.write_csv('/out/r.csv', include_header=False, separator='|')\n"
            "print('ROWS', df.height)\n")
    pkg = os.path.dirname(apitap.__file__)
    o = subprocess.run(["docker", "run", "--rm", "--name", "e2e-minmax-parts", "--network", "host", "--cpus=0.5", "--memory=256m",
                        "--memory-swap=256m", "-u", f"{os.getuid()}:{os.getgid()}", "-e", "HOME=/tmp", "-e", "APITAP_DEBUG=1",
                        "-e", "PYTHONPATH=/w/pkg:/qa/py312", "-v", os.path.expanduser("~/qa") + ":/qa:ro", "-v", f"{out_dir}:/out",
                        "-v", f"{pkg}:/w/pkg/apitap:ro", "python:3.12-slim", "python", "-c", code],
                       capture_output=True, text=True)
    want = pg("SELECT g, MIN(s), MAX(s) FROM mm_big GROUP BY g ORDER BY g")
    try:
        got = [l for l in open(os.path.join(out_dir, "r.csv")).read().splitlines() if l]
    except OSError:
        got = []
    import shutil
    shutil.rmtree(out_dir, ignore_errors=True)
    parts = [l for l in o.stderr.splitlines() if "answered in" in l and "parts" in l]
    good = o.returncode == 0 and got == want and bool(parts)
    why = f"exit {o.returncode}, {len(got)} rows, {'equal' if got == want else 'DIFFER'} to the server's {len(want)}; " + \
        (parts[0].strip() if parts else f"no parts line ({o.stderr.strip()[-300:]})")
    return good, why


def main():
    setup()
    strip = lambda rs: ["|".join(c.rstrip() for c in r.split("|")) for r in rs]  # noqa: E731  bpchar padding
    cases = [
        ("pg GROUP BY", "SELECT g, MIN(s) AS lo, MAX(s) AS hi FROM pg.mm_t GROUP BY g ORDER BY g",
         pg, "SELECT g, MIN(s), MAX(s) FROM mm_t GROUP BY g ORDER BY g", None),
        ("pg global", "SELECT MIN(s) AS lo, MAX(s) AS hi, COUNT(s) AS n FROM pg.mm_t",
         pg, "SELECT MIN(s), MAX(s), COUNT(s) FROM mm_t", None),
        ("pg bpchar PAD", "SELECT g, MIN(b) AS lo, MAX(b) AS hi FROM pg.mm_t GROUP BY g ORDER BY g",
         pg, "SELECT g, MIN(b), MAX(b) FROM mm_t GROUP BY g ORDER BY g", strip),
        ("pg ORDER BY MIN DESC LIMIT", "SELECT g, MIN(s) AS lo FROM pg.mm_t GROUP BY g ORDER BY lo DESC LIMIT 3",
         pg, "SELECT g, MIN(s) AS lo FROM mm_t GROUP BY g ORDER BY lo DESC LIMIT 3", None),
        ("pg CTE stage column", "WITH m AS (SELECT g, MIN(s) AS lo FROM pg.mm_t GROUP BY g) SELECT MIN(lo) AS a, MAX(lo) AS b FROM m",
         pg, "WITH m AS (SELECT g, MIN(s) AS lo FROM mm_t GROUP BY g) SELECT MIN(lo), MAX(lo) FROM m", None),
        ("pg CTE pass-through column", "WITH x AS (SELECT g, s FROM pg.mm_t WHERE g < 4) SELECT g, MIN(s) AS lo, MAX(s) AS hi "
         "FROM x GROUP BY g ORDER BY g",
         pg, "WITH x AS (SELECT g, s FROM mm_t WHERE g < 4) SELECT g, MIN(s), MAX(s) FROM x GROUP BY g ORDER BY g", None),
        ("pg join probe", "SELECT d.grp, MIN(t.s) AS lo, MAX(t.s) AS hi FROM pg.mm_d d JOIN pg.mm_t t ON t.j = d.id "
         "GROUP BY d.grp ORDER BY d.grp",
         pg, "SELECT d.grp, MIN(t.s), MAX(t.s) FROM mm_d d JOIN mm_t t ON t.j = d.id GROUP BY d.grp ORDER BY d.grp", None),
        ("pg x my join probe", "SELECT d.grp, MIN(t.s) AS lo, MAX(t.s) AS hi FROM pg.mm_d d JOIN my.mm_t t ON t.j = d.id "
         "GROUP BY d.grp ORDER BY d.grp",
         my, "SELECT d.grp, MIN(t.s), MAX(t.s) FROM mm_d d JOIN mm_t t ON t.j = d.id GROUP BY d.grp ORDER BY d.grp", None),
        ("my _bin GROUP BY", "SELECT g, MIN(s) AS lo, MAX(s) AS hi FROM my.mm_t GROUP BY g ORDER BY g",
         my, "SELECT g, MIN(s), MAX(s) FROM mm_t GROUP BY g ORDER BY g", None),
        ("my ORDER BY MIN LIMIT (NULL low)", "SELECT g, MIN(s) AS lo FROM my.mm_t GROUP BY g ORDER BY lo LIMIT 2",
         my, "SELECT g, MIN(s) AS lo FROM mm_t GROUP BY g ORDER BY lo LIMIT 2", None),
        ("my _ci groups combine", "SELECT gc, MIN(s) AS lo, MAX(s) AS hi, COUNT(*) AS n FROM my.mm_t GROUP BY gc ORDER BY lo",
         my, "SELECT gc, MIN(s), MAX(s), COUNT(*) FROM mm_t GROUP BY gc ORDER BY MIN(s)",
         lambda rs: ["|".join([r.split("|")[0].lower()] + r.split("|")[1:]) for r in rs]),
    ]
    ok = True
    for name, q, srv, truth, norm in cases:
        want = srv(truth)
        try:
            got = got_rows(q)
            if norm:
                got, want = norm(got), norm(want)
            good, why = got == want, f"{got} (server {want})"
        except Exception as e:  # noqa: BLE001
            good, why = False, f"error: {str(e)[:200]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why}", flush=True)
    good, why = parts_case()
    ok &= good
    print(f"  {'ok ' if good else 'BAD'} pg parts path (256m/0.5 cage, 600K groups): {why}", flush=True)
    for name, q, want in [
        ("pg en_US refused", "SELECT MIN(l) FROM pg.mm_t", "does not order text by its bytes"),
        ("my 0900_ai_ci refused", "SELECT g, MAX(l) FROM my.mm_t GROUP BY g", "does not order text by its bytes"),
    ]:
        try:
            r = got_rows(q)
            good, why = False, f"answered {r} (a linguistic collation: must be refused)"
        except Exception as e:  # noqa: BLE001
            good, why = want in str(e), f"refused: {str(e)[:220]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {name}: {why}", flush=True)
    pg("DROP TABLE IF EXISTS mm_t, mm_d, mm_big")
    my("DROP TABLE IF EXISTS mm_t, mm_d")
    print("SQL MINMAX TEXT E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
