"""GROUP BY and DISTINCT on text agree with the source column's collation.

A MySQL `utf8mb4_0900_ai_ci` column holds 'a', 'A' and 'b': MySQL's GROUP BY
and DISTINCT answer two rows, not three. A Postgres `citext` column likewise.
Through 0.67.2 GROUP BY folded the MySQL `_ci` groups already, but DISTINCT
(no aggregate) was refused, and a `citext` column grouped by bytes: three rows
where Postgres has two, with no error (W1 round P). A `_bin` / `text` column
keeps 'a' and 'A' apart, as its server does.

Each case compares the number of rows and, per row, the count, against the
server's own answer (values compared case-folded: which spelling of a folded
group comes back is the server's choice).

Usage: e2e_sql_collation.py
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}
VALS = ["a", "A", "b", "a", "B", "c", "A"]


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l for l in o.stdout.splitlines() if l]


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "--default-character-set=utf8mb4",
                        "-N", "-B", "bench", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l.replace("\t", "|") for l in o.stdout.splitlines() if l]


def norm(rows):
    return sorted("|".join(str(x).lower() for x in r) if isinstance(r, tuple) else r.lower() for r in rows)


def wipe():
    pg("DROP TABLE IF EXISTS coll_ci; DROP TABLE IF EXISTS coll_cs")
    my("DROP TABLE IF EXISTS coll_ci; DROP TABLE IF EXISTS coll_cs; DROP TABLE IF EXISTS coll_gen; DROP TABLE IF EXISTS coll_gena")


# A MySQL collation from before 0900 (general_ci): its weights are not 0900's
# ('Æ' is its own letter, not 'ae'; 'ß' = 's'). apitap folds only ASCII
# there: a GROUP BY or a compare that meets other text is refused by name;
# an ASCII-only column answers exactly (QA C10, W1 round P).
GEN = ["e", "é", "E", "ae", "Æ", "æ", "ss", "ß", "s", "b"]
GEN_CASES = [
    ("general_ci GROUP BY, non-ASCII", "SELECT s, COUNT(*) AS n FROM my.coll_gen GROUP BY s", "SELECT s, COUNT(*) FROM coll_gen GROUP BY s"),
    ("general_ci DISTINCT, non-ASCII", "SELECT DISTINCT s FROM my.coll_gen", "SELECT DISTINCT s FROM coll_gen"),
    ("general_ci stage WHERE = 'ae'", "WITH c AS (SELECT id, s FROM my.coll_gen) SELECT COUNT(*) AS n FROM c WHERE s = 'ae'",
     "SELECT COUNT(*) FROM coll_gen WHERE s = 'ae'"),
    ("general_ci stage WHERE = 's'", "WITH c AS (SELECT id, s FROM my.coll_gen) SELECT COUNT(*) AS n FROM c WHERE s = 's'",
     "SELECT COUNT(*) FROM coll_gen WHERE s = 's'"),
    ("general_ci ASCII-only GROUP BY", "SELECT s, COUNT(*) AS n FROM my.coll_gena GROUP BY s", "SELECT s, COUNT(*) FROM coll_gena GROUP BY s"),
]


def main():
    wipe()
    vals = ", ".join(f"({i}, '{v}')" for i, v in enumerate(VALS))
    pg("CREATE EXTENSION IF NOT EXISTS citext;"
       f"CREATE TABLE coll_ci (id int PRIMARY KEY, s citext); INSERT INTO coll_ci VALUES {vals};"
       f"CREATE TABLE coll_cs (id int PRIMARY KEY, s text); INSERT INTO coll_cs VALUES {vals}")
    my(f"CREATE TABLE coll_ci (id int PRIMARY KEY, s varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci);"
       f"INSERT INTO coll_ci VALUES {vals};"
       f"CREATE TABLE coll_cs (id int PRIMARY KEY, s varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);"
       f"INSERT INTO coll_cs VALUES {vals}")
    gv = ", ".join(f"({i}, '{v}')" for i, v in enumerate(GEN))
    my(f"CREATE TABLE coll_gen (id int PRIMARY KEY, s varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci);"
       f"INSERT INTO coll_gen VALUES {gv};"
       f"CREATE TABLE coll_gena (id int PRIMARY KEY, s varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci);"
       f"INSERT INTO coll_gena VALUES {vals}")
    ok = True
    for kind, q, truth in GEN_CASES:
        want = norm(my(truth))
        try:
            got = norm(apitap.sql(q, sources=SRC).to_polars().rows())
            good, why = got == want, f"{got} (server {want})"
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            good = "before 0900" in msg and "ASCII" in msg
            why = f"refused: {msg[:160]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {kind}: {why}", flush=True)
    # Two statements at once: the one that meets non-ASCII under general_ci
    # is refused, the ASCII-only one beside it answers (the mark is the
    # statement's own).
    import threading
    qa = "WITH c AS (SELECT id, s FROM my.coll_gen) SELECT COUNT(*) AS n FROM c WHERE s = 'b'"
    qb = "WITH c AS (SELECT id, s FROM my.coll_gena) SELECT COUNT(*) AS n FROM c WHERE s = 'b'"
    wb = my("SELECT COUNT(*) FROM coll_gena WHERE s = 'b'")[0]
    res = {"a": [], "b": []}

    def loop(k, q):
        for _ in range(6):
            try:
                res[k].append(str(apitap.sql(q, sources=SRC).to_polars().rows()[0][0]))
            except Exception as e:  # noqa: BLE001
                res[k].append("ERR " + str(e)[:80])
    ts = [threading.Thread(target=loop, args=("a", qa)), threading.Thread(target=loop, args=("b", qb))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    good = all(x.startswith("ERR") and "not ASCII" in x for x in res["a"]) and all(x == wb for x in res["b"])
    ok &= good
    print(f"  {'ok ' if good else 'BAD'} concurrent: non-ASCII statement {res['a'][:2]}, ASCII-only beside it {res['b']} (want {wb})", flush=True)
    for side, run in (("my", my), ("pg", pg)):
        for t in ("coll_ci", "coll_cs"):
            for kind, q, truth in (
                ("GROUP BY", f"SELECT s, COUNT(*) AS n FROM {side}.{t} GROUP BY s", f"SELECT s, COUNT(*) FROM {t} GROUP BY s"),
                ("DISTINCT", f"SELECT DISTINCT s FROM {side}.{t}", f"SELECT DISTINCT s FROM {t}"),
            ):
                want = norm(run(truth))
                try:
                    got = norm(apitap.sql(q, sources=SRC).to_polars().rows())
                    good, why = got == want, f"{len(got)} rows {got} (server {len(want)}: {want})"
                except Exception as e:  # noqa: BLE001
                    good, why = False, f"error: {str(e)[:160]}"
                ok &= good
                print(f"  {'ok ' if good else 'BAD'} {side}.{t} {kind}: {why}", flush=True)
    wipe()
    print("SQL COLLATION E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
