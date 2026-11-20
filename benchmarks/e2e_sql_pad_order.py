"""Padded text (Postgres bpchar, MySQL CHAR) compares as its own server does, below a space too.

QA (p8): MIN of a Postgres char(5) COLLATE "C" column answered E'\\t' where
Postgres answers ''. The two servers differ on padded text: Postgres's bpchar
strips trailing spaces and then compares bytes ('' < E'\\t', 'a' < E'a\\t');
MySQL's PAD SPACE pads the shorter value with spaces (E'a\\t' < 'a'). And a
trailing tab is not padding on either (E'a\\t' <> 'a'), where apitap's GROUP BY
fold trimmed tabs too. p10 applies each column's own server's rule wherever
padded text is compared or ordered: MIN/MAX, ORDER BY (and LIMIT), GROUP BY
and DISTINCT, WHERE.

Every case is compared with the server's own answer, the text as hex with
trailing spaces dropped on both sides (bpchar prints them, MySQL CHAR does not).

Usage: e2e_sql_pad_order.py
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
SRC = {"pg": PG, "my": MY}
VALS = [(1, 1, ""), (2, 1, "\t"), (3, 2, "a"), (4, 2, "a\t"), (5, 2, "a "), (6, 3, "b"), (7, 3, None), (8, 1, "a\t")]


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.split("\n")[:-1] if o.stdout else []


def my(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "--default-character-set=utf8mb4", "-uroot", "-pbench",
                        "-N", "-B", "--raw", "bench", "-e", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return ["|".join("" if c == "NULL" else c for c in l.split("\t")) for l in (o.stdout.split("\n")[:-1] if o.stdout else [])]


def lit_pg(v):
    return "NULL" if v is None else "E'" + v.replace("\t", "\\t") + "'"


def lit_my(v):
    return "NULL" if v is None else "'" + v.replace("\t", "\\t") + "'"


def main():
    pg("DROP TABLE IF EXISTS pad_t; CREATE TABLE pad_t (id int PRIMARY KEY, g int, s char(5) COLLATE \"C\"); "
       f"INSERT INTO pad_t VALUES {', '.join(f'({i}, {g}, {lit_pg(v)})' for i, g, v in VALS)}")
    my("DROP TABLE IF EXISTS pad_t; CREATE TABLE pad_t (id int PRIMARY KEY, g int, s char(5) COLLATE utf8mb4_bin) "
       f"CHARACTER SET utf8mb4; INSERT INTO pad_t VALUES {', '.join(f'({i}, {g}, {lit_my(v)})' for i, g, v in VALS)}")
    # The text goes back as hex (a tab inside a value would split the client's
    # rows); trailing spaces dropped first on both sides ({h} is the server's
    # hex of the column: bpchar::text and MySQL CHAR already drop them).
    H = {"pg": "encode(convert_to(rtrim({c}::text, ' '), 'UTF8'), 'hex')", "my": "LOWER(HEX(TRIM(TRAILING ' ' FROM {c})))"}
    cases = [
        ("MIN/MAX per group", "SELECT g, MIN(s) AS lo, MAX(s) AS hi FROM {s}.pad_t GROUP BY g ORDER BY g",
         "SELECT g, {lo}, {hi} FROM pad_t GROUP BY g ORDER BY g", {"lo": "MIN(s)", "hi": "MAX(s)"}),
        ("MIN/MAX overall", "SELECT MIN(s) AS lo, MAX(s) AS hi FROM {s}.pad_t", "SELECT {lo}, {hi} FROM pad_t",
         {"lo": "MIN(s)", "hi": "MAX(s)"}),
        ("ORDER BY s, id", "SELECT id, s FROM {s}.pad_t ORDER BY s, id", "SELECT id, {x} FROM pad_t ORDER BY s, id", {"x": "s"}),
        ("ORDER BY s DESC LIMIT", "SELECT id, s FROM {s}.pad_t ORDER BY s DESC, id LIMIT 4",
         "SELECT id, {x} FROM pad_t ORDER BY s DESC, id LIMIT 4", {"x": "s"}),
        ("GROUP BY s count", "SELECT COUNT(*) AS n, s FROM {s}.pad_t GROUP BY s ORDER BY s",
         "SELECT COUNT(*), {x} FROM pad_t GROUP BY s ORDER BY s", {"x": "s"}),
        ("DISTINCT s", "SELECT DISTINCT s FROM {s}.pad_t ORDER BY s", "SELECT {x} FROM (SELECT DISTINCT s FROM pad_t) d ORDER BY s",
         {"x": "s"}),
        ("WHERE s = 'a'", "SELECT id FROM {s}.pad_t WHERE s = 'a' ORDER BY id", "SELECT id FROM pad_t WHERE s = 'a' ORDER BY id", {}),
    ]

    def hexed(v):
        return "" if v is None else v.rstrip(" ").encode().hex()

    ok = True
    for src, srv in (("pg", pg), ("my", my)):
        for name, q, truth, cols in cases:
            try:
                t = truth.format(**{k: H[src].format(c=c) for k, c in cols.items()})
                want = srv(t)
                df = apitap.sql(q.format(s=src), sources=SRC).to_polars()
                text = [i for i, c in enumerate(df.columns) if c in ("lo", "hi", "s")]
                got = ["|".join(hexed(x) if i in text else ("" if x is None else str(x)) for i, x in enumerate(r)) for r in df.rows()]
                good, why = got == want, f"{got} (server {want})"
            except Exception as e:  # noqa: BLE001
                good, why = False, f"error: {str(e)[:200]}"
            ok &= good
            print(f"  {'ok ' if good else 'BAD'} {src} {name}: {why}", flush=True)
    pg("DROP TABLE IF EXISTS pad_t")
    my("DROP TABLE IF EXISTS pad_t")
    print("SQL PAD ORDER E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
