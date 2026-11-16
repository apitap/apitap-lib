"""ORDER BY on text under a locale answers the source's order, or is refused by name.

apitap sorts text by its bytes. A Postgres column under an ICU or glibc
locale orders 'a' < 'A-D' < 'AC' < 'B' ('und-x-icu'), bytes order
'A-D' < 'AC' < 'B' < 'a': `ORDER BY s LIMIT 2` returned other rows than the
source, with no error, through the round-P p2 wheel. The sort is now checked
pair by pair (sql/order.rs): a pair the collation may order otherwise is
refused, naming the column, the collation and the pair; a column whose
values the check can vouch for (uppercase codes, digits) still answers, in
the source's order. A column under a nondeterministic collation (equality
by the collation's rules) is refused by name when the statement names it.

Cases (each against Postgres's own answer):
  1  mixed case and punctuation under und-x-icu, ORDER BY s LIMIT 2: refused
     by name (RED before: answered other rows)
  2  the same with ORDER BY s DESC (no LIMIT): refused by name
  3  uppercase codes and numbers under und-x-icu, ORDER BY s LIMIT 3, and
     ORDER BY n, s: answered, equal to Postgres
  4  the same codes under COLLATE "C": answered (never checked)
  5  citext, ORDER BY s: answered (case-folded order; ties may differ, so
     compared case-folded)
  6  a nondeterministic case-insensitive collation, GROUP BY s: refused by
     name (RED before: grouped by bytes, more groups than Postgres)

Usage: e2e_sql_order_locale.py
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
MA = "mysql://root:bench@127.0.0.1:3309/bench"
SRC = {"pg": PG, "my": MY, "ma": MA}
NAMES = [" a", "1-0", "a b", "ab", "A-1", "Zed", "zed", "10", "b", "B"]


def mysql(container, cli, sql):
    o = subprocess.run(["docker", "exec", container, cli, "-uroot", "-pbench", "--default-character-set=utf8mb4", "-N", "-B",
                        "bench", "-e", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l for l in o.stdout.splitlines() if l]


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                        "-v", "ON_ERROR_STOP=1", "-AtF", "|", "-c", sql], capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return [l for l in o.stdout.splitlines() if l]


def wipe():
    pg("DROP TABLE IF EXISTS ord_def; DROP TABLE IF EXISTS ord_icu; DROP TABLE IF EXISTS ord_codes; DROP TABLE IF EXISTS ord_cs; "
       "DROP TABLE IF EXISTS ord_cit; DROP TABLE IF EXISTS ord_nd; DROP COLLATION IF EXISTS ord_ci_nd")


def rows(df):
    return ["|".join("" if v is None else str(v) for v in r) for r in df.rows()]


def main():
    wipe()
    mixed = ["a", "B", "AC", "A-D", "b", "é", "A"]
    codes = ["MAIL", "AIR", "FOB", "RAIL", "SHIP", "TRUCK", "REG AIR", "AIR"]
    pg("CREATE EXTENSION IF NOT EXISTS citext;"
       'CREATE TABLE ord_icu (id int, s text COLLATE "und-x-icu");'
       f"INSERT INTO ord_icu VALUES {', '.join(f'({i}, {chr(39)}{v}{chr(39)})' for i, v in enumerate(mixed))};"
       'CREATE TABLE ord_codes (id int, n int, s text COLLATE "und-x-icu");'
       f"INSERT INTO ord_codes VALUES {', '.join(f'({i}, {i % 3}, {chr(39)}{v}{chr(39)})' for i, v in enumerate(codes))};"
       'CREATE TABLE ord_cs (id int, s text COLLATE "C"); INSERT INTO ord_cs SELECT id, s FROM ord_icu;'
       "CREATE TABLE ord_cit (id int, s citext);"
       "INSERT INTO ord_cit VALUES (1, 'b'), (2, 'A'), (3, 'c'), (4, 'B'), (5, 'a');"
       "CREATE TABLE ord_def (id int, s text);"
       "INSERT INTO ord_def VALUES (1, ' a'), (2, '1-0'), (3, 'a b'), (4, 'ab'), (5, 'A-1'), (6, 'Zed'), (7, 'zed'), (8, '10');"
       "CREATE COLLATION ord_ci_nd (provider = icu, locale = 'und-u-ks-level2', deterministic = false);"
       'CREATE TABLE ord_nd (id int, s text COLLATE ord_ci_nd);'
       "INSERT INTO ord_nd VALUES (1, 'a'), (2, 'A'), (3, 'b');")
    ok = True

    def case(label, q, truth=None, refused=None, fold=False):
        nonlocal ok
        try:
            got = rows(apitap.sql(q, sources=SRC).to_polars())
            err = None
        except Exception as e:  # noqa: BLE001
            got, err = None, str(e)
        if refused:
            good = err is not None and all(x in err for x in refused)
            why = f"refused: {err[:220]}" if err else f"ANSWERED {got}"
        else:
            want = pg(truth)
            if fold and got is not None:
                good = [x.lower() for x in got] == [x.lower() for x in want]
            else:
                good = got == want
            why = f"error: {err[:200]}" if err else f"{got} (server {want})"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} {label}: {why}", flush=True)

    case("1 und-x-icu mixed, ORDER BY s LIMIT 2", "SELECT s FROM pg.ord_icu ORDER BY s LIMIT 2",
         refused=["ORDER BY s", "und-x-icu", "COLLATE \"C\""])
    case("2 und-x-icu mixed, ORDER BY s DESC", "SELECT s FROM pg.ord_icu ORDER BY s DESC", refused=["ORDER BY s", "und-x-icu"])
    case("3a codes, ORDER BY s LIMIT 3", "SELECT s FROM pg.ord_codes ORDER BY s LIMIT 3", "SELECT s FROM ord_codes ORDER BY s LIMIT 3")
    case("3b codes, ORDER BY n, s", "SELECT n, s FROM pg.ord_codes ORDER BY n, s", "SELECT n, s FROM ord_codes ORDER BY n, s")
    case("3c codes, GROUP BY s ORDER BY s DESC", "SELECT s, COUNT(*) AS c FROM pg.ord_codes GROUP BY s ORDER BY s DESC",
         "SELECT s, COUNT(*) FROM ord_codes GROUP BY s ORDER BY s DESC")
    case("4 COLLATE C, ORDER BY s LIMIT 3", "SELECT s FROM pg.ord_cs ORDER BY s LIMIT 3", "SELECT s FROM ord_cs ORDER BY s LIMIT 3")
    case("5 citext, ORDER BY s", "SELECT s FROM pg.ord_cit ORDER BY s", "SELECT s FROM ord_cit ORDER BY s", fold=True)
    # 7: the database's default collation. On this server (Alpine/musl) its
    # en_US orders by bytes: the pairs the check cannot vouch for (spaces,
    # punctuation, case) go to the server, which agrees, so it answers (round
    # P p3 refused it, QA C1/C17). On a glibc en_US server the same pairs
    # would come back the other way and the statement would be refused.
    case("7 default collation, ORDER BY s LIMIT 5", "SELECT s FROM pg.ord_def ORDER BY s LIMIT 5",
         "SELECT s FROM ord_def ORDER BY s LIMIT 5")
    case("7b default collation, GROUP BY s ORDER BY s DESC", "SELECT s, COUNT(*) AS c FROM pg.ord_def GROUP BY s ORDER BY s DESC",
         "SELECT s, COUNT(*) FROM ord_def GROUP BY s ORDER BY s DESC")
    # 8: MySQL 8.0 and MariaDB 10.6 (no JSON_TABLE needed): a general_ci
    # column with spaces/punctuation/case; the pairs go to the server
    # (STRCMP under the column's collation): exact where it agrees.
    nv = ", ".join(f"({i}, '{v}')" for i, v in enumerate(NAMES))
    for name, cont, cli in (("my", "apitap-bench-my", "mysql"), ("ma", "apitap-bench-mariadb", "mariadb")):
        mysql(cont, cli, "DROP TABLE IF EXISTS ord_gen; CREATE TABLE ord_gen (id int PRIMARY KEY, "
              f"s varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci); INSERT INTO ord_gen VALUES {nv}")
        q = f"SELECT s, COUNT(*) AS c FROM {name}.ord_gen GROUP BY s ORDER BY s LIMIT 6"
        try:
            got = [r[0].lower() for r in apitap.sql(q, sources=SRC).to_polars().rows()]
            want = [l.split("\t")[0].lower() for l in mysql(cont, cli, "SELECT s, COUNT(*) FROM ord_gen GROUP BY s ORDER BY s LIMIT 6")]
            good, why = got == want, f"{got} (server {want})"
        except Exception as e:  # noqa: BLE001
            good, why = False, f"error: {str(e)[:200]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} 8 {name} general_ci GROUP BY s ORDER BY s LIMIT 6: {why}", flush=True)
        mysql(cont, cli, "DROP TABLE IF EXISTS ord_gen")
    # 9: DESC (QA, p6): a pair is asked of the server in ascending terms; p6
    # asked a DESC pair the wrong way round and answered x_y where MySQL's
    # 0900_ai_ci and ICU put x-y first. Every case must equal the server or
    # be refused by name; never another answer. Single key, multi-key with
    # mixed directions, LIMIT and none.
    dv = ["B", "a", "b", "C", "x-y", "x_y"]
    # m = id % 2 as a column: the mixed-direction case keys on a column (an
    # expression in the SELECT list is refused as such, L13).
    dvals = ", ".join(f"({i + 1}, {(i + 1) % 2}, '{v}')" for i, v in enumerate(dv))
    mysql("apitap-bench-my", "mysql", "DROP TABLE IF EXISTS ord_desc; CREATE TABLE ord_desc (id int PRIMARY KEY, m int, "
          f"t varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci); INSERT INTO ord_desc VALUES {dvals}")
    pg(f'DROP TABLE IF EXISTS ord_desc; CREATE TABLE ord_desc (id int, m int, t text COLLATE "und-x-icu"); INSERT INTO ord_desc VALUES {dvals}')
    for src, run_truth in (("my", lambda q: [l.replace("\t", "|") for l in mysql("apitap-bench-my", "mysql", q)]), ("pg", pg)):
        for label, q in (
            ("DESC LIMIT 1", "SELECT t, COUNT(*) AS n FROM {s}.ord_desc GROUP BY t ORDER BY t DESC LIMIT 1"),
            ("DESC", "SELECT t FROM {s}.ord_desc ORDER BY t DESC"),
            ("DESC with WHERE", "SELECT t FROM {s}.ord_desc WHERE id IN (5, 6) ORDER BY t DESC"),
            ("mixed ASC/DESC", "SELECT m, t FROM {s}.ord_desc ORDER BY m, t DESC"),
        ):
            qq = q.format(s=src)
            truth = qq.replace(f"{src}.", "")
            try:
                got = ["|".join(str(x) for x in r) for r in apitap.sql(qq, sources=SRC).to_polars().rows()]
                want = run_truth(truth)
                good, why = got == want, f"{got} (server {want})"
            except Exception as e:  # noqa: BLE001
                good, why = "may order" in str(e), f"refused: {str(e)[:150]}"
            ok &= good
            print(f"  {'ok ' if good else 'BAD'} 9 {src} {label}: {why}", flush=True)
    mysql("apitap-bench-my", "mysql", "DROP TABLE IF EXISTS ord_desc")
    pg("DROP TABLE IF EXISTS ord_desc")
    case("6 nondeterministic, GROUP BY s", "SELECT s, COUNT(*) AS c FROM pg.ord_nd GROUP BY s",
         refused=["ord_nd.s", "nondeterministic", "ord_ci_nd"])
    wipe()
    print("SQL ORDER LOCALE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
