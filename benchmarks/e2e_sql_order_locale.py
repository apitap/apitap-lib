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
  11 a musl-built server (the rig's Alpine Postgres): the premise that its
     default libc collation orders by bytes, checked against the server; a
     300K-row ORDER BY s LIMIT 6 under it past an 8 MiB group budget answers
     (RED p11: refused after the read); the same under und-x-icu with an
     integer column selected answers exactly through the parts path and the
     oracle (11c, RED p12: refused before the read); under und-x-icu with
     text columns only (no column to part by) it is refused before the read,
     naming the collation by its human name (11d); with an integer column the
     parts path cannot split by (two distinct values, 11e; 90% NULL, 11f) it
     is refused before the read too, naming that column; with statistics too
     stale for the up-front check (11g) it falls to the parts path and answers;
     where even 64 parts do not fit (11h) the refusal names the collation and
     the parts tried

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
    # 10: a plain ORDER BY on a pre-0900 _ci column (utf8mb4_general_ci, PAD
    # SPACE) with non-ASCII values (QA C10o, p10): p10 answered in byte order
    # (a:44, fi:1, Z:27, ä:29 where MySQL gives ä, a, fi, Z) because its PAD
    # SPACE key skipped the pair guard. Equal to MySQL or refused by name;
    # never another order. ASC, DESC LIMIT, and a bigger mixed set.
    gv = [(1, "fi"), (29, "\u00e4"), (44, "a"), (27, "Z"), (2, "b "), (3, "B"), (4, "\u00c4b"), (5, "ab"), (6, "a b"), (7, "z")]
    mysql("apitap-bench-my", "mysql", "DROP TABLE IF EXISTS ord_gc; CREATE TABLE ord_gc (id int PRIMARY KEY, "
          "tg varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci) CHARACTER SET utf8mb4; INSERT INTO ord_gc VALUES "
          + ", ".join(f"({i}, '{v}')" for i, v in gv))
    for label, q, tq in (
        ("4 rows ORDER BY tg, id", "SELECT tg, id FROM my.ord_gc WHERE id IN (1, 29, 44, 27) ORDER BY tg, id",
         "SELECT tg, id FROM ord_gc WHERE id IN (1, 29, 44, 27) ORDER BY tg, id"),
        ("all ORDER BY tg, id", "SELECT tg, id FROM my.ord_gc ORDER BY tg, id", "SELECT tg, id FROM ord_gc ORDER BY tg, id"),
        ("ORDER BY tg DESC, id LIMIT 5", "SELECT tg, id FROM my.ord_gc ORDER BY tg DESC, id LIMIT 5",
         "SELECT tg, id FROM ord_gc ORDER BY tg DESC, id LIMIT 5"),
    ):
        try:
            got = ["|".join(str(x) for x in r) for r in apitap.sql(q, sources=SRC).to_polars().rows()]
            want = [l.replace("\t", "|") for l in mysql("apitap-bench-my", "mysql", tq)]
            good, why = got == want, f"{got} (server {want})"
        except Exception as e:  # noqa: BLE001
            good, why = "may order" in str(e) or "not ASCII" in str(e), f"refused: {str(e)[:160]}"
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} 10 my general_ci {label}: {why}", flush=True)
    mysql("apitap-bench-my", "mysql", "DROP TABLE IF EXISTS ord_gc")
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
            # id last: 'B' and 'b' tie under 0900_ai_ci, and a tie may
            # come back either way round (a flake, not an answer: p8 and p9).
            ("mixed ASC/DESC", "SELECT m, t, id FROM {s}.ord_desc ORDER BY m, t DESC, id"),
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
    # 11 (round P p12, QA L8d/L8d2): a libc collation on a musl-built server
    # orders by bytes (musl's strcoll is strcmp), so ORDER BY … LIMIT streams
    # its top N with no guard. 11a checks the rule's premise against this
    # server; 11b is QA's shape past an 8 MiB group budget (p11: refused after
    # the whole read); 11c the same table under ICU, which keeps the guard:
    # with an integer column selected the parts path answers it (p11 did; p12
    # refused it before the read, round P p13); 11d selects text only, so no
    # path can hold it and it is refused before the read, from the server's
    # statistics.
    musl = "-linux-musl" in pg("SELECT version()")[0]
    if musl:
        prem = pg("SELECT ('B' < 'a')::text || '|' || ('a b' < 'a-b')::text || '|' || ('Zed' < 'ab')::text || '|' || datcollate "
                  "FROM pg_database WHERE datname = current_database()")[0]
        good = prem.startswith("true|true|true|") and not prem.endswith(("|C", "|POSIX"))
        ok &= good
        print(f"  {'ok ' if good else 'BAD'} 11a premise: the server is musl and its default collation orders by bytes: {prem}", flush=True)
        pg("DROP TABLE IF EXISTS ord_big, ord_big_icu; CREATE TABLE ord_big (id int, s text); "
           "INSERT INTO ord_big SELECT i, md5(i::text) || ' ' || (i % 7) FROM generate_series(1, 300000) i; "
           'CREATE TABLE ord_big_icu (id int, s text COLLATE "und-x-icu"); INSERT INTO ord_big_icu SELECT * FROM ord_big; '
           'CREATE TABLE ord_big_icu2 (s text COLLATE "und-x-icu", flag int, nid int); '
           "INSERT INTO ord_big_icu2 SELECT s, id % 2, CASE WHEN id % 10 = 0 THEN id END FROM ord_big; "
           "ANALYZE ord_big; ANALYZE ord_big_icu; ANALYZE ord_big_icu2; "
           # 11g: statistics taken at 1000 rows, autovacuum off (QA G7)
           'CREATE TABLE ord_big_stale (id int, s text COLLATE "und-x-icu") WITH (autovacuum_enabled = false); '
           "INSERT INTO ord_big_stale SELECT * FROM ord_big WHERE id <= 1000; ANALYZE ord_big_stale; "
           "INSERT INTO ord_big_stale SELECT * FROM ord_big WHERE id > 1000")
        import os
        old_gb = os.environ.get("APITAP_SQL_GROUP_BYTES")
        os.environ["APITAP_SQL_GROUP_BYTES"] = str(8 << 20)
        try:
            case("11b musl default collation, 300K rows, ORDER BY s LIMIT 6 (8 MiB budget)",
                 "SELECT s, id FROM pg.ord_big ORDER BY s LIMIT 6", "SELECT s, id FROM ord_big ORDER BY s LIMIT 6")
            case("11b2 the same, ORDER BY s DESC LIMIT 6", "SELECT s, id FROM pg.ord_big ORDER BY s DESC LIMIT 6",
                 "SELECT s, id FROM ord_big ORDER BY s DESC LIMIT 6")
            import time
            t0 = time.time()
            case("11c und-x-icu, 300K rows, an integer column selected, ORDER BY s LIMIT 6: answered (parts path + oracle)",
                 "SELECT s, id FROM pg.ord_big_icu ORDER BY s LIMIT 6", "SELECT s, id FROM ord_big_icu ORDER BY s LIMIT 6")
            print(f"     11c in {time.time() - t0:.1f} s", flush=True)
            t0 = time.time()
            case("11d und-x-icu, 300K rows, text only, ORDER BY s LIMIT 6: refused before the read, human collation name",
                 "SELECT s FROM pg.ord_big_icu ORDER BY s LIMIT 6", refused=["before reading", "und-x-icu", "ORDER BY s"])
            print(f"     11d refused in {time.time() - t0:.1f} s", flush=True)
            # 11e/11f (architect, p13): an integer column the parts path cannot
            # split by (two distinct values; 90% NULL, all in part 0): no path
            # holds it, so it is refused before the read, naming the column.
            for lab, col in (("11e two distinct values", "flag"), ("11f 90% NULL", "nid")):
                t0 = time.time()
                case(f"{lab}: und-x-icu, 300K rows, ORDER BY s LIMIT 6 with {col}: refused before the read, the column named",
                     f"SELECT s, {col} FROM pg.ord_big_icu2 ORDER BY s LIMIT 6", refused=["before reading", "und-x-icu", f"({col}:"])
                print(f"     {lab} refused in {time.time() - t0:.1f} s", flush=True)
            # 11g (QA G7): the same shape as 11c with statistics too stale for
            # the up-front check: the streaming read passes the budget and the
            # statement falls to the parts path (p12: refused after the read,
            # "rows so far hold …", the parts path never tried).
            t0 = time.time()
            case("11g und-x-icu, stale statistics (1000 rows of 300K), an integer column, ORDER BY s LIMIT 6: answered (parts path)",
                 "SELECT s, id FROM pg.ord_big_stale ORDER BY s LIMIT 6", "SELECT s, id FROM ord_big_stale ORDER BY s LIMIT 6")
            print(f"     11g in {time.time() - t0:.1f} s", flush=True)
            # 11h (architect, p13): the 11g shape at a budget so small that
            # even 64 parts by id each pass it: the refusal keeps the
            # collation's explanation and says what the parts did, never the
            # bare "groups past their memory budget".
            os.environ["APITAP_SQL_GROUP_BYTES"] = str(256 << 10)
            t0 = time.time()
            case("11h the 11g shape at a 256 KiB budget: refused, collation named and the parts' failure said",
                 "SELECT s, id FROM pg.ord_big_stale ORDER BY s LIMIT 6", refused=["does not order by bytes", "und-x-icu", "into 64 parts", 'COLLATE "C"'])
            print(f"     11h refused in {time.time() - t0:.1f} s", flush=True)
            os.environ["APITAP_SQL_GROUP_BYTES"] = str(8 << 20)
        finally:
            if old_gb is None:
                os.environ.pop("APITAP_SQL_GROUP_BYTES", None)
            else:
                os.environ["APITAP_SQL_GROUP_BYTES"] = old_gb
            pg("DROP TABLE IF EXISTS ord_big, ord_big_icu, ord_big_icu2, ord_big_stale")
    else:
        print("  -- 11 skipped: this Postgres is not built against musl", flush=True)
    wipe()
    print("SQL ORDER LOCALE E2E: " + ("PASSED" if ok else "FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
