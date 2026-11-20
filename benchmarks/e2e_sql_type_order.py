"""apitap.sql() on types that arrive as text but do not order or compare as text.

QA's type sweep (2026-11-19, round P p19): a Postgres enum, inet, interval,
an unconstrained numeric, money, pg_lsn, oid, a domain over int, arrays,
ranges, a composite, timetz, jsonb, float NaN/-0, and MySQL ENUM, SET, TIME,
JSON were ordered, MIN/MAX-ed, grouped or DISTINCT-ed by the bytes of their
text form: silent wrong answers (MIN/MAX and DISTINCT on several of them new
in round P). p20 classifies every column by how it arrives (an allowlist):
a use the class cannot carry is refused by name before any read; an
unconstrained numeric compares as a decimal; floats follow the source
(NaN above every number, -0 = 0); a domain reads as its base type.

Per column (QA's fixtures, copied under the leg's own names): projection,
ORDER BY asc/desc, MIN, MAX, GROUP BY, DISTINCT, each judged by row ids
against the server itself (so renderings cannot hide a difference):
exact, or refused by name — never a different answer, and never an answer
where the server itself refuses. Plus:
  N  unconstrained numeric '10' vs '9' (ORDER BY, MIN/MAX, GROUP BY 1.0 =
     1.00, COUNT(DISTINCT)), and numeric(p,s) native
  W  WHERE over a CTE (evaluated by apitap): enum = label answered, enum >
     refused; numeric > 9 answered as a number; inet = and interval = refused
  B  MySQL COUNT(DISTINCT varbinary / blob / bit) (answered 0 through p19)
  D  domains: over int -> an integer column, over numeric(10,2) through two
     domains -> a decimal, over text -> text, over an enum -> refused order
  X  transfers untouched: a table with a domain over int lands pg->pg with
     the same destination type as before

RED (p19): the WRONG answers above. Usage: e2e_sql_type_order.py
"""
import os
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
S = {"pg": PG, "my": MY}
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + str(detail)[:230] if detail else ''}", flush=True)
    ok = ok and bool(good)


def srv(eng, sql, container=None):
    if eng == "pg":
        o = subprocess.run(["docker", "exec", container or "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                            "apitap_bench_dst" if container else "apitap_bench_src", "-AtF", "\x1f", "-R", "\x1d",
                            "-P", "null=\x1e", "-c", sql], capture_output=True, text=True, errors="replace")
        if o.returncode:
            return "ERR " + o.stderr.strip().split("\n")[0][:120]
        recs = o.stdout.rstrip("\n").split("\x1d") if o.stdout.strip("\n") else []
        return [[None if x == "\x1e" else x for x in r.split("\x1f")] for r in recs]
    o = subprocess.run(["docker", "exec", "apitap-bench-my", "mysql", "--default-character-set=utf8mb4", "-uroot", "-pbench",
                        "bench", "-N", "-B", "-e", sql], capture_output=True, text=True, errors="replace", stdin=subprocess.DEVNULL)
    if o.returncode:
        return "ERR " + o.stderr.strip().split("\n")[-1][:120]
    return [[None if x == "NULL" else x for x in l.split("\t")] for l in o.stdout.split("\n") if l != ""]


def run(eng, sql):
    r = srv(eng, sql)
    if isinstance(r, str):
        raise RuntimeError(f"{eng}: {r}: {sql[:200]}")
    return r


def ap(q):
    try:
        return "ok", [[None if v is None else str(v) for v in r] for r in apitap.sql(q, sources=S).to_polars().rows()]
    except Exception as e:  # noqa: BLE001
        return " ".join(str(e).split())[:240], None


def refused_by_name(st, c):
    # p20's wording ("ORDER BY on c (type) is refused: …") or an older
    # refusal that names the column ("min(c): …", "max(c) over a Binary …").
    return st.startswith("sql:") and (f" on {c} " in st or f"({c})" in st)


def ids_equal(eng, t, c, ty, v):
    """The rows whose value equals apitap's answer `v`, by the server's own equality."""
    if v is None:
        return sorted(int(r[0]) for r in run(eng, f"SELECT id FROM {t} WHERE {c} IS NULL"))
    lit = v.replace("'", "''")
    w = f"{c} = CAST('{lit}' AS {ty})" if eng == "pg" else f"{c} <=> '{lit}'"
    r = srv(eng, f"SELECT id FROM {t} WHERE {w} ORDER BY id")
    return None if isinstance(r, str) else [int(x[0]) for x in r]


def judge_col(eng, src, t, c, ty, known_proj_err=False):
    """Every use of column c: exact against the server, or refused by name."""
    rend = {}

    def ids_of(v):
        # By the server's equality; else (binary cells) by apitap's own rendering.
        by_srv = ids_equal(eng, t, c, ty, v)
        by_rend = sorted(i for i, rv in rend.items() if rv == v)
        return by_srv, by_rend

    for k, q in {
        "P": f"SELECT id, {c} FROM {src}.{t}",
        "O": f"SELECT {c}, id FROM {src}.{t} ORDER BY {c}, id",
        "OD": f"SELECT {c}, id FROM {src}.{t} ORDER BY {c} DESC, id",
        "MN": f"SELECT min({c}) AS m FROM {src}.{t}",
        "MX": f"SELECT max({c}) AS m FROM {src}.{t}",
        "G": f"SELECT {c}, count(*) AS k FROM {src}.{t} GROUP BY {c}",
        "D": f"SELECT count(*) AS k FROM (SELECT DISTINCT {c} FROM {src}.{t}) x",
    }.items():
        st, rows = ap(q)
        if k == "P":
            case(f"{eng}.{c} ({ty}) projection", rows is not None or known_proj_err, st if rows is None else f"{len(rows)} rows")
            rend = {int(r[0]): r[1] for r in rows or []}
            continue
        if k in ("O", "OD"):
            truth = srv(eng, f"SELECT id FROM {t} ORDER BY {c}{' DESC' if k == 'OD' else ''}, id")
            truth = truth if isinstance(truth, str) else [int(r[0]) for r in truth]
            got = None if rows is None else [int(r[1]) for r in rows]
        elif k in ("MN", "MX"):
            fn = "min" if k == "MN" else "max"
            truth = srv(eng, f"SELECT id FROM {t} WHERE {c} = (SELECT {fn}({c}) FROM {t}) ORDER BY id")
            truth = truth if isinstance(truth, str) else [int(r[0]) for r in truth]
            if rows is None:
                got = None
            else:
                a, b = ids_of(rows[0][0])
                got = b if b == truth else a
        elif k == "G":
            agg = "string_agg(id::text, ',' ORDER BY id)" if eng == "pg" else "GROUP_CONCAT(id ORDER BY id)"
            truth = srv(eng, f"SELECT {agg} FROM {t} GROUP BY {c}")
            truth = truth if isinstance(truth, str) else sorted(r[0] for r in truth)
            if rows is None:
                got = None
            else:
                parts = []
                for v, n in rows:
                    a, b = ids_of(v)
                    # apitap's own rendering first (binary cells); the
                    # server's equality when a group holds two spellings.
                    pick = b if len(b) == int(n) else (a or [])
                    parts.append(",".join(str(i) for i in pick) if len(pick) == int(n) else f"COUNT {n} vs {pick}")
                got = sorted(parts)
        else:
            truth = srv(eng, f"SELECT count(*) FROM (SELECT DISTINCT {c} FROM {t}) x")
            truth = truth if isinstance(truth, str) else int(truth[0][0])
            got = None if rows is None else int(rows[0][0])
        if got is None:
            good = refused_by_name(st, c) or isinstance(truth, str) or known_proj_err
            case(f"{eng}.{c} ({ty}) {k}: refused by name", good, st)
        elif isinstance(truth, str):
            case(f"{eng}.{c} ({ty}) {k}: the server refuses, so must apitap", False, f"answered {rows[:3]}; server {truth}")
        else:
            case(f"{eng}.{c} ({ty}) {k}: exact", got == truth, f"apitap {got} server {truth}")


PG_COLS = [("e", "tyo_enum"), ("ip", "inet"), ("cd", "cidr"), ("mac", "macaddr"), ("iv", "interval"), ("ttz", "timetz"),
           ("tm", "time"), ("u", "uuid"), ("js", "json"), ("jb", "jsonb"), ("ta", "text[]"), ("ia", "int[]"), ("nu", "numeric"),
           ("fl", "float8"), ("by", "bytea"), ("bo", "bool"), ("bt", "bit(4)"), ("vb", "varbit"), ("mo", "money"),
           ("lsn", "pg_lsn"), ("oi", "oid"), ("rg", "int4range"), ("cp", "tyo_comp"), ("de", "tyo_de"), ("di", "tyo_di"),
           ("dtx", "tyo_dt"), ("dn", "tyo_dn2"), ("x", "xml"), ("tsv", "tsvector"), ("pt", "point")]
MY_COLS = [("en", "enum"), ("st", "set"), ("tm", "time"), ("yr", "year"), ("bt", "bit"), ("js", "json"), ("dc", "decimal"),
           ("dbig", "decimal"), ("dt6", "datetime"), ("tsf", "timestamp"), ("bn", "binary"), ("vbn", "varbinary"), ("bl", "blob"),
           ("tb", "tinyint"), ("ub", "bigint"), ("fl", "float"), ("db", "double"), ("vc", "varchar")]


def setup():
    wipe()
    run("pg", """
CREATE TYPE tyo_enum AS ENUM ('zeta','alpha','Mid');
CREATE DOMAIN tyo_de AS tyo_enum; CREATE DOMAIN tyo_di AS int; CREATE DOMAIN tyo_dt AS text;
CREATE DOMAIN tyo_dn1 AS numeric(10,2); CREATE DOMAIN tyo_dn2 AS tyo_dn1;
CREATE TYPE tyo_comp AS (a int, b text);
CREATE TABLE tyo_ty (id int PRIMARY KEY, e tyo_enum, ip inet, cd cidr, mac macaddr, iv interval, ttz timetz, tm time, u uuid,
  js json, jb jsonb, ta text[], ia int[], nu numeric, fl float8, by bytea, bo bool, bt bit(4), vb varbit,
  mo money, lsn pg_lsn, oi oid, rg int4range, cp tyo_comp, de tyo_de, di tyo_di, dtx tyo_dt, dn tyo_dn2, x xml, tsv tsvector, pt point);
INSERT INTO tyo_ty VALUES
 (1,'zeta','10.0.0.2','10.0.0.0/8','08:00:2b:01:02:03','1 day','10:00+00','09:00','00000000-0000-0000-0000-00000000000a','{"b":1,"a":2}','{"b":1,"a":2}','{10}','{10}',1.0,'NaN','\\x00',true,B'1010',B'1','$10.00','0/10',10,'[1,10)',ROW(2,'a'),'zeta',10,'b',10.5,'<a/>','b a','(1,2)'),
 (2,'alpha','9.0.0.1','9.0.0.0/8','08-00-2b-01-02-03','24:00:00','12:00+02','10:00','00000000-0000-0000-0000-000000000009','{"a":2,"b":1}','{"a":2,"b":1}','{9}','{9}',1.00,'-0','\\xff',false,B'0101',B'10','$9.50','0/9',9,'[2,3)',ROW(10,'B'),'alpha',9,'B',9.5,'<b/>','a','(0,0)'),
 (3,'Mid','10.0.0.10','10.0.0.0/16','aa:00:00:00:00:00','1 mon','09:00+00','23:59:59.5','ffffffff-0000-0000-0000-000000000000','1','1','{a,B}','{9,1}','NaN',0,'\\x0a',NULL,B'1111',B'01','-$1.00','1/0',4000000000,'empty',ROW(9,'c'),'Mid',-1,'a',-1,'<c/>','c','(2,2)'),
 (4,'zeta','10.0.0.10/32','192.168.0.0/16','0a:00:00:00:00:00','30 days','23:00-05','00:00','0000000a-0000-0000-0000-000000000000','1.0','1.0','{a}','{}','Infinity','Infinity','\\x','t',B'0000',B'0','$100.00','0/A',1,'(,5)',ROW(2,'B'),'zeta',100,'C',100,'<a/>','b a','(1,2)'),
 (5,'Mid','::1','10.0.0.0/8','08:00:2b:01:02:04','10 days','01:00+00','24:00','00000000-0000-0000-0000-00000000000a','"x"','"x"','{}','{1,2}',-0,'-Infinity','\\x00ff','f',B'0001',B'11','$0.99','0/100',100,'[1,10)',ROW(10,'a'),'Mid',1,'_x',1,'<b/>','a b','(0,1)'),
 (6,'alpha','10.0.0.0/8','::/0','01:00:00:00:00:00','2 days','10:00+00','10:00','00000000-0000-0000-0000-000000000009','[1,2]','[1,2]','{9,1}','{10}',9.5,1.5,'\\x41',true,B'1000',B'100','$10.00','0/10',9,'[10,20)',ROW(2,'a'),'alpha',9,'-x',9,'<c/>','x','(5,5)');
CREATE TABLE tyo_num (id int PRIMARY KEY, n numeric, nd numeric(12,3));
INSERT INTO tyo_num VALUES (1,9,9),(2,10,10),(3,1.0,1),(4,1.00,1),(5,-2,-2),(6,100.5,100.5),(7,123456789012345678901234567890.5,NULL),(8,NULL,0.001);
""")
    run("my", """
SET SESSION sql_mode = '';
CREATE TABLE tyo_tym (id INT PRIMARY KEY, en ENUM('zeta','alpha','Mid'), st SET('b','a','c'), tm TIME(1), yr YEAR, bt BIT(4), js JSON,
  dc DECIMAL(10,2), dbig DECIMAL(65,30), dt6 DATETIME(6), tsf TIMESTAMP(3) NULL, bn BINARY(4), vbn VARBINARY(8), bl BLOB, tb TINYINT(1),
  ub BIGINT UNSIGNED, fl FLOAT, db DOUBLE, vc VARCHAR(10) COLLATE utf8mb4_bin);
INSERT INTO tyo_tym VALUES
 (1,'zeta','b','-01:00:00',1999,b'1010','{"b":1,"a":2}',1.00,9,'2020-01-01 00:00:00.000001','2020-01-01 00:00:00.500',0x00,0x00,0x00,1,18446744073709551615,1.5,1.5,'a '),
 (2,'alpha','a','100:00:00',2000,b'0101','{"a":2,"b":1}',-0.00,10,'2020-01-01 00:00:00.000010','2020-01-01 00:00:00.050',0x61,0x6120,0x6100,0,9223372036854775808,-0.0,-0.0,'a'),
 (3,'Mid','c','09:00:00',2155,b'1111','1',9.50,1.5,'1999-12-31 23:59:59.999999','1999-12-31 23:59:59.999',0x61202020,0x61,0x61,2,1,0.1,0.1,'B'),
 (4,'zeta','a,b','10:00:00',1901,b'0000','1.0',10.00,-2,'2020-01-01 00:00:00.000001',NULL,0xff,0xff,0xff,-1,9223372036854775807,1e30,1e300,'b'),
 (5,'Mid','b,c','-100:00:00',1999,b'0001','"x"',0.00,100.5,'1000-01-01 00:00:00.000000','2038-01-19 03:14:07.000',0x0a,0x0a,0x0a,1,0,-1.5,-1.5,'A'),
 (6,'alpha','','23:59:59.5',2000,b'1000','[1,2]',100.00,9,'9999-12-31 23:59:59.999999','2020-01-01 00:00:00.500',0x00,'',NULL,NULL,18446744073709551614,NULL,2.5,'_x');
""")


LOGROLE, LOGPW = "tyo_log", "tyo_log_pw"


def pg_reads(q):
    """(apitap's answer or error, statements of the leg's role that read tyo_ty/tyo_num data) — the server's own log
    between two markers (the role logs its statements; nothing global changes)."""
    import time
    import uuid
    url = f"postgres://{LOGROLE}:{LOGPW}@127.0.0.1:5544/apitap_bench_src"
    mark = uuid.uuid4().hex[:10]
    mk = lambda t: subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", LOGROLE, "-d", "apitap_bench_src",
                                   "-c", f"SELECT 'tyomark-{t}-{mark}'"], capture_output=True, text=True)  # noqa: E731
    mk("a")
    try:
        got = apitap.sql(q, sources={"pg": url}).to_polars().rows()
    except Exception as e:  # noqa: BLE001
        got = e
    mk("b")
    log = ""
    for _ in range(40):
        o = subprocess.run(["docker", "logs", "--tail", "20000", "apitap-bench-pg-src"], capture_output=True, text=True)
        log = o.stdout + o.stderr
        if f"tyomark-b-{mark}" in log:
            break
        mk("n")
        time.sleep(0.5)
    a, b = log.rfind(f"tyomark-a-{mark}"), log.rfind(f"tyomark-b-{mark}")
    seg = log[a:b] if 0 <= a < b else ""
    reads = [ln for ln in seg.splitlines() if ("LOG:  statement:" in ln or "LOG:  execute" in ln)
             and ('"tyo_ty"' in ln or '"tyo_num"' in ln)]
    return got, len(reads), a >= 0 and b > a


def my_reads(q):
    """MySQL: the leg user's statements naming tyo_tym outside information_schema (general_log to TABLE for the
    run, put back after)."""
    url = f"mysql://{LOGROLE}:{LOGPW}@127.0.0.1:3307/bench"
    before = run("my", "SELECT @@global.general_log, @@global.log_output")[0]
    run("my", "SET GLOBAL log_output = 'TABLE'; SET GLOBAL general_log = 'ON'")
    try:
        t0 = run("my", "SELECT NOW(6)")[0][0]
        try:
            got = apitap.sql(q, sources={"my": url}).to_polars().rows()
        except Exception as e:  # noqa: BLE001
            got = e
        rows = run("my", f"SELECT CONVERT(argument USING utf8mb4) FROM mysql.general_log WHERE event_time >= '{t0}' "
                         f"AND user_host LIKE '{LOGROLE}%' AND command_type IN ('Query', 'Execute')")
        reads = [r[0] for r in rows if r[0] and "tyo_tym" in r[0] and "information_schema" not in r[0].lower()]
    finally:
        run("my", f"SET GLOBAL general_log = '{'ON' if before[0] == '1' else 'OFF'}'; SET GLOBAL log_output = '{before[1]}'")
    return got, len(reads), True


def before_any_read():
    print("════════ R: a class refusal comes before any read (CTEs, stages, joins) ════════", flush=True)
    run("pg", f"CREATE ROLE {LOGROLE} LOGIN PASSWORD '{LOGPW}'; ALTER ROLE {LOGROLE} SET log_statement = 'all';"
              f"GRANT SELECT ON tyo_ty, tyo_num TO {LOGROLE}")
    srv("my", f"DROP USER IF EXISTS '{LOGROLE}'@'%'")
    run("my", f"CREATE USER '{LOGROLE}'@'%' IDENTIFIED BY '{LOGPW}'; GRANT SELECT ON bench.tyo_tym TO '{LOGROLE}'@'%'")
    W = "WITH w AS (SELECT e, id FROM pg.tyo_ty)"
    refused = [
        ("pg", "CTE, ORDER BY an enum", f"{W} SELECT e, id FROM w ORDER BY e, id", "e"),
        ("pg", "CTE over a CTE", f"WITH a AS (SELECT e, id FROM pg.tyo_ty), b AS (SELECT e, id FROM a) SELECT e, id FROM b ORDER BY e, id", "e"),
        ("pg", "a CTE referenced twice", f"{W} SELECT x.e, y.id FROM w x JOIN w y ON x.id = y.id ORDER BY x.e", "e"),
        ("pg", "a CTE joined to a base table", f"{W} SELECT w.e, t.id FROM w JOIN pg.tyo_num t ON w.id = t.id ORDER BY w.e", "e"),
        ("pg", "MAX over a CTE", "WITH w AS (SELECT e FROM pg.tyo_ty) SELECT max(e) AS m FROM w", "e"),
        ("pg", "GROUP BY an interval over a CTE", "WITH w AS (SELECT iv FROM pg.tyo_ty) SELECT iv, count(*) AS k FROM w GROUP BY iv", "iv"),
        ("pg", "COUNT(DISTINCT) jsonb over a CTE", "WITH w AS (SELECT jb FROM pg.tyo_ty) SELECT count(DISTINCT jb) AS k FROM w", "jb"),
        ("pg", "a literal over a CTE (enum >)", "WITH w AS (SELECT e, id FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE e > 'alpha'", "e"),
        ("pg", "a literal over a CTE (interval =)", "WITH w AS (SELECT id, iv FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE iv = '1 day'", "iv"),
        ("my", "MySQL TIME vs a literal over a CTE", "WITH w AS (SELECT id, tm FROM my.tyo_tym) SELECT count(*) AS k FROM w WHERE tm > '10:00:00'", "tm"),
        ("pg", "a literal over a CTE (inet)", "WITH w AS (SELECT id, ip FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE ip = '10.0.0.10'", "ip"),
        ("pg", "a derived table, ORDER BY an enum", "SELECT x.e FROM (SELECT e FROM pg.tyo_ty) x ORDER BY 1", "e"),
        ("my", "MySQL CTE, ORDER BY an ENUM", "WITH w AS (SELECT en, id FROM my.tyo_tym) SELECT en, id FROM w ORDER BY en, id", "en"),
        ("my", "MySQL CTE, MIN of a TIME", "WITH w AS (SELECT tm FROM my.tyo_tym) SELECT min(tm) AS m FROM w", "tm"),
    ]
    for eng, label, q, col in refused:
        got, n, seen = (pg_reads if eng == "pg" else my_reads)(q)
        case(f"R: {label}: refused by name", isinstance(got, Exception) and refused_by_name(str(got), col), str(got)[:200])
        case(f"R: {label}: before any read of the table", seen and n == 0, f"{n} reading statement(s)")
    # Next round (reports/round-p/p23c-work): the APITAP_TEST_DRY_NO_CONN lever and its case (the dry pass gets no
    # connection: the run plans, reads its stage and refuses by name, late). 0.68.0 (p22) has no lever.
    # B1: a refused statement leaves nothing behind — no destination table, no slot, no publication.
    slots0 = run("pg", "SELECT count(*) FROM pg_replication_slots")
    pubs0 = run("pg", "SELECT count(*) FROM pg_publication")
    for label, kw in [("sql(dest=…)", {}), ("an incremental view", {"mode": "log_based"})]:
        q = ("WITH w AS (SELECT e, id FROM pg.tyo_ty) SELECT e, id FROM w ORDER BY e, id" if not kw
             else "SELECT iv, count(*) AS k FROM pg.tyo_ty GROUP BY iv")
        try:
            apitap.sql(q, sources=S, dest=DST, table="tyo_refused", **kw)
            got = None
        except Exception as e:  # noqa: BLE001
            got = str(e)
        dst = srv("pg", "SELECT count(*) FROM pg_tables WHERE tablename LIKE 'tyo_refused%'", container="apitap-bench-pg-dst")
        case(f"B1: a refused {label} is refused by name", bool(got) and "is refused" in got, (got or "it RAN")[:200])
        case(f"B1: and leaves no destination table", dst == [["0"]], f"{dst}")
        case(f"B1: no replication slot, no publication",
             run("pg", "SELECT count(*) FROM pg_replication_slots") == slots0 and run("pg", "SELECT count(*) FROM pg_publication") == pubs0,
             f"slots {run('pg', 'SELECT count(*) FROM pg_replication_slots')} vs {slots0}")
    # A refusal from the dry pass releases every connection it opened (none of the role's left).
    import time as _t
    for eng, q in (("pg", "WITH w AS (SELECT e, id FROM pg.tyo_ty) SELECT e, id FROM w ORDER BY e, id"),
                   ("my", "WITH w AS (SELECT en, id FROM my.tyo_tym) SELECT en, id FROM w ORDER BY en, id")):
        (pg_reads if eng == "pg" else my_reads)(q)
        left = None
        for _ in range(10):
            left = (run("pg", f"SELECT count(*) FROM pg_stat_activity WHERE usename = '{LOGROLE}'") if eng == "pg"
                    else run("my", f"SELECT count(*) FROM information_schema.processlist WHERE user = '{LOGROLE}'"))
            if left == [["0"]]:
                break
            _t.sleep(0.3)
        case(f"R: a refused statement leaves no connection of its role open ({eng})", left == [["0"]], f"{left}")
    got, n, _ = pg_reads("WITH w AS (SELECT id, iv FROM pg.tyo_ty) SELECT id, iv FROM w ORDER BY id")
    case("R: a CTE that only projects an interval, ordered by id, still answers", isinstance(got, list) and len(got) == 6, str(got)[:120])


def wipe():
    srv("pg", "DROP TABLE IF EXISTS tyo_ty, tyo_num, tyo_dx; DROP DOMAIN IF EXISTS tyo_de, tyo_di, tyo_dt, tyo_dn2, tyo_dn1;"
              "DROP TYPE IF EXISTS tyo_comp, tyo_enum")
    srv("pg", "DROP TABLE IF EXISTS tyo_dx; DROP DOMAIN IF EXISTS tyo_di, tyo_dn2, tyo_dn1", container="apitap-bench-pg-dst")
    subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
                    "-q", "DROP TABLE IF EXISTS tyo_dx"], capture_output=True)
    srv("my", "DROP TABLE IF EXISTS tyo_tym")
    srv("pg", f"DROP OWNED BY {LOGROLE}")
    srv("pg", f"DROP ROLE IF EXISTS {LOGROLE}")
    srv("my", f"DROP USER IF EXISTS '{LOGROLE}'@'%'")


def main():
    setup()
    try:
        print(f"apitap {apitap.__version__}", flush=True)
        print("════════ Postgres: every use, exact or refused by name ════════", flush=True)
        for c, ty in PG_COLS:
            judge_col("pg", "pg", "tyo_ty", c, ty)
        print("════════ MySQL: every use, exact or refused by name ════════", flush=True)
        for c, ty in MY_COLS:
            judge_col("my", "my", "tyo_tym", c, ty)

        print("════════ N: unconstrained numeric as a number; numeric(p,s) native ════════", flush=True)
        for c, ty in (("n", "numeric"), ("nd", "numeric")):
            judge_col("pg", "pg", "tyo_num", c, ty)
        st, rows = ap("SELECT max(n) AS m, min(n) AS lo FROM pg.tyo_num")
        case("N: max(n) = 123456789012345678901234567890.5, min(n) = -2 (not by bytes: '9' > '10')",
             rows == [["123456789012345678901234567890.5", "-2"]], rows or st)
        st, rows = ap("SELECT count(DISTINCT n) AS k FROM pg.tyo_num")
        case("N: COUNT(DISTINCT n) = 6 (1.0 = 1.00)", rows == [["6"]], rows or st)
        st, rows = ap("SELECT n, id FROM pg.tyo_num WHERE id IN (3, 4) ORDER BY id")
        case("N: projection keeps the source's spelling (1.0, 1.00)", rows == [["1.0", "3"], ["1.00", "4"]], rows or st)
        st, rows = ap("SELECT sum(n) AS s FROM pg.tyo_num")
        print(f"   .. N: SUM(n) today (reported, not changed): {rows or st}", flush=True)

        print("════════ W: comparisons apitap evaluates (over a CTE) ════════", flush=True)
        for label, q, want in [
            ("enum = 'alpha' answered", "WITH w AS (SELECT id, e FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE e = 'alpha'", "SELECT count(*) FROM tyo_ty WHERE e = 'alpha'"),
            ("enum IN ('Mid','zeta') answered", "WITH w AS (SELECT id, e FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE e IN ('Mid', 'zeta')", "SELECT count(*) FROM tyo_ty WHERE e IN ('Mid', 'zeta')"),
            ("numeric > 9 answered as a number", "WITH w AS (SELECT id, n FROM pg.tyo_num) SELECT count(*) AS k FROM w WHERE n > 9", "SELECT count(*) FROM tyo_num WHERE n > 9"),
            ("numeric = 1 answered (1.0 and 1.00)", "WITH w AS (SELECT id, n FROM pg.tyo_num) SELECT count(*) AS k FROM w WHERE n = 1", "SELECT count(*) FROM tyo_num WHERE n = 1"),
        ]:
            st, rows = ap(q)
            case(f"W: {label}", rows == run("pg", want), f"apitap {rows or st} pg {run('pg', want)}")
        for label, q, col in [
            ("enum > 'alpha' refused", "WITH w AS (SELECT id, e FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE e > 'alpha'", "e"),
            ("inet = '10.0.0.10' refused", "WITH w AS (SELECT id, ip FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE ip = '10.0.0.10'", "ip"),
            ("interval = '1 day' refused", "WITH w AS (SELECT id, iv FROM pg.tyo_ty) SELECT count(*) AS k FROM w WHERE iv = '1 day'", "iv"),
        ]:
            st, rows = ap(q)
            case(f"W: {label}", rows is None and refused_by_name(st, col), rows or st)

        before_any_read()

        print("════════ B: MySQL COUNT(DISTINCT) of binary columns ════════", flush=True)
        for c in ("vbn", "bl", "bt", "bn"):
            st, rows = ap(f"SELECT count(DISTINCT {c}) AS k FROM my.tyo_tym")
            want = run("my", f"SELECT count(DISTINCT {c}) FROM tyo_tym")
            case(f"B: count(DISTINCT {c})", rows == want, f"apitap {rows or st} mysql {want}")

        print("════════ D: domains read as their base type ════════", flush=True)
        df = apitap.sql("SELECT id, di, dn, dtx FROM pg.tyo_ty", sources=S).to_polars()
        case("D: a domain over int is an integer column", str(df.schema["di"]).startswith("Int"), df.schema["di"])
        case("D: two domains over numeric(10,2) are a decimal", str(df.schema["dn"]).startswith("Decimal"), df.schema["dn"])
        case("D: a domain over text is text", str(df.schema["dtx"]) in ("String", "Utf8"), df.schema["dtx"])
        st, rows = ap("SELECT de, id FROM pg.tyo_ty ORDER BY de, id")
        case("D: a domain over an enum is refused for ORDER BY", rows is None and refused_by_name(st, "de"), st)

        print("════════ X: transfers keep the domain's own type ════════", flush=True)
        run("pg", "CREATE TABLE tyo_dx (id int PRIMARY KEY, di tyo_di, dn tyo_dn2); INSERT INTO tyo_dx VALUES (1, 10, 1.5), (2, 9, NULL)")
        # A same-engine transfer mirrors the domain by name (as through
        # 0.67.2): the destination has the domains.
        srv("pg", "CREATE DOMAIN tyo_di AS int; CREATE DOMAIN tyo_dn1 AS numeric(10,2); CREATE DOMAIN tyo_dn2 AS tyo_dn1",
            container="apitap-bench-pg-dst")
        apitap.transfer(PG, DST, table="tyo_dx", mode="replace")
        got = srv("pg", "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'tyo_dx'::regclass "
                        "AND attnum > 0 ORDER BY attnum", container="apitap-bench-pg-dst")
        rows = srv("pg", "SELECT id, di, dn FROM tyo_dx ORDER BY id", container="apitap-bench-pg-dst")
        case("X: pg->pg keeps the domains", got == [["integer"], ["tyo_di"], ["tyo_dn2"]], got)
        case("X: pg->pg lands every value", rows == [["1", "10", "1.50"], ["2", "9", None]], rows)
        apitap.transfer(PG, CH, table="tyo_dx", mode="replace")
        o = subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
                            "-q", "SELECT name, type FROM system.columns WHERE database = 'default' AND table = 'tyo_dx' ORDER BY position "
                                  "FORMAT TSV"], capture_output=True, text=True)
        o2 = subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
                             "-q", "SELECT id, di, dn FROM tyo_dx ORDER BY id FORMAT TSV"], capture_output=True, text=True)
        print(f"   .. X: pg->ch types {o.stdout.split()}", flush=True)
        case("X: pg->ch lands every value", o2.stdout.split("\n")[0].split("\t")[:2] == ["1", "10"], o2.stdout.strip() or o2.stderr[-200:])
    finally:
        wipe()
    print("SQL TYPE ORDER E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
