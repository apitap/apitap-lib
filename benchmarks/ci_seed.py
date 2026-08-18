"""Seed the CI services with small tables that still exercise the real lanes."""
import os
import subprocess
import sys

PGH = os.environ.get("PGHOST", "127.0.0.1")
ROWS = int(os.environ.get("CI_ROWS", "20000"))


def psql(sql, db="apitap_ci"):
    env = dict(os.environ, PGPASSWORD="bench")
    r = subprocess.run(["psql", "-h", PGH, "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1",
                        "-Atc", sql], capture_output=True, text=True, env=env)
    if r.returncode:
        print(r.stderr, file=sys.stderr)
        raise SystemExit(1)
    return r.stdout.strip()


def mysql(sql):
    r = subprocess.run(["mysql", "-h", "127.0.0.1", "-uroot", "-pbench", "-D", "apitap_ci",
                        "-N", "-e", sql], capture_output=True, text=True)
    if r.returncode:
        print(r.stderr, file=sys.stderr)
        raise SystemExit(1)
    return r.stdout.strip()


psql("DROP TABLE IF EXISTS ci_src")
psql("""CREATE TABLE ci_src (
          id bigint PRIMARY KEY, name text, amount numeric(12,2),
          flag boolean, ts timestamptz, payload bytea, notes text)""")
psql(f"""INSERT INTO ci_src
         SELECT g, 'row-'||g, (g % 10000)::numeric/100, g %% 2 = 0,
                '2026-01-01'::timestamptz + (g || ' seconds')::interval,
                decode(lpad(to_hex(g), 8, '0'), 'hex'),
                CASE WHEN g %% 7 = 0 THEN NULL ELSE repeat('n', g %% 50) END
         FROM generate_series(1, {ROWS}) g""")
print("postgres:", psql("SELECT count(*) FROM ci_src"), "rows")

mysql("DROP TABLE IF EXISTS ci_src_my")
mysql("""CREATE TABLE ci_src_my (
           id BIGINT PRIMARY KEY, name VARCHAR(64), amount DECIMAL(12,2),
           flag TINYINT, ts TIMESTAMP NULL, notes VARCHAR(120))""")
mysql(f"""INSERT INTO ci_src_my (id, name, amount, flag, ts, notes)
          WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < {ROWS})
          SELECT n, CONCAT('row-', n), (n MOD 10000)/100, n MOD 2,
                 '2026-01-01 00:00:00', IF(n MOD 7 = 0, NULL, REPEAT('n', n MOD 50))
          FROM s""")
print("mysql:", mysql("SELECT count(*) FROM ci_src_my"), "rows")
