"""Seed the CDC stress rig: 10 Postgres tables x 1M rows."""
import sys
import time

import psycopg2

TABLES = [f"cdc_t{i:02d}" for i in range(1, 11)]
PER_TABLE = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000

conn = psycopg2.connect("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
conn.autocommit = True
cur = conn.cursor()

t0 = time.time()
for t in TABLES:
    cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute(
        f"""CREATE TABLE {t} (
              id       BIGSERIAL PRIMARY KEY,
              cust_id  INT           NOT NULL,
              payload  TEXT          NOT NULL,
              amount   NUMERIC(12,2) NOT NULL,
              ts       TIMESTAMPTZ   NOT NULL
            )"""
    )
    cur.execute(
        f"""INSERT INTO {t} (cust_id, payload, amount, ts)
            SELECT (g % 100000)::int,
                   'row-' || g || '-' || md5(g::text),
                   (g % 100000)::numeric / 100,
                   timestamptz '2026-01-01 00:00:00+00' + (g % 86400) * interval '1 second'
            FROM generate_series(1, {PER_TABLE}) g"""
    )
print(f"seeded {len(TABLES)} tables x {PER_TABLE:,} rows in {time.time()-t0:.1f}s", flush=True)
cur.execute("SELECT sum(n) FROM (SELECT count(*) n FROM cdc_t01 UNION ALL SELECT count(*) FROM cdc_t10) x")
print("  spot check (t01 + t10):", cur.fetchone()[0])
