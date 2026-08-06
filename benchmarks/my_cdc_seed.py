"""Seed the MySQL CDC stress rig: 10 tables x 1M rows, 10k-row transactions."""
import sys
import time

import pymysql

TABLES = [f"my_cdc_t{i:02d}" for i in range(1, 11)]
PER_TABLE = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
BATCH = 10_000

conn = pymysql.connect(host="127.0.0.1", port=3307, user="root", password="bench", database="bench")
cur = conn.cursor()
cur.execute("SET SESSION cte_max_recursion_depth = 100000")

t0 = time.time()
for t in TABLES:
    cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.execute(
        f"""CREATE TABLE {t} (
              id       BIGINT AUTO_INCREMENT PRIMARY KEY,
              cust_id  INT           NOT NULL,
              payload  VARCHAR(120)  NOT NULL,
              amount   DECIMAL(12,2) NOT NULL,
              ts       DATETIME(6)   NOT NULL
            ) ENGINE=InnoDB"""
    )
    for _ in range(PER_TABLE // BATCH):
        cur.execute(
            f"""INSERT INTO {t} (cust_id, payload, amount, ts)
                WITH RECURSIVE seq(n) AS (
                  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < {BATCH}
                )
                SELECT n % 100000, CONCAT('row-', n, '-', MD5(n)),
                       (n % 100000) / 100, NOW(6)
                FROM seq"""
        )
        conn.commit()
print(f"seeded {len(TABLES)} tables x {PER_TABLE:,} rows in {time.time()-t0:.1f}s", flush=True)
cur.execute(f"SELECT COUNT(*) FROM {TABLES[0]}")
print("  spot check:", cur.fetchone()[0])
