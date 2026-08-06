"""One MySQL write burst: 1M rows into EACH of 10 tables, 10k per transaction."""
import sys
import time

import pymysql

TABLES = [f"my_cdc_t{i:02d}" for i in range(1, 11)]
PER_TABLE = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
BATCH = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000

conn = pymysql.connect(host="127.0.0.1", port=3307, user="root", password="bench", database="bench")
cur = conn.cursor()
cur.execute("SET SESSION cte_max_recursion_depth = 100000")

t0 = time.time()
txns = 0
for t in TABLES:
    for _ in range(PER_TABLE // BATCH):
        cur.execute(
            f"""INSERT INTO {t} (cust_id, payload, amount, ts)
                WITH RECURSIVE seq(n) AS (
                  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < {BATCH}
                )
                SELECT n % 100000, CONCAT('burst-', n, '-', MD5(n)),
                       (n % 100000) / 100, NOW(6)
                FROM seq"""
        )
        conn.commit()
        txns += 1
wall = time.time() - t0
cur.execute("SHOW BINARY LOGS")
binlog_mb = sum(r[1] for r in cur.fetchall()) / 1e6
print(
    f"  burst: {len(TABLES) * PER_TABLE:,} rows in {txns:,} transactions, {wall:.1f}s "
    f"({len(TABLES) * PER_TABLE / wall / 1000:.0f}K rows/s), binlogs total {binlog_mb:.0f} MB",
    flush=True,
)
