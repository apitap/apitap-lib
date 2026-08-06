"""One write burst: 1M fresh rows into EACH of the 10 tables (10M changes)."""
import sys
import time

import psycopg2

TABLES = [f"cdc_t{i:02d}" for i in range(1, 11)]
PER_TABLE = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
# Transaction size matters to a CDC reader far more than row count: logical
# decoding hands over a transaction as a UNIT, so one 1M-row INSERT is one
# ~200 MB window no budget can shrink. Real writers commit in batches; 10k
# rows per commit is still coarse for OLTP and bounds the window at ~1.5 MB.
BATCH = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000

conn = psycopg2.connect("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
cur = conn.cursor()
t0 = time.time()
txns = 0
for t in TABLES:
    for lo in range(1, PER_TABLE + 1, BATCH):
        hi = min(lo + BATCH - 1, PER_TABLE)
        cur.execute(
            f"""INSERT INTO {t} (cust_id, payload, amount, ts)
                SELECT (g % 100000)::int,
                       'burst-' || g || '-' || md5(g::text),
                       (g % 100000)::numeric / 100,
                       now()
                FROM generate_series({lo}, {hi}) g"""
        )
        conn.commit()
        txns += 1
wall = time.time() - t0
cur.execute("SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots LIMIT 1")
lag = cur.fetchone()
print(
    f"  burst: {len(TABLES) * PER_TABLE:,} rows in {txns:,} transactions, {wall:.1f}s "
    f"({len(TABLES) * PER_TABLE / wall / 1000:.0f}K rows/s)"
    + (f", slot holds {lag[0]} of WAL" if lag else ""),
    flush=True,
)
