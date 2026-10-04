#!/usr/bin/env python3
"""The stress writer's statement stream, one SQL file per psql session.

The question is whether a 0.5 CPU / 256 MB drain can keep up with 1,000,000
changed rows per table per minute across 30 tables in ONE CDC group, so the
writer has to (a) offer that rate from an UNCAPPED source and (b) not fill the
box. Both are met by the same change shape:

    ONE transaction = 800 UPDATEs + 100 INSERTs + 100 DELETEs = 1000 changes
      UPDATE  a FIXED 800-row band of the table's own seed, `regular_int + 1`
      INSERT  100 fresh ids from the seed's own generator expressions
      DELETE  those SAME 100 ids, in the same transaction

Three properties, each of which the arithmetic in the report depends on:

* **Row-neutral, with no lag to reconcile.** Every transaction inserts 100 rows
  and deletes 100 rows, so a table's row count is constant across a whole window —
  90,000,000 changes do NOT grow the source by 90,000,000 rows — and no fresh id
  ever survives a transaction. That last part is the reason for pairing a
  transaction's INSERT with its OWN deletes rather than with the previous
  transaction's: a lagging delete leaves the LAST transaction's 100 rows live, so
  "the rows that exist at the watermark" becomes "I_k .. I_N-1", a range whose far
  end depends on how far the writer got before it was stopped. It was implemented
  that way first, and the applied-prefix reconstruction emitted "I_0 .. I_k-1" and
  was wrong for every k except the one the control tests.
* **The update band is FIXED and every transaction re-runs the identical
  statement.** After k transactions every band row has `regular_int + k`,
  exactly. That is what makes "the source as of the drain's watermark" a closed
  form: undo the (k_now - k_at_watermark) increments with one subtraction in the
  row text, and the row SET is identical at every k — so the whole reconstruction
  is one corrected column.
* **1,000 changes per transaction** is deliberate: logical decoding hands a
  transaction over as a unit (benchmarks/cdc-stress.md), and the project's own
  measured ceiling is that 1,000,000 rows in one commit OOMs a 256 MB worker
  while 10,000-row commits ran at 87 MB. 1,000 is three orders of magnitude
  below that ceiling and one order above the CDC transaction the drain would
  naturally want.

The INSERT/DELETE pair is a real CDC change pair, not a no-op: pgoutput ships
both events and the destination has to apply both in LSN order for the row to be
absent afterwards. A drain that dropped either one shows up immediately as a
count mismatch in this window's verification.

Every transaction also prints one LEDGER line carrying the WAL LSN *after* its
commit, which is what turns "which changes had landed when the drain stopped"
into a lookup instead of a guess.

    python3 bench-capped-pg-ch-cdc-stress-writer.py SESSION_ID TABLES TXNS TICK_S BAND_BASE
"""
import sys

sess = int(sys.argv[1])
tables = [int(x) for x in sys.argv[2].split(",") if x]
ntxn = int(sys.argv[3])
tick_s = float(sys.argv[4])
# Each window gets its OWN 800-row band, 1,600 ids apart, so the two windows
# touch disjoint rows and neither one's "how many increments does this row
# carry" arithmetic depends on the other. Both bands sit inside the seed's
# untouched tail (the previous campaign moved `regular_int` on 1..900,000 and
# deleted 900,001..909,000), and both end below 1,000,000 for t=30.
band_base = int(sys.argv[5]) if len(sys.argv) > 5 else 950_001

# One fixed 800-row update band per table, inside the seed's untouched tail
# (950,001..1,000,000): the previous campaign's window moved `regular_int` on
# 1..900,000 and deleted 900,001..909,000, so a band above 950,000 is a band
# whose starting value this campaign is the only author of.
BAND_W = 800
# The graveyard: 100 rows this campaign inserts itself, before the CDC group
# exists, so transaction 0 has real rows to delete and the window is row-neutral
# from its first transaction rather than from its second.
GRD_LO = 40_000_000
# Fresh ids for INSERTs, per table, monotonically increasing, never colliding
# with the graveyard or with anything a previous campaign wrote (max id 2,044,999).
INS_LO = 50_000_000
INS_W, UPD_W, DEL_W = 100, 800, 100
TXN_CHANGES = UPD_W + INS_W + DEL_W

COLS = ("id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, "
        "float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_tz, "
        "json_val, extra_text").replace("ts_tz_tz", "ts_tz_val")

# The seed's own generator expressions (bench-capped-pg-ch-schema.sql), so a row
# this writer inserts is byte-identical in shape to a seeded one and the
# checksum later compares like with like. Substituted with str.replace and never
# str.format / printf: the text carries JSON braces and '% 97', and both of
# those read those as their own syntax.
VALS = """
       SELECT g,
              substr(md5(g::text), 1, 8),
              CASE WHEN g % 97  = 0 THEN NULL ELSE substr(md5((g * 7)::text), 1, 40) END,
              substr(md5((g * 13)::text), 1, 200),
              (g % 100)::smallint,
              g,
              g::bigint * 1000000,
              (g % 10000)::double precision / 8,
              CASE WHEN g % 89  = 0 THEN NULL ELSE ((g % 1000000)::numeric(18,4) / 100) END,
              (g % 2 = 0),
              date '2024-01-01' + (g % 900),
              timestamp '2024-01-01 00:00:00' + (g % 1000000) * interval '1 microsecond',
              timestamptz '2024-01-01 00:00:00+00' + (g % 1000000) * interval '1 microsecond',
              CASE WHEN g % 83  = 0 THEN NULL
                   ELSE ('{"k":' || g || ',"s":"' || substr(md5(g::text), 1, 16) || '"}')::json END,
              CASE WHEN g % 79  = 0 THEN NULL ELSE 'extra-' || substr(md5((g * 3)::text), 1, 60) END
       FROM generate_series(__A__, __B__) AS g"""

# The same expressions as ONE row, for the graveyard block the harness inserts
# before the CDC group is established.
ROW1 = ("""(%(g)s, substr(md5(%(g)s::text),1,8),
        CASE WHEN %(g)s %% 97 = 0 THEN NULL ELSE substr(md5((%(g)s*7)::text),1,40) END,
        substr(md5((%(g)s*13)::text),1,200), (%(g)s %% 100)::smallint, %(g)s,
        %(g)s::bigint*1000000, (%(g)s %% 10000)::double precision/8,
        CASE WHEN %(g)s %% 89 = 0 THEN NULL ELSE ((%(g)s %% 1000000)::numeric(18,4)/100) END,
        (%(g)s %% 2 = 0), date '2024-01-01' + (%(g)s %% 900),
        timestamp '2024-01-01' + (%(g)s %% 1000000) * interval '1 microsecond',
        timestamptz '2024-01-01 00:00:00+00' + (%(g)s %% 1000000) * interval '1 microsecond',
        CASE WHEN %(g)s %% 83 = 0 THEN NULL
             ELSE ('{"k":' || %(g)s || ',"s":"' || substr(md5(%(g)s::text),1,16) || '"}')::json END,
        CASE WHEN %(g)s %% 79 = 0 THEN NULL ELSE 'extra-' || substr(md5((%(g)s*3)::text),1,60) END)""")

# Emitted when this file is run with mode `graveyard`, which the harness calls
# once per table before the CDC group exists.
if len(sys.argv) > 6 and sys.argv[6] == "graveyard":
    vals = []
    for t in tables:
        base = GRD_LO + t * 100_000
        for j in range(INS_W):
            vals.append(ROW1 % {"g": base + j})
    print(f"INSERT INTO cdc_pg_t{tables[0]:02d} ({COLS}) VALUES " + ",\n".join(vals) + ";")
    sys.exit(0)

out = sys.stdout
out.write(f"-- stress writer session {sess}: tables {tables}, {ntxn} transactions x "
          f"{TXN_CHANGES} changes each, {tick_s}s between transactions, "
          f"band base {band_base}\n")
out.write("-- one transaction = %d UPDATEs (fixed band) + %d INSERTs (fresh ids) + "
          "%d DELETEs (the previous transaction's inserts)\n"
          % (UPD_W, INS_W, DEL_W))
out.write("SET synchronous_commit = off;\n")

k = 0
for i in range(ntxn):
    for t in tables:
        tbl = f"cdc_pg_t{t:02d}"
        # The per-TABLE transaction index, not the loop's: a session owns two
        # tables and interleaves them, and a shared counter would make each
        # table's DELETE aim at ids inserted into the OTHER table, hit zero rows,
        # and turn the window's row count into 1,036,000 + 100·transactions.
        kt = k
        k += 1
        band = band_base + (t - 1) * 1000
        ins = INS_LO + t * 1_000_000 + kt * INS_W
        vals = VALS.replace("__A__", str(ins)).replace("__B__", str(ins + INS_W - 1))
        out.write("BEGIN;\n")
        out.write(f"UPDATE {tbl} SET regular_int = regular_int + 1 "
                  f"WHERE id BETWEEN {band} AND {band + UPD_W - 1};\n")
        out.write(f"INSERT INTO {tbl} ({COLS}){vals};\n")
        out.write(f"DELETE FROM {tbl} WHERE id BETWEEN {ins} AND {ins + INS_W - 1};\n")
        out.write("COMMIT;\n")
        # One extra round trip per transaction, ~30 us, for the ledger line. It is
        # what makes "the source as of the drain's watermark" computable instead
        # of asserted, so it is bought on purpose.
        out.write("SELECT pg_current_wal_lsn() AS l \\gset\n")
        out.write(f"\\echo LEDGER s{sess} {tbl} {kt + 1} :l\n")
    if tick_s > 0 and i + 1 < ntxn:
        out.write(f"SELECT pg_sleep({tick_s});\n")
    if i % 50 == 0:
        out.write(f"\\echo WINDOW_PROGRESS session {sess} txn {i + 1}/{ntxn}\n")

out.write(f"\\echo WRITER_DONE session {sess} txns {ntxn} tables {len(tables)} "
          f"changes {ntxn * len(tables) * TXN_CHANGES}\n")