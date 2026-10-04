"""The CDC window's change stream for MySQL: exact statements, piped into mysql.

One generator, one mysql client session on the source, every statement its own
implicit transaction (autocommit, no BEGIN anywhere), and a `SLEEP` between ticks
so the stream is a steady ~TICK_S-paced load rather than a single burst. The file
is never written to disk — it is generated on the host and piped into
`docker exec -i … mysql -N -B`.

Counts are exact BY CONSTRUCTION, not counted afterwards, from three disjoint id
bands:

  per tick, per table   INSERT 100 rows. The rows are CLONED out of the seed's own
                              first 100*tick ids and re-inserted at +2,000,000, so
                              an inserted row is byte-for-byte the shape of a seeded
                              one apart from three integers, and no generator logic
                              has to be re-derived to get there. `id` and
                              `regular_int` become the new id (which is the seed's
                              own relationship), every other column is copied.
  UPDATE                100 rows: one contiguous band of the seed's ids 1..899,999,
                              which no other band touches, setting
                              `regular_int = regular_int + 1`.
  DELETE                20 rows: ids from 900,001 upward, disjoint from the update
                              band and from every inserted id.

So a table's row count after the window is exactly SEED_ROWS + INS*tick - DEL*tick,
which the harness asserts against the source itself and which the per-table checksum
then has to agree with in the destination.

The UPDATE changes a column that is inside the checksum's row text on purpose: an
UPDATE that rewrote a row to the values it already held logs nothing at all and
makes the whole checksum a test of nothing (benchmarks/cdc-test-traps.md #1).

    python3 bench-capped-my-ch-cdc-window.py TICKS TICK_S N_TABLES [INS UPD DEL]
                                              [INS_LO UPD_LO DEL_LO]
"""
import sys

ticks = int(sys.argv[1])
tick_s = float(sys.argv[2])
n_tables = int(sys.argv[3])
ins, upd, dele = (int(x) for x in sys.argv[4:7])
ins_lo, upd_lo, del_lo = (int(x) for x in sys.argv[7:10])

COLS = ("id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, "
        "float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, "
        "extra_text")

out = sys.stdout
out.write(f"-- the CDC window's change stream: {ticks} ticks x {n_tables} tables, "
          f"{tick_s}s between ticks\n")
out.write(f"-- per tick per table: {ins} INSERT + {upd} UPDATE + {dele} DELETE rows\n")
out.write(f"-- insert band: clone seed ids [1, {ins * ticks}] to id + {ins_lo}\n")
out.write(f"-- update band: ids {upd_lo}.., delete band: ids {del_lo}..\n")
for k in range(1, ticks + 1):
    for i in range(1, n_tables + 1):
        t = f"cdc_my_t{i:02d}"
        out.write(f"-- t{k:05d} {t}\n")
        a = 1 + ins * (k - 1)
        b = a + ins - 1
        # The clone reads its own table. With binlog_format=ROW every one of the
        # inserted rows is logged as a row event, so the window's INSERT is a real
        # binlog change and not a statement the decoder has to re-derive.
        out.write(
            f"INSERT INTO {t} ({COLS})\n"
            f"SELECT s.id + {ins_lo}, s.small_str, s.medium_str, s.large_str,\n"
            f"       s.tiny_int, s.id + {ins_lo}, s.big_int, s.float_val,\n"
            f"       s.decimal_val, s.bool_val, s.date_val, s.ts_val, s.ts_tz_val,\n"
            f"       s.json_val, s.extra_text\n"
            f"  FROM {t} s WHERE s.id BETWEEN {a} AND {b};\n")
        u = upd_lo + upd * (k - 1)
        out.write(f"UPDATE {t} SET regular_int = regular_int + 1 "
                  f"WHERE id BETWEEN {u} AND {u + upd - 1};\n")
        d = del_lo + dele * (k - 1)
        out.write(f"DELETE FROM {t} WHERE id BETWEEN {d} AND {d + dele - 1};\n")
    if k < ticks:
        out.write(f"SELECT SLEEP({tick_s});\n")
    if k % 100 == 0:
        out.write(f"SELECT CONCAT('WINDOW_PROGRESS tick {k}/{ticks}');\n")
out.write(f"SELECT CONCAT('WINDOW_DONE ticks {ticks} per_table_ins {ins * ticks} "
          f"per_table_upd {upd * ticks} per_table_del {dele * ticks}');\n")