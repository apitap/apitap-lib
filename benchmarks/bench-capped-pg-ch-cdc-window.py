"""The CDC window's change stream: exact statements, piped straight into psql.

One generator, one psql session on the source, every statement its own implicit
transaction (no BEGIN anywhere), and a `pg_sleep` between ticks so the stream is
a steady ~TICK_S-paced load rather than a single burst. The file is never written
to disk — it is generated on the host and piped into `docker exec -i … psql`.

Counts are exact BY CONSTRUCTION, not counted afterwards:

  per tick, per table   INSERT 100 rows (ids from 2,000,000 upward, never a
                             deleted id)
                        UPDATE 100 rows (one contiguous band of 1..899,999,
                             which nothing else ever touches)
                        DELETE  20 rows (ids from 900,001 upward)

The three bands are disjoint by construction, so a table's row count after the
window is exactly 1_000_000 + INS·ticks - DEL·ticks — which the harness asserts
against the source itself, and which the per-table checksum then has to agree
with in the destination.

The UPDATE sets `regular_int = regular_int + 1`, a column inside the validator's
row text: an UPDATE that rewrote a row to the values it already held would log
nothing at all and make the whole checksum a test of nothing
(benchmarks/cdc-test-traps.md #1).

    python3 bench-capped-pg-ch-cdc-window.py TICKS TICK_S N_TABLES [INS UPD DEL]
"""
import sys

ticks = int(sys.argv[1])
tick_s = float(sys.argv[2])
n_tables = int(sys.argv[3])
ins, upd, dele = (int(x) for x in sys.argv[4:7])

INS_LO, UPD_LO, DEL_LO = 2_000_000, 1, 900_001
COLS = "id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, " \
       "float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, json_val, extra_text"
# The seed's own expressions (bench-capped-pg-ch-schema.sql), so an inserted row is
# indistinguishable from a seeded one and the checksum compares like with like.
VALUES = """
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

out = sys.stdout
out.write(f"-- the CDC window's change stream: {ticks} ticks x {n_tables} tables, "
          f"{tick_s}s between ticks\n")
out.write(f"-- per tick per table: {ins} INSERT + {upd} UPDATE + {dele} DELETE rows\n")
out.write("SET application_name = 'apitap-bench-cdc-window';\n")
for k in range(1, ticks + 1):
    for i in range(1, n_tables + 1):
        t = f"cdc_pg_t{i:02d}"
        out.write(f"-- t{k:05d} {t}\n")
        # Substituted, not formatted: VALUES carries the seed's own JSON text
        # ('{"k":' …), and str.format() reads those braces as a field name and
        # dies with KeyError: '"k"' — silently, on stderr, with an empty log.
        vals = VALUES.replace("__A__", str(INS_LO + ins * (k - 1))) \
                     .replace("__B__", str(INS_LO + ins * (k - 1) + ins - 1))
        out.write(f"INSERT INTO {t} ({COLS})" + vals + ";\n")
        u = UPD_LO + upd * (k - 1)
        out.write(f"UPDATE {t} SET regular_int = regular_int + 1 "
                  f"WHERE id BETWEEN {u} AND {u + upd - 1};\n")
        d = DEL_LO + dele * (k - 1)
        out.write(f"DELETE FROM {t} WHERE id BETWEEN {d} AND {d + dele - 1};\n")
    if k < ticks:
        out.write(f"SELECT pg_sleep({tick_s});\n")
    if k % 100 == 0:
        out.write(f"\\echo WINDOW_PROGRESS tick {k}/{ticks}\n")
out.write(f"\\echo WINDOW_DONE ticks {ticks} per_table_ins {ins * ticks} "
          f"per_table_upd {upd * ticks} per_table_del {dele * ticks} "
          f"expected_rows {1_000_000 + ins * ticks - dele * ticks}\n")