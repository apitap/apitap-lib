#!/usr/bin/env python3
"""The source AS OF a watermark, as a per-table digest the destination can be
compared against.

A drain that fell behind leaves the destination at "everything up to W", not at
"everything". Comparing those two is the only honest verdict available, and it is
usually waved off as too hard to do exactly. Here it is exact, because the
writer's shape (bench-capped-pg-ch-cdc-stress-writer.py) makes it a closed form:

  * every transaction of table t re-runs the IDENTICAL statement over a FIXED
    800-row band, so after k transactions every band row carries exactly `+k` on
    regular_int. Undoing (k_now - k_at_W) increments is one subtraction in the
    row text — no per-row ledger join and no replay;
  * every transaction INSERTs 100 fresh ids and DELETEs those same 100 in the
    SAME transaction, so no fresh id ever survives one and the table's row SET
    is identical at every watermark. The whole reconstruction is therefore one
    corrected column, carried as a per-row `undo` so a single aggregate covers
    the update band and everything else;
  * so the control is not a formality. The first implementation lagged the delete
    by one transaction, which leaves the last transaction's 100 rows live and
    makes the reconstruction depend on how far the writer got before it was
    stopped — a variable the k=0 control cannot see, because k=0 is correct in
    both shapes.

Usage:
  bench-cdc-stress-prefix.py TABLE WATERMARK_LSN LEDGER_DIR [BAND_BASE]
  bench-cdc-stress-prefix.py --control [BAND_BASE] TABLE [TABLE ...]

THE CONTROL is the point of this file. A prefix reconstruction that is wrong in
any region produces a digest that looks exactly like a data verdict, so the
control runs the same three regions with k forced to 0 — "no window-S
transaction had committed yet" — which must reproduce the digest WINDOW C's own
30/30 verification already signed off. If the row text, the generator, the band
or the correction is wrong, the control prints MISMATCH and the campaign stops
rather than reporting a prefix it invented.
"""
import os
import re
import sys

BAND_W = 800
# The band base the STRESS window used. It arrives as an argument so the two
# windows can own disjoint bands; the plain default here is only a fallback.
BAND_LO = 951_601
GRD_LO = 40_000_000
INS_LO = 50_000_000
INS_W, INS_STRIDE = 100, 1_000_000
SEED_HI = 1_000_000

# The shared validator's row text (benchmarks/bench-capped-pg-ch-validator.sh),
# with ONE difference: `regular_int` becomes `regular_int - undo`, where `undo`
# is 0 everywhere except the update band. Everything else — the chr(31)
# separator, the <NUL> sentinel, the scaled decimals, the normalised bool, the
# AT TIME ZONE on the timestamptz — is the validator's own text, copied, so the
# two sides cannot drift.
PG_ROWTEXT = """concat_ws(chr(31),
    id::text, coalesce(small_str,'<NUL>'), coalesce(medium_str,'<NUL>'),
    coalesce(large_str,'<NUL>'), coalesce(tiny_int::text,'<NUL>'),
    coalesce((regular_int - undo)::text,'<NUL>'), coalesce(big_int::text,'<NUL>'),
    coalesce(trunc(float_val*1000000)::bigint::text,'<NUL>'),
    coalesce(trunc(decimal_val*10000)::bigint::text,'<NUL>'),
    coalesce(CASE WHEN bool_val THEN '1' ELSE '0' END,'<NUL>'),
    coalesce(to_char(date_val,'YYYY-MM-DD'),'<NUL>'),
    coalesce(to_char(ts_val,'YYYY-MM-DD HH24:MI:SS.US'),'<NUL>'),
    coalesce(to_char(ts_tz_val AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.US'),'<NUL>'),
    coalesce(json_val::text,'<NUL>'), coalesce(extra_text,'<NUL>'))"""

COLS15 = ("id, small_str, medium_str, large_str, tiny_int, regular_int, big_int, "
          "float_val, decimal_val, bool_val, date_val, ts_val, ts_tz_val, "
          "json_val, extra_text")

# The seed's own generator expressions, column for column, so a row this emits is
# the row the writer inserted. Substituted, never formatted.
GEN = """SELECT g AS id,
       substr(md5(g::text),1,8) AS small_str,
       CASE WHEN g % 97 = 0 THEN NULL ELSE substr(md5((g*7)::text),1,40) END AS medium_str,
       substr(md5((g*13)::text),1,200) AS large_str,
       (g % 100)::smallint AS tiny_int,
       g AS regular_int,
       g::bigint*1000000 AS big_int,
       (g % 10000)::double precision/8 AS float_val,
       CASE WHEN g % 89 = 0 THEN NULL ELSE ((g % 1000000)::numeric(18,4)/100) END AS decimal_val,
       (g % 2 = 0) AS bool_val,
       date '2024-01-01' + (g % 900) AS date_val,
       timestamp '2024-01-01 00:00:00' + (g % 1000000)*interval '1 microsecond' AS ts_val,
       timestamptz '2024-01-01 00:00:00+00' + (g % 1000000)*interval '1 microsecond' AS ts_tz_val,
       CASE WHEN g % 83 = 0 THEN NULL
            ELSE ('{"k":' || g || ',"s":"' || substr(md5(g::text),1,16) || '"}')::json END AS json_val,
       CASE WHEN g % 79 = 0 THEN NULL ELSE 'extra-' || substr(md5((g*3)::text),1,60) END AS extra_text
  FROM generate_series({a}, {b}) AS g"""

TBL = re.compile(r"^cdc_pg_t(\d+)$")
LEDGER = re.compile(
    r"^LEDGER\s+s\d+\s+(cdc_pg_t\d+)\s+(\d+)\s+([0-9A-F]+/[0-9A-F]+)\s*$")


def lsn_int(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def ledger_lsns(ledger_dir, label):
    """{table: [commit LSN, ...]} in commit order.

    COUNTING the lines, not reading the k field. A session owns two tables and
    interleaves them, so the k it prints is the SESSION's loop index: table t01
    gets 1, 3, 5 ... and t02 gets 2, 4, 6 ..., and 120 of t01's transactions carry
    k up to 239. Taking max(k) as "how many increments this table has" then undoes
    twice too many, which is a mismatch on all thirty tables and looks like a data
    verdict. A session's transactions commit in order, so the number of a table's
    ledger lines at or before the watermark IS its transaction count at the
    watermark, and the k field is not needed at all.
    """
    lsns = {}
    for name in sorted(os.listdir(ledger_dir)):
        if not name.startswith(label + "-s"):
            continue
        with open(os.path.join(ledger_dir, name), errors="replace") as fh:
            for line in fh:
                m = LEDGER.match(line.strip())
                if not m:
                    continue
                lsns.setdefault(m.group(1), []).append(lsn_int(m.group(3)))
    for t in lsns:
        lsns[t].sort()
    return lsns


def emitted(n, undo_band, k):
    """Every row of table n as it stood at the watermark, as one UNION ALL.

    Two disjoint regions, and that is the whole point of the writer's shape:
      band  seed ids BAND_LO+(n-1)*1000 .. +799, undo = undo_band
      rest  every other row the table holds, undo = 0

    There is no third region because no fresh id survives a writer transaction:
    each one INSERTs 100 rows and DELETEs those same 100 in the same
    transaction, so the row SET of the table is identical at every k and the
    only thing the watermark changes is the band's regular_int. An
    implementation that lagged the delete by one transaction had a third region
    whose far end depended on how far the writer got before it was stopped, and
    the control — which only exercises k=0 — could not see it.
    """
    t = "cdc_pg_t%02d" % n
    band_lo = BAND_LO + (n - 1) * 1000
    band_hi = band_lo + BAND_W - 1
    return (
        f"SELECT {COLS15}, {undo_band}::bigint AS undo FROM public.{t}"
        f" WHERE id BETWEEN {band_lo} AND {band_hi}"
        f" UNION ALL"
        f" SELECT {COLS15}, 0::bigint AS undo FROM public.{t}"
        f" WHERE id < {band_lo} OR id > {band_hi}")


def digest_sql(n, undo_band, k):
    body = emitted(n, undo_band, k)
    tbl = "cdc_pg_t%02d" % n
    # The table name and the digest come back on ONE unaligned line, separated by
    # a real tab (chr(9)), because that is the shape the campaign's verifier
    # already reads. A generator that emitted `name<TAB>` and the SQL on separate
    # lines looked fine and produced a syntax error instead.
    return (f"SELECT concat('{tbl}', chr(9), concat_ws('|', count(*)::text,"
            f" sum(((chr(120)||substr(md5({PG_ROWTEXT}),1,8))::bit(32)::bigint))::text,"
            f" min(id)::text, max(id)::text,"
            f" sum(coalesce(length(medium_str),0))::text,"
            f" sum(coalesce(length(large_str),0))::text,"
            f" sum(coalesce(length(extra_text),0))::text,"
            f" sum((medium_str IS NULL)::int)::text,"
            f" sum((extra_text IS NULL)::int)::text,"
            f" sum((json_val IS NULL)::int)::text,"
            f" sum((decimal_val IS NULL)::int)::text))"
            f" FROM ({body}) AS q")


def main(argv):
    global BAND_LO

    if argv and argv[0] == "--control":
        if len(argv) > 1 and argv[1].isdigit():
            BAND_LO = int(argv[1])
            argv = [argv[0]] + argv[2:]
        # k forced to 0 for every table: "no window-S transaction had committed".
        # The control is SELF-CONSISTENCY, not "the destination at k=0": the
        # destination is at whatever k the drain reached, which is not 0 whenever
        # the drain applied anything, so a k=0-vs-destination control is guaranteed
        # to print thirty mismatches and says nothing. What must hold is that the
        # two-region reconstruction with NO correction reproduces the plain source
        # digest exactly — that is what tests the row text, the band arithmetic and
        # the region split, none of which the drain is involved in.
        #
        # One `;` per statement: the caller pipes all thirty into ONE psql, and
        # thirty unterminated SELECTs concatenated is a syntax error whose only
        # symptom is an empty digest, which then prints as thirty mismatches.
        for tbl in sorted(argv[1:] or []):
            n = int(TBL.match(tbl).group(1))
            print(digest_sql(n, 0, 0) + ";")
        return 0
    # k_at (how many of this table's transactions the DRAIN applied) comes from
    # apitap's own per-table counter, not from the ledger's LSN bracket. The ledger
    # line prints pg_current_wal_lsn() AFTER its COMMIT, which under fifteen
    # concurrent sessions writing 163 MB/s of WAL is an upper bound tens of
    # kilobytes wide, and one table's seventh transaction read a position already
    # past the watermark while its own commit had not: the ledger counted 7, the
    # drain applied 6, and the reconstruction was one increment too high on
    # exactly one table of thirty. apitap's `per_table_rows tNN=` is the product
    # saying what IT did, and it is what the claim has to be made against.
    tbl, k_at, k_now = argv[0], int(argv[1]), int(argv[2])
    if len(argv) > 3:
        BAND_LO = int(argv[3])
    n = int(TBL.match(tbl).group(1))
    sys.stderr.write(f"# {tbl}: drain applied {k_at} of the source's {k_now} transactions, "
                     f"undo {k_now - k_at}\n")
    print(digest_sql(n, k_now - k_at, k_at) + ";")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))