#!/usr/bin/env bash
# The validator for the capped MySQL -> ClickHouse campaign: ONE definition per
# engine, used for the source, for apitap's landing and for ingestr's landing.
#
# Per table it is order-independent and cheap — COUNT, an order-independent SUM
# of per-row CRC32 over 14 of the 15 columns, an order-independent SUM of CRC32
# over the raw JSON text, and min/max of the primary key. No string_agg: that is
# what breaks past ~100M rows. float and decimal are folded to exact integers on
# both sides so a different on-disk type cannot change the verdict, and the
# date/time columns are rendered by each engine's own text form.
#
# Three things this had to be shaped around, all found by the control leg:
#   * this ClickHouse build returns an EMPTY string from JSONExtract* (even for a
#     literal), so the JSON column is validated by CRC32 over its raw text, which
#     MySQL and ClickHouse render identically — verified byte-for-byte on the
#     control table before any timed run;
#   * a row digest must not contain a float or a decimal rendered as text (the two
#     engines print those differently), so each row's float and decimal enter the
#     digest as exact scaled integers;
#   * the field separator is built as CHAR(31) on MySQL and unhex('1f') on
#     ClickHouse on purpose: MySQL reads '\x1f' as the FOUR characters \x1f, while
#     ClickHouse reads it as the one byte 0x1f. That alone made every row digest
#     differ while all 30 per-column aggregates agreed — the per-column diff is
#     what caught it.
#
# `bash bench-capped-my-ch-validator.sh src|dst TABLE` prints the aggregate (an
# optional third argument is a WHERE clause); `raw` keeps the server's stderr;
# `cols` prints the 30 per-column aggregates one per line; `row` prints ONE row's
# digest as hex, which is what names the column a mismatch lives in.
#
# The three container/database names are overridable so the capped MySQL -> CH CDC
# campaign can reuse these digest definitions verbatim rather than carrying a second
# copy that could drift (that is the same move the PostgreSQL campaign made on its
# own validator). With nothing exported the behaviour is byte-identical to what the
# bulk campaign validated against.

MY_C=${MY_C:-apitap-bench-my}
CH_C=${CH_C:-apitap-bench-ch}
MY_DB=${MY_DB:-bench}

myq()  { docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" -e "$1"; }
chq()  { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1"; }
myq_() { docker exec -i "$MY_C" mysql -uroot -pbench -N -B "$MY_DB" -e "$1" 2>/dev/null; }
chq_() { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1" 2>/dev/null; }

MY_AGG="SELECT CONCAT_WS('|',
  CAST(COUNT(*) AS CHAR),
  CAST(SUM(CRC32(CONCAT(
    IFNULL(id,'~'),CHAR(31),IFNULL(small_str,'~'),CHAR(31),IFNULL(medium_str,'~'),CHAR(31),
    IFNULL(large_str,'~'),CHAR(31),CAST(IFNULL(tiny_int,'~') AS CHAR),CHAR(31),
    CAST(IFNULL(regular_int,'~') AS CHAR),CHAR(31),CAST(IFNULL(big_int,'~') AS CHAR),CHAR(31),
    CAST(IFNULL(TRUNCATE(float_val*1000000,0),0) AS CHAR),CHAR(31),
    CAST(IFNULL(ROUND(decimal_val*10000),0) AS CHAR),CHAR(31),
    CAST(IFNULL(bool_val,'~') AS CHAR),CHAR(31),
    IFNULL(DATE_FORMAT(date_val,'%Y-%m-%d'),'~'),CHAR(31),
    IFNULL(DATE_FORMAT(ts_val,'%Y-%m-%d %H:%i:%s.%f'),'~'),CHAR(31),
    IFNULL(DATE_FORMAT(ts_tz_val,'%Y-%m-%d %H:%i:%s.%f'),'~'),CHAR(31),
    IFNULL(extra_text,'~')))) AS CHAR),
  CAST(SUM(IFNULL(CRC32(CAST(json_val AS CHAR)),0)) AS CHAR),
  CAST(MIN(id) AS CHAR), CAST(MAX(id) AS CHAR)) FROM @TBL@"

CH_AGG="SELECT concat(
  toString(count()),'|',
  toString(toUInt64(sum(CRC32(concat(
    toString(id),unhex('1f'),toString(small_str),unhex('1f'),toString(medium_str),unhex('1f'),
    toString(large_str),unhex('1f'),toString(tiny_int),unhex('1f'),
    toString(regular_int),unhex('1f'),toString(big_int),unhex('1f'),
    toString(toInt64(float_val*1000000)),unhex('1f'),
    toString(toInt64(decimal_val*10000)),unhex('1f'),
    toString(bool_val),unhex('1f'),toString(date_val),unhex('1f'),
    toString(ts_val),unhex('1f'),toString(ts_tz_val),unhex('1f'),
    toString(extra_text)))))),'|',
  toString(toUInt64(sum(CRC32(CAST(json_val AS String))))),'|',
  toString(min(id)),'|',toString(max(id))) FROM @TBL@"

# @TBL@ is the whole tail of the FROM clause: a backquoted table name, plus an
# optional WHERE. It is NOT %s — the MySQL aggregate carries %s inside
# DATE_FORMAT('%H:%i:%s.%f'), and a %s placeholder silently ate it (which is how
# one whole campaign nearly measured a validator that dropped the seconds).
# An optional second argument is a WHERE clause, so a debug query is built from
# the SAME aggregate the measurement uses and cannot drift from it.
#
# FINAL on the ClickHouse side, but only where it means something: a CDC lane can
# land into ReplacingMergeTree (ingestr's `merge` strategy does), where a query
# without FINAL can read several versions of one row and double-count the digest,
# while a plain MergeTree — which apitap's CDC lane creates — REJECTS the keyword
# outright ("Storage MergeTree doesn't support FINAL"). The engine therefore
# decides, per table, rather than the validator assuming one shape for every tool.
ch_final() {
    local e
    e=$(chq_ "SELECT engine FROM system.tables
              WHERE database='default' AND name='$1'")
    case "$e" in
    Replacing*|VersionedCollapsing*|CollapsingMergeTree*) echo " FINAL" ;;
    *) echo "" ;;
    esac
}
srcsum() { local w=""; [[ -n "${2:-}" ]] && w=" WHERE $2"; myq_ "${MY_AGG//@TBL@/\`$1\`$w}"; }
chsum()  { local w=""; [[ -n "${2:-}" ]] && w=" WHERE $2"
           chq_ "${CH_AGG//@TBL@/\`default\`.\`$1\`$(ch_final "$1")$w}"; }
chrows() { chq_ "SELECT count() FROM \`default\`.\`$1\`$(ch_final "$1")${2:+ WHERE $2}"; }

# 30 per-column aggregates, one per line, so a mismatch can be NAMED instead of
# inferred (this is what found the CHAR(31) trap above).
#
# The closing paren of CONCAT_WS( was missing here until 2026-10-04, so this branch
# answered with `ERROR 1064 ... near 'FROM ...'` for every table and the caller —
# which pipes stderr away — saw an EMPTY result. A debug aid that silently returns
# nothing is worse than one that errors loudly: `grep '^json_crc='` against empty
# output prints nothing and reads as "compared, found no difference".
MY_COLS="SELECT CONCAT_WS('|',
  'id_sum=', SUM(id), ' id_min=', MIN(id), ' id_max=', MAX(id),
  'small_str_crc=', SUM(IFNULL(CRC32(small_str),0)), ' small_str_len=', SUM(IFNULL(CHAR_LENGTH(small_str),0)),
  'medium_str_crc=', SUM(IFNULL(CRC32(medium_str),0)), ' medium_str_len=', SUM(IFNULL(CHAR_LENGTH(medium_str),0)),
  'large_str_crc=', SUM(IFNULL(CRC32(large_str),0)), ' large_str_len=', SUM(IFNULL(CHAR_LENGTH(large_str),0)),
  'tiny_int_sum=', SUM(IFNULL(tiny_int,0)),
  'regular_int_sum=', SUM(IFNULL(regular_int,0)),
  'big_int_sum=', SUM(IFNULL(big_int,0)),
  'float_scaled_sum=', SUM(IFNULL(TRUNCATE(float_val*1000000,0),0)),
  'decimal_scaled_sum=', SUM(IFNULL(ROUND(decimal_val*10000),0)),
  'bool_sum=', SUM(IFNULL(bool_val,0)),
  'date_crc=', SUM(IFNULL(CRC32(IFNULL(DATE_FORMAT(date_val,'%Y-%m-%d'),'~')),0)),
  'ts_crc=', SUM(IFNULL(CRC32(IFNULL(DATE_FORMAT(ts_val,'%Y-%m-%d %H:%i:%s.%f'),'~')),0)),
  'tstz_crc=', SUM(IFNULL(CRC32(IFNULL(DATE_FORMAT(ts_tz_val,'%Y-%m-%d %H:%i:%s.%f'),'~')),0)),
  'json_crc=', SUM(IFNULL(CRC32(CAST(json_val AS CHAR)),0)),
  'extra_crc=', SUM(IFNULL(CRC32(extra_text),0)), ' extra_len=', SUM(IFNULL(CHAR_LENGTH(extra_text),0))) FROM @TBL@"

CH_COLS="SELECT arrayStringConcat([
  'id_sum=', toString(sum(id)), ' id_min=', toString(min(id)), ' id_max=', toString(max(id)),
  'small_str_crc=', toString(sum(CRC32(small_str))), ' small_str_len=', toString(sum(length(small_str))),
  'medium_str_crc=', toString(sum(CRC32(medium_str))), ' medium_str_len=', toString(sum(length(medium_str))),
  'large_str_crc=', toString(sum(CRC32(large_str))), ' large_str_len=', toString(sum(length(large_str))),
  'tiny_int_sum=', toString(sum(tiny_int)),
  'regular_int_sum=', toString(sum(regular_int)),
  'big_int_sum=', toString(sum(big_int)),
  'float_scaled_sum=', toString(sum(toInt64(float_val*1000000))),
  'decimal_scaled_sum=', toString(sum(toInt64(decimal_val*10000))),
  'bool_sum=', toString(sum(bool_val)),
  'date_crc=', toString(sum(CRC32(toString(date_val)))),
  'ts_crc=', toString(sum(CRC32(toString(ts_val)))),
  'tstz_crc=', toString(sum(CRC32(toString(ts_tz_val)))),
  'json_crc=', toString(sum(CRC32(CAST(json_val AS String)))),
  'extra_crc=', toString(sum(CRC32(extra_text))), ' extra_len=', toString(sum(length(extra_text)))
], '|') FROM @TBL@ FORMAT TSVRaw"

# ONE row's digest, as hex: the client cannot be trusted with raw control bytes,
# and this is what shows WHICH field of WHICH row diverges.
MY_ROW="SELECT HEX(CONCAT(
    IFNULL(id,'~'),CHAR(31),IFNULL(small_str,'~'),CHAR(31),IFNULL(medium_str,'~'),CHAR(31),
    IFNULL(large_str,'~'),CHAR(31),CAST(IFNULL(tiny_int,'~') AS CHAR),CHAR(31),
    CAST(IFNULL(regular_int,'~') AS CHAR),CHAR(31),CAST(IFNULL(big_int,'~') AS CHAR),CHAR(31),
    CAST(IFNULL(TRUNCATE(float_val*1000000,0),0) AS CHAR),CHAR(31),
    CAST(IFNULL(ROUND(decimal_val*10000),0) AS CHAR),CHAR(31),
    CAST(IFNULL(bool_val,'~') AS CHAR),CHAR(31),
    IFNULL(DATE_FORMAT(date_val,'%Y-%m-%d'),'~'),CHAR(31),
    IFNULL(DATE_FORMAT(ts_val,'%Y-%m-%d %H:%i:%s.%f'),'~'),CHAR(31),
    IFNULL(DATE_FORMAT(ts_tz_val,'%Y-%m-%d %H:%i:%s.%f'),'~'),CHAR(31),
    IFNULL(extra_text,'~'))) FROM @TBL@"

CH_ROW="SELECT hex(concat(
    toString(id),unhex('1f'),toString(small_str),unhex('1f'),toString(medium_str),unhex('1f'),
    toString(large_str),unhex('1f'),toString(tiny_int),unhex('1f'),
    toString(regular_int),unhex('1f'),toString(big_int),unhex('1f'),
    toString(toInt64(float_val*1000000)),unhex('1f'),
    toString(toInt64(decimal_val*10000)),unhex('1f'),
    toString(bool_val),unhex('1f'),toString(date_val),unhex('1f'),
    toString(ts_val),unhex('1f'),toString(ts_tz_val),unhex('1f'),
    toString(extra_text))) FROM @TBL@ FORMAT TSVRaw"

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    case "${1:-}" in
    src) srcsum "${2:?table}" "${3:-}" ;;
    dst) chsum "${2:?table}" "${3:-}" ;;
    raw)
        case "${2:-}" in
        src) myq "${MY_AGG//@TBL@/\`${3:?table}\`}" ;;
        dst) chq "${CH_AGG//@TBL@/\`default\`.\`${3:?table}\`}" ;;
        esac
        ;;
    cols)
        case "${2:-}" in
        src) myq_ "${MY_COLS//@TBL@/\`${3:?table}\`}" | tr '|' '\n' ;;
        dst) chq_ "${CH_COLS//@TBL@/\`default\`.\`${3:?table}\`}" | tr '|' '\n' ;;
        *) echo "usage: $0 cols src|dst TABLE" >&2; exit 2 ;;
        esac
        ;;
    row)
        case "${2:-}" in
        src) myq_ "${MY_ROW//@TBL@/\`${3:?table}\` WHERE id = ${4:?id}}" ;;
        dst) chq_ "${CH_ROW//@TBL@/\`default\`.\`${3:?table}\` WHERE id = ${4:?id}}" ;;
        *) echo "usage: $0 row src|dst TABLE ID" >&2; exit 2 ;;
        esac
        ;;
    *) sed -n 2,19p "$0"; exit 2 ;;
    esac
fi