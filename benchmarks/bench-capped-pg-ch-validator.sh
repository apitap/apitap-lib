#!/usr/bin/env bash
# The validator for the capped PostgreSQL -> ClickHouse campaign: ONE definition
# per engine, used for the source, for apitap's landing and for walshadow's
# landing. A number without this behind it is not a result.
#
# Per table it is order-independent and cheap: COUNT, an order-independent SUM
# of the first four bytes of each row's md5 over all 15 columns, MIN/MAX of the
# primary key, and the length/NULL totals that localise a mismatch. No
# string_agg, and no accumulating per-row string: both are what breaks past
# ~100M rows.
#
# Three things this had to be shaped around, each found by a probe against the
# live engines BEFORE any timed run (benchmarks/bench-capped-pg-ch-probe*.sh):
#
#   * ClickHouse's reinterpretAsUInt32 is LITTLE-endian, so taking the first 8
#     hex characters of an md5 and reinterpreting them yields a different
#     integer than PostgreSQL's bit(32) of the same prefix (3558706393 vs
#     3649838548 on the empty string). The CH side therefore rebuilds the prefix
#     in reversed byte order — substr 7,5,3,1 — checked equal to PostgreSQL on
#     three inputs and then on a 1M-row sum, where both engines returned
#     2148800382821315 for the same million rows.
#   * toString(Decimal(18,4)) prints '1234.56' where PostgreSQL prints
#     '1234.5600', so a decimal enters the digest as a scaled integer
#     (value * 10000) on both sides, never as text.
#   * ClickHouse renders Bool as true/false and PostgreSQL as t/f, so the flag is
#     normalised to '1'/'0' on both sides. The four NULLable columns carry NULLs
#     on a prime modulus of ids and render as the sentinel <NUL>, which no
#     generated value can contain.
#   * ifNull does NOT rescue CAST(x AS String) in ClickHouse: the cast is
#     evaluated first and a NULL column raises "Cannot convert NULL value to
#     non-Nullable type" before ifNull is reached (the seed has NULLs in
#     json_val on 1% of rows, so this fires). The JSON column therefore renders
#     as ifNull(toString(json_val), '<NUL>'), which stays Nullable all the way.
#
# The row text and the md5 prefix are each written ONCE and substituted into the
# aggregate, so the two sides cannot drift apart.
#
#   bash bench-capped-pg-ch-validator.sh src|dst TABLE [WHERE]   the aggregate
#   bash bench-capped-pg-ch-validator.sh agg  src|dst           the SQL itself
#   bash bench-capped-pg-ch-validator.sh cols src|dst TABLE     per column, one per line
#   bash bench-capped-pg-ch-validator.sh row  src|dst TABLE ID   one row's digest, hex

# Overridable so a second campaign can reuse these digest definitions verbatim
# rather than carrying a second copy that could drift: the CDC campaign exports
# its own isolated containers and nothing else about this file changes.
PG_C=${PG_C:-apitap-bench-ws-pg}
CH_C=${CH_C:-apitap-bench-ch}
PG_DB=${PG_DB:-bench}

pgq()  { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "$1"; }
chq()  { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1"; }
pgq_() { docker exec -i "$PG_C" psql -U postgres -d "$PG_DB" -Atc "$1" 2>/dev/null; }
chq_() { docker exec -i "$CH_C" clickhouse-client --password bench -q "$1" 2>/dev/null; }

# ── the canonical row text: 15 fields, chr(31) between them ──────────────────────
# (unhex('1f') on the ClickHouse side — the same single byte — and <NUL> for a
# NULL value, on both sides.)
PG_ROWTEXT="concat_ws(chr(31),
    id::text, coalesce(small_str,'<NUL>'), coalesce(medium_str,'<NUL>'),
    coalesce(large_str,'<NUL>'), coalesce(tiny_int::text,'<NUL>'),
    coalesce(regular_int::text,'<NUL>'), coalesce(big_int::text,'<NUL>'),
    coalesce(trunc(float_val*1000000)::bigint::text,'<NUL>'),
    coalesce(trunc(decimal_val*10000)::bigint::text,'<NUL>'),
    coalesce(CASE WHEN bool_val THEN '1' ELSE '0' END,'<NUL>'),
    coalesce(to_char(date_val,'YYYY-MM-DD'),'<NUL>'),
    coalesce(to_char(ts_val,'YYYY-MM-DD HH24:MI:SS.US'),'<NUL>'),
    coalesce(to_char(ts_tz_val AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS.US'),'<NUL>'),
    coalesce(json_val::text,'<NUL>'), coalesce(extra_text,'<NUL>'))"

CH_ROWTEXT="concat(toString(id),unhex('1f'),ifNull(small_str,'<NUL>'),unhex('1f'),
    ifNull(medium_str,'<NUL>'),unhex('1f'),ifNull(large_str,'<NUL>'),unhex('1f'),
    ifNull(toString(tiny_int),'<NUL>'),unhex('1f'),ifNull(toString(regular_int),'<NUL>'),
    unhex('1f'),ifNull(toString(big_int),'<NUL>'),unhex('1f'),
    ifNull(toString(toInt64(toFloat64(float_val)*1000000)),'<NUL>'),unhex('1f'),
    ifNull(toString(toInt64(decimal_val*10000)),'<NUL>'),unhex('1f'),
    if(ifNull(bool_val,false),'1','0'),unhex('1f'),
    ifNull(toString(date_val),'<NUL>'),unhex('1f'),
    ifNull(toString(ts_val),'<NUL>'),unhex('1f'),
    ifNull(toString(ts_tz_val),'<NUL>'),unhex('1f'),
    ifNull(toString(json_val),'<NUL>'),unhex('1f'),
    ifNull(extra_text,'<NUL>'))"

# The first 32 bits of that text's md5, summed. 1M rows of a 32-bit value cannot
# overflow UInt64/bigint, so the two engines sum in the same integer domain.
PG_ROW_MD5="((chr(120)||substr(md5($PG_ROWTEXT),1,8))::bit(32)::bigint)"
CH_ROW_MD5="toUInt64(reinterpretAsUInt32(unhex(substr(lower(hex(MD5($CH_ROWTEXT))),7,2)||substr(lower(hex(MD5($CH_ROWTEXT))),5,2)||substr(lower(hex(MD5($CH_ROWTEXT))),3,2)||substr(lower(hex(MD5($CH_ROWTEXT))),1,2))))"

PG_AGG="SELECT concat_ws('|', count(*)::text, sum($PG_ROW_MD5)::text,
    min(id)::text, max(id)::text,
    sum(coalesce(length(medium_str),0))::text,
    sum(coalesce(length(large_str),0))::text,
    sum(coalesce(length(extra_text),0))::text,
    sum((medium_str IS NULL)::int)::text,
    sum((extra_text IS NULL)::int)::text,
    sum((json_val IS NULL)::int)::text,
    sum((decimal_val IS NULL)::int)::text) FROM @TBL@"

CH_AGG="SELECT concat(toString(count()),'|',toString(sum($CH_ROW_MD5)),'|',
    toString(min(id)),'|',toString(max(id)),'|',
    toString(sum(ifNull(toInt32(length(medium_str)),0))),'|',
    toString(sum(ifNull(toInt32(length(large_str)),0))),'|',
    toString(sum(ifNull(toInt32(length(extra_text)),0))),'|',
    toString(sum(if(isNull(medium_str),1,0))),'|',
    toString(sum(if(isNull(extra_text),1,0))),'|',
    toString(sum(if(isNull(json_val),1,0))),'|',
    toString(sum(if(isNull(decimal_val),1,0)))) FROM @TBL@"

# Per-column aggregates, one per line, so a mismatch can be NAMED instead of
# inferred. This is what found the CHAR(31) trap in the MySQL campaign: every
# per-column aggregate agreed while every row digest differed.
PG_COLS="SELECT concat_ws('|',
    'count=', count(*),
    'id_sum=', sum(id), ' id_min=', min(id), ' id_max=', max(id),
    'small_str_len=', sum(coalesce(length(small_str),0)),
    'medium_str_len=', sum(coalesce(length(medium_str),0)), ' medium_str_null=', sum((medium_str IS NULL)::int),
    'large_str_len=', sum(coalesce(length(large_str),0)),
    'extra_len=', sum(coalesce(length(extra_text),0)), ' extra_null=', sum((extra_text IS NULL)::int),
    'tiny_int_sum=', sum(coalesce(tiny_int,0)),
    'regular_int_sum=', sum(coalesce(regular_int,0)),
    'big_int_sum=', sum(coalesce(big_int,0)),
    'float_scaled_sum=', sum(coalesce(trunc(float_val*1000000)::bigint,0)),
    'decimal_scaled_sum=', sum(coalesce(trunc(decimal_val*10000)::bigint,0)), ' decimal_null=', sum((decimal_val IS NULL)::int),
    'bool_sum=', sum(coalesce(bool_val::int,0)),
    'date_sum=', sum(coalesce((date_val - DATE '1970-01-01')::bigint,0)),
    'ts_sum=', sum(coalesce((extract(epoch from ts_val)*1000000)::bigint,0)),
    'tstz_sum=', sum(coalesce((extract(epoch from ts_tz_val)*1000000)::bigint,0)),
    'json_len=', sum(coalesce(length(json_val::text),0)), ' json_null=', sum((json_val IS NULL)::int))
    FROM @TBL@"

CH_COLS="SELECT concat_ws('|',
    'count=', toString(count()),
    'id_sum=', toString(sum(id)), ' id_min=', toString(min(id)), ' id_max=', toString(max(id)),
    'small_str_len=', toString(sum(ifNull(toInt32(length(small_str)),0))),
    'medium_str_len=', toString(sum(ifNull(toInt32(length(medium_str)),0))), ' medium_str_null=', toString(sum(if(isNull(medium_str),1,0))),
    'large_str_len=', toString(sum(ifNull(toInt32(length(large_str)),0))),
    'extra_len=', toString(sum(ifNull(toInt32(length(extra_text)),0))), ' extra_null=', toString(sum(if(isNull(extra_text),1,0))),
    'tiny_int_sum=', toString(sum(ifNull(tiny_int,0))),
    'regular_int_sum=', toString(sum(ifNull(regular_int,0))),
    'big_int_sum=', toString(sum(ifNull(big_int,0))),
    'float_scaled_sum=', toString(sum(ifNull(toInt64(toFloat64(float_val)*1000000),0))),
    'decimal_scaled_sum=', toString(sum(ifNull(toInt64(decimal_val*10000),0))), ' decimal_null=', toString(sum(if(isNull(decimal_val),1,0))),
    'bool_sum=', toString(sum(ifNull(toInt64(bool_val),0))),
    'date_sum=', toString(sum(ifNull(toInt32(date_val),0))),
    'ts_sum=', toString(sum(ifNull(toInt64(toUnixTimestamp64Micro(toDateTime64(ts_val,6,'UTC'))),0))),
    'tstz_sum=', toString(sum(ifNull(toInt64(toUnixTimestamp64Micro(toDateTime64(ts_tz_val,6,'UTC'))),0))),
    'json_len=', toString(sum(ifNull(toInt32(length(toString(json_val))),0))), ' json_null=', toString(sum(if(isNull(json_val),1,0)))
) FROM @TBL@ FORMAT TSVRaw"

# FINAL on the ClickHouse side, but only where it means something: walshadow
# creates ReplacingMergeTree, where a query without FINAL can read several
# versions of one row and double-count the digest, while apitap creates a plain
# MergeTree, which REJECTS the keyword outright ("Storage MergeTree doesn't
# support FINAL"). The engine therefore decides, per table, rather than the
# validator assuming one shape for both tools.
ch_final() {
    local e
    e=$(chq_ "SELECT engine FROM system.tables
              WHERE database='default' AND name='$1'")
    case "$e" in
    Replacing*|VersionedCollapsing*|CollapsingMergeTree*) echo " FINAL" ;;
    *) echo "" ;;
    esac
}

srcsum() { local w=""; [[ -n "${2:-}" ]] && w=" WHERE $2"; pgq_ "${PG_AGG//@TBL@/public.\"$1\"$w}"; }
chsum()  { local w=""; [[ -n "${2:-}" ]] && w=" WHERE $2"
           chq_ "${CH_AGG//@TBL@/\`default\`.\`$1\`$(ch_final "$1")$w}"; }
chrows() { chq_ "SELECT count() FROM \`default\`.\`$1\`$(ch_final "$1")${2:+ WHERE $2}"; }

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    case "${1:-}" in
    src) srcsum "${2:?table}" "${3:-}" ;;
    dst) chsum "${2:?table}" "${3:-}" ;;
    raw)
        case "${2:-}" in
        src) pgq "${PG_AGG//@TBL@/public.\"${3:?table}\"}" ;;
        dst) chq "${CH_AGG//@TBL@/\`default\`.\`${3:?table}\`$(ch_final "${3:?table}")}" ;;
        esac
        ;;
    agg)
        case "${2:-}" in
        src) printf '%s\n' "${PG_AGG//@TBL@/public.\"TABLE\"}" ;;
        dst) printf '%s\n' "${CH_AGG//@TBL@/\`default\`.\`TABLE\` FINAL}" ;;
        esac
        ;;
    cols)
        case "${2:-}" in
        src) pgq_ "${PG_COLS//@TBL@/public.\"${3:?table}\"}" | tr '|' '\n' ;;
        dst) chq_ "${CH_COLS//@TBL@/\`default\`.\`${3:?table}\`$(ch_final "${3:?table}")}" | tr '|' '\n' ;;
        *) echo "usage: $0 cols src|dst TABLE" >&2; exit 2 ;;
        esac
        ;;
    row)
        case "${2:-}" in
        src) pgq_ "SELECT encode(convert_to($PG_ROWTEXT,'UTF8'),'hex') FROM public.\"${3:?table}\" WHERE id = ${4:?id}" ;;
        dst) chq_ "SELECT hex($CH_ROWTEXT) FROM \`default\`.\`${3:?table}\`$(ch_final "${3:?table}") WHERE id = ${4:?id} FORMAT TSVRaw" ;;
        *) echo "usage: $0 row src|dst TABLE ID" >&2; exit 2 ;;
        esac
        ;;
    *) sed -n 2,30p "$0"; exit 2 ;;
    esac
fi