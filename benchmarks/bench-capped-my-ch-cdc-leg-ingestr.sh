#!/usr/bin/env bash
# The ingestr arm of the capped MySQL -> ClickHouse CDC campaign, run INSIDE the
# capped container (--cpus=0.5 --memory=256m --memory-swap=256m).
#
# ingestr's MySQL CDC path, exactly as its own docs specify it
# (docs/supported-sources/mysql.md#change-data-capture, and
# docs/getting-started/cdc.md#multi-table-cdc):
#
#   * the `mysql+cdc://` scheme, which reads the binary log after a consistent
#     snapshot and resumes from durable CDC state recorded in the DESTINATION;
#   * a COMMA-SEPARATED --source-table, which its docs state is still a multi-table
#     run, with dest_schema deciding where the output lands — so all thirty tables
#     are one ingestr invocation, which is ingestr's OWN multi-table CDC mode and
#     not apitap's model imposed on it;
#   * `dest_schema`, passed as the SOURCE-URI PARAMETER its docs document (there is
#     no --dest-schema flag: its own cdc.md once named one that does not exist), and
#     --cdc-table-naming table so the landing carries the same thirty names apitap's
#     does instead of the default schema_table flattening (bench.cdc_my_t01 ->
#     default.bench_cdc_my_t01). Both are documented; naming is cosmetic;
#   * server_id pinned, because its docs ask for a unique replication id on every
#     scheduled or overlapping run;
#   * one-shot (no --stream): "By default each invocation catches up and exits", which
#     is the same shape apitap's one-call catch-up is measured against;
#   * every documented knob left at its default (--extract-parallelism 5,
#     --page-size 25000, --loader-file-size 25000, --batch-size 512 MiB,
#     --sql-limit 0 = no limit). ING_SQL_LIMIT etc. are only set by the campaign's
#     `tuned` arm, and that arm exists so "ingestr failed" is only an honest
#     sentence if it also failed with smaller settings of its own.
#
# Its docs also state the one architectural difference this campaign must disclose
# rather than paper over: "Multi-table CDC snapshots each selected table
# independently and then stream each table from its own snapshot position. Each table
# is consistent on its own, but a multi-table run is not a single global
# point-in-time snapshot across all tables." apitap's ONE group mints ONE binlog
# coordinate before its load and reads ONE stream. On a static pre-seeded source the
# two agree; on a mutating one they need not, and that is a difference in model, not
# in speed.
#
# ingestr 1.1.x is no longer a Python program: the pip package is a thin wrapper
# that downloads and execs a 254 MB native binary. That exact executable is
# bind-mounted read-only, so nothing is downloaded inside a measurement.
set -u

SRC_URI="${ING_SOURCE_URI:?ING_SOURCE_URI is required}"
DST_URI="${ING_DEST_URI:?ING_DEST_URI is required}"
SRC_TABLE="${ING_SOURCE_TABLE:?ING_SOURCE_TABLE is required}"
# This leg runs under `sh` (the orchestrator's `sh /job/…`), so it is POSIX-only.
# The control caught the alternative: a bash `[[ "$SRC_URI" != *dest_schema=* ]]`
# printed "[[: not found" under dash and SILENTLY skipped the append below, so the
# run reached ingestr with no destination namespace at all.
case "$SRC_URI" in
    *dest_schema=*) ;;
    # `&` when the URI already carries a query (it does: the orchestrator pins
    # ?server_id=), `?` when it does not. Appending "?" unconditionally produced
    # "...?server_id=18888?dest_schema=default", which ingestr rejects as
    # "server_id must be a positive uint32" — the second `?` folds the rest of the
    # query into the parameter's value.
    *\?*) SRC_URI="${SRC_URI}&dest_schema=${ING_DEST_SCHEMA:-default}" ;;
    *)   SRC_URI="${SRC_URI}?dest_schema=${ING_DEST_SCHEMA:-default}" ;;
esac
# --cdc-table-naming defaults to schema_table, which flattens the SOURCE schema
# into the destination name (bench.cdc_my_t01 -> default.bench_cdc_my_t01). Both
# spellings are documented; `table` keeps only the table name, so ingestr's landing
# carries the SAME thirty names apitap's does and one validator computes both
# digests with no special-casing. Naming is cosmetic — it moves no row and costs no
# byte of the cage — and the report says which one was used.
CDC_TABLE_NAMING="${ING_CDC_TABLE_NAMING:-table}"
# ...but ingestr REFUSES that flag on the single-table path ("cdc-table-naming
# \"table\" applies to multi-table ingestion only; use --dest-table to choose a
# single table's destination"), which is the shape the control's rival leg runs. So
# the flag is passed only when the run really is multi-table, and the single-table
# case names its destination the way ingestr documents for it.
NTAB=$(printf '%s' "$SRC_TABLE" | tr ',' '\n' | grep -c .)
if [ "$NTAB" -gt 1 ]; then
    NAMING_FLAG="--cdc-table-naming $CDC_TABLE_NAMING"
else
    NAMING_FLAG="--dest-table $SRC_TABLE"
fi

echo "INGESTR_VERSION $(ingestr --version 2>&1 | head -1)"
echo "INGESTR_SOURCE_URI $(printf '%s' "$SRC_URI" | sed 's|//[^@]*@|//***@|')"
echo "INGESTR_TABLES $NTAB"
echo "INGESTR_DEST_SCHEMA ${ING_DEST_SCHEMA:-default} (source-uri parameter dest_schema)"
echo "INGESTR_DEST_NAMING $NAMING_FLAG"
echo "INGESTR_DEFAULTS extract_parallelism=5 page_size=25000 loader_file_size=25000 batch_size=512 sql_limit=${ING_SQL_LIMIT:-0}"
# Its own resolved configuration, so the report can quote the tool rather than the
# wrapper script. This is a no-work call: it prints and exits before any row moves.
ingestr ingest --source-uri "$SRC_URI" --source-table "$SRC_TABLE" \
    --dest-uri "$DST_URI" --yes --debug --progress log \
    $NAMING_FLAG 2>&1 \
    | grep -iE '^\s*(\[CONFIG\]|CONFIG)' | head -20 | sed 's/^/INGESTR_CONFIG /'

t0=$(date +%s.%N)
set +e
ingestr ingest \
    --source-uri "$SRC_URI" \
    --source-table "$SRC_TABLE" \
    --dest-uri "$DST_URI" \
    $NAMING_FLAG \
    --yes \
    --progress log \
    ${ING_SQL_LIMIT:+--sql-limit "$ING_SQL_LIMIT"} \
    ${ING_EXTRACT_PARALLELISM:+--extract-parallelism "$ING_EXTRACT_PARALLELISM"} \
    ${ING_BATCH_SIZE:+--batch-size "$ING_BATCH_SIZE"} \
    ${ING_LOADER_FILE_SIZE:+--loader-file-size "$ING_LOADER_FILE_SIZE"}
rc=$?
set -e
t1=$(date +%s.%N)
echo "INGESTR_WALL_S $(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')"
echo "EXITCODE $rc"
exit $rc