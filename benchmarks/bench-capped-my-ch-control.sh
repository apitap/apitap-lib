#!/usr/bin/env bash
# CONTROL for the capped MySQL -> ClickHouse campaign. Run this BEFORE believing
# any timed number, and re-run it after changing the validator.
#
# It proves three things, on a 1000-row control table, uncapped — this leg is
# about the validator and the invocations, not about the cap:
#
#   GREEN 1  the cross-engine validator AGREES on a table that arrived intact;
#   GREEN 2  it still DISAGREES when a single source value changes (so a MATCH
#            means something);
#   GREEN 3  ingestr's documented invocation starts and its landing validates
#            against the same validator.
#
# Anything else is a RED that must stop the campaign: a validator that cannot
# tell an intact table from a changed one makes every number meaningless.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
V="$HERE/bench-capped-my-ch-validator.sh"
ING_V=${INGESTR_VERSION:-1.1.61}
ING_BIN=${ING_BIN:-$HOME/.cache/ingestr/bin/v${ING_V}/Linux_x86_64/ingestr}
SP=${SP_APITAP:-$HOME/apitap-057-pullback/lib/python3.13/site-packages}
myq() { docker exec -i apitap-bench-my mysql -uroot -pbench -N -B bench -e "$1" 2>/dev/null; }
chq() { docker exec -i apitap-bench-ch clickhouse-client --password bench -q "$1" 2>/dev/null; }
red()   { echo "  RED: $*"; exit 1; }
green() { echo "  GREEN: $*"; }

echo "== control table (1000 rows, the same 15 columns as the seed)"
myq "DROP TABLE IF EXISTS cmp_ctrl_t;
     CREATE TABLE cmp_ctrl_t LIKE bench.bench_my_1m;
     INSERT INTO cmp_ctrl_t SELECT * FROM bench.bench_my_1m WHERE id <= 1000;"
myq "SELECT CONCAT('  src rows=', COUNT(*), ' cols=',
      (SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema='bench' AND table_name='cmp_ctrl_t')) FROM cmp_ctrl_t"

echo
echo "== (1) apitap, uncapped"
docker run --rm --network=host -v "$SP:/py:ro" -e PYTHONPATH=/py python:3.13-slim \
    python -c "
import apitap
print('  version', apitap.__version__)
r = apitap.transfer('mysql://root:bench@127.0.0.1:3307/bench',
                    'clickhouse://default:bench@127.0.0.1:8124/default',
                    table='cmp_ctrl_t')
print('  rows', r.rows, 'pipes', r.parallel)"
echo "  --- what apitap created in ClickHouse:"
chq "SELECT concat('  ', name, ' ', engine, ' ', total_rows) FROM system.tables
      WHERE database='default' AND name LIKE '%cmp_ctrl%'"
chq "SELECT concat('  ', name, ' ', type) FROM system.columns
      WHERE database='default' AND table='cmp_ctrl_t' ORDER BY position"

SRC=$(bash "$V" src cmp_ctrl_t)
DST=$(bash "$V" dst cmp_ctrl_t)
echo "  mysql : $SRC"
echo "  ch    : $DST"
[[ -n "$DST" && "$SRC" == "$DST" ]] || red "the validator disagrees on an INTACT table — the campaign cannot measure anything"
green "the validator agrees across engines on an intact table"

echo
echo "== (2) sensitivity: perturb ONE source row, the verdict must notice"
myq "UPDATE cmp_ctrl_t SET extra_text = 'PERTURBED' WHERE id = 777;"
SRC2=$(bash "$V" src cmp_ctrl_t)
[[ "$SRC2" != "$DST" ]] || red "the validator cannot see a changed row — every MATCH would be worthless"
green "one changed value changes the digest (${DST#*|} -> ${SRC2#*|})"

echo
echo "== (3) ingestr ${ING_V}, documented invocation, uncapped"
docker run --rm --network=host -v "$ING_BIN:/usr/local/bin/ingestr:ro" python:3.13-slim sh -c '
set -u
ingestr --version
ingestr ingest \
  --source-uri "mysql://root:bench@127.0.0.1:3307/bench" \
  --source-table cmp_ctrl_t \
  --dest-uri "clickhouse://default:bench@127.0.0.1:9124?http_port=8124" \
  --dest-table cmp_ctrl_t_ingestr \
  --yes --full-refresh --progress log 2>&1 | tail -20'
echo "  --- what ingestr created in ClickHouse:"
chq "SELECT concat('  ', name, ' ', engine, ' ', total_rows) FROM system.tables
      WHERE database='default' AND name LIKE '%cmp_ctrl%'"
chq "SELECT concat('  ', name, ' ', type) FROM system.columns
      WHERE database='default' AND table='cmp_ctrl_t_ingestr' ORDER BY position"
DST3=$(bash "$V" dst cmp_ctrl_t_ingestr)
echo "  mysql : $SRC2"
echo "  ch    : $DST3"
if [[ -n "$DST3" && "$SRC2" == "$DST3" ]]; then
    green "ingestr's landing validates against the same validator"
else
    echo "  NOTE: the same validator does not match ingestr's landing — diff it column"
    echo "        by column: bash bench-capped-my-ch-mismatch.sh cols cmp_ctrl_t_ingestr"
    exit 1
fi

echo
echo "== control cleanup"
bash "$HERE/bench-capped-my-ch-0.57.sh" drop >/dev/null
myq "DROP TABLE IF EXISTS cmp_ctrl_t;"
echo "  left behind: $(chq "SELECT count() FROM system.tables WHERE name LIKE '%cmp_ctrl%'") ch table(s), $(myq "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='bench' AND table_name='cmp_ctrl_t'") my table(s)"