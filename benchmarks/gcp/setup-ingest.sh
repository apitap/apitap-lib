#!/usr/bin/env bash
# Install the three tools in isolated venvs on the bench-ingest VM.
# Each tool gets its own venv so their dependency trees never collide.
#
#   APITAP_WHEEL=~/apitap-*.whl ./setup-ingest.sh
#
# APITAP_WHEEL is optional: unset -> install the released `pip install apitap`
# (Round A). Set it to a locally-built wheel to benchmark a branch (Round B used
# a feat/mysql-sink build, since MySQL->MySQL is not on PyPI yet).
set -euo pipefail
sudo apt-get update -qq
sudo apt-get install -y -qq python3-pip python3-venv mysql-client-core-8.0 postgresql-client curl

python3 -m venv ~/va_apitap
~/va_apitap/bin/pip install -q --upgrade pip
~/va_apitap/bin/pip install -q "${APITAP_WHEEL:-apitap}"

python3 -m venv ~/va_ingestr
~/va_ingestr/bin/pip install -q --upgrade pip
~/va_ingestr/bin/pip install -q ingestr

python3 -m venv ~/va_dlt
~/va_dlt/bin/pip install -q --upgrade pip
~/va_dlt/bin/pip install -q "dlt[postgres,clickhouse,parquet]" pymysql sqlalchemy pyarrow

echo "== versions =="
echo "apitap : $(~/va_apitap/bin/python -c 'import apitap; print(getattr(apitap,"__version__","ok"))')"
echo "ingestr: $(~/va_ingestr/bin/ingestr --version)"
echo "dlt    : $(~/va_dlt/bin/python -c 'import dlt,pyarrow; print(dlt.version.__version__, "pyarrow", pyarrow.__version__)')"
