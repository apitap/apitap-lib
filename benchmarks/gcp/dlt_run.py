#!/usr/bin/env python3
"""Minimal dlt runner used by the GCP benchmark harnesses.

Runs a single full-table copy with dlt's sql_database source. Handles the two
non-obvious things a stock benchmark hits:

  * MySQL sources need an explicit SQLAlchemy driver (mysql+pymysql://).
  * dlt's ClickHouse destination defaults to TLS (native 9000 secure, HTTP
    8443). A stock, non-TLS ClickHouse needs secure=0 AND http_port=8123, and
    dlt only honours those from an explicit credentials dict (not the URL
    query string).

Usage:
  dlt_run.py SRC_URI (pg|ch) DST_URI DATASET TABLE [BACKEND] [SOURCE_SCHEMA]

  BACKEND        "" (default SQLAlchemy) or "pyarrow"
  SOURCE_SCHEMA  "public" for Postgres, the database name for MySQL ("bench")
"""
import sys
import shutil
from urllib.parse import urlparse

import dlt
from dlt.sources.sql_database import sql_database

src_uri, kind, dst_uri, dataset, table, backend = sys.argv[1:7]
schema = sys.argv[7] if len(sys.argv) > 7 else "public"

# Fresh pipeline working dir every run — otherwise stale load packages from a
# previous (possibly failed) run get retried and error out.
pdir = "/tmp/dltpipe"
shutil.rmtree(pdir, ignore_errors=True)

if src_uri.startswith("mysql://"):
    src_uri = "mysql+pymysql://" + src_uri[len("mysql://"):]

kw = dict(schema=schema, table_names=[table])
if backend:
    kw["backend"] = backend
source = sql_database(src_uri, **kw)

if kind == "pg":
    dest = dlt.destinations.postgres(credentials=dst_uri)
else:
    u = urlparse(dst_uri)
    dest = dlt.destinations.clickhouse(credentials={
        "host": u.hostname,
        "port": u.port or 9000,      # native protocol
        "http_port": 8123,           # stock CH plaintext HTTP (dlt default 8443 = HTTPS)
        "username": u.username,
        "password": u.password,
        "database": (u.path.lstrip("/") or "default"),
        "secure": 0,                 # stock CH has no TLS
    })

pipe = dlt.pipeline(pipeline_name="benchp", destination=dest,
                    dataset_name=dataset, pipelines_dir=pdir)
pipe.run(source, write_disposition="replace", table_name=table)
