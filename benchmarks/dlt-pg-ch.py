#!/usr/bin/env python3
"""dlt runner: sql_database (pyarrow backend) -> ClickHouse, for the 100 GB showdown.
Usage: dlt-pg-ch.py SRC_URI DST_URI DATASET TABLE [BACKEND]"""
import sys, shutil
import dlt
from dlt.sources.sql_database import sql_database

src_uri, dst_uri, dataset, table = sys.argv[1:5]
backend = sys.argv[5] if len(sys.argv) > 5 else ""

pdir = "/tmp/dltpipe"
shutil.rmtree(pdir, ignore_errors=True)

kw = dict(schema="public", table_names=[table])
if backend:
    kw["backend"] = backend
source = sql_database(src_uri, **kw)

dest = dlt.destinations.clickhouse(credentials=dst_uri)
pipe = dlt.pipeline(pipeline_name="benchp", destination=dest,
                    dataset_name=dataset, pipelines_dir=pdir)
info = pipe.run(source, write_disposition="replace", table_name=table)
print(info)
