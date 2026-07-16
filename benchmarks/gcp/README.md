# Reproduce the GCP dedicated-instance benchmark

Everything needed to re-run the benchmark written up in
[`../gcp-benchmark.md`](../gcp-benchmark.md) — three separate machines (source
DB, tool host, destination DB) talking over **internal IPs**, every tool run
uncapped, every result checksum-validated. If a number there looks unfair, this
is the harness that produced it; it is small enough to read in one sitting.

## What you need

- A GCP project with Compute Engine enabled and `gcloud` authenticated
  (`gcloud auth login`). Pin the project on every command or export
  `PROJECT=<id>` as the scripts expect.
- These files (all in this directory) plus `../seed.sql` and `../seed_mysql.sql`.
- Heads-up on cost: two `n2-highcpu-32` + one `n2-highcpu-16` in `us-east1-b`.
  Delete them the moment a round finishes — see `teardown.sh`.

## Round A — Postgres source (PG→PG, PG→ClickHouse)

```bash
export PROJECT=your-project ZONE=us-east1-b
./provision.sh                       # prints the internal IPs — note them

# copy the repo's seeds + these scripts to each VM (scp), then:
# on bench-source:
ROLE=source FAMILY=pg ./setup-dbs.sh
# on bench-dest:
ROLE=dest ./setup-dbs.sh
# on bench-ingest (uses the released `pip install apitap`):
./setup-ingest.sh

# on bench-ingest — SRC_IP/DST_IP are the internal IPs from provision.sh:
SRC_IP=10.a.b.c DST_IP=10.d.e.f ./bench_a.sh bench_data_1m bench_data_10m

./teardown.sh                        # delete Round A VMs before Round B
```

## Round B — MySQL source (MySQL→MySQL, MySQL→ClickHouse)

MySQL→MySQL is not on PyPI yet, so Round B installs apitap from a locally-built
wheel of the `feat/mysql-sink` branch. Build it with `../pgo-build.sh` (or a
plain `maturin build --release -m py-apitap/Cargo.toml`) and copy the `.whl` to
bench-ingest.

```bash
export PROJECT=your-project ZONE=us-east1-b
./provision.sh

# on bench-source:
ROLE=source FAMILY=mysql ./setup-dbs.sh
# on bench-dest:
ROLE=dest ./setup-dbs.sh
# on bench-ingest (point at the branch wheel):
APITAP_WHEEL=~/apitap-0.5.0-*.whl ./setup-ingest.sh

# on bench-ingest:
SRC_IP=10.a.b.c DST_IP=10.d.e.f ./bench_b.sh bench_my_1m bench_my_10m

./teardown.sh
```

## Reading the output

Each run prints one line:

```
RESULT|apitap PG->PG   |bench_data_1m|wall=0:03.97|cpu=33%|mem=323MB|rc=0|MATCH
```

- **wall** — `/usr/bin/time -v` Elapsed (real time).
- **cpu** — average Percent-of-CPU over the run (>100 % = multi-core).
- **mem** — peak resident set size of the tool process.
- **MATCH / MISMATCH / CAPPED>5min** — destination checksum vs source
  (`count | sum(id) | sum(big_int) | sum(len(medium_str))`). Float columns are
  excluded because MySQL and Postgres round `/` differently. `CAPPED>5min`
  means the run hit the 5-minute hard cap (`timeout -s KILL 300`) and was
  killed — it did not finish.

## Notes / gotchas baked into the scripts

- **No DB tuning.** All containers run image defaults. The one exception is
  MySQL's `local_infile=1` — the standard bulk-load switch (off by default);
  `LOAD DATA LOCAL INFILE` is the only bulk path into MySQL and every tool needs
  it.
- **dlt → ClickHouse.** dlt's clickhouse destination defaults to TLS (native
  9000 secure, HTTP 8443). Against a stock non-TLS server it fails until you set
  `secure=0` **and** `http_port=8123` — `dlt_run.py` does this via an explicit
  credentials dict (dlt ignores those in the URL query string).
- **dlt has no MySQL destination**, so MySQL→MySQL is apitap vs ingestr only.
- **dlt is run per pipeline with a fresh `pipelines_dir`** and the destination's
  `dltbench___*` tables dropped first; otherwise stale load-state from a prior
  run makes it fail on the next.
