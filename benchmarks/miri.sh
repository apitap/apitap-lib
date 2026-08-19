#!/usr/bin/env bash
# Undefined behaviour in the unsafe code, checked by an interpreter that knows
# the aliasing rules the compiler is allowed to assume.
#
# apitap-core carries ~17 `unsafe` blocks, most of them in `wire/arrowcol.rs`,
# reading through offsets staged from a Postgres binary COPY stream. The tests
# and the torture harness prove those return the right ANSWERS; Miri proves
# they do not do so by breaking a rule the optimiser relies on — a class that
# never shows up as a wrong result until a compiler upgrade turns it into one.
#
# Verified to work, not assumed: planting `+ 1` on one staged offset in
# arrowcol makes this run fail immediately with an out-of-bounds read. A Miri
# run that is green because it silently skipped the code is worth nothing, so
# that canary is how this script was checked in.
#
# WHAT IT COVERS
#   The `wire::` modules — arrowcol, pgoutput, mybinlog, rowbinary — and the
#   buffer-fed torture harness. That is where every `unsafe` in this crate
#   lives, and running Miri over hostile input is stricter than running it over
#   the happy path.
#
#   The modules are named ONE BY ONE rather than filtered as `wire::` for a
#   concrete reason, not tidiness: anything touching parquet calls zstd, and
#   Miri cannot execute a foreign function. A run that aborts on
#   `ZSTD_createCCtx` has checked nothing at all — and it aborts the whole
#   run, so one unlisted module silently costs every module after it. An
#   explicit list fails loudly when a module is added and forgotten, which is
#   the failure mode worth having.
#
# WHAT IT CANNOT COVER, and why — so nobody reads a green run as more than it
# is:
#   * `py-apitap/src/capsule.rs`, the Arrow C Data Interface layer with the
#     other 16 unsafe blocks. It calls into libpython through FFI, which Miri
#     cannot execute. Its guard is the e2e capsule leg (gate 1) plus the
#     `unsafe_op_in_unsafe_fn` lint.
#   * The socket torture tests. Miri has no real network; they are excluded by
#     name rather than by silence.
#
# Slow by nature — the interpreter is roughly a hundred times slower than the
# machine — so this is run on demand, like the audit, and is not a gate leg.
set -uo pipefail
cd "$(dirname "$0")/.."
REG=${CARGO_REG_VOLUME:-apitap-bench-cargo}
docker run --rm -v "$PWD":/io -w /io -v "$REG":/root/.cargo/registry \
  --entrypoint sh ghcr.io/pyo3/maturin -c '
    rustup component add --toolchain nightly miri rust-src >/dev/null 2>&1
    for m in arrowcol pgoutput mybinlog rowbinary pgcopy mytsv pgmytsv torture; do
      echo "── wire::$m"
      cargo +nightly miri test -p apitap-core --lib "wire::$m" -- \
        --skip walsender_frames --skip mysql_packets --skip walsender_live || exit 1
    done
  '
