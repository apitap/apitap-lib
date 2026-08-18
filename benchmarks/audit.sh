#!/usr/bin/env bash
# Supply-chain check over the whole dependency graph.
#
# apitap ships a COMPILED artifact to PyPI, so its dependency tree is not a
# development detail — every crate in it runs on the user's machine. This runs
# `cargo audit` against the locked graph (which is why `Cargo.lock` has to be
# current: an audit of a stale lock file audits software nobody is running).
#
# It is not part of the release gate, deliberately: an advisory can land on a
# day when nothing in this repo changed, and a gate that goes red for that is a
# gate people learn to override. Run it when releasing, and read it.
#
# Findings carried knowingly, as of 2026-08-18 — each is here because it has no
# fix available, not because nobody looked:
#
#   RUSTSEC-2023-0071  rsa 0.9.10, Marvin timing side-channel, severity 5.9.
#     Reaches us through sqlx-mysql, which uses it for MySQL's RSA password
#     exchange. No fixed version exists. apitap's own fast MySQL plane does not
#     use that exchange (it refuses full sha2 auth on an unverified channel
#     instead), so the exposure is limited to the sqlx lane's own handshake.
#
#   RUSTSEC-2024-0436  paste, unmaintained. Build-time macro only.
#   RUSTSEC-2026-0221  event-listener, unsound `!Send` tag across threads.
#   RUSTSEC-2026-0253  lru, use-after-free on panic in `pop()`.
#   spin 0.9.8, yanked.
#     All three arrive transitively; none is reachable from an apitap API.
#
# Anything NOT on that list is new and wants a decision, not a scroll past.
set -uo pipefail
cd "$(dirname "$0")/.."
REG=${CARGO_REG_VOLUME:-apitap-bench-cargo}
docker run --rm -v "$PWD":/io -w /io -v "$REG":/root/.cargo/registry \
  --entrypoint sh ghcr.io/pyo3/maturin -c \
  'cargo install cargo-audit --locked -q >/dev/null 2>&1; cargo audit'
