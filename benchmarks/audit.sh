#!/usr/bin/env bash
# Supply-chain check over the whole dependency graph.
#
# apitap ships a COMPILED artifact to PyPI, so its dependency tree is not a
# development detail — every crate in it runs on the user's machine. This runs
# `cargo audit` against the locked graph (which is why `Cargo.lock` has to be
# current: an audit of a stale lock file audits software nobody is running).
#
# ── This used to be kept OUT of the release gate, and now is in it ──────────
#
# The old reasoning was sound: an advisory can land on a day when nothing in
# this repo changed, and a gate that goes red for that is a gate people learn to
# override. What happened in practice was worse than the thing it avoided. The
# audit sat red for weeks with five findings, everyone scrolled past, and "red"
# stopped carrying information at all — which is the same failure, reached by a
# different road.
#
# What changed on 2026-08-20 is that red now MEANS something. Every carried
# finding is written down in `.cargo/audit.toml` with a reason that can be
# re-checked against the dependency's source, so the audit exits 0 today. It can
# only go red for something NEW — and a new advisory in a shipped binary is
# exactly the thing worth stopping a release for. Adding a documented ignore
# takes two minutes if that is the right call; the point is that it is a
# decision someone made, not a line someone scrolled past.
#
# ── Fixed rather than carried, 2026-08-20 ──────────────────────────────────
#
#   RUSTSEC-2026-0221  event-listener 5.4.1 -> 5.4.2  (unsound: !Send tags)
#   RUSTSEC-2026-0253  lru 0.18.1 -> 0.18.2           (unsound: UAF in pop())
#
# Both were patch bumps inside the existing semver range — nothing else in the
# graph moved. Look for that before carrying anything.
#
# ── Carried knowingly; the reasons live in .cargo/audit.toml ────────────────
#
#   RUSTSEC-2023-0071  rsa 0.9.10, Marvin timing side-channel, 5.9.
#     No fixed version exists. The vulnerable operation is not in our path:
#     Marvin recovers a PRIVATE key by timing DECRYPTION, and sqlx-mysql only
#     ever calls `RsaPublicKey::encrypt` — verified in its source, which imports
#     no `RsaPrivateKey` and contains no decrypt call. apitap holds no RSA
#     private key. (An earlier version of this note said the exposure was
#     "limited to the sqlx lane's handshake", which conceded more than the code
#     does.)
#   RUSTSEC-2024-0436  paste, unmaintained. Build-time macro; contributes no
#     code to the shipped binary.
#   spin 0.9.8, yanked. No advisory id, so nothing to ignore by; it stays a
#     warning, and warnings do not fail the audit.
#
# Anything NOT on that list is new and wants a decision.
set -uo pipefail
cd "$(dirname "$0")/.."
REG=${CARGO_REG_VOLUME:-apitap-bench-cargo}
BIN=${CARGO_AUDIT_VOLUME:-apitap-audit-bin}
# cargo-audit is cached in its own volume rather than rebuilt each run: it lives
# in CARGO_HOME/bin, which is not the registry volume, so `cargo install` on
# every invocation costs minutes and buys nothing.
docker run --rm -v "$PWD":/io -w /io \
  -v "$REG":/root/.cargo/registry -v "$BIN":/opt/auditbin \
  --entrypoint sh ghcr.io/pyo3/maturin -c '
    if [ ! -x /opt/auditbin/cargo-audit ]; then
      cargo install cargo-audit --root /opt/audit-tmp -q >/dev/null 2>&1 \
        && cp /opt/audit-tmp/bin/cargo-audit /opt/auditbin/
    fi
    PATH=/opt/auditbin:$PATH cargo audit'
