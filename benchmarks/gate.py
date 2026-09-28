#!/usr/bin/env python3
"""Run the release gate: every e2e leg, in one command, with one verdict.

Until now the gate was a list in someone's head — `docs/stability.md` names
"the release gate runs itself" as the first thing 1.0 waits on, and the reason
it does not is that starting it meant remembering 30-odd script names and which
rig each one needs. This does not make CI run it, but it removes the
remembering, and it makes a partial gate impossible to mistake for a full one.

    python3 benchmarks/gate.py                # everything the rig can run
    python3 benchmarks/gate.py --only tls     # legs whose name matches
    python3 benchmarks/gate.py --list         # what would run, and why not

Run it from the repo root on the bench VPS, against the containers
`benchmarks/run-server.sh` brings up, with the release wheel installed in the
interpreter you invoke it with:

    ~/gate-venv/bin/python benchmarks/gate.py

**A skipped leg is a reported leg.** Cloud legs need `BQ_SA`; if it is unset
they are listed as SKIP in the summary and the exit code still reflects that
the gate was partial. A gate that quietly runs 30 of 44 and prints "all green"
is worse than no gate, because it reads like proof.
"""
import argparse
import collections
import glob
import os
import re
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "benchmarks"))

import _claims  # noqa: E402  (benchmarks/ is not a package)

# One leg: a script, what it proves in words, extra env, the capabilities it
# needs, its argv, and the CLAIMS cells it proves.
#
# `requires` is a set of capability names, not container names: the point is to
# say WHY a leg cannot run, in words the operator can act on. A leg runs only
# when every capability it names is present.
#
# `argv` is why the leg count is larger than the script count: a few scripts
# take a selector and ARE several legs. e2e_logbased_dests.py is three — the
# same drain into ClickHouse, MySQL and Iceberg — and running it bare is not a
# lighter version of the gate, it is a crash (IndexError on sys.argv[1]). The
# first draft of this file ran it bare and reported a FAIL that was mine.
#
# `proves` maps a claim id (benchmarks/_claims.py) to the engines this leg
# proves it on, and may not be empty: a leg that backs no named claim backs its
# own regression claim, `leg.<stem>[.<argv>]`, listed in benchmarks/README.md.
Leg = collections.namedtuple("Leg", "script what env requires argv proves")


def leg(script, what, requires=(), argv=(), proves=None, env=None):
    argv = list(argv)
    return Leg(script, what, env or {}, frozenset(requires), argv,
               proves or {_claims.leg_claim_id(script, argv): (_claims.LEG_ENGINE,)})


LEGS = [
    leg("e2e_failure_modes.py",    "what a killed run leaves behind"),
    leg("e2e_concurrent_runs.py",  "two runs of one table cannot collide",
        proves={"guard.bulk-vs-bulk": ("pg",)}),
    leg("e2e_replace_hazards.py",  "replace never publishes a partial table"),
    leg("e2e_state_contract.py",   "_apitap_state means one thing in both lanes",
        proves={"state.cross-lane-refusal": ("pg",)}),
    leg("e2e_long_names.py",       "a name at the identifier limit is safe"),
    leg("e2e_url_errors.py",       "bad URLs fail at probe, not mid-copy"),
    leg("e2e_progress.py",         "the progress record says what it means"),
    leg("e2e_read.py",             "read() -> Arrow/polars, typed end to end"),
    leg("e2e_savepoint.py",        "a streamed savepoint rolls back for real"),
    leg("e2e_http_deadline.py",    "HTTP deadlines bound a stuck destination"),

    leg("e2e_logbased.py",         "Postgres CDC: every operation the WAL saw",
        proves={"truncate.reaches-dest": ("pg-src",)}),
    leg("e2e_logbased_dests.py",   "the same drain into ClickHouse", argv=["ch"]),
    leg("e2e_logbased_dests.py",   "the same drain into MySQL", argv=["my"]),
    leg("e2e_logbased_dests.py",   "the same drain into Iceberg", {"iceberg"}, ["ice"]),
    leg("e2e_logbased_multi.py",   "many tables share ONE replication slot"),
    leg("e2e_cdc_types.py",        "bootstrap and drain agree on every type"),
    leg("e2e_cdc_retention.py",    "a schedule paused past retention is refused"),
    leg("e2e_toast_rekey.py",      "a key-changing UPDATE keeps its TOAST cols"),
    leg("e2e_partitioned.py",      "a partitioned table replicates at all"),
    leg("e2e_sigterm.py",          "SIGTERM lands the window in flight",
        proves={"graceful.stop-releases-lock": ("ch(pg-src)",)}),
    leg("e2e_sigterm_my.py",       "the same, on the MySQL lane",
        proves={"graceful.stop-releases-lock": ("ch(my-src)",)}),

    leg("e2e_mysql84.py",          "MySQL 8.4 both ways", {"mysql84"},
        proves={"truncate.reaches-dest": ("my84-src",)}),
    leg("e2e_mariadb_cdc.py",      "MariaDB binlog as a CDC source", {"mariadb"},
        proves={"truncate.reaches-dest": ("maria-src",)}),
    leg("e2e_changelog_replay.py", "a replayed changelog window is not appended twice", {"mariadb"},
        proves={"changelog.replay-not-doubled": ("ch",)}),
    leg("e2e_cdc_guard.py",        "a drain and a bulk run refuse each other",
        proves={"guard.drain-vs-bulk": ("ch", "pg"), "guard.drain-vs-drain": ("ch", "pg")}),
    leg("e2e_bootstrap_lock.py",   "a bootstrap keeps the drain's lock to its last statement", argv=["pg"],
        proves={"bootstrap.keeps-lock": ("pg",), "guard.drain-vs-drain": ("pg",)}),
    leg("e2e_bootstrap_lock.py",   "a MySQL-source bootstrap keeps the drain's lock", {"mariadb"}, ["my"],
        proves={"bootstrap.keeps-lock": ("my",)}),
    leg("e2e_cdc_lease.py",        "a killed drain's lock clears itself",
        proves={"lease.killed-drain-self-heals": ("ch",), "lease.live-never-collected": ("ch",),
                "guard.no-lease-no-collect": ("ch",)}),
    leg("e2e_cdc_evict_ch.py",     "an evicted ClickHouse drain stays evicted", {"mariadb"},
        proves={"check.evicted-stops-within-statement": ("ch",), "ch.lease-survives-ttl-merge": ("ch",),
                "lease.killed-drain-self-heals": ("ch",)}),
    leg("e2e_cdc_fence_my.py",     "the MySQL lease, asked of the MySQL server",
        proves={"fence.evicted-writes-nothing": ("my",), "lease.killed-drain-self-heals": ("my",),
                "lease.live-never-collected": ("my",), "collect.claim-then-crash": ("my",)}),
    leg("e2e_my_gtid_dest.py",     "CDC into a GTID-enforced MySQL", {"my-gtid"},
        proves={"my.gtid-destination": ("my-gtid",)}),
    leg("e2e_cdc_fence.py",        "an evicted drain writes nothing more",
        proves={"fence.evicted-writes-nothing": ("pg",), "keeper.skips-own-held-row": ("pg",),
                "lease.killed-drain-self-heals": ("pg",)}),
    leg("e2e_cdc_apply_orphan.py", "a drain that loses its source joins its apply before letting go",
        {"cpu>0.6"}, proves={"cdc.apply-joined-on-error": ("pg",)}),
    leg("e2e_slots.py",            "slots=N: each group holds its own tenure",
        proves={"slots.groups-independent": ("pg",)}),
    leg("e2e_my_liveness.py",      "a dead binlog peer is noticed"),

    leg("e2e_ch_source.py",        "ClickHouse -> ClickHouse, RowBinary relayed"),
    leg("e2e_ch_cluster.py",       "a replicated destination is refused, not scattered", {"ch-cluster"}),
    leg("e2e_ch_body_cap.py",      "APITAP_CH_MAX_BODY for proxied ClickHouse", {"ch-proxy"}),
    leg("e2e_changelog_ch.py",     "changelog=True on ClickHouse"),
    leg("e2e_changelog_my.py",     "changelog=True on MySQL"),

    leg("e2e_tls.py",              "Postgres TLS, verified not just offered", {"tls-pg"}),
    leg("e2e_tls_mysql.py",        "MySQL TLS, same", {"tls-my"}),

    leg("e2e_review_gate.py",      "the findings of the 0.42.0 review, as proofs"),

    leg("e2e_guard_matrix.py",     "the guard asks Postgres the same questions", argv=["pg"],
        proves={"guard.drain-vs-bulk": ("pg",), "guard.no-lease-no-collect": ("pg",),
                "collect.claim-then-crash": ("pg",)}),
    leg("e2e_guard_matrix.py",     "the guard asks MySQL the same questions", argv=["my"],
        proves={"guard.bulk-vs-bulk": ("my",), "guard.drain-vs-bulk": ("my",),
                "guard.no-lease-no-collect": ("my",), "collect.claim-then-crash": ("my",)}),
    leg("e2e_guard_matrix.py",     "the guard asks ClickHouse the same questions", argv=["ch"],
        proves={"guard.bulk-vs-bulk": ("ch",), "guard.drain-vs-bulk": ("ch",),
                "guard.no-lease-no-collect": ("ch",), "collect.claim-then-crash": ("ch",)}),
    leg("e2e_guard_matrix.py",     "the guard asks S3 (MinIO) the same questions", {"iceberg"}, ["s3"],
        proves={"guard.bulk-vs-bulk": ("s3",)}),
    leg("e2e_guard_matrix.py",     "the guard asks Iceberg the same questions", {"iceberg"}, ["ice"],
        proves={"guard.bulk-vs-bulk": ("ice",)}),

    leg("e2e_bq_cdc.py",           "CDC into BigQuery via staging + MERGE", {"bq"}),
    leg("e2e_bq_guard.py",         "a BigQuery bulk run meets a drain's announcement", {"bq"},
        proves={"bq.bulk-meets-cdc-lock": ("bq",), "guard.no-lease-no-collect": ("bq",)}),
    leg("e2e_guard_matrix.py",     "the guard asks BigQuery the same questions", {"bq"}, ["bq"],
        proves={"guard.bulk-vs-bulk": ("bq",), "guard.drain-vs-bulk": ("bq",),
                "collect.claim-then-crash": ("bq",)}),
    leg("e2e_cdc_lease_bq.py",     "a BigQuery drain's lease and fence, asked of BigQuery", {"bq", "mariadb"},
        proves={"lease.killed-drain-self-heals": ("bq",), "lease.live-never-collected": ("bq",),
                "guard.no-lease-no-collect": ("bq",), "fence.evicted-writes-nothing": ("bq",)}),
    leg("e2e_bq_multi_drain.py",   "four drains in one dataset, and a transaction past the TTL", {"bq"},
        proves={"bq.multi-drain-one-dataset": ("bq",)}),
    leg("e2e_bq_state_compact.py", "a state compaction never erases a row a sibling committed", {"bq"},
        proves={"bq.multi-drain-one-dataset": ("bq",)}),
    leg("e2e_cdc_lease_bq.py",     "a 0.56.0 BigQuery victim beside this collector: the residual",
        {"bq", "mariadb", "wheel-0560"}, ["0560"], proves={"compat.bq-0560-victim": ("bq@0560",)}),

    leg("e2e_rolling_upgrade.py",  "0.55.1 beside this release, Postgres", {"wheel-0551"}, ["0551", "pg"],
        proves={"compat.old-bulk-refused-by-new-drain": ("pg@0551",),
                "compat.new-drain-refused-by-old-bulk": ("pg@0551",),
                "compat.old-drain-writes-nothing": ("pg@0551",),
                "compat.rollback-leaks": ("pg@0551",)}),
    leg("e2e_rolling_upgrade.py",  "0.55.1 beside this release, ClickHouse", {"wheel-0551"}, ["0551", "ch"],
        proves={"compat.old-bulk-refused-by-new-drain": ("ch@0551",),
                "compat.new-drain-refused-by-old-bulk": ("ch@0551",)}),
    leg("e2e_rolling_upgrade.py",  "0.55.1 beside this release, BigQuery", {"wheel-0551", "bq"}, ["0551", "bq"],
        proves={"compat.old-bulk-refused-by-new-drain": ("bq@0551",),
                "compat.new-drain-refused-by-old-bulk": ("bq@0551",)}),
    leg("e2e_rolling_upgrade.py",  "0.56.0 beside this release, Postgres", {"wheel-0560"}, ["0560", "pg"],
        proves={"compat.old-bulk-refused-by-new-drain": ("pg@0560",)}),
    leg("e2e_rolling_upgrade.py",  "0.56.0 beside this release, ClickHouse", {"wheel-0560"}, ["0560", "ch"],
        proves={"compat.old-bulk-refused-by-new-drain": ("ch@0560",)}),
    leg("e2e_rolling_upgrade.py",  "0.56.0 beside this release, BigQuery", {"wheel-0560", "bq"}, ["0560", "bq"],
        proves={"compat.old-bulk-refused-by-new-drain": ("bq@0560",)}),
    leg("e2e_changelog_bq.py",     "changelog=True on BigQuery", {"bq"}),
    leg("e2e_changelog_group.py",  "changelog partition/order overrides", {"bq"}),
    leg("e2e_changelog_percolumn.py", "per-column changelog config", {"bq"}),
]


def label_of(l):
    return f"{l.script} {' '.join(l.argv)}".strip()


# Uppercase FAILED is only ever a verdict in these legs — checked across all 34
# at the time of writing, the sole other occurrence is inside a module docstring
# (e2e_long_names.py), which is never printed. Lowercase "failed" IS ordinary
# prose ("✓ failed as the topology dictates") so the match is case-sensitive and
# word-bounded.
_VERDICT_FAILED = re.compile(r"\bFAILED\b")


def leg_verdict(returncode, stdout):
    """PASS/FAIL for one leg, and WHY — the whole rule, in one testable place.

    Returncode alone was the rule until 0.55.1, and four changelog legs printed
    their verdict without ever exiting on it: `ok` was computed, FAILED was
    printed, the process exited 0, and the gate recorded PASS. Those four now
    exit properly, but a leg written tomorrow can forget again, so the gate no
    longer trusts the exit code by itself. A leg that SAYS it failed has failed,
    whatever it returns.

    Returns (ok: bool, why: str) — `why` is empty when the two agree.
    """
    said_failed = bool(_VERDICT_FAILED.search(stdout or ""))
    if returncode != 0:
        return False, ""
    if said_failed:
        return False, ("exited 0 but its output says FAILED — the leg is not "
                       "turning its verdict into an exit code")
    return True, ""


def wheel_status(py, want):
    """(ok, why) for an interpreter that must hold apitap==`want` — and must NOT
    be the gate's own install. An upgrade leg run with OLD == NEW compares a
    wheel with itself and passes whatever the code does, so the second check is
    the one that matters."""
    r = subprocess.run([py, "-c", "import apitap,os;print(apitap.__version__);"
                        "print(os.path.dirname(os.path.realpath(apitap.__file__)))"],
                       capture_output=True, text=True)
    got, where = (r.stdout.split() + ["", ""])[:2]
    import apitap as mine
    if r.returncode or got != want:
        return False, f"has apitap {got or '(none)'}, want {want}"
    if where == os.path.dirname(os.path.realpath(mine.__file__)):
        return False, (f"resolves to the gate's own apitap at {where} — an upgrade leg "
                       "would compare a wheel with itself")
    return True, ""


def cgroup_cores():
    """The CPU this process may actually use: the cgroup quota when one is set."""
    q = open("/sys/fs/cgroup/cpu.max").read().split() \
        if os.path.exists("/sys/fs/cgroup/cpu.max") else ["max"]
    return os.cpu_count() if q[0] == "max" else int(q[0]) / int(q[1])


def capabilities():
    """What this box can actually exercise. Reported, never silently assumed."""
    have, why = set(), {}
    running = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                             capture_output=True, text=True).stdout.split()
    def need(cap, container, hint):
        if container in running:
            have.add(cap)
        else:
            why[cap] = f"container {container} is not running — {hint}"
    need("mysql84",    "apitap-bench-my84",    "benchmarks/run-server.sh brings it up")
    need("mariadb",    "apitap-bench-mariadb", "benchmarks/run-server.sh brings it up")
    need("ch-cluster", "apitap-bench-ch-a",    "the 2-node ClickHouse + keeper set")
    need("ch-proxy",   "apitap-bench-chproxy", "the body-cap proxy")
    need("tls-pg",     "apitap-tls-pg",        "the TLS-only Postgres")
    need("tls-my",     "apitap-tls-my",        "the TLS-only MySQL")
    need("iceberg",    "apitap-bench-icecat",  "the Iceberg REST catalog + MinIO")
    if os.environ.get("BQ_SA"):
        have.add("bq")
    else:
        why["bq"] = "BQ_SA is unset — export it to the service-account JSON path"

    # The previous releases, for the upgrade and rollback legs. The gate never
    # installs anything: ~/gate-0551-venv and ~/gate-0560-venv are prepared
    # once from PyPI (benchmarks/README.md, "The release gate").
    def need_wheel(cap, env, want):
        py = os.environ.get(env)
        if not py:
            why[cap] = f"{env} is unset — point it at a venv python holding apitap=={want}"
            return
        ok, reason = wheel_status(py, want)
        if ok:
            have.add(cap)
        else:
            why[cap] = f"{env} {reason}"
    need_wheel("wheel-0551", "APITAP_PY_0551", "0.55.1")
    need_wheel("wheel-0560", "APITAP_PY_0560", "0.56.0")

    cpu = cgroup_cores()
    if cpu > 0.6:
        have.add("cpu>0.6")
    else:
        why["cpu>0.6"] = f"cgroup quota {cpu:.2f} core"
    if (os.cpu_count() or 1) >= 4:
        have.add("cores>=4")
    else:
        why["cores>=4"] = f"{os.cpu_count()} cores"
    if os.environ.get("APITAP_MY_GTID_URL"):
        have.add("my-gtid")
    else:
        why["my-gtid"] = ("APITAP_MY_GTID_URL unset — a GTID-enforced MySQL "
                          "(apitap-bench-my-gtid, benchmarks/run-server.sh)")
    return have, why


# ── the claims matrix ────────────────────────────────────────────────────

GAP = "GAP"
MARKER = re.compile(r"<!-- claim: (\S+) -->")


def claim_docs():
    """Where claim markers may live. docs/review/ is excluded on purpose: the
    reviews QUOTE the markers they ask for, and a quote is not a claim."""
    out = [os.path.join(REPO, "README.md"), os.path.join(REPO, "py-apitap", "README.md"),
           os.path.join(REPO, "benchmarks", "README.md")]
    out += sorted(glob.glob(os.path.join(REPO, "docs", "*.md")))
    out += sorted(glob.glob(os.path.join(REPO, "docs", "design", "*.md")))
    return [p for p in out if os.path.exists(p)]


def cells(legs, claims):
    """{(claim, engine): [leg labels] | Waiver | GAP} for every cell of `claims`."""
    out = {}
    for cid, c in claims.items():
        for eng in c.engines:
            provers = [label_of(l) for l in legs if eng in l.proves.get(cid, ())]
            out[(cid, eng)] = provers or _claims.WAIVED.get((cid, eng), GAP)
    return out


def paragraphs(paths):
    """{claim id: [(path, paragraph)]}: the text from each marker to the next
    blank line or the next marker."""
    out = collections.defaultdict(list)
    for p in paths:
        lines = open(p, encoding="utf-8").read().split("\n")
        text = "\n".join(lines)
        for m in MARKER.finditer(text):
            rest = text[m.end():]
            nxt = MARKER.search(rest)
            end = min([i for i in (rest.find("\n\n"), nxt.start() if nxt else -1) if i >= 0],
                      default=len(rest))
            out[m.group(1)].append((os.path.relpath(p, REPO), rest[:end]))
    return out


def matrix_problems(legs, claims, docs):
    """Everything that makes the matrix fail, as sentences. Empty = sound."""
    bad = []
    for l in legs:
        if not l.proves:
            bad.append(f"{label_of(l)}: proves nothing")
        for cid, engs in l.proves.items():
            if cid not in claims:
                bad.append(f"{label_of(l)}: proves unknown claim {cid}")
                continue
            for e in engs:
                if e not in claims[cid].engines:
                    bad.append(f"{label_of(l)}: proves {cid} on {e}, which the claim does not name")
    for (cid, eng), v in sorted(cells(legs, claims).items()):
        if v == GAP:
            bad.append(f"GAP: {cid} on {eng} — no leg proves it and no waiver says why")
    paras = paragraphs(docs)
    for cid in sorted(paras):
        if cid not in claims:
            where = ", ".join(sorted({p for p, _ in paras[cid]}))
            bad.append(f"unknown claim {cid} in {where} — the docs claim something the gate does not know")
    for cid, c in sorted(claims.items()):
        found = paras.get(cid, [])
        if not found:
            bad.append(f"dead claim {cid}: no <!-- claim: {cid} --> marker in the docs ({c.doc})")
            continue
        for path, para in found:
            for phrase in c.must_say:
                if phrase not in para:
                    bad.append(f"{cid} in {path}: the paragraph no longer says {phrase!r}")
        for (wid, eng), w in _claims.WAIVED.items():
            if wid == cid and not any(w.caveat in para for _, para in found):
                bad.append(f"{cid} on {eng} is WAIVED but no paragraph publishes the caveat "
                           f"{w.caveat!r}")
    return bad


def print_matrix(table, status=None):
    """One line per cell. `status` (after a run) maps a leg label to PASS, FAIL
    or SKIP, and the cell shows the verdict of the legs that prove it."""
    for (cid, eng), v in sorted(table.items()):
        if isinstance(v, _claims.Waiver):
            print(f"  WAIVED {cid:<40} {eng:<11} {v.caveat}")
        elif v == GAP:
            print(f"  GAP    {cid:<40} {eng:<11}")
        elif status is None:
            print(f"  ok     {cid:<40} {eng:<11} {', '.join(v)}")
        else:
            got = [status[x] for x in v if x in status]
            cell = "FAIL" if "FAIL" in got else "SKIP" if got and all(g == "SKIP" for g in got) \
                else "PASS"
            print(f"  {cell:<6} {cid:<40} {eng:<11} {', '.join(v)}")


def run_matrix(legs, docs=None):
    """`--matrix`: print every cell, then the problems. Exit 3 on any."""
    claims = _claims.with_leg_claims(legs)
    print_matrix(cells(legs, claims))
    bad = matrix_problems(legs, claims, claim_docs() if docs is None else docs)
    gaps = sum(1 for b in bad if b.startswith("GAP"))
    print(f"\nmatrix: {len(cells(legs, claims))} cells, {gaps} GAP, "
          f"{len(bad) - gaps} other problem(s)")
    for b in bad:
        print(f"  !! {b}")
    return 3 if bad else 0


def self_test():
    """Prove the gate can record a FAIL. Run it after touching leg_verdict().

    A gate is a claim about other code, and this file spent a release unable to
    fail four of its legs. The unit cases pin the rule; the end-to-end case
    spawns a real leg that prints FAILED and exits 0 — the exact shape that
    slipped through — and asserts the gate calls it FAIL.
    """
    import tempfile
    bad = 0

    def case(label, got, want):
        nonlocal bad
        if got != want:
            bad += 1
            print(f"  XX {label}: got {got!r}, want {want!r}")
        else:
            print(f"  OK {label}")

    print("gate self-test: the rule")
    case("clean exit, clean output -> pass",
         leg_verdict(0, "CH CHANGELOG E2E: ALL GREEN")[0], True)
    case("non-zero exit -> fail",
         leg_verdict(1, "CH CHANGELOG E2E: ALL GREEN")[0], False)
    case("exit 0 but printed FAILED -> fail",
         leg_verdict(0, "MYSQL CHANGELOG E2E: FAILED")[0], False)
    case("...and says why",
         "not turning its verdict into an exit code" in leg_verdict(0, "X: FAILED")[1], True)
    # The two shapes that must NOT trip it, or the gate cries wolf and gets
    # ignored — which is how a real failure hides.
    case("lowercase prose is not a verdict",
         leg_verdict(0, "   OK failed as the topology dictates")[0], True)
    case("FAILURE-MODE ... ALL GREEN is not a verdict",
         leg_verdict(0, "FAILURE-MODE E2E: ALL GREEN")[0], True)

    print("gate self-test: end to end")
    with tempfile.TemporaryDirectory() as d:
        leg = os.path.join(d, "e2e_selftest_liar.py")
        with open(leg, "w") as fh:
            fh.write('print("LIAR E2E: FAILED")\n')          # prints failure, exits 0
        r = subprocess.run([sys.executable, leg], capture_output=True, text=True)
        case("a leg that prints FAILED and exits 0 is recorded FAIL",
             leg_verdict(r.returncode, r.stdout)[0], False)
        case("(rig) that leg really did exit 0", r.returncode, 0)

    print("gate self-test: the legs and the claims")
    missing = [l.script for l in LEGS if not os.path.exists(os.path.join(REPO, "benchmarks", l.script))]
    case("every leg's script exists", missing, [])
    claims = _claims.with_leg_claims(LEGS)
    wiring = [b for b in matrix_problems(LEGS, claims, [])
              if not b.startswith(("GAP", "dead claim"))]
    case("every proves key is a claim, every engine one the claim names", wiring, [])

    # The matrix's own RED control. The cell is the one 0.56.0 shipped without
    # a proof: a killed BigQuery drain's lock clearing itself. The probe leg
    # stands in for e2e_cdc_lease_bq.py until that leg exists, so the control
    # tests the matrix, not the state of the tree.
    cell = ("lease.killed-drain-self-heals", "bq")
    probe = Leg("e2e_cdc_lease_bq.py", "a killed BigQuery drain clears itself", {},
                frozenset({"bq"}), [], {cell[0]: (cell[1],)})
    with_probe = LEGS if any(l.script == probe.script for l in LEGS) else LEGS + [probe]
    without = [l for l in with_probe if l.script != probe.script]
    case("matrix: without e2e_cdc_lease_bq.py, (self-heals, bq) is a GAP",
         cells(without, claims)[cell], GAP)
    case("matrix: with it, the cell is proven", cells(with_probe, claims)[cell] != GAP, True)
    gap_line = f"GAP: {cell[0]} on {cell[1]}"
    case("matrix: that GAP is a problem, so --matrix exits 3",
         any(b.startswith(gap_line) for b in matrix_problems(without, claims, [])), True)

    with tempfile.TemporaryDirectory() as d:
        def doc(name, text):
            path = os.path.join(d, name)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
            return path
        bogus = doc("bogus.md", "<!-- claim: bogus --> The docs promise something.\n")
        case("a marker the gate does not know fails",
             any(b.startswith("unknown claim bogus") for b in matrix_problems(LEGS, claims, [bogus])),
             True)
        lost = doc("lost.md", "<!-- claim: guard.bulk-vs-bulk --> The second run waits.\n\n"
                              "It is refused, but in the NEXT paragraph, which does not count.\n")
        case("a paragraph that lost a must_say phrase fails",
             any(b.startswith("guard.bulk-vs-bulk in") and "'refused'" in b
                 for b in matrix_problems(LEGS, claims, [lost])), True)
        kept = doc("kept.md", "<!-- claim: guard.bulk-vs-bulk --> The second run is refused. "
                              + _claims.WAIVED[("guard.bulk-vs-bulk", "gcs")].caveat + "\n")
        case("...and the same paragraph WITH the phrase and the caveat passes",
             [b for b in matrix_problems(LEGS, claims, [kept]) if b.startswith("guard.bulk-vs-bulk")],
             [])
        bare = doc("bare.md", "<!-- claim: guard.bulk-vs-bulk --> The second run is refused.\n")
        case("a waived cell whose caveat is not published fails",
             any(b.startswith("guard.bulk-vs-bulk on gcs is WAIVED")
                 for b in matrix_problems(LEGS, claims, [bare])), True)

    print("gate self-test: the old-release interpreters")
    try:
        import apitap
        ok, why = wheel_status(sys.executable, apitap.__version__)
        case("need_wheel refuses the gate's own install", ok, False)
        case("...and says it would compare a wheel with itself",
             "compare a wheel with itself" in why, True)
    except ImportError as e:
        case("apitap imports in the gate's interpreter", str(e), "")

    print(f"\ngate self-test: {'PASSED' if not bad else str(bad) + ' CASES WRONG'}")
    return 0 if not bad else 1


def main():
    # Line-buffered, because this runs redirected to a log for tens of minutes
    # and a silent log is indistinguishable from a hung one.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:                                    # pragma: no cover
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="substring filter on the leg's script name")
    ap.add_argument("--list", action="store_true", help="show the plan, run nothing")
    ap.add_argument("--timeout", type=int, default=3600, help="per-leg seconds")
    ap.add_argument("--self-test", action="store_true",
                    help="prove the gate can fail a leg, then exit")
    ap.add_argument("--matrix", action="store_true",
                    help="check every claim has a leg and a doc paragraph, run nothing; "
                         "the release pre-flight (exit 3 on any GAP or doc problem)")
    args = ap.parse_args()

    if args.self_test:
        return self_test()
    if args.matrix:
        return run_matrix(LEGS)

    have, why = capabilities()
    legs = [l for l in LEGS if not args.only or args.only in l.script]

    print(f"gate: {len(legs)} legs, python {sys.executable}")
    try:
        import apitap
        print(f"      apitap {apitap.__version__} from {os.path.dirname(apitap.__file__)}")
    except Exception as e:                                    # noqa: BLE001
        print(f"      !! apitap does not import: {e}")
        return 2
    for cap, reason in sorted(why.items()):
        print(f"      no {cap}: {reason}")
    print()

    if args.list:
        for l in legs:
            mark = "run " if l.requires <= have else "SKIP"
            print(f"  {mark}  {label_of(l):<38} {l.what}")
        return 0

    passed, failed, skipped, started = [], [], [], time.time()
    status = {}
    for i, l in enumerate(legs, 1):
        label = label_of(l)
        if not l.requires <= have:
            lacking = ", ".join(sorted(l.requires - have))
            skipped.append((label, lacking))
            status[label] = "SKIP"
            print(f"[{i:2}/{len(legs)}] SKIP {label:<38} needs {lacking}")
            continue
        env = {**os.environ, **l.env}
        t0 = time.time()
        r = subprocess.run([sys.executable, os.path.join("benchmarks", l.script), *l.argv],
                           cwd=REPO, env=env, capture_output=True, text=True,
                           timeout=args.timeout)
        dt = time.time() - t0
        tail = (r.stdout.strip().splitlines() or [""])[-1][:90]
        good, why_bad = leg_verdict(r.returncode, r.stdout)
        if good:
            passed.append(label)
            status[label] = "PASS"
            print(f"[{i:2}/{len(legs)}] PASS {label:<38} {dt:6.1f}s  {tail}")
        else:
            failed.append((label, r, why_bad))
            status[label] = "FAIL"
            print(f"[{i:2}/{len(legs)}] FAIL {label:<38} {dt:6.1f}s  {tail}")
            if why_bad:
                print(f"{'':9} ^^ {why_bad}")

    print(f"\n{'='*70}")
    print(f"gate: {len(passed)} passed, {len(failed)} failed, {len(skipped)} skipped "
          f"in {time.time()-started:.0f}s")
    for label, lacking in skipped:
        print(f"  SKIPPED {label} (needs {lacking}) — this gate is PARTIAL")
    for label, r, why_bad in failed:
        print(f"\n--- {label} ---")
        if why_bad:
            print(f"    !! {why_bad}")
        for line in (r.stdout or "").strip().splitlines()[-15:]:
            print(f"    {line}")
        for line in (r.stderr or "").strip().splitlines()[-8:]:
            print(f"  ! {line}")

    # The claims this run touched, cell by cell. A cell every prover of which
    # was skipped is a claim this gate did NOT check, whatever the leg count
    # says. GAP cells are `--matrix`'s business and do not change the exit.
    claims = _claims.with_leg_claims(LEGS)
    touched = {k: v for k, v in cells(legs, claims).items()
               if isinstance(v, list) and any(x in status for x in v)}
    print(f"\nclaims touched by this run ({len(touched)} cells):")
    print_matrix(touched, status)
    gaps = sum(1 for v in cells(LEGS, claims).values() if v == GAP)
    print(f"  ({gaps} GAP cell(s) in the whole matrix — `gate.py --matrix` lists them)")
    skip_cells = [k for k, v in touched.items() if all(status.get(x) == "SKIP" for x in v if x in status)]

    # A partial gate is not a green gate: exit non-zero so a release script
    # cannot read "no failures" as "everything ran".
    return 1 if failed else (3 if skipped or skip_cells else 0)


if __name__ == "__main__":
    sys.exit(main())
