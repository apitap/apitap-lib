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
import os
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (script, what it proves, extra env, requires, argv)
#
# `requires` is a capability name, not a container name: the point is to say
# WHY a leg cannot run, in words the operator can act on.
#
# `argv` is why the leg count is larger than the script count: a few scripts
# take a selector and ARE several legs. e2e_logbased_dests.py is three — the
# same drain into ClickHouse, MySQL and Iceberg — and running it bare is not a
# lighter version of the gate, it is a crash (IndexError on sys.argv[1]). The
# first draft of this file ran it bare and reported a FAIL that was mine.
LEGS = [
    ("e2e_failure_modes.py",    "what a killed run leaves behind",            {}, None, []),
    ("e2e_concurrent_runs.py",  "two runs of one table cannot collide",       {}, None, []),
    ("e2e_replace_hazards.py",  "replace never publishes a partial table",    {}, None, []),
    ("e2e_state_contract.py",   "_apitap_state means one thing in both lanes", {}, None, []),
    ("e2e_long_names.py",       "a name at the identifier limit is safe",     {}, None, []),
    ("e2e_url_errors.py",       "bad URLs fail at probe, not mid-copy",       {}, None, []),
    ("e2e_progress.py",         "the progress record says what it means",     {}, None, []),
    ("e2e_read.py",             "read() -> Arrow/polars, typed end to end",   {}, None, []),
    ("e2e_savepoint.py",        "a streamed savepoint rolls back for real",   {}, None, []),
    ("e2e_http_deadline.py",    "HTTP deadlines bound a stuck destination",   {}, None, []),

    ("e2e_logbased.py",         "Postgres CDC: every operation the WAL saw",  {}, None, []),
    ("e2e_logbased_dests.py",   "the same drain into ClickHouse",             {}, None, ["ch"]),
    ("e2e_logbased_dests.py",   "the same drain into MySQL",                  {}, None, ["my"]),
    ("e2e_logbased_dests.py",   "the same drain into Iceberg",                {}, "iceberg", ["ice"]),
    ("e2e_logbased_multi.py",   "many tables share ONE replication slot",     {}, None, []),
    ("e2e_cdc_types.py",        "bootstrap and drain agree on every type",    {}, None, []),
    ("e2e_cdc_retention.py",    "a schedule paused past retention is refused", {}, None, []),
    ("e2e_toast_rekey.py",      "a key-changing UPDATE keeps its TOAST cols", {}, None, []),
    ("e2e_partitioned.py",      "a partitioned table replicates at all",      {}, None, []),
    ("e2e_sigterm.py",          "SIGTERM lands the window in flight",         {}, None, []),
    ("e2e_sigterm_my.py",       "the same, on the MySQL lane",                {}, None, []),

    ("e2e_mysql84.py",          "MySQL 8.4 both ways",                        {}, "mysql84", []),
    ("e2e_mariadb_cdc.py",      "MariaDB binlog as a CDC source",             {}, "mariadb", []),
    ("e2e_my_liveness.py",      "a dead binlog peer is noticed",              {}, None, []),

    ("e2e_ch_source.py",        "ClickHouse -> ClickHouse, RowBinary relayed", {}, None, []),
    ("e2e_ch_cluster.py",       "a replicated destination is refused, not scattered", {}, "ch-cluster", []),
    ("e2e_ch_body_cap.py",      "APITAP_CH_MAX_BODY for proxied ClickHouse",  {}, "ch-proxy", []),
    ("e2e_changelog_ch.py",     "changelog=True on ClickHouse",               {}, None, []),
    ("e2e_changelog_my.py",     "changelog=True on MySQL",                    {}, None, []),

    ("e2e_tls.py",              "Postgres TLS, verified not just offered",    {}, "tls-pg", []),
    ("e2e_tls_mysql.py",        "MySQL TLS, same",                            {}, "tls-my", []),

    ("e2e_review_gate.py",      "the findings of the 0.42.0 review, as proofs", {}, None, []),

    ("e2e_bq_cdc.py",           "CDC into BigQuery via staging + MERGE",      {}, "bq", []),
    ("e2e_changelog_bq.py",     "changelog=True on BigQuery",                 {}, "bq", []),
    ("e2e_changelog_group.py",  "changelog partition/order overrides",        {}, "bq", []),
    ("e2e_changelog_percolumn.py", "per-column changelog config",             {}, "bq", []),
]


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
    return have, why


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
    args = ap.parse_args()

    have, why = capabilities()
    legs = [l for l in LEGS if not args.only or args.only in l[0]]

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
        for script, what, _, req, argv in legs:
            mark = "run " if (req is None or req in have) else "SKIP"
            label = f"{script} {' '.join(argv)}".strip()
            print(f"  {mark}  {label:<38} {what}")
        return 0

    passed, failed, skipped, started = [], [], [], time.time()
    for i, (script, what, extra, req, argv) in enumerate(legs, 1):
        label = f"{script} {' '.join(argv)}".strip()
        if req is not None and req not in have:
            skipped.append((label, req))
            print(f"[{i:2}/{len(legs)}] SKIP {label:<38} needs {req}")
            continue
        env = {**os.environ, **extra}
        t0 = time.time()
        r = subprocess.run([sys.executable, os.path.join("benchmarks", script), *argv],
                           cwd=REPO, env=env, capture_output=True, text=True,
                           timeout=args.timeout)
        dt = time.time() - t0
        tail = (r.stdout.strip().splitlines() or [""])[-1][:90]
        if r.returncode == 0:
            passed.append(label)
            print(f"[{i:2}/{len(legs)}] PASS {label:<38} {dt:6.1f}s  {tail}")
        else:
            failed.append((label, r))
            print(f"[{i:2}/{len(legs)}] FAIL {label:<38} {dt:6.1f}s  {tail}")

    print(f"\n{'='*70}")
    print(f"gate: {len(passed)} passed, {len(failed)} failed, {len(skipped)} skipped "
          f"in {time.time()-started:.0f}s")
    for script, req in skipped:
        print(f"  SKIPPED {script} (needs {req}) — this gate is PARTIAL")
    for script, r in failed:
        print(f"\n--- {script} ---")
        for line in (r.stdout or "").strip().splitlines()[-15:]:
            print(f"    {line}")
        for line in (r.stderr or "").strip().splitlines()[-8:]:
            print(f"  ! {line}")
    # A partial gate is not a green gate: exit non-zero so a release script
    # cannot read "no failures" as "everything ran".
    return 1 if failed else (3 if skipped else 0)


if __name__ == "__main__":
    sys.exit(main())
