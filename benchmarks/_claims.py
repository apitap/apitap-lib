"""What the docs promise, and which engines each promise covers.

Until 0.57.0 the gate proved legs, not claims: a leg could be green while the
sentence it was supposed to back said something else, and a claim could name an
engine no leg ever touched. The v0.56.0 audit found both — BigQuery's apply
"read the lease" in the docs and in no code, and "a killed drain's lock clears
itself" held on Postgres and ClickHouse only.

So a claim is now a row here, a marker in the docs, and a set of legs:

  * `CLAIMS[id]` says which doc carries the claim, which engines it covers, and
    phrases the marked paragraph must contain (`must_say`);
  * the docs carry `<!-- claim: <id> -->` at the start of the paragraph;
  * `gate.py` legs declare `proves={id: (engine, ...)}`.

`gate.py --matrix` joins the three and fails on a cell no leg proves (GAP), a
marker the table does not know, a claim no doc carries, or a paragraph that has
lost its words. A cell nobody can prove on this rig is WAIVED here, with the
caveat that must be published beside the claim, verbatim.

The matrix checks that words are present, not that they are true. Review stays
human; this only makes a missing proof impossible to overlook.
"""
import collections

Claim = collections.namedtuple("Claim", "doc engines must_say")    # must_say: phrases the marked paragraph must contain
Waiver = collections.namedtuple("Waiver", "caveat why")             # caveat: published VERBATIM in that claim's paragraph

TWO_RUNS = "failure-modes.md#two-runs-one-table"
UPGRADE = "failure-modes.md#upgrading-and-rolling-back"
BQ_DEST = "usage.md#bigquery-destination"

CLAIMS = {
    # the concurrency guard
    "guard.bulk-vs-bulk":            Claim(TWO_RUNS, ("pg", "my", "ch", "bq", "s3", "gcs", "ice"), ("refused",)),
    "guard.drain-vs-bulk":           Claim(TWO_RUNS, ("pg", "my", "ch", "bq"), ("both directions",)),
    "guard.drain-vs-drain":          Claim(TWO_RUNS, ("pg", "my", "ch", "bq"), ("two drains",)),
    "guard.no-lease-no-collect":     Claim(TWO_RUNS, ("pg", "my", "ch", "bq"), ("nothing collects",)),
    "bootstrap.keeps-lock":          Claim(TWO_RUNS, ("pg", "my"), ("keeps its lock",)),

    # the lease, and where it is a fence
    "lease.killed-drain-self-heals": Claim(TWO_RUNS, ("pg", "my", "ch", "bq"), ("clears itself",)),
    "lease.live-never-collected":    Claim(TWO_RUNS, ("pg", "my", "ch", "bq"), ("renewed",)),
    "fence.evicted-writes-nothing":  Claim(TWO_RUNS, ("pg", "my", "bq"), ("Postgres", "MySQL", "BigQuery", "writes nothing")),
    "check.evicted-stops-within-statement": Claim(TWO_RUNS, ("ch",), ("ClickHouse", "one statement")),
    "ch.lease-survives-ttl-merge":   Claim(TWO_RUNS, ("ch",), ("merge",)),
    "collect.claim-then-crash":      Claim(TWO_RUNS, ("pg", "my", "ch", "bq"), ("next run finishes",)),
    "keeper.skips-own-held-row":     Claim(TWO_RUNS, ("pg", "my"), ("SKIP LOCKED",)),
    "cdc.apply-joined-on-error":     Claim(TWO_RUNS, ("pg",), ("nothing is written after",)),
    "graceful.stop-releases-lock":   Claim("usage.md#stopping-a-run-on-purpose", ("ch(pg-src)", "ch(my-src)"), ("takes its lock back",)),

    # apply contracts
    "state.cross-lane-refusal":      Claim("usage.md", ("pg", "my", "ch", "bq", "ice"), ("refused",)),
    "truncate.reaches-dest":         Claim("usage.md", ("pg-src", "maria-src", "my84-src"), ("TRUNCATE",)),
    "truncate.only-window":          Claim("usage.md", ("maria-src", "my84-src"), ("no rows",)),
    "slots.groups-independent":      Claim("usage.md#parallel-slots-slotsn", ("pg",), ("independent",)),
    "changelog.replay-not-doubled":  Claim("failure-modes.md", ("ch", "bq"), ("appends nothing twice",)),
    "changelog.group-replay":        Claim("failure-modes.md", ("ch", "bq"), ("group",)),
    "changelog.seq-continuation":    Claim("usage.md", ("ch", "bq"), ("never restarts",)),
    "changelog.rewind-refused":      Claim("usage.md", ("ch",), ("refused",)),
    "changelog.rekey-toast":         Claim("design/log_based.md", ("ch",), ("TOAST",)),
    "changelog.upgrade-stamp-boundary": Claim(UPGRADE, ("ch", "bq"), ("_apitap_seq",)),

    # BigQuery
    "bq.replica-one-row-per-key":    Claim("design/log_based.md", ("bq",), ("exactly once",)),
    "bq.replica-rekey-toast":        Claim(BQ_DEST, ("bq",), ("TOAST",)),
    "bq.job-identity":               Claim(BQ_DEST, ("bq",), ("job id",)),
    "bq.bulk-meets-cdc-lock":        Claim(BQ_DEST, ("bq", "bq-sandbox"), ("LockedError",)),
    "bq.multi-drain-one-dataset":    Claim(BQ_DEST, ("bq",), ("same dataset",)),

    # compatibility across releases
    "compat.old-bulk-refused-by-new-drain": Claim(UPGRADE, ("pg@0551", "ch@0551", "bq@0551", "pg@0560", "ch@0560", "bq@0560"), ("0.55.1",)),
    "compat.new-drain-refused-by-old-bulk": Claim(UPGRADE, ("pg@0551", "ch@0551", "bq@0551"), ("per-worker",)),
    "compat.old-drain-writes-nothing":      Claim(UPGRADE, ("pg@0551",), ("upgrade the `log_based` jobs first",)),
    "compat.rollback-leaks":         Claim(UPGRADE, ("pg@0551",), ("_apitap_lease", "_apitap_cdc_pending")),
    "compat.bq-0560-victim":         Claim(UPGRADE, ("bq@0560",), ("stop every 0.56.0 BigQuery drain",)),
    "my.gtid-destination":           Claim("usage.md", ("my-gtid",), ("GTID",)),

    # bulk lifecycle and memory
    "bulk.sibling-cancel":           Claim("failure-modes.md", ("pg", "my", "ch", "s3", "bq"), ("other workers stop",)),
    "memory.parquet-fits-cage":      Claim("usage.md", ("s3", "gcs"), ("row group",)),
    "memory.iceberg-merge-flat":     Claim("usage.md", ("ice",), ("memory does not grow",)),
}

_GCS = ("the GCS lane has no gate leg: the gate's service account is BigQuery-only",
        "project memory gcp-sa-scope: the SA cannot create or list GCS buckets")
WAIVED = {
    ("guard.bulk-vs-bulk", "gcs"):       Waiver(*_GCS),
    ("memory.parquet-fits-cage", "gcs"): Waiver(*_GCS),
    ("bq.bulk-meets-cdc-lock", "bq-sandbox"): Waiver(
        "a sandbox (no-billing) project is not exercised by the gate",
        "the gate project is billing-enabled; the sandbox cdc_query==query path is untestable here"),
}

# Every leg that proves no named claim is a regression claim of its own,
# `leg.<stem>` (plus `.<argv>` for a leg that takes a selector), carried by its
# row in benchmarks/README.md. The one "engine" is the leg itself.
LEG_ENGINE = "leg"


def leg_claim_id(script, argv):
    stem = script[:-3] if script.endswith(".py") else script
    return "leg." + ".".join([stem, *argv])


def with_leg_claims(legs):
    """CLAIMS plus one regression claim per leg that proves only itself."""
    out = dict(CLAIMS)
    for leg in legs:
        for cid in leg.proves:
            if cid.startswith("leg.") and cid not in out:
                out[cid] = Claim("benchmarks/README.md", (LEG_ENGINE,), (leg.script,))
    return out
