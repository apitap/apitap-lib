"""A `_apitap_state` compaction never erases a row a sibling committed (BigQuery).

Every run that writes into a dataset shares its `_apitap_state`: sibling drains
commit their watermark rows into it inside their fenced scripts, and bulk runs
append theirs. A state READ that finds the table past 512 rows compacts it.
0.56.0 compacted by SELECTing the newest row per key and loading exactly those
back with WRITE_TRUNCATE, so every row committed between its SELECT and its
load was erased. For a Postgres-source drain, the erased row is a window it has
already confirmed to its slot: its next run finds the watermark BEHIND the
slot's confirmed LSN and refuses until the state is cleared and the table
re-bootstrapped. Waiting for a sibling's commit to fall in that window is
luck; this leg makes it happen:

  bloat   600 rows of one key (`cmp_bloat`) are planted, so the table is past
          the threshold and a compaction has 599 superseded rows to delete;
  probes  while a run reads state, a probe INSERT starts every 2 s, each for a
          key nobody else writes (`cmp_probe`, `<tag><n>`) — the statement a
          sibling drain's script runs to commit its watermark — and every one
          BigQuery acknowledged is kept. A probe is the newest (only) row of
          its key, so no correct compaction may delete one;
  then    the bloat key holds exactly its newest row (the compaction RAN; if
          it did not, the round proved nothing and says so), and every
          acknowledged probe is still there;
  and     BigQuery's job history shows at least one probe of the case that
          COMMITTED INSIDE a compaction's own window — after its snapshot,
          before its commit (0.56.0: from its SELECT to its load; now: the
          DELETE's own run). Without one, nothing overlapped and the case
          proved nothing, so it fails as a rig failure.

  case 1  the reader is a pg -> BigQuery log_based drain (read_state).
  case 2  the reader is a pg -> BigQuery bulk append (dest_state).

Each case runs ROUNDS rounds. Why paced, and why rounds: BigQuery throttles
table updates (about five per ten seconds per table) by queueing DML, so
probes fired as fast as they return commit in bursts with quiet gaps of
several seconds, and a compaction (two to six seconds from its SELECT to its
load's commit) could fall in a gap: eight free-running writers lost probes in
both drain rounds tried and in neither bulk round. Paced under the throttle,
0.56.0 lost probes in all six rounds (1, 2, 1 and 1, 1, 2), each one a probe
the job history places inside that round's compaction window.

    python benchmarks/e2e_bq_state_compact.py

Rig: pg-src :5544, BigQuery dataset `apitap_cdc_e2e` (BQ_SA). The probes are
DML INSERTs (billed at the 10 MB minimum each: well under a cent a case).
RED: the 0.56.0 wheel — acknowledged probes are gone after its compaction.
"""
import subprocess
import sys
import threading
import time

import requests

import _rig

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
BQ = _rig.bq_url()
P, D = _rig.BQ_PROJECT, _rig.BQ_DATASET
CDC_T, BULK_T = "cmp_drain", "cmp_bulk"
BLOAT, PROBE = "cmp_bloat", "cmp_probe"
BLOAT_ROWS = 600
PACE = 2.0
ROUNDS = 3
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} {name}: {detail}", flush=True)


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


bq = _rig.bq
STATE = f"`{P}.{D}._apitap_state`"


def state_num_rows():
    """`tables.get` numRows — what apitap's compaction trigger reads."""
    r = requests.get(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/datasets/{D}/tables/_apitap_state",
        headers={"Authorization": f"Bearer {_rig._bq_token()}"})
    if r.status_code == 404:
        return 0
    return int(r.json().get("numRows", 0))


def clean_state_rows():
    if "_apitap_state" in _rig.bq_tables():
        bq(f"DELETE FROM {STATE} WHERE dest_table IN ('{BLOAT}', '{PROBE}', '{CDC_T}', '{BULK_T}')")


def clean():
    for t in (CDC_T, BULK_T):
        pg(f"DROP TABLE IF EXISTS {t} CASCADE")
        pg(f"DROP PUBLICATION IF EXISTS apitap_pub_{t}")
    pg("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
       "WHERE slot_name LIKE 'apitap_%' AND NOT active")
    names = _rig.bq_tables()
    for n in names:
        if any(n == t or n.startswith(t + "_") or n.startswith(t + "__") for t in (CDC_T, BULK_T)):
            _rig.bq_delete_table(n)
    clean_state_rows()
    if "_apitap_lease" in names:
        bq(f"DELETE FROM `{P}.{D}._apitap_lease` WHERE dest_key IN ('{D}.{CDC_T}', '{D}.{BULK_T}')")


def plant_bloat():
    """600 rows of one key, a second apart; the newest carries watermark 600."""
    bq(f"DELETE FROM {STATE} WHERE dest_table IN ('{BLOAT}', '{PROBE}')")
    bq(f"INSERT INTO {STATE} (dest_table, source_id, cursor_col, watermark, mode, last_rows, synced_at) "
       f"SELECT '{BLOAT}', 'b', '_lsn', CAST(i AS STRING), 'log_based', 0, "
       f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {BLOAT_ROWS + 60} - i SECOND) "
       f"FROM UNNEST(GENERATE_ARRAY(1, {BLOAT_ROWS})) i")
    # The trigger is the table's numRows; wait until it shows the bloat.
    if not _rig.wait_for(lambda: state_num_rows() > 512, 60, 2):
        _rig.rig_fail(f"_apitap_state numRows stayed at {state_num_rows()} after the plant")


class Probes:
    """A probe INSERT started every PACE seconds, each on its own thread and
    for a fresh key, until stopped. `acked` holds every probe BigQuery
    acknowledged as committed; `failed` every other outcome, with why."""

    def __init__(self, tag):
        self.tag, self.acked, self.failed = tag, [], []
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.token = _rig._bq_token()
        self.inflight = []
        self.pump = threading.Thread(target=self.run, daemon=True)

    def run(self):
        n = 0
        while True:
            n += 1
            t = threading.Thread(target=self.one, args=(f"{self.tag}{n}",), daemon=True)
            t.start()
            self.inflight.append(t)
            if self.stop.wait(PACE):
                return

    def one(self, pid):
        sql = (f"INSERT INTO {STATE} (dest_table, source_id, cursor_col, watermark, mode, "
               f"last_rows, synced_at) VALUES ('{PROBE}', '{pid}', '_lsn', '1', 'log_based', 0, "
               "CURRENT_TIMESTAMP())")
        try:
            r = requests.post(
                f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/queries",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"query": sql, "useLegacySql": False, "timeoutMs": 90000, "location": "US"},
                timeout=120)
            j = r.json()
            good = (r.status_code < 400 and j.get("jobComplete") is True
                    and not j.get("errors") and j.get("numDmlAffectedRows") == "1")
        except Exception as e:                                # noqa: BLE001
            good, j = False, {"exception": str(e)}
        with self.lock:
            (self.acked if good else self.failed).append(pid if good else f"{pid}: {str(j)[:200]}")

    def __enter__(self):
        self.pump.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.pump.join()
        for t in self.inflight:
            t.join(timeout=150)


def transfer(**kw):
    args = ", ".join(f"{k}={v!r}" for k, v in kw.items())
    return subprocess.run([sys.executable, "-c", f"import apitap; apitap.transfer({PG!r}, {BQ!r}, {args})"],
                          capture_output=True, text=True, timeout=900)


def judge(label, tag, probes):
    """One round: did the compaction run, and which acknowledged probes are gone."""
    rows = bq(f"SELECT watermark FROM {STATE} WHERE dest_table = '{BLOAT}'")
    wms = sorted(int(r[0]) for r in rows)
    if len(wms) == BLOAT_ROWS:
        _rig.rig_fail(f"{label}: the bloat key still holds all {BLOAT_ROWS} rows — the run never "
                      "compacted, so this round proved nothing")
    case(f"{label}: the compaction ran and kept the bloat key's newest row", wms == [BLOAT_ROWS],
         f"{len(wms)} row(s) left, watermarks {wms[:5]}{'...' if len(wms) > 5 else ''}")
    if len(probes.acked) < 5:
        _rig.rig_fail(f"{label}: only {len(probes.acked)} probe(s) were acknowledged during the run "
                      f"({len(probes.failed)} refused: {probes.failed[:2]}) — too few to overlap a compaction")
    present = {r[0] for r in bq(f"SELECT source_id FROM {STATE} WHERE dest_table = '{PROBE}' "
                                f"AND STARTS_WITH(source_id, '{tag}')")}
    lost = sorted(p for p in probes.acked if p not in present)
    print(f"      {label}: {len(probes.acked)} probe(s) acknowledged, {len(lost)} gone {lost[:8]}; "
          f"{len(probes.failed)} not acknowledged {[f[:90] for f in probes.failed[:2]]}", flush=True)
    return len(probes.acked), lost


J = "`region-us`.INFORMATION_SCHEMA.JOBS_BY_USER"


def inside_windows(since, prefix):
    """The acknowledged-and-committed probes of this case (source_id starting
    with `prefix`) whose INSERT committed inside a compaction's own window:
    the DELETE's run (this release), or a SELECT up to the next state load's
    commit (0.56.0). JOBS_BY_USER lags a little, so wait for ROUNDS
    compactions to show."""
    comp, loads = [], []
    for _ in range(12):
        comp = bq(f"SELECT UNIX_MICROS(start_time), UNIX_MICROS(end_time), "
                  f"query LIKE '%MAX(synced_at) AS newest%' FROM {J} "
                  f"WHERE creation_time >= TIMESTAMP_MICROS({since}) AND error_result IS NULL "
                  "AND ((STARTS_WITH(query, 'DELETE FROM') AND query LIKE '%MAX(synced_at) AS newest%') "
                  "  OR (STARTS_WITH(query, 'SELECT TO_JSON_STRING(t)') AND query LIKE '%ROW_NUMBER() OVER%')) "
                  "AND query LIKE '%_apitap_state%' ORDER BY 1")
        if len(comp) >= ROUNDS:
            break
        time.sleep(5)
    loads = [int(r[0]) for r in bq(
        f"SELECT UNIX_MICROS(end_time) FROM {J} WHERE creation_time >= TIMESTAMP_MICROS({since}) "
        "AND job_type = 'LOAD' AND destination_table.table_id = '_apitap_state' "
        "AND error_result IS NULL ORDER BY 1")]
    probes = bq(f"SELECT UNIX_MICROS(end_time), REGEXP_EXTRACT(query, r\"'{PROBE}', '([^']+)'\") FROM {J} "
                f"WHERE creation_time >= TIMESTAMP_MICROS({since}) AND statement_type = 'INSERT' "
                f"AND query LIKE \"%VALUES ('{PROBE}', '{prefix}%\" AND error_result IS NULL")
    windows = []
    for s0, e0, is_delete in comp:
        s0, e0 = int(s0), int(e0)
        if is_delete != "true":
            e0 = next((l for l in loads if l > e0), e0)
        windows.append((s0, e0))
    return len(comp), sorted({p for t, p in probes if any(a <= int(t) <= b for a, b in windows)})


def rounds(name, table, run_kw):
    """ROUNDS rounds of: a source change, the bloat, probes around one run."""
    since = int((time.time() - 5) * 1e6)
    acked, lost = 0, []
    for i in range(1, ROUNDS + 1):
        label, tag = f"{name} round {i}", f"{name[-1]}{i}_"
        base = 30 + 10 * i
        pg(f"INSERT INTO {table} SELECT g, 'w'||g FROM generate_series({base - 9}, {base}) g")
        plant_bloat()
        with Probes(tag) as probes:
            time.sleep(3)
            r = transfer(table=table, **run_kw)
            time.sleep(3)
        case(f"{label}: the run succeeded", r.returncode == 0,
             r.stderr.strip()[-300:] if r.returncode else "rc=0")
        a, gone = judge(label, tag, probes)
        acked, lost = acked + a, lost + gone
        n = bq(f"SELECT COUNT(*) FROM `{P}.{D}.{table}`")[0][0]
        case(f"{label}: the destination equals its source", int(n) == base, f"{n} rows of {base}")
    case(f"{name}: every probe BigQuery acknowledged survived every compaction", not lost,
         f"{acked} acknowledged over {ROUNDS} rounds, {len(lost)} gone {lost[:10]}")
    n_comp, inside = inside_windows(since, name[-1])
    if not inside:
        _rig.rig_fail(f"{name}: no probe committed inside any of the {n_comp} compaction window(s) "
                      "the job history shows — nothing overlapped, so this case proved nothing")
    case(f"{name}: probes did commit inside a compaction's own window", True,
         f"{len(inside)} across {n_comp} compaction(s): {inside[:10]}")


try:
    print("== reset ==", flush=True)
    clean()

    print("== case 1: a log_based drain reads state while siblings commit ==", flush=True)
    pg(f"CREATE TABLE {CDC_T} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {CDC_T} SELECT g, 'v'||g FROM generate_series(1, 30) g")
    r = transfer(table=CDC_T, mode="log_based")
    if r.returncode:
        _rig.rig_fail(f"bootstrap of {CDC_T} failed: {r.stderr.strip()[-300:]}")
    rounds("case 1", CDC_T, {"mode": "log_based"})

    print("== case 2: a bulk append reads state while siblings commit ==", flush=True)
    pg(f"CREATE TABLE {BULK_T} (id int PRIMARY KEY, v text)")
    pg(f"INSERT INTO {BULK_T} SELECT g, 'v'||g FROM generate_series(1, 30) g")
    r = transfer(table=BULK_T, mode="append", cursor="id")
    if r.returncode:
        _rig.rig_fail(f"first append of {BULK_T} failed: {r.stderr.strip()[-300:]}")
    rounds("case 2", BULK_T, {"mode": "append", "cursor": "id"})
finally:
    print("== cleanup ==", flush=True)
    try:
        clean()
    except Exception as ex:                                   # noqa: BLE001
        print(f"   cleanup: {ex}", flush=True)

print("\nBQ STATE COMPACT E2E: " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
