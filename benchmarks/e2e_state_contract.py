"""The _apitap_state contract: a watermark is only usable in its own vocabulary.

One state table, two writers. The bulk lane stores cursor-column watermarks
("last id I shipped"); the CDC lane stores LSNs. Before the contract, the bulk
read used the row's VALUE and ignored its vocabulary — so a table that had been
CDC'd and was later run with mode="append" adopted an LSN as a cursor value and
resumed from a position that meant nothing, skipping or repeating rows while
reporting success. And the two lanes spelled the key differently (schema.bare
vs bare), so a replace's state clear could miss the CDC row entirely and a
later log_based run resumed from a watermark that predated the replace.

Since 0.57.0 every state read on every destination selects the row's whole
vocabulary (watermark, cursor_col, mode) with no predicate on it, and one rule
(`naming::state_verdict`) decides whose it is — in both lanes, and in the bulk
lane BEFORE the empty-table early return.

Usage: e2e_state_contract.py <pg|my|ch|bq|ice>     (no argument = pg)

  leg 1  CDC then append          — must REFUSE, naming the LSN problem; the
                                    state row, the data and the catalog are
                                    untouched. 1b: the same with the
                                    destination EMPTIED first (not ice) — an
                                    emptied table used to return before the
                                    state read and overwrite the drain's row
  leg 2  CDC then replace then CDC— (pg) the replace must clear the CDC
                                    watermark: the following log_based run
                                    re-bootstraps and lands EXACTLY the source
  leg 3  append(a) then append(b) — cursor mismatch must REFUSE, naming both
  leg 4  the other lane's row     — after an append bootstrap and a sentinel
                                    row written straight into the destination,
                                    log_based must REFUSE naming the cursor;
                                    the sentinel, the row count and the state
                                    row stay. Clearing the state as the message
                                    says hands the table to CDC cleanly
  leg 5  TRUNCATE-to-resync       — (not ice) the lane's OWN row on an emptied
                                    table still means "reload everything": the
                                    verdict moved above the emptiness check
                                    must not turn that into a skip. A guard:
                                    0.56.0 passes it too

On Iceberg an append that bootstraps the table records no cursor property
(the data is the state until an incremental run writes one), so legs 3 and 4
first run one incremental append to give the next run a state to disagree
with.

RED on the 0.56.0 wheel: my — legs 1, 1b, 3, 4 (its drain filtered
`mode = 'log_based'` and re-bootstrapped over the sentinel; its bulk read took
the LSN as an id); ch, bq — legs 1, 1b, 3; ice — legs 1, 3 (a cursor switch
re-bootstrapped from the data); pg — leg 1b (its bulk verdict ran after the
emptiness return, so the append overwrote the emptied table and wrote a
second, append-mode state row beside the drain's). Leg 4 on pg, ch, bq and
ice, leg 5 everywhere and leg 2 are guards that 0.56.0 passes too.

Neither pg spelling was changed. Rewriting them to one canonical key was the
first attempt and it broke the recovery instructions apitap itself prints —
every "clear the state row" message, runbook and fixture names the bare one.
What closes the gap is that every READ and every DELETE covers both.

Rig: `apitap-bench-pg-src` on :5544 is the source; the destination is
`apitap-bench-pg-dst` :5545, `apitap-bench-my` :3307, `apitap-bench-ch` :8124,
BigQuery `apitap_cdc_e2e` (BQ_SA), or the Iceberg REST catalog :8181 over
MinIO :9100. Every leg ends by dropping the destination table it made.
"""
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _rig  # noqa: E402

WHICH = sys.argv[1] if len(sys.argv) > 1 else "pg"
if WHICH not in ("pg", "my", "ch", "bq", "ice"):
    raise SystemExit(f"usage: {sys.argv[0]} <pg|my|ch|bq|ice>")

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
# One source table per destination, so no two selectors ever share a slot.
T = "state_contract" if WHICH == "pg" else f"state_contract_{WHICH}"
SENTINEL = (900000, -7)

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    return _rig.psql(sql, _rig.PG_SRC)


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}", flush=True)
    ok = ok and bool(good)


def last_line(r):
    return r.stderr.strip().splitlines()[-1][:170] if r.stderr.strip() else "(empty)"


# ── the destination, asked directly ──────────────────────────────────────
#
# Each engine answers the same questions: the table's fingerprint, the state
# rows for T as one string (any write changes it: synced_at is in it), the
# modes those rows record, and every artifact named for T.

class Pg:
    url = os.environ.get("PGD_URL", "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst")
    where = f"dest_table IN ('{T}', 'public.{T}')"

    def q(self, sql):
        return _rig.psql(sql)

    def fp(self):
        return self.q(f"SELECT count(*) || '|' || coalesce(sum(id),0) || '|' || coalesce(sum(n),0) FROM {T}")

    def has_state(self):
        return self.q("SELECT to_regclass('_apitap_state') IS NOT NULL") == "t"

    def state(self):
        return self.q("SELECT coalesce(string_agg(concat_ws('|', dest_table, source_id, cursor_col, "
                      "watermark, mode, synced_at), ',' ORDER BY dest_table, source_id), '') "
                      f"FROM _apitap_state WHERE {self.where}") if self.has_state() else ""

    def modes(self):
        return self.q("SELECT coalesce(string_agg(DISTINCT mode, ','), '') FROM _apitap_state "
                      f"WHERE {self.where}") if self.has_state() else ""

    def clear_state(self):
        if self.has_state():
            self.q(f"DELETE FROM _apitap_state WHERE {self.where}")

    def insert_sentinel(self):
        self.q(f"INSERT INTO {T} (id, n) VALUES {SENTINEL}")

    def has_sentinel(self):
        return self.q(f"SELECT count(*) FROM {T} WHERE id = {SENTINEL[0]} AND n = {SENTINEL[1]}") == "1"

    def empty(self):
        self.q(f"TRUNCATE {T}")

    def drop(self):
        self.q(f"DROP TABLE IF EXISTS {T}")

    def artifacts(self):
        return [n for n in _rig._pg_names(T, _rig.PG_DST) if n.startswith(T + "_") and "__apitap_" in n]


class My:
    url = "mysql://root:bench@127.0.0.1:3307/bench"
    where = f"dest_table IN ('{T}', 'bench.{T}')"

    def q(self, sql):
        return _rig.mysql(sql)

    def fp(self):
        return self.q(f"SELECT CONCAT(COUNT(*), '|', COALESCE(SUM(id),0), '|', COALESCE(SUM(n),0)) FROM {T}")

    def has_state(self):
        return self.q("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
                      "AND table_name = '_apitap_state'") == "1"

    def state(self):
        return self.q("SELECT COALESCE(GROUP_CONCAT(CONCAT_WS('|', dest_table, source_id, cursor_col, "
                      "watermark, mode, synced_at) ORDER BY dest_table, source_id SEPARATOR ','), '') "
                      f"FROM _apitap_state WHERE {self.where}") if self.has_state() else ""

    def modes(self):
        return self.q("SELECT COALESCE(GROUP_CONCAT(DISTINCT mode ORDER BY mode), '') FROM _apitap_state "
                      f"WHERE {self.where}") if self.has_state() else ""

    def clear_state(self):
        if self.has_state():
            self.q(f"DELETE FROM _apitap_state WHERE {self.where}")

    def insert_sentinel(self):
        self.q(f"INSERT INTO {T} (id, n) VALUES {SENTINEL}")

    def has_sentinel(self):
        return self.q(f"SELECT COUNT(*) FROM {T} WHERE id = {SENTINEL[0]} AND n = {SENTINEL[1]}") == "1"

    def empty(self):
        self.q(f"TRUNCATE TABLE {T}")

    def drop(self):
        self.q(f"DROP TABLE IF EXISTS {T}")

    def artifacts(self):
        return [n for n in _rig._my_names(T, _rig.MY_DST) if n.startswith(T + "_") and "__apitap_" in n]


class Ch:
    url = "clickhouse://default:bench@127.0.0.1:8124/default"

    def q(self, sql):
        return _rig.clickhouse(sql)

    def fp(self):
        return self.q(f"SELECT concat(toString(count()), '|', toString(ifNull(sum(id), 0)), '|', "
                      f"toString(ifNull(sum(n), 0))) FROM {T}")

    def has_state(self):
        return self.q("SELECT count() FROM system.tables WHERE database = currentDatabase() "
                      "AND name = '_apitap_state'") == "1"

    def state(self):
        # FINAL: the table is a ReplacingMergeTree on (dest_table, source_id);
        # synced_at moves with every write, so any write changes this string.
        return self.q("SELECT arrayStringConcat(groupArray(concat(dest_table, '|', source_id, '|', "
                      "cursor_col, '|', watermark, '|', mode, '|', toString(synced_at))), ',') FROM "
                      f"(SELECT * FROM _apitap_state FINAL WHERE dest_table = '{T}' ORDER BY source_id)"
                      ) if self.has_state() else ""

    def modes(self):
        return self.q("SELECT arrayStringConcat(arraySort(groupUniqArray(mode)), ',') FROM _apitap_state "
                      f"FINAL WHERE dest_table = '{T}'") if self.has_state() else ""

    def clear_state(self):
        if self.has_state():
            self.q(f"DELETE FROM _apitap_state WHERE dest_table = '{T}' SETTINGS mutations_sync = 2")

    def insert_sentinel(self):
        self.q(f"INSERT INTO {T} (id, n) VALUES {SENTINEL}")

    def has_sentinel(self):
        return self.q(f"SELECT count() FROM {T} WHERE id = {SENTINEL[0]} AND n = {SENTINEL[1]}") == "1"

    def empty(self):
        self.q(f"TRUNCATE TABLE {T}")

    def drop(self):
        self.q(f"DROP TABLE IF EXISTS {T} SYNC")

    def artifacts(self):
        return [n for n in _rig._ch_names(T, _rig.CH_DST) if n.startswith(T + "_") and "__apitap_" in n]


class Bq:
    url = None  # BQ_SA is read only when this engine is selected

    def __init__(self):
        self.url = _rig.bq_url()
        self.fq = f"`{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}`"

    def q(self, sql):
        return _rig.bq(sql)

    def one(self, sql):
        rows = self.q(sql)
        return (rows[0][0] if rows and rows[0] else None) or ""

    def fp(self):
        return self.one(f"SELECT CONCAT(CAST(COUNT(*) AS STRING), '|', CAST(IFNULL(SUM(id),0) AS STRING), "
                        f"'|', CAST(IFNULL(SUM(n),0) AS STRING)) FROM {self.fq}.{T}")

    def has_state(self):
        return "_apitap_state" in _rig.bq_tables()

    def state(self):
        # Append-only: the NEWEST row per source is what every reader resolves,
        # and a state compaction (a read-side chore) deletes only superseded
        # rows — so compare the newest rows, which any write replaces.
        return self.one(
            "SELECT IFNULL(STRING_AGG(line, ',' ORDER BY line), '') FROM ("
            "SELECT CONCAT(source_id, '|', IFNULL(cursor_col,''), '|', IFNULL(watermark,''), '|', "
            "IFNULL(mode,''), '|', CAST(synced_at AS STRING)) AS line "
            f"FROM {self.fq}._apitap_state WHERE dest_table = '{T}' "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY synced_at DESC) = 1)"
        ) if self.has_state() else ""

    def modes(self):
        return self.one(
            "SELECT IFNULL(STRING_AGG(DISTINCT mode ORDER BY mode), '') FROM ("
            f"SELECT mode FROM {self.fq}._apitap_state WHERE dest_table = '{T}' AND source_id != '*' "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY synced_at DESC) = 1)"
        ) if self.has_state() else ""

    def clear_state(self):
        if self.has_state():
            self.q(f"DELETE FROM {self.fq}._apitap_state WHERE dest_table = '{T}'")

    def insert_sentinel(self):
        self.q(f"INSERT INTO {self.fq}.{T} (id, n) VALUES {SENTINEL}")

    def has_sentinel(self):
        return self.one(f"SELECT CAST(COUNT(*) AS STRING) FROM {self.fq}.{T} "
                        f"WHERE id = {SENTINEL[0]} AND n = {SENTINEL[1]}") == "1"

    def empty(self):
        self.q(f"TRUNCATE TABLE {self.fq}.{T}")

    def drop(self):
        _rig.bq_delete_table(T)
        for n in self.artifacts():
            _rig.bq_delete_table(n)

    def artifacts(self):
        return [n for n in _rig.bq_tables() if n.startswith(T + "_") and "__apitap_" in n]


class Ice:
    NS = "cdc_e2e"
    url = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
           "&access_key_id=bench&secret_access_key=benchbench")
    table_url = f"http://127.0.0.1:8181/v1/namespaces/{NS}/tables/{T}"
    _duck = None

    def meta(self):
        try:
            with urllib.request.urlopen(self.table_url) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def props(self):
        m = self.meta()
        return {} if m is None else {k: v for k, v in m["metadata"].get("properties", {}).items()
                                     if k.startswith("apitap.watermark")}

    def duck(self):
        if Ice._duck is None:
            import duckdb
            Ice._duck = duckdb.connect()
            Ice._duck.execute("INSTALL iceberg; LOAD iceberg;")
            Ice._duck.execute(
                "SET s3_endpoint='127.0.0.1:9100'; SET s3_use_ssl=false; SET s3_url_style='path'; "
                "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; "
                "SET s3_region='us-east-1';")
        return Ice._duck

    def fp(self):
        m = self.meta()
        if m is None:
            return "(no table)"
        c, a, b = self.duck().execute(
            f"SELECT count(*), coalesce(sum(id),0), coalesce(sum(n),0) "
            f"FROM iceberg_scan('{m['metadata-location']}')").fetchone()
        return f"{c}|{a}|{b}"

    def state(self):
        # Iceberg's state IS the table properties, and a commit of any kind
        # moves the current snapshot or the metadata: both are in the string.
        m = self.meta()
        if m is None:
            return ""
        return json.dumps(self.props(), sort_keys=True) + "#" + str(m["metadata"].get("current-snapshot-id"))

    def modes(self):
        # Iceberg records no mode; the cursor property says which lane wrote it.
        return ",".join(sorted({"log_based" if v == "_lsn" else f"cursor:{v}"
                                for k, v in self.props().items() if k.startswith("apitap.watermark-cursor.")}))

    def clear_state(self):
        keys = sorted(self.props())
        if keys:
            req = urllib.request.Request(
                self.table_url, method="POST", headers={"Content-Type": "application/json"},
                data=json.dumps({"requirements": [],
                                 "updates": [{"action": "remove-properties", "removals": keys}]}).encode())
            urllib.request.urlopen(req).read()

    def drop(self):
        req = urllib.request.Request(self.table_url + "?purgeRequested=true", method="DELETE")
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

    def artifacts(self):
        return []  # an Iceberg run's claims live in object storage, asked by e2e_guard_matrix ice


D = {"pg": Pg, "my": My, "ch": Ch, "bq": Bq, "ice": Ice}[WHICH]()


def run(mode, cursor=None):
    kw = f", cursor={cursor!r}" if cursor else ""
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {D.url!r}, table={T!r}, mode={mode!r}{kw})\n"
            "print('ROWS', r.rows, flush=True)\n")
    return sh([sys.executable, "-c", code])


def fp_src():
    return src(f"SELECT count(*) || '|' || coalesce(sum(id),0) || '|' || coalesce(sum(n),0) FROM {T}")


_SLOTS_BEFORE = set(src("SELECT slot_name FROM pg_replication_slots").split())
_PUBS_BEFORE = set(src("SELECT pubname FROM pg_publication").split())


def drop_our_slots_and_pubs():
    for s in sorted(set(src("SELECT slot_name FROM pg_replication_slots").split()) - _SLOTS_BEFORE):
        src(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
            f"WHERE slot_name='{s}' AND NOT active")
    for p in sorted(set(src("SELECT pubname FROM pg_publication").split()) - _PUBS_BEFORE):
        src(f'DROP PUBLICATION IF EXISTS "{p}"')


def reset():
    src(f"DROP TABLE IF EXISTS {T}")
    D.drop()
    D.clear_state()
    drop_our_slots_and_pubs()
    src(f"CREATE TABLE {T} (id bigint PRIMARY KEY, n bigint)")
    src(f"INSERT INTO {T} SELECT g, g * 3 FROM generate_series(1, 500) g")


def untouched(label, before_fp, before_state):
    """The refused run wrote nothing: the data, the state rows and every
    artifact named for T are as they were."""
    after_fp, after_state, left = D.fp(), D.state(), D.artifacts()
    case(f"{label}: the destination's rows are untouched", after_fp == before_fp,
         f"{before_fp} -> {after_fp}")
    case(f"{label}: its state rows are untouched", after_state == before_state,
         f"\n        before {before_state}\n        after  {after_state}")
    case(f"{label}: nothing named for {T} was left behind", left == [], ", ".join(left))


EMPTIES = WHICH != "ice"  # emptying an Iceberg table is a delete commit, not a TRUNCATE


def record_cursor_state():
    """On Iceberg an append that BOOTSTRAPS the table records no cursor
    property — the data is the state until an incremental run writes one — so
    there would be nothing for the next run to disagree with. One incremental
    append (cursor=id) records it. The SQL destinations record a row at the
    bootstrap already."""
    if WHICH != "ice":
        return
    src(f"INSERT INTO {T} VALUES (1500, 4500)")
    r = run("append", cursor="id")
    case("(rig) an incremental append records the cursor property",
         r.returncode == 0 and D.modes() == "cursor:id", last_line(r) if r.returncode else D.modes())

try:
    # -----------------------------------------------------------------------
    print(f"== [{WHICH}] leg 1: a CDC-managed table run with mode='append' must refuse ==")
    reset()
    r = run("log_based")
    case("CDC bootstrap", r.returncode == 0 and D.fp() == fp_src(), last_line(r) if r.returncode else "")
    case("(rig) the drain's row says log_based", D.modes() == "log_based", D.modes())
    fp0, st0 = D.fp(), D.state()
    r = run("append", cursor="id")
    case("append against the CDC watermark is REFUSED", r.returncode != 0,
         "it ran and 'succeeded' — the LSN was adopted as a cursor value"
         if r.returncode == 0 else "")
    if r.returncode != 0:
        case("and the message names the vocabulary problem",
             "CDC-managed" in r.stderr or "LSN" in r.stderr, last_line(r))
    untouched("append", fp0, st0)
    case("the state row still says log_based", D.modes() == "log_based", D.modes())

    if EMPTIES:
        print(f"== [{WHICH}] leg 1b: ...and still refuses once the destination is EMPTY ==")
        # The bulk lane's "an empty table carries no watermark" return used to
        # come BEFORE its state read, so an emptied CDC-managed table ran a
        # full append and rewrote the drain's row as mode=append.
        D.empty()
        fp0, st0 = D.fp(), D.state()
        r = run("append", cursor="id")
        case("append onto the emptied CDC-managed table is REFUSED", r.returncode != 0,
             "it ran — the empty-table return skipped the state verdict" if r.returncode == 0 else "")
        if r.returncode != 0:
            case("and the message names the vocabulary problem",
                 "CDC-managed" in r.stderr or "LSN" in r.stderr, last_line(r))
        untouched("append onto empty", fp0, st0)
        case("the state row still says log_based", D.modes() == "log_based", D.modes())

    # -----------------------------------------------------------------------
    if WHICH == "pg":
        print(f"== [{WHICH}] leg 2: replace must clear the CDC watermark, not strand it ==")
        # Same table, CDC-managed again. A replace rebuilds the table from
        # scratch; the CDC watermark that pointed into the OLD table's history
        # must go with it.
        reset()
        r = run("log_based")
        case("CDC bootstrap", r.returncode == 0, last_line(r) if r.returncode else "")
        src(f"UPDATE {T} SET n = n + 1 WHERE id <= 100")   # changes the CDC lane never saw
        r = run("replace")
        case("the replace runs", r.returncode == 0, r.stderr.strip()[-200:])
        state_rows = D.q(f"SELECT count(*) FROM _apitap_state WHERE {D.where}")
        case("no state row survives the replace (either spelling)", state_rows == "0",
             f"{state_rows} rows left — a log_based run would resume from a pre-replace LSN")
        src(f"INSERT INTO {T} SELECT g, g * 3 FROM generate_series(1000, 1100) g")
        r = run("log_based")
        case("the following log_based run re-bootstraps cleanly", r.returncode == 0,
             r.stderr.strip()[-200:])
        case("and the destination is exactly the source", D.fp() == fp_src(),
             f"src {fp_src()} vs dst {D.fp()}")

    # -----------------------------------------------------------------------
    print(f"== [{WHICH}] leg 3: two appends with different cursors must refuse, naming both ==")
    reset()
    r = run("append", cursor="id")
    case("first append (cursor=id) bootstraps", r.returncode == 0 and D.fp() == fp_src(),
         last_line(r) if r.returncode else "")
    record_cursor_state()
    src(f"INSERT INTO {T} VALUES (2000, 6000)")
    fp0, st0 = D.fp(), D.state()
    r = run("append", cursor="n")
    case("append with a DIFFERENT cursor is refused", r.returncode != 0,
         "resumed an id-watermark as an n-watermark (or re-bootstrapped from the data)"
         if r.returncode == 0 else "")
    if r.returncode != 0:
        case("and the message names both cursors", "'id'" in r.stderr and "'n'" in r.stderr, last_line(r))
    untouched("append(cursor=n)", fp0, st0)

    # -----------------------------------------------------------------------
    print(f"== [{WHICH}] leg 4: the CDC lane must SEE a row the bulk lane wrote ==")
    # The mirror of leg 1. Here the bulk lane goes first, and a row lands in the
    # destination that the source never had — the operator's own data, say.
    # The drain must find the append's state row and refuse, because a cursor
    # value is not an LSN. A drain that saw no row (0.56.0 on MySQL filtered
    # `mode = 'log_based'`) re-bootstrapped: a full reload that looks like
    # success, and the sentinel is gone.
    #
    # On pg the two lanes also key the table differently: the CDC lane writes
    # the bare name, the bulk lane schema.bare — the CDC read must cover both.
    reset()
    r = run("append", cursor="id")
    case("an append bootstraps the table", r.returncode == 0 and D.fp() == fp_src(),
         last_line(r) if r.returncode else "")
    record_cursor_state()
    if WHICH == "pg":
        keys = D.q(f"SELECT string_agg(dest_table, ',' ORDER BY dest_table) FROM _apitap_state WHERE {D.where}")
        case("(rig) the bulk lane wrote the QUALIFIED spelling", keys == f"public.{T}", keys)
    if WHICH != "ice":
        D.insert_sentinel()
        case("(rig) the sentinel is in the destination", D.has_sentinel())
    src(f"INSERT INTO {T} VALUES (4000, 12000)")
    fp0, st0, m0 = D.fp(), D.state(), D.modes()
    # A bootstrapped append lands as a replace, so its row says mode=replace
    # with cursor id: the cursor lane's all the same.
    case("(rig) the bootstrap's row is the cursor lane's",
         m0 == "cursor:id" if WHICH == "ice" else m0 in ("append", "replace"), m0)
    r = run("log_based")
    case("log_based against a cursor-managed table is REFUSED", r.returncode != 0,
         "it re-bootstrapped instead — the other lane's row was invisible" if r.returncode == 0 else "")
    if r.returncode != 0:
        case("and the refusal names the cursor it tracks",
             "'id'" in r.stderr and ("LSN" in r.stderr or "cursor" in r.stderr), last_line(r))
    if WHICH != "ice":
        kept = D.has_sentinel()
        case("the sentinel is still there", kept, "" if kept else "gone — the table was reloaded from the source")
    untouched("log_based", fp0, st0)
    case("the state row still records the append", D.modes() == m0, f"{m0} -> {D.modes()}")

    if WHICH == "pg":
        # The reverse spelling still reaches it: clearing by the name apitap
        # tells operators to use must actually clear the row, whichever lane
        # wrote it.
        D.q(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")
        left = D.q(f"SELECT count(*) FROM _apitap_state WHERE {D.where}")
        case("clearing by the bare name is NOT enough on its own (both spellings exist)",
             left in ("0", "1"), f"{left} rows left")
    D.clear_state()
    r = run("log_based")
    case("with the state cleared as the message says, log_based bootstraps cleanly",
         r.returncode == 0, r.stderr.strip()[-200:])
    case("and lands exactly the source", D.fp() == fp_src(), f"src {fp_src()} vs dst {D.fp()}")
    case("and the table is the drain's now", D.modes() == "log_based", D.modes())

    # -----------------------------------------------------------------------
    if EMPTIES:
        print(f"== [{WHICH}] leg 5: the lane's own row on an EMPTIED table still resyncs everything ==")
        reset()
        r = run("append", cursor="id")
        case("an append bootstraps the table", r.returncode == 0 and D.fp() == fp_src(),
             last_line(r) if r.returncode else "")
        D.empty()
        src(f"INSERT INTO {T} VALUES (5000, 15000)")
        r = run("append", cursor="id")
        case("the append after TRUNCATE runs", r.returncode == 0, last_line(r) if r.returncode else "")
        case("and reloads EVERY row, not just those past the old watermark", D.fp() == fp_src(),
             f"src {fp_src()} vs dst {D.fp()}")
finally:
    print(f"== [{WHICH}] cleanup ==")
    try:
        src(f"DROP TABLE IF EXISTS {T}")
        D.drop()
        D.clear_state()
        drop_our_slots_and_pubs()
    except Exception as e:  # a failed cleanup is reported, and fails the leg
        case("cleanup", False, str(e)[:200])

print("\nSTATE CONTRACT E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
