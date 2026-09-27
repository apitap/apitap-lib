"""The concurrency guard, the same questions on every destination.

`guard.rs` is one announce / scan / collect loop over a per-engine adapter that
only SPELLS catalog calls. This leg asks each engine the same questions through
that adapter, with plants rather than races (a plant is exactly what the guard
reads, and "slow enough to overlap" is not a property a test can assert), and
takes every answer from the server's own catalog:

  A  a live-looking REPLACE's staging (a fresh token; on BigQuery a worker's
     `_0` table) refuses a replace BY TYPE, and is left in place; removed, the
     control replace runs                                 (guard.bulk-vs-bulk)
  B  a live DRAIN's lock with a live lease behind it refuses a drain BY TYPE,
     and is left in place                                 (guard.drain-vs-bulk)
  C  a drain's lock with NO lease — an older apitap's, or an operator's plant —
     refuses a replace and is never collected       (guard.no-lease-no-collect)
  D  a drain whose collector died half way (row `collected`, markers still
     there) is collected by the next run, which proceeds
                                                      (collect.claim-then-crash)

    python benchmarks/e2e_guard_matrix.py <pg|my|ch|bq|s3|ice>

Rig: `apitap-bench-pg-src` :5544 as the source; MySQL :3307, ClickHouse :8124,
the gate's BigQuery dataset (BQ_SA), the bench MinIO (:9100), or the Iceberg
REST catalog (:8181) over it as the destination.
"""
import sys

import apitap

import _rig

ENGINE = sys.argv[1]
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = f"gm_{ENGINE}"
ok = True


def case(name, passed, detail=""):
    global ok
    ok &= bool(passed)
    print(f"   {'OK' if passed else 'XX'} [{ENGINE}] {name}: {detail}")


def refusal(fn):
    try:
        fn()
        return None
    except Exception as e:                                    # noqa: BLE001
        return f"{type(e).__name__}: {e}"


class Bq:
    url = None
    key = None

    def __init__(self):
        self.url = _rig.bq_url()
        self.key = f"{_rig.BQ_DATASET}.{T}"
        self.lease_t = f"`{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}._apitap_lease`"

    def names(self):
        return sorted(n for n in _rig.bq_tables() if n.startswith(T))

    def plant(self, name):
        _rig.bq_create_table(name)

    def unplant(self, name):
        _rig.bq_delete_table(name)

    def live_lease(self, tok):
        if "_apitap_lease" not in _rig.bq_tables():
            _rig.bq_create_table("_apitap_lease", [
                {"name": "dest_key", "type": "STRING"}, {"name": "token", "type": "STRING"},
                {"name": "expires_at", "type": "TIMESTAMP"}, {"name": "collected", "type": "BOOL"}])
        _rig.bq(f"INSERT INTO {self.lease_t} (dest_key, token, expires_at, collected) VALUES "
                f"('{self.key}', '{tok}', TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), FALSE)")

    def collected_lease(self, tok):
        self.live_lease(tok)
        _rig.bq(f"UPDATE {self.lease_t} SET collected = TRUE WHERE dest_key = '{self.key}' "
                f"AND token = '{tok}'")

    def lease_row(self, tok):
        if "_apitap_lease" not in _rig.bq_tables():
            return None
        r = _rig.bq(f"SELECT collected FROM {self.lease_t} WHERE dest_key = '{self.key}' AND token = '{tok}'")
        return r[0][0] if r else None

    def count(self):
        r = _rig.bq(f"SELECT COUNT(*) FROM `{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}.{T}`")
        return r[0][0] if r else None

    def clean(self):
        for n in self.names():
            _rig.bq_delete_table(n)
        if "_apitap_lease" in _rig.bq_tables():
            _rig.bq(f"DELETE FROM {self.lease_t} WHERE dest_key = '{self.key}'")
        for s in ("_apitap_state",):
            if s in _rig.bq_tables():
                _rig.bq(f"DELETE FROM `{_rig.BQ_PROJECT}.{_rig.BQ_DATASET}.{s}` WHERE dest_table = '{T}'")

    # A bulk BigQuery run writes one table per worker; the plant is worker 0.
    staging_decoration = "_0"


class Pg:
    url = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
    key = f"public.{T}"
    staging_decoration = ""

    def names(self):
        return [n for n in _rig.psql(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE '{T}%'").splitlines() if n]

    def plant(self, name):
        _rig.psql(f'CREATE UNLOGGED TABLE public."{name}" ()')

    def unplant(self, name):
        _rig.psql(f'DROP TABLE IF EXISTS public."{name}"')

    def _lease_table(self):
        _rig.psql("CREATE TABLE IF NOT EXISTS public._apitap_lease (dest_key text NOT NULL, "
                  "token text NOT NULL, expires_at timestamptz NOT NULL, "
                  "collected boolean NOT NULL DEFAULT false, PRIMARY KEY (dest_key, token))")

    def live_lease(self, tok):
        self._lease_table()
        _rig.psql(f"INSERT INTO public._apitap_lease VALUES ('{self.key}', '{tok}', "
                  "now() + interval '1 hour', false)")

    def collected_lease(self, tok):
        self._lease_table()
        _rig.psql(f"INSERT INTO public._apitap_lease VALUES ('{self.key}', '{tok}', "
                  "now() + interval '1 hour', true)")

    def lease_row(self, tok):
        if _rig.psql("SELECT to_regclass('public._apitap_lease') IS NULL") == "t":
            return None
        return _rig.psql(f"SELECT collected FROM public._apitap_lease WHERE dest_key = '{self.key}' "
                         f"AND token = '{tok}'") or None

    def count(self):
        return _rig.psql(f"SELECT count(*) FROM public.{T}")

    def clean(self):
        for n in self.names():
            _rig.psql(f'DROP TABLE IF EXISTS public."{n}" CASCADE')
        for t, w in (("_apitap_lease", f"dest_key = '{self.key}'"),
                     ("_apitap_state", f"dest_table IN ('{T}', 'public.{T}')")):
            if _rig.psql(f"SELECT to_regclass('public.{t}') IS NOT NULL") == "t":
                _rig.psql(f"DELETE FROM public.{t} WHERE {w}")


class My:
    url = "mysql://root:bench@127.0.0.1:3307/bench"
    key = f"bench.{T}"
    staging_decoration = ""

    def names(self):
        return [n for n in _rig.mysql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() "
            f"AND table_name LIKE '{T}%'").splitlines() if n]

    def plant(self, name):
        _rig.mysql(f"CREATE TABLE `{name}` (t TINYINT) ENGINE=MEMORY")

    def unplant(self, name):
        _rig.mysql(f"DROP TABLE IF EXISTS `{name}`")

    def live_lease(self, tok):
        _rig.mysql("CREATE TABLE IF NOT EXISTS _apitap_lease (dest_key VARCHAR(320) NOT NULL, "
                   "token VARCHAR(64) NOT NULL, expires_at DATETIME(6) NOT NULL, "
                   "collected TINYINT NOT NULL DEFAULT 0, PRIMARY KEY (dest_key, token)) ENGINE=InnoDB")
        _rig.mysql(f"INSERT INTO _apitap_lease VALUES ('{self.key}', '{tok}', "
                   "UTC_TIMESTAMP(6) + INTERVAL 3600 SECOND, 0)")

    def collected_lease(self, tok):
        self.live_lease(tok)
        _rig.mysql(f"UPDATE _apitap_lease SET collected = 1 WHERE dest_key = '{self.key}' "
                   f"AND token = '{tok}'")

    def lease_row(self, tok):
        return _rig.mysql(f"SELECT collected FROM _apitap_lease WHERE dest_key = '{self.key}' "
                          f"AND token = '{tok}'") or None

    def count(self):
        return _rig.mysql(f"SELECT COUNT(*) FROM `{T}`")

    def clean(self):
        for n in self.names():
            _rig.mysql(f"DROP TABLE IF EXISTS `{n}`")
        for t, w in (("_apitap_lease", f"dest_key = '{self.key}'"),
                     ("_apitap_state", f"dest_table = '{T}'")):
            if _rig.mysql("SELECT count(*) FROM information_schema.tables WHERE "
                          f"table_schema = DATABASE() AND table_name = '{t}'") != "0":
                _rig.mysql(f"DELETE FROM {t} WHERE {w}")


class Ch:
    url = "clickhouse://default:bench@127.0.0.1:8124/default"
    key = f"default.{T}"
    staging_decoration = ""

    def names(self):
        return [n for n in _rig.clickhouse(
            "SELECT name FROM system.tables WHERE database = currentDatabase() "
            f"AND startsWith(name, '{T}')").splitlines() if n]

    def plant(self, name):
        _rig.clickhouse(f"CREATE TABLE `{name}` (t UInt8) ENGINE = Memory")

    def unplant(self, name):
        _rig.clickhouse(f"DROP TABLE IF EXISTS `{name}`")

    def live_lease(self, tok):
        _rig.clickhouse("CREATE TABLE IF NOT EXISTS `_apitap_lease` (dest_key String, token String, "
                        "expires_at DateTime64(6, 'UTC'), collected UInt8, seq UInt64) "
                        "ENGINE = ReplacingMergeTree(seq) ORDER BY (dest_key, token)")
        _rig.clickhouse(f"INSERT INTO `_apitap_lease` SELECT '{self.key}', '{tok}', "
                        "now64(6) + INTERVAL 3600 SECOND, 0, toUnixTimestamp64Micro(now64(6))")

    def collected_lease(self, tok):
        self.live_lease(tok)
        _rig.clickhouse(f"INSERT INTO `_apitap_lease` SELECT '{self.key}', '{tok}', "
                        "now64(6) + INTERVAL 3600 SECOND, 1, toUnixTimestamp64Micro(now64(6)) + 1")

    def lease_row(self, tok):
        return _rig.clickhouse(f"SELECT argMax(collected, seq) FROM `_apitap_lease` WHERE "
                               f"dest_key = '{self.key}' AND token = '{tok}' HAVING count() > 0") or None

    def count(self):
        return _rig.clickhouse(f"SELECT count() FROM `{T}`")

    def clean(self):
        for n in self.names():
            _rig.clickhouse(f"DROP TABLE IF EXISTS `{n}`")
        _rig.clickhouse(f"DROP VIEW IF EXISTS `{T}__current`")
        for t, w in (("_apitap_lease", f"dest_key = '{self.key}'"),
                     ("_apitap_state", f"dest_table = '{T}'")):
            if _rig.clickhouse(f"SELECT count() FROM system.tables WHERE name = '{t}'") != "0":
                _rig.clickhouse(f"ALTER TABLE `{t}` DELETE WHERE {w} SETTINGS mutations_sync = 1")


class S3:
    """No lease store and no CDC lane on an object store: case A only. A run
    lives in a SEGMENT under the table's staging root; the plant is one part
    of a live-looking replace's segment."""
    prefix = "gm"
    url = _rig.s3_url(prefix)
    staging_decoration = None
    root = f"{prefix}/{T}__apitap_staging/"

    def names(self):
        return _rig.s3_list(f"{self.prefix}/{T}")

    def plant(self, name):
        _rig.s3_put(name, b"PAR1")

    def unplant(self, name):
        _rig.s3_delete(name)

    def count(self):
        import duckdb
        d = duckdb.connect()
        d.execute(f"SET s3_endpoint='{_rig.S3_ENDPOINT}'; SET s3_use_ssl=false; SET s3_url_style='path'; "
                  "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; "
                  f"SET s3_region='{_rig.S3_REGION}';")
        return str(d.execute(f"SELECT count(*) FROM read_parquet('s3://{_rig.S3_BUCKET}/"
                             f"{self.prefix}/{T}/*.parquet')").fetchone()[0])

    def clean(self):
        for k in _rig.s3_list(f"{self.prefix}/"):
            _rig.s3_delete(k)


class Ice:
    """An Iceberg table's claims live under the table's own location, in
    `metadata/apitap-runs/`, spelled with the STAGING suffix every Iceberg run
    since 0.55.1 scans for. The location is the catalog's to say, so the table
    is created first and the plant goes where the catalog put it."""
    ns = "cdc_e2e"
    url = (f"iceberg://127.0.0.1:8181/{ns}?endpoint=http://{_rig.S3_ENDPOINT}"
           "&access_key_id=bench&secret_access_key=benchbench")
    table_url = f"http://127.0.0.1:8181/v1/namespaces/{ns}/tables/{T}"
    staging_decoration = None

    def _meta(self):
        import requests
        r = requests.get(self.table_url)
        return r.json() if r.status_code == 200 else None

    def claim_prefix(self):
        loc = self._meta()["metadata"]["location"]              # s3://bucket/key/prefix
        return loc.split("/", 3)[3].rstrip("/") + "/metadata/apitap-runs/"

    def names(self):
        return _rig.s3_list(self.claim_prefix()) if self._meta() else []

    def plant(self, name):
        _rig.s3_put(name, b"planted claim\n")

    def unplant(self, name):
        _rig.s3_delete(name)

    def count(self):
        import duckdb
        d = duckdb.connect()
        d.execute("INSTALL iceberg; LOAD iceberg;")
        d.execute(f"SET s3_endpoint='{_rig.S3_ENDPOINT}'; SET s3_use_ssl=false; SET s3_url_style='path'; "
                  "SET s3_access_key_id='bench'; SET s3_secret_access_key='benchbench'; "
                  f"SET s3_region='{_rig.S3_REGION}';")
        loc = self._meta()["metadata-location"]
        return str(d.execute(f"SELECT count(*) FROM iceberg_scan('{loc}')").fetchone()[0])

    def clean(self):
        import requests
        if self._meta():
            for k in _rig.s3_list(self.claim_prefix()):
                _rig.s3_delete(k)
            requests.delete(self.table_url + "?purgeRequested=true")


E = {"pg": Pg, "bq": Bq, "my": My, "ch": Ch, "s3": S3, "ice": Ice}[ENGINE]()

# Engines whose claim cannot yet take an already-collected row: MySQL's claim
# counts CHANGED rows, and setting `collected = 1` on a row that already says 1
# changes nothing — so a collection that died half way is refused for ever.
# Fixed by the MySQL claim-by-read (handoff §3 step 17), which adds `my` here.
FINISHES_A_COLLECTION = {"pg", "ch", "bq"}
_SLOTS = set(_rig.psql("SELECT slot_name FROM pg_replication_slots", _rig.PG_SRC).split())


def src_clean():
    for p in _rig.psql(f"SELECT DISTINCT pubname FROM pg_publication_tables WHERE tablename = '{T}'",
                       _rig.PG_SRC).split():
        _rig.psql(f"DROP PUBLICATION IF EXISTS {p}", _rig.PG_SRC)
    _rig.psql(f"DROP TABLE IF EXISTS {T} CASCADE", _rig.PG_SRC)
    for s in set(_rig.psql("SELECT slot_name FROM pg_replication_slots", _rig.PG_SRC).split()) - _SLOTS:
        _rig.psql(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
                  f"WHERE slot_name = '{s}' AND NOT active", _rig.PG_SRC)


print(f"== reset ({ENGINE}) ==")
E.clean()
src_clean()
_rig.psql(f"CREATE TABLE {T} (id int PRIMARY KEY, v text)", _rig.PG_SRC)
_rig.psql(f"INSERT INTO {T} SELECT g, 'v'||g FROM generate_series(1,100) g", _rig.PG_SRC)
try:
    print("== A. a live replace's staging refuses a replace, and stays ==")
    if ENGINE == "s3":
        staging = f"{E.root}{_rig.fresh_token('r')}/part-00000.parquet"
    elif ENGINE == "ice":
        # The claim lives under the table's location, so the table must exist.
        apitap.transfer(PG, E.url, table=T, mode="replace")
        staging = f"{E.claim_prefix()}{T}{_rig.fresh_token('r')}__apitap_staging"
    else:
        staging = f"{T}{_rig.fresh_token('r')}__apitap_staging{E.staging_decoration}"
    E.plant(staging)
    e = refusal(lambda: apitap.transfer(PG, E.url, table=T, mode="replace"))
    case("the replace is refused BY TYPE", bool(e) and e.startswith("LockedError"),
         (e or "it was ALLOWED")[:170])
    case("and the plant is still listed", staging in E.names(), f"{E.names()}")
    E.unplant(staging)
    e = refusal(lambda: apitap.transfer(PG, E.url, table=T, mode="replace"))
    case("CONTROL: with the plant gone, the replace runs", e is None, e or "ran")
    case("and landed every row", E.count() == "100", f"{E.count()}")

    # B needs a CDC lane into the engine, which an object store does not have.
    if hasattr(E, "live_lease"):
        print("== B. a live drain's lock (and lease) refuses a drain, and stays ==")
        tok = _rig.fresh_token("l", "live")
        lock = f"{T}{tok}__apitap_lock"
        E.plant(lock)
        E.live_lease(tok)
        e = refusal(lambda: apitap.transfer(PG, E.url, table=T, mode="log_based"))
        case("the drain is refused BY TYPE", bool(e) and e.startswith("LockedError"),
             (e or "it was ALLOWED")[:170])
        case("and the plant is still listed", lock in E.names(), f"{E.names()}")
        E.unplant(lock)

        print("== C. a drain's lock with NO lease is never collected ==")
        orphan = f"{T}{_rig.fresh_token('l', 'nole')}__apitap_lock"
        E.plant(orphan)
        e = refusal(lambda: apitap.transfer(PG, E.url, table=T, mode="replace"))
        case("a replace is refused BY TYPE", bool(e) and e.startswith("LockedError"),
             (e or "it was ALLOWED")[:170])
        case("and says nothing collects it", bool(e) and "nothing collects it" in e, (e or "")[-160:])
        case("and the plant is still listed", orphan in E.names(), f"{E.names()}")
        E.unplant(orphan)

        if ENGINE in FINISHES_A_COLLECTION:
            print("== D. a collection that died half way is finished by the next run ==")
            # A collector claimed this drain (collected = TRUE) and died before
            # dropping its markers. The row's `expires_at` is an hour ahead, so
            # nothing but the collected flag says the drain is gone.
            tok = _rig.fresh_token("l", "half")
            left = [f"{T}{tok}__apitap_lock", f"{T}{tok}__apitap_staging{E.staging_decoration or ''}"]
            if ENGINE == "bq":
                left[1] = f"{T}{tok}__apitap_staging"
            for n in left:
                E.plant(n)
            E.collected_lease(tok)
            e = refusal(lambda: apitap.transfer(PG, E.url, table=T, mode="replace"))
            case("the next run finishes the collection and runs", e is None, e or "ran")
            case("both markers are gone", not (set(left) & set(E.names())), f"{E.names()}")
            case("and the victim's row is still there, collected", str(E.lease_row(tok)) in
                 ("t", "1", "true"), f"{E.lease_row(tok)}")
finally:
    print("== cleanup ==")
    E.clean()
    src_clean()

print(f"\nGUARD MATRIX E2E ({ENGINE}): " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
