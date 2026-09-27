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

    python benchmarks/e2e_guard_matrix.py <bq|my|ch|s3>

Rig: `apitap-bench-pg-src` :5544 as the source; MySQL :3307, ClickHouse :8124,
the gate's BigQuery dataset (BQ_SA), or the bench MinIO (:9100) as the destination.
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


E = {"bq": Bq, "my": My, "ch": Ch, "s3": S3}[ENGINE]()
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
finally:
    print("== cleanup ==")
    E.clean()
    src_clean()

print(f"\nGUARD MATRIX E2E ({ENGINE}): " + ("PASSED" if ok else "FAILED"))
sys.exit(0 if ok else 1)
