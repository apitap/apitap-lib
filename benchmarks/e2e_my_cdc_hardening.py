"""MySQL CDC hardening: tagged GTIDs, commented TRUNCATE, the transaction cap, state keys by case.

Four P2 findings (docs/review/2026-10-18-p2-triage.md), each against a real
server:
  tagged   MySQL 8.3+ writes GTID_TAGGED_LOG_EVENT (42) for
           `SET gtid_next='AUTOMATIC:<tag>'`; the drain refused it and every
           rerun stopped at the same event. (Its own MySQL 8.4 container,
           gtid_mode=ON.)
  truncate `/* app */ TRUNCATE TABLE t` was classed DDL and then not seen as
           a truncate: skipped silently, the destination kept the rows.
  txcap    one MySQL transaction buffered without bound (the Postgres lane
           caps it at APITAP_TX_BUF_BYTES): a big UPDATE OOM-looped. It is
           now refused naming the table, and the watermark stays.
  state    `_apitap_state` keys compared under the server's _ci collation:
           Postgres tables "Orders" and "orders" appended into one MySQL
           database shared one state row.
  enum     COLUMN_TYPE writes an enum label's backslash, LF and NUL escaped
           with a backslash; the CDC label table kept them escaped, so a CDC
           row carried two backslashes where the bulk load wrote one.
Usage: e2e_my_cdc_hardening.py tagged|truncate|txcap|state|enum
"""
import os
import subprocess
import sys
import time

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
MY = "mysql://root:bench@127.0.0.1:3307/bench"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


def mysql(box, sql, db="bench", extra=()):
    o = sh(["docker", "exec", box, "mysql", "-uroot", "-pbench", "-N", *extra, db, "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch_wipe(t):
    ch(f"DROP TABLE IF EXISTS {t}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{t}' SETTINGS mutations_sync = 1")


def tagged():
    name, port = "apitap-qa-my84-gtid", 18312
    url = f"mysql://root:bench@127.0.0.1:{port}/bench"
    t = "my_tagged_gtid"
    sh(["docker", "rm", "-f", name])
    r = sh(["docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:3306",
            "-e", "MYSQL_ROOT_PASSWORD=bench", "-e", "MYSQL_DATABASE=bench", "mysql:8.4",
            "--server-id=9", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
            "--binlog-format=ROW", "--binlog-row-image=FULL"])
    assert r.returncode == 0, r.stderr
    ch_wipe(t)
    try:
        for _ in range(120):
            if sh(["docker", "exec", name, "mysql", "-uroot", "-pbench", "-e", "SELECT 1"]).returncode == 0:
                break
            time.sleep(1)
        mysql(name, f"CREATE TABLE {t} (id int PRIMARY KEY, v varchar(10)); INSERT INTO {t} VALUES (1, 'a')")
        apitap.transfer(url, CH, table=t, mode="log_based")
        mysql(name, f"SET gtid_next = 'AUTOMATIC:apitap'; INSERT INTO {t} VALUES (2, 'tagged')")
        apitap.transfer(url, CH, table=t, mode="log_based")
        got = ch(f"SELECT arrayStringConcat(groupArray(concat(toString(id), ':', v)), ',') "
                 f"FROM (SELECT id, v FROM {t} ORDER BY id)")
        assert got == "1:a,2:tagged", f"the tagged-GTID transaction must land, got {got!r}"
        print(f"  MySQL 8.4 tagged GTID transaction drained: {got}")
    finally:
        sh(["docker", "rm", "-f", name])
        ch_wipe(t)


def truncate():
    t = "my_trunc_comment"
    mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
    ch_wipe(t)
    try:
        mysql("apitap-bench-my", f"CREATE TABLE {t} (id int PRIMARY KEY); INSERT INTO {t} VALUES (1), (2), (3)")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        # --comments: the client keeps the comment, so the binlog QUERY text
        # starts with it, as it does for an application's statements.
        mysql("apitap-bench-my", f"/* app */ TRUNCATE TABLE {t}", extra=("--comments",))
        mysql("apitap-bench-my", f"INSERT INTO {t} VALUES (9)")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        got = ch(f"SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM {t} ORDER BY id)")
        assert got == "9", f"a commented TRUNCATE must reach the destination, got {got!r}"
        print("  /* app */ TRUNCATE TABLE: applied, destination holds only the later row")
    finally:
        mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
        ch_wipe(t)


def txcap():
    t = "my_tx_cap"
    mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
    ch_wipe(t)
    try:
        mysql("apitap-bench-my", f"CREATE TABLE {t} (id int PRIMARY KEY, v varchar(200)); "
              f"INSERT INTO {t} WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < 1000) "
              f"SELECT x, REPEAT('a', 200) FROM g")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        before = ch(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table = '{t}'")
        mysql("apitap-bench-my", f"UPDATE {t} SET v = REPEAT('b', 200)")       # ~800 KB in one transaction
        code = f"import apitap\napitap.transfer({MY!r}, {CH!r}, table={t!r}, mode='log_based')\n"
        r = sh([sys.executable, "-c", code], env=dict(os.environ, APITAP_TX_BUF_BYTES="256K"))
        assert r.returncode != 0 and "one transaction" in r.stderr and t in r.stderr, \
            f"a transaction past the cap must be refused naming the table:\n{r.stderr[-500:]}"
        after = ch(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table = '{t}'")
        assert after == before, f"the watermark must stay: {before} -> {after}"
        r = sh([sys.executable, "-c", code])
        assert r.returncode == 0, r.stderr[-500:]
        assert ch(f"SELECT countIf(v LIKE 'b%') FROM {t}") == "1000", "with the default cap it lands"
        print("  an 800 KB transaction under a 256 KB cap: refused naming the table, watermark kept; lands at the default")
    finally:
        mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
        ch_wipe(t)


def state():
    pg = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
    db = "apitap_state_ci"
    dst = f"mysql://root:bench@127.0.0.1:3308/{db}"

    def psql(sql):
        o = sh(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d", "apitap_bench_src",
                "-v", "ON_ERROR_STOP=1", "-Atc", sql])
        if o.returncode:
            raise RuntimeError(o.stderr)
        return o.stdout.strip()

    def wipe():
        psql('DROP TABLE IF EXISTS "StOrders"; DROP TABLE IF EXISTS storders')
        mysql("apitap-bench-my-dst", f"DROP DATABASE IF EXISTS {db}")

    wipe()
    try:
        # Its own database, its state table as a release before this fix
        # created it (keys under the server's _ci collation): the run must
        # convert it, and a fresh one is created binary by the same DDL.
        mysql("apitap-bench-my-dst", f"CREATE DATABASE {db}; CREATE TABLE {db}._apitap_state ("
              "dest_table VARCHAR(255) NOT NULL, source_id VARCHAR(512) NOT NULL, cursor_col VARCHAR(255), "
              "watermark VARCHAR(255), mode VARCHAR(16), last_rows BIGINT, synced_at DATETIME(6), "
              "PRIMARY KEY (dest_table, source_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        psql('CREATE TABLE "StOrders" (id int PRIMARY KEY); INSERT INTO "StOrders" SELECT generate_series(1, 100)')
        psql("CREATE TABLE storders (id int PRIMARY KEY); INSERT INTO storders SELECT generate_series(1, 5)")
        apitap.transfer(pg, dst, table="public.StOrders", dest_table="StOrders", mode="append", cursor="id")
        apitap.transfer(pg, dst, table="storders", mode="append", cursor="id")
        rows = mysql("apitap-bench-my-dst", "SELECT CONCAT(dest_table, '=', watermark) FROM _apitap_state "
                     "ORDER BY dest_table", db=db)
        assert rows.split() == ["StOrders=100", "storders=5"], f"two tables, two state rows: {rows.split()}"
        psql("INSERT INTO storders SELECT generate_series(6, 8)")
        apitap.transfer(pg, dst, table="storders", mode="append", cursor="id")
        got = mysql("apitap-bench-my-dst", "SELECT COUNT(*) FROM storders", db=db)
        assert got == "8", f"storders resumes from its own watermark (5), not StOrders' (100): {got} rows"
        print("  \"StOrders\" and \"storders\" over a _ci state table: converted, two rows, each resumes from its own")
    finally:
        wipe()


def enum():
    t = "my_enum_escapes"
    mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
    ch_wipe(t)
    try:
        mysql("apitap-bench-my", f"CREATE TABLE {t} (id int PRIMARY KEY, e enum('a\\\\b', 'n\\nl', 'plain')); "
              f"INSERT INTO {t} VALUES (1, 1), (2, 2)")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        mysql("apitap-bench-my", f"INSERT INTO {t} VALUES (11, 1), (12, 2)")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        src = mysql("apitap-bench-my", f"SELECT GROUP_CONCAT(HEX(e) ORDER BY id) FROM {t}")
        got = ch(f"SELECT arrayStringConcat(groupArray(hex(toString(e))), ',') FROM (SELECT e FROM {t} ORDER BY id)")
        assert got == src, f"bulk rows 1,2 and CDC rows 11,12 must carry the source's bytes: source {src}, destination {got}"
        print(f"  ENUM labels a\\b and n<LF>l, bulk and CDC: {got}")
    finally:
        mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
        ch_wipe(t)


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"tagged": tagged, "truncate": truncate, "txcap": txcap, "state": state, "enum": enum}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_my_cdc_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ MySQL CDC hardening: {which[0]}")
