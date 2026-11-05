"""MySQL CDC hardening: tagged GTIDs, commented TRUNCATE, the transaction cap, state keys by case.

Four P2 findings (docs/review/2026-10-18-p2-triage.md), each against a real
server:
  tagged   MySQL 8.3+ writes GTID_TAGGED_LOG_EVENT (42) for
           `SET gtid_next='AUTOMATIC:<tag>'`; the drain refused it and every
           rerun stopped at the same event. (Its own MySQL 8.4 container,
           gtid_mode=ON.)
  truncate `/* app */ TRUNCATE TABLE t` was classed DDL and then not seen as
           a truncate: skipped silently, the destination kept the rows.
  txcap    one MySQL transaction buffered without bound: a big UPDATE
           OOM-looped. Through 0.65.0 it was refused at APITAP_TX_BUF_BYTES;
           from 0.66 it spills to disk and lands whole in chunks (a
           changelog=True lane still refuses, watermark kept).
  state    `_apitap_state` keys compared under the server's _ci collation:
           Postgres tables "Orders" and "orders" appended into one MySQL
           database shared one state row.
  enum     COLUMN_TYPE writes an enum label's backslash, LF and NUL escaped
           with a backslash; the CDC label table kept them escaped, so a CDC
           row carried two backslashes where the bulk load wrote one.
  dropped  writes to a table, then DROP TABLE, drained after the drop: the
           drain died on "bench.t not found" every rerun, with no recovery.
Usage: e2e_my_cdc_hardening.py tagged|truncate|txcap|state|enum|dropped
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


def state_rows(t):
    # Every state row of the table, sorted: there is more than one, and an
    # unordered first row made the comparison flip between runs.
    return ch(f"SELECT arrayStringConcat(arraySort(groupArray(concat(mode, '=', watermark))), ',') "
              f"FROM _apitap_state FINAL WHERE dest_table = '{t}'")


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
    """One transaction past the cap spills to disk and lands whole (A2); a
    changelog=True lane still refuses it naming the table and the watermark
    stays."""
    t = "my_tx_cap"
    n = 10000
    mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
    ch_wipe(t)
    ch_wipe(t + "_cl")
    seed = (f"SET SESSION cte_max_recursion_depth = {n + 1}; "
            f"INSERT INTO {t} WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < {n}) "
            f"SELECT x, REPEAT('a', 200) FROM g")
    digest = "SELECT MD5(GROUP_CONCAT(CONCAT(id, ':', v) ORDER BY id SEPARATOR ','))"
    try:
        mysql("apitap-bench-my", f"CREATE TABLE {t} (id int PRIMARY KEY, v varchar(200)); {seed}")
        mysql("apitap-bench-my", "SET SESSION group_concat_max_len = 1 << 30")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        apitap.transfer(MY, CH, table=t, dest_table=t + "_cl", mode="log_based", changelog=True)
        # ~5 MB in one transaction: every row rewritten, 500 new, 300 gone.
        mysql("apitap-bench-my", f"BEGIN; UPDATE {t} SET v = REPEAT('b', 200); "
              f"INSERT INTO {t} SELECT id + {n}, 'new' FROM {t} WHERE id <= 500; "
              f"DELETE FROM {t} WHERE id % 33 = 0; COMMIT;")
        env = dict(os.environ, APITAP_TX_BUF_BYTES="256K", APITAP_CDC_WINDOW_BYTES=str(1 << 20),
                   APITAP_DEBUG="1")
        code = f"import apitap\napitap.transfer({MY!r}, {CH!r}, table={t!r}, mode='log_based')\n"
        r = sh([sys.executable, "-c", code], env=env)
        chunks = [l for l in r.stderr.splitlines() if "spilled transaction: chunk" in l]
        assert r.returncode == 0, f"a transaction past the cap must land (exit {r.returncode}):\n{r.stderr[-1500:]}"
        assert len(chunks) >= 2 and "(last)" in chunks[-1], f"replayed in chunks: {chunks}"
        want = mysql("apitap-bench-my", f"SET SESSION group_concat_max_len = 1 << 30; {digest} FROM {t}")
        got = ch(f"SELECT lower(hex(MD5(arrayStringConcat(groupArray(s), ',')))) FROM "
                 f"(SELECT concat(toString(id), ':', v) AS s FROM {t} ORDER BY id)")
        assert got == want, f"the destination equals the source: dest {got} source {want}"
        before = state_rows(t + "_cl")
        code = (f"import apitap\napitap.transfer({MY!r}, {CH!r}, table={t!r}, dest_table={t + '_cl'!r}, "
                f"mode='log_based', changelog=True)\n")
        r = sh([sys.executable, "-c", code], env=env)
        assert r.returncode != 0 and "one transaction" in r.stderr and t in r.stderr \
            and "changelog=True" in r.stderr, f"changelog still refuses naming why:\n{r.stderr[-1500:]}"
        assert state_rows(t + "_cl") == before, "the changelog lane's watermark stays"
        print(f"  a ~5 MB transaction under a 256 KB cap: {len(chunks)} chunks, destination equals the source; "
              "changelog=True still refuses, watermark kept")
    finally:
        mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
        ch_wipe(t)
        ch_wipe(t + "_cl")
        ch(f"DROP TABLE IF EXISTS {t}_cl__current")


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


def dropped():
    t = "my_dropped_mid"
    mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
    ch_wipe(t)
    try:
        mysql("apitap-bench-my", f"CREATE TABLE {t} (id int PRIMARY KEY); INSERT INTO {t} VALUES (1)")
        apitap.transfer(MY, CH, table=t, mode="log_based")
        before = state_rows(t)
        mysql("apitap-bench-my", f"INSERT INTO {t} SELECT 1 + n FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3) x; "
              f"DROP TABLE {t}")
        code = f"import apitap\napitap.transfer({MY!r}, {CH!r}, table={t!r}, mode='log_based')\n"
        r = sh([sys.executable, "-c", code])
        assert r.returncode != 0 and "dropped or renamed" in r.stderr, \
            f"a table dropped after its writes must be named, with the recovery (exit {r.returncode}):\n{r.stderr[-600:]}"
        assert state_rows(t) == before, "the watermark must stay"
        print("  writes then DROP TABLE, drained after: refused naming the drop and the recovery")
    finally:
        mysql("apitap-bench-my", f"DROP TABLE IF EXISTS {t}")
        ch_wipe(t)


if __name__ == "__main__":
    which = sys.argv[1:]
    cases = {"tagged": tagged, "truncate": truncate, "txcap": txcap, "state": state, "enum": enum, "dropped": dropped}
    assert len(which) == 1 and which[0] in cases, "usage: e2e_my_cdc_hardening.py " + "|".join(cases)
    cases[which[0]]()
    print(f"✓ MySQL CDC hardening: {which[0]}")
