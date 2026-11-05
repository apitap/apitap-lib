"""MySQL CDC into each destination: binary and time cells land as the source holds them.

The MySQL lane ships BLOB/BINARY/BIT cells as their raw bytes and TIMESTAMP as
naive UTC text. Into Postgres the bytes were copied as COPY text: a 0x00 or an
invalid UTF-8 byte wedged the lane, and a `\\101` payload re-decoded as
escape-format bytea, silently (MASTER review M-16). A timestamptz column read
the naive text in the destination's session zone, so a server set to
Europe/Vilnius stored every CDC'd TIMESTAMP 2-3 h off (M-25). A zero
TIMESTAMP was decoded as 1970-01-01, an instant the source never held (M-66).

Into ClickHouse, a DECIMAL(65,30) travels as text, and the binlog decoder
zero-padded it past its first group — '0000000000000000000000000123.45…' on
CDC'd rows beside the bulk load's '123.45…' for the same value (M-40).

Proof: bootstrap a table of hostile bytes and timestamps, then insert, update,
rekey and delete through the binlog — twice — into a destination whose
TimeZone is Europe/Vilnius, and compare a full canonical digest of every row
(hex of every binary cell, the UTC instant of every timestamp) after each
drain. Usage: e2e_my_cdc_dests.py pg
"""
import hashlib
import subprocess
import sys

import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
DB = "qa_vilnius"
PG = f"postgres://postgres:bench@127.0.0.1:5545/{DB}"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "mycdc_bin"
TK = "mycdc_binkey"
TD = "mycdc_dec"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def my(sql):
    o = sh(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench",
            "-e", "SET time_zone = '+00:00'; " + sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def pg(sql, db=DB):
    o = sh(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", db,
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
            "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def drain(table, dest=PG):
    return apitap.transfer(MY, dest, table=table, mode="log_based")


# One canonical text per row, the same on both sides: every binary cell as
# upper-case hex, every timestamp as its UTC instant to the microsecond.
MY_ROW = {
    T: "CONCAT_WS('|', id, IFNULL(HEX(b), 'N'), IFNULL(HEX(bn), 'N'), IFNULL(LPAD(HEX(bt), 2, '0'), 'N'), "
       "IFNULL(HEX(vb), 'N'), IFNULL(DATE_FORMAT(ts, '%Y-%m-%d %H:%i:%s.%f'), 'N'), "
       "IFNULL(DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s.%f'), 'N'), IFNULL(HEX(t), 'N'))",
    TK: "CONCAT_WS('|', HEX(k), IFNULL(HEX(b), 'N'), v)",
}
PG_ROW = {
    T: "concat_ws('|', id, coalesce(upper(encode(b, 'hex')), 'N'), coalesce(upper(encode(bn, 'hex')), 'N'), "
       "coalesce(upper(encode(bt, 'hex')), 'N'), coalesce(upper(encode(vb, 'hex')), 'N'), "
       "coalesce(to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), 'N'), "
       "coalesce(to_char(dt, 'YYYY-MM-DD HH24:MI:SS.US'), 'N'), "
       "coalesce(upper(encode(convert_to(t, 'UTF8'), 'hex')), 'N'))",
    TK: "concat_ws('|', upper(encode(k, 'hex')), coalesce(upper(encode(b, 'hex')), 'N'), v)",
}


def digests(table):
    s = sorted(my(f"SELECT {MY_ROW[table]} FROM {table}").splitlines())
    d = sorted(pg(f"SELECT {PG_ROW[table]} FROM {table}").splitlines())
    return s, d


def agree(table, stage):
    s, d = digests(table)
    if s != d:
        only_s = sorted(set(s) - set(d))[:5]
        only_d = sorted(set(d) - set(s))[:5]
        raise AssertionError(f"{table} {stage}: source {len(s)} rows vs destination {len(d)}\n"
                             f"  source only: {only_s}\n  destination only: {only_d}")
    h = hashlib.sha256("\n".join(s).encode()).hexdigest()[:12]
    print(f"  {table} {stage}: {len(s)} rows, digest {h} on both sides")


def wipe():
    for t in (T, TK):
        my(f"DROP TABLE IF EXISTS {t}")
        pg(f"DROP TABLE IF EXISTS {t}")
    pg(f"DO $$ BEGIN IF to_regclass('_apitap_state') IS NOT NULL THEN "
       f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', '{TK}', 'public.{T}', 'public.{TK}'); "
       f"END IF; END $$")


def main_pg():
    if pg(f"SELECT count(*) FROM pg_database WHERE datname = '{DB}'", db="postgres") == "0":
        pg(f"CREATE DATABASE {DB}", db="postgres")
    pg(f"ALTER DATABASE {DB} SET TimeZone = 'Europe/Vilnius'", db="postgres")
    assert pg("SHOW TimeZone") == "Europe/Vilnius"
    wipe()

    my(f"CREATE TABLE {T} (id int PRIMARY KEY, b blob, bn binary(4), bt bit(1), vb varbinary(16), "
       f"ts timestamp(6) NULL, dt datetime(6), t text)")
    my(f"INSERT INTO {T} SELECT n, UNHEX(LPAD(HEX(n), 4, '0')), UNHEX('0A0B0C0D'), b'1', UNHEX('41'), "
       f"'2024-01-02 03:04:05.5' + INTERVAL n HOUR, '2024-07-01 12:00:00', CONCAT('row ', n) "
       f"FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6) s")
    # A BINARY(16) key that ends in 0x00, so the delete phase's keys and the
    # residue's key binds are bytes too.
    my(f"CREATE TABLE {TK} (k binary(16) PRIMARY KEY, b blob, v int)")
    my(f"INSERT INTO {TK} VALUES (UNHEX('00112233445566778899AABBCCDDEE00'), UNHEX('00'), 1), "
       f"(UNHEX('FF112233445566778899AABBCCDDEEFF'), UNHEX('5C313031'), 2), "
       f"(UNHEX('01000000000000000000000000000000'), NULL, 3)")
    for t in (T, TK):
        drain(t)
        agree(t, "after the bootstrap")

    # Hostile bytes through the binlog: NUL, invalid UTF-8, a `\101` escape,
    # the COPY framing bytes, BIT 0 and 1, summer and winter timestamps.
    my(f"INSERT INTO {T} VALUES "
       f"(10, UNHEX('00'), UNHEX('00000001'), b'0', UNHEX('FF00'), '2024-06-02 03:04:05.5', '2024-01-02 03:04:05', 'nul'), "
       f"(11, UNHEX('5C313031'), UNHEX('5C783030'), b'1', UNHEX('5C'), '2024-12-31 23:59:59.999999', NULL, 'esc'), "
       f"(12, UNHEX('090A0D5C'), UNHEX('FFFEFDFC'), b'1', UNHEX(''), '2024-03-31 01:30:00', '2024-03-31 03:30:00', 'tabs\\there'), "
       f"(13, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    my(f"UPDATE {T} SET b = UNHEX('C328'), ts = '2024-10-27 00:30:00' WHERE id = 2")
    my(f"UPDATE {T} SET id = 102, b = UNHEX('00FF00') WHERE id = 3")   # rekey: the residue binds
    my(f"DELETE FROM {T} WHERE id = 4")
    my(f"UPDATE {TK} SET b = UNHEX('FF'), v = 10 WHERE k = UNHEX('00112233445566778899AABBCCDDEE00')")
    my(f"UPDATE {TK} SET k = UNHEX('00000000000000000000000000000000') "
       f"WHERE k = UNHEX('FF112233445566778899AABBCCDDEEFF')")         # rekey on a bytes key
    my(f"DELETE FROM {TK} WHERE k = UNHEX('01000000000000000000000000000000')")
    my(f"INSERT INTO {TK} VALUES (UNHEX('0A090D5C000000000000000000000000'), UNHEX('5C783431'), 4)")
    for t in (T, TK):
        drain(t)
        agree(t, "after drain 1")

    my(f"UPDATE {T} SET vb = UNHEX('80'), bt = b'0', ts = ts + INTERVAL 1 SECOND WHERE id IN (1, 10, 11)")
    my(f"DELETE FROM {T} WHERE id = 12")
    my(f"UPDATE {TK} SET v = v + 1")
    for t in (T, TK):
        drain(t)
        agree(t, "after drain 2")

    # A zero TIMESTAMP (allowed with an empty sql_mode) is relayed as the zero
    # literal, which Postgres refuses: loud, where it used to land as 1970.
    my(f"SET sql_mode = ''; INSERT INTO {T} (id, ts) VALUES (20, '0000-00-00 00:00:00')")
    try:
        drain(T)
        raise AssertionError("a zero TIMESTAMP must not land in Postgres as a real instant")
    except RuntimeError as e:
        assert "0000-00-00" in str(e), f"the refusal must name the zero value: {e}"
    assert pg(f"SELECT count(*) FROM {T} WHERE id = 20") == "0"
    my(f"DELETE FROM {T} WHERE id = 20")
    drain(T)
    agree(T, "after the zero TIMESTAMP was refused and then deleted at the source")

    wipe()
    print("✓ MySQL CDC into Postgres: bytes and instants land as the source holds them")


def ch_wipe():
    my(f"DROP TABLE IF EXISTS {TD}")
    ch(f"DROP TABLE IF EXISTS {TD}")
    if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{TD}' SETTINGS mutations_sync = 1")


def main_ch():
    ch_wipe()
    # Three value classes, each written once by the bulk load and again by
    # the binlog: a class must read back with ONE spelling per column.
    vals = {1: "123.45, -0.5, 0.01", 2: "0.000001, 123456789012345.6789, -99999999.99", 3: "-123.45, 0, 0"}
    my(f"CREATE TABLE {TD} (id int PRIMARY KEY, c int, d65 decimal(65,30), d19 decimal(19,4), d10 decimal(10,2))")
    my(f"INSERT INTO {TD} VALUES " + ", ".join(f"({c}, {c}, {v})" for c, v in vals.items()))
    drain(TD, CH)
    my(f"INSERT INTO {TD} VALUES " + ", ".join(f"({10 + c}, {c}, {v})" for c, v in vals.items()))
    my(f"UPDATE {TD} SET c = 1, d65 = 123.45, d19 = -0.5, d10 = 0.01 WHERE id = 2")
    drain(TD, CH)
    n = my(f"SELECT COUNT(*) FROM {TD}")
    got = ch(f"SELECT c, uniqExact(toString(d65)), uniqExact(toString(d19)), uniqExact(toString(d10)), "
             f"groupUniqArray(toString(d65)) FROM {TD} GROUP BY c ORDER BY c FORMAT TSV")
    for line in got.splitlines():
        c, u65, u19, u10, spell = line.split("\t")
        assert (u65, u19, u10) == ("1", "1", "1"), f"class {c}: one value, several spellings {spell}"
    assert ch(f"SELECT count() FROM {TD}") == n, "row count"
    print(f"  {TD}: {n} rows, every DECIMAL value spelled one way by bulk and binlog alike")
    ch_wipe()
    print("✓ MySQL CDC into ClickHouse: a DECIMAL is spelled the same by the bulk load and the binlog")


# ── BigQuery (D8) ───────────────────────────────────────────────────────────
# MySQL's binary columns land BYTES in BigQuery from 0.65.0 (0.64.0 refused
# them at the bootstrap). The CDC apply read every BYTES cell as Postgres's
# `\x`-hex text (FROM_HEX(SUBSTR(c, 3))), while the MySQL lane ships raw
# bytes: BIT(1) 0x01 would land as empty bytes, silently, and bytes that are
# not UTF-8 would wedge the lane. Every binary cell is digest-checked.
TB, TBK, TBC = "mycdc_bq_bin", "mycdc_bq_binkey", "mycdc_bq_bin_cl"
BQ_ROW = {
    TB: "ARRAY_TO_STRING([CAST(id AS STRING), IFNULL(UPPER(TO_HEX(b)), 'N'), IFNULL(UPPER(TO_HEX(bn)), 'N'), "
        "IFNULL(UPPER(TO_HEX(bt)), 'N'), IFNULL(UPPER(TO_HEX(vb)), 'N'), "
        "IFNULL(UPPER(TO_HEX(CAST(t AS BYTES))), 'N')], '|')",
    TBK: "ARRAY_TO_STRING([UPPER(TO_HEX(k)), IFNULL(UPPER(TO_HEX(b)), 'N'), CAST(v AS STRING)], '|')",
}
BQ_ROW[TBC] = BQ_ROW[TB]
MY_BQ_ROW = {
    TB: "CONCAT_WS('|', id, IFNULL(HEX(b), 'N'), IFNULL(HEX(bn), 'N'), IFNULL(LPAD(HEX(bt), 2, '0'), 'N'), "
        "IFNULL(HEX(vb), 'N'), IFNULL(HEX(t), 'N'))",
    TBK: MY_ROW[TK],
}
MY_BQ_ROW[TBC] = MY_BQ_ROW[TB]


def bq_env():
    import os
    import requests
    from google.oauth2 import service_account
    import google.auth.transport.requests as gt
    project = os.environ.get("BQ_PROJECT", "apitap")
    ds = os.environ.get("BQ_DATASET", "apitap_cdc_e2e")
    creds = service_account.Credentials.from_service_account_file(
        os.environ["BQ_SA"], scopes=["https://www.googleapis.com/auth/bigquery"])

    def bq(sql):
        creds.refresh(gt.Request())
        r = requests.post(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries",
            headers={"Authorization": f"Bearer {creds.token}"},
            json={"query": sql, "useLegacySql": False, "timeoutMs": 120000, "location": "US"})
        j = r.json()
        if r.status_code >= 400 or j.get("jobComplete") is False:
            raise RuntimeError(f"BQ query failed: {j.get('error', j)}")
        return [[c.get("v") for c in row.get("f", [])] for row in j.get("rows", [])]
    url = f"bigquery://{project}/{ds}?credentials={os.environ['BQ_SA']}"
    return bq, ds, url


def main_bq():
    bq, ds, url = bq_env()

    def bq_wipe():
        for t in (TB, TBK, TBC):
            my(f"DROP TABLE IF EXISTS {t}")
            try:
                bq(f"DROP VIEW IF EXISTS `{ds}.{t}__current`")
            except Exception:
                pass
            for x in (t, f"{t}__apitap_cl", f"{t}__apitap_cdc"):
                try:
                    bq(f"DROP TABLE IF EXISTS `{ds}.{x}`")
                except Exception:
                    pass
            for st in ("_apitap_state", "_apitap_cdc_pending"):
                try:
                    bq(f"DELETE FROM `{ds}.{st}` WHERE dest_table = '{t}'")
                except Exception:
                    pass

    def bq_agree(table, stage):
        rel = f"{table}__current" if table == TBC else table
        s = sorted(my(f"SELECT {MY_BQ_ROW[table]} FROM {table}").splitlines())
        d = sorted(r[0] for r in bq(f"SELECT {BQ_ROW[table]} FROM `{ds}.{rel}`"))
        if s != d:
            only_s = sorted(set(s) - set(d))[:5]
            only_d = sorted(set(d) - set(s))[:5]
            raise AssertionError(f"{table} {stage}: source {len(s)} rows vs BigQuery {len(d)}\n"
                                 f"  source only: {only_s}\n  BigQuery only: {only_d}")
        h = hashlib.sha256("\n".join(s).encode()).hexdigest()[:12]
        print(f"  {table} {stage}: {len(s)} rows, digest {h} on both sides", flush=True)

    def bq_drain(t):
        return apitap.transfer(MY, url, table=t, mode="log_based", changelog=(t == TBC))

    bq_wipe()
    try:
        for t in (TB, TBC):
            my(f"CREATE TABLE {t} (id int PRIMARY KEY, b blob, bn binary(4), bt bit(1), vb varbinary(16), t text)")
            my(f"INSERT INTO {t} SELECT n, UNHEX(LPAD(HEX(n), 4, '0')), UNHEX('0A0B0C0D'), b'1', UNHEX('41'), "
               f"CONCAT('row ', n) FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) s")
        my(f"CREATE TABLE {TBK} (k binary(16) PRIMARY KEY, b blob, v int)")
        my(f"INSERT INTO {TBK} VALUES (UNHEX('00112233445566778899AABBCCDDEE00'), UNHEX('00'), 1), "
           f"(UNHEX('FF112233445566778899AABBCCDDEEFF'), UNHEX('5C313031'), 2), "
           f"(UNHEX('01000000000000000000000000000000'), NULL, 3)")
        # 0.65.0 opened the bulk lane for MySQL's binary columns into BigQuery
        # (BYTES); 0.64.0 refused them at the bootstrap. A refusal now fails.
        bq_drain(TB)
        bq_agree(TB, "after the bootstrap")
        for t in (TBK, TBC):
            bq_drain(t)
            bq_agree(t, "after the bootstrap")

        # Hostile bytes through the binlog: NUL, invalid UTF-8, a `\101` escape,
        # BIT 0 and 1 (0x01 is what decoded to empty bytes).
        for t in (TB, TBC):
            my(f"INSERT INTO {t} VALUES "
               f"(10, UNHEX('00'), UNHEX('00000001'), b'0', UNHEX('FF00'), 'nul'), "
               f"(11, UNHEX('5C313031'), UNHEX('5C783030'), b'1', UNHEX('5C'), 'esc'), "
               f"(12, UNHEX('090A0D5C'), UNHEX('FFFEFDFC'), b'1', UNHEX(''), 'tabs'), "
               f"(13, NULL, NULL, NULL, NULL, NULL)")
            my(f"UPDATE {t} SET b = UNHEX('C328') WHERE id = 2")
            my(f"UPDATE {t} SET id = 102, b = UNHEX('00FF00') WHERE id = 3")
            my(f"DELETE FROM {t} WHERE id = 4")
        my(f"UPDATE {TBK} SET b = UNHEX('FF'), v = 10 WHERE k = UNHEX('00112233445566778899AABBCCDDEE00')")
        my(f"UPDATE {TBK} SET k = UNHEX('00000000000000000000000000000000') "
           f"WHERE k = UNHEX('FF112233445566778899AABBCCDDEEFF')")
        my(f"DELETE FROM {TBK} WHERE k = UNHEX('01000000000000000000000000000000')")
        my(f"INSERT INTO {TBK} VALUES (UNHEX('0A090D5C000000000000000000000000'), UNHEX('5C783431'), 4)")
        for t in (TB, TBK, TBC):
            bq_drain(t)
            bq_agree(t, "after drain 1")

        for t in (TB, TBC):
            my(f"UPDATE {t} SET vb = UNHEX('80'), bt = b'0' WHERE id IN (1, 10, 11)")
            my(f"DELETE FROM {t} WHERE id = 12")
        my(f"UPDATE {TBK} SET v = v + 1")
        for t in (TB, TBK, TBC):
            bq_drain(t)
            bq_agree(t, "after drain 2")
    finally:
        bq_wipe()
    print("✓ MySQL CDC into BigQuery: binary cells land BYTES as the source holds them (replica and changelog)")


if __name__ == "__main__":
    dest = sys.argv[1:]
    assert dest in (["pg"], ["ch"], ["bq"]), "usage: e2e_my_cdc_dests.py pg|ch|bq"
    {"pg": main_pg, "ch": main_ch, "bq": main_bq}[dest[0]]()
