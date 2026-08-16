"""MariaDB source e2e — binlog CDC into ClickHouse, collapse + changelog.

MariaDB is NOT MySQL on the wire that matters here: 10.x still writes the v1
rows events (23/24/25) MySQL retired in 5.6, opens each transaction with a
GTID event instead of a `BEGIN` query, interleaves ANNOTATE_ROWS frames, and
returns a four-column `SHOW MASTER STATUS`. Until this lane existed the
source was refused outright at the precheck; this file is what lets that
refusal be lifted without taking the divergences on faith.

Shape mirrors a real deployment (MariaDB 10.6 → ClickHouse): a money table
with DECIMAL/TIMESTAMP/VARBINARY/unicode columns, seeded, bootstrapped, then
every operation class replayed and digest-verified against the source.

Rig: `apitap-bench-mariadb` on :3309 (root/bench), `apitap-bench-ch` on :8124.
"""
import subprocess
import apitap

MA = "mysql://root:bench@127.0.0.1:3309/bench"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "ma_transfer"
TC = "ma_transfer_cl"

DDL = """CREATE TABLE bench.{t} (
  id BIGINT PRIMARY KEY,
  ref VARCHAR(64) NOT NULL,
  amount DECIMAL(18,2) NOT NULL,
  currency CHAR(3) NOT NULL,
  status TINYINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NULL,
  note VARCHAR(200) NULL,
  payload VARBINARY(32) NULL
)"""


def ma(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-mariadb", "mariadb",
         "-uroot", "-pbench", "-N", "-D", "bench", "-e", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


# Two renderings that are NOT data differences and must not be compared as
# text: ClickHouse's toString(Decimal) drops trailing zeros (verified on
# 24.8 — toString(toDecimal64(102.50,2)) = '102.5'), and its DateTime64(6)
# prints microseconds MySQL's TIMESTAMP does not. Money therefore travels
# as integer cents on both sides, and timestamps as an explicit format.
def src_digest(table):
    return ma(
        f"SELECT CONCAT_WS('|', id, ref, CAST(ROUND(amount*100) AS SIGNED), "
        f"currency, status, IFNULL(DATE_FORMAT(created_at,'%Y-%m-%d %H:%i:%s'),'<N>'), "
        f"IFNULL(note,'<N>')) "
        f"FROM bench.{table} ORDER BY id")


def dst_digest(table):
    return ch(
        f"SELECT concatWithSeparator('|', toString(id), ref, "
        f"toString(toInt64(round(amount*100))), "
        f"currency, toString(status), "
        f"ifNull(formatDateTime(created_at,'%Y-%m-%d %H:%i:%S'),'<N>'), "
        f"ifNull(note,'<N>')) "
        f"FROM {table} ORDER BY id")


def binary_column_agrees(stage):
    """VARBINARY travels its own way per destination — compare it as bytes."""
    s = ma(f"SELECT CONCAT_WS('|', id, IFNULL(HEX(payload),'<N>')) "
           f"FROM bench.{T} WHERE id <= 6 ORDER BY id")
    d = ch(f"SELECT concatWithSeparator('|', toString(id), "
           f"ifNull(upper(hex(payload)),'<N>')) FROM {T} WHERE id <= 6 ORDER BY id")
    if s == d:
        print(f"   ✓ {stage}: VARBINARY bytes identical")
        return True
    print(f"   ✗ {stage}: VARBINARY differs\n   src:\n{s}\n   dst:\n{d}")
    return False


def matches(stage, table=T):
    s, d = src_digest(table), dst_digest(table)
    if s == d:
        print(f"   ✓ {stage}: {len(s.splitlines())} rows identical")
        return True
    sl, dl = s.splitlines(), d.splitlines()
    print(f"   ✗ {stage}: MISMATCH  src={len(sl)} dst={len(dl)}")
    for a, b in list(zip(sl, dl))[:5]:
        if a != b:
            print(f"       src: {a}\n       dst: {b}")
    return False


ok = True
print("== reset: empty world on both sides ==")
for t in (T, TC):
    ma(f"DROP TABLE IF EXISTS bench.{t}")
    ch(f"DROP TABLE IF EXISTS {t}")
    ch(f"DROP TABLE IF EXISTS {t}__apitap_cl")
    ch(f"DROP TABLE IF EXISTS {t}__apitap_cdc_del")
    ch(f"DROP VIEW IF EXISTS {t}__current")
    if ch("SELECT count() FROM system.tables WHERE name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{t}' SETTINGS mutations_sync=1")
print(f"   server: {ma('SELECT VERSION()')}")

print("== seed 100 rows, then bootstrap ==")
ma(DDL.format(t=T))
ma(f"INSERT INTO bench.{T} (id, ref, amount, currency, status, created_at, note, payload) "
   f"SELECT seq, CONCAT('REF-', LPAD(seq,6,'0')), seq*1.25, 'EUR', seq MOD 3, "
   f"'2026-01-01 10:00:00', IF(seq MOD 7 = 0, NULL, CONCAT('n', seq)), "
   f"IF(seq MOD 5 = 0, NULL, UNHEX('414243')) FROM seq_1_to_100")
r = apitap.transfer(MA, CH, table=T, mode="log_based")
print(f"   bootstrap: {r}")
ok &= matches("bootstrap")

print("== every operation class, one transaction each ==")
ma(f"INSERT INTO bench.{T} VALUES (101,'REF-000101',9.99,'USD',1,'2026-02-02 08:30:00','añadido',UNHEX('FFEE'))")
ma(f"UPDATE bench.{T} SET amount = amount + 100, status = 2 WHERE id IN (1,2,3)")
ma(f"UPDATE bench.{T} SET note = NULL, payload = NULL WHERE id = 4")
ma(f"UPDATE bench.{T} SET note = 'ünïcodé ✓', created_at = NULL WHERE id = 5")
ma(f"DELETE FROM bench.{T} WHERE id IN (10,11)")
ma(f"UPDATE bench.{T} SET id = 500 WHERE id = 12")          # PK-changing update
ma(f"INSERT INTO bench.{T} VALUES (600,'REF-000600',0.01,'GBP',0,NULL,NULL,NULL)")
ma(f"DELETE FROM bench.{T} WHERE id = 600")                  # insert+delete nets out
r = apitap.transfer(MA, CH, table=T, mode="log_based")
print(f"   drain: {r}")
ok &= matches("after mixed ops")
ok &= binary_column_agrees("after mixed ops")

print("== multi-statement transaction + empty drain ==")
ma(f"START TRANSACTION; "
   f"UPDATE bench.{T} SET amount = 1 WHERE id = 20; "
   f"DELETE FROM bench.{T} WHERE id = 21; "
   f"INSERT INTO bench.{T} VALUES (700,'REF-000700',7.77,'EUR',1,NOW(),'tx',NULL); "
   f"COMMIT;")
apitap.transfer(MA, CH, table=T, mode="log_based")
ok &= matches("after one transaction")
before = ch(f"SELECT count() FROM {T}")
r = apitap.transfer(MA, CH, table=T, mode="log_based")
after = ch(f"SELECT count() FROM {T}")
if before == after:
    print(f"   ✓ empty drain: idempotent ({after} rows, report {r})")
else:
    ok = False
    print(f"   ✗ empty drain changed the table: {before} -> {after}")

print("== volume: 20K changes, multi-row events, one drain ==")
ma(f"INSERT INTO bench.{T} (id, ref, amount, currency, status, created_at, note, payload) "
   f"SELECT 10000+seq, CONCAT('BULK-', seq), seq*0.07, 'IDR', seq MOD 3, "
   f"'2026-03-03 12:00:00', CONCAT('b', seq), NULL FROM seq_1_to_10000")
ma(f"UPDATE bench.{T} SET amount = amount + 0.01 WHERE id > 10000")
r = apitap.transfer(MA, CH, table=T, mode="log_based")
print(f"   drain: {r}")
n_src = (ma(f"SELECT COUNT(*) FROM bench.{T}"),
         ma(f"SELECT CAST(SUM(ROUND(amount*100)) AS SIGNED) FROM bench.{T}"))
n_dst = (ch(f"SELECT count() FROM {T}"),
         ch(f"SELECT toInt64(sum(round(amount*100))) FROM {T}"))
if n_src == n_dst:
    print(f"   ✓ {n_src[0]} rows, {n_src[1]} summed cents — identical on both sides")
else:
    ok = False
    print(f"   ✗ volume mismatch  src={n_src}  dst={n_dst}")
ok &= matches("after 20K changes")

print("== changelog=True: every operation kept, PK change = D then U ==")
ma(DDL.format(t=TC))
ma(f"INSERT INTO bench.{TC} VALUES (1,'A',1.00,'EUR',0,NULL,NULL,NULL),"
   f"(2,'B',2.00,'EUR',0,NULL,NULL,NULL)")
apitap.transfer(MA, CH, table=TC, mode="log_based", changelog=True)
ma(f"UPDATE bench.{TC} SET amount = 1.10 WHERE id = 1")
ma(f"UPDATE bench.{TC} SET amount = 1.20 WHERE id = 1")
ma(f"UPDATE bench.{TC} SET amount = 1.30 WHERE id = 1")
ma(f"UPDATE bench.{TC} SET id = 9 WHERE id = 2")
apitap.transfer(MA, CH, table=TC, mode="log_based", changelog=True)
ops = ch(f"SELECT _apitap_op, count() FROM {TC} GROUP BY _apitap_op ORDER BY _apitap_op")
print(f"   ops in the changelog:\n{ops}")
n_u = ch(f"SELECT count() FROM {TC} WHERE _apitap_op='U' AND id=1")
n_d = ch(f"SELECT count() FROM {TC} WHERE _apitap_op='D' AND id=2")
n_new = ch(f"SELECT count() FROM {TC} WHERE _apitap_op='U' AND id=9")
if n_u == "3":
    print("   ✓ three updates to one key landed as three rows")
else:
    ok = False
    print(f"   ✗ expected 3 update rows for id=1, got {n_u}")
if n_d == "1" and n_new == "1":
    print("   ✓ PK-changing update = D on the old id, U on the new one")
else:
    ok = False
    print(f"   ✗ PK change wrong: D(id=2)={n_d} U(id=9)={n_new}")
cur = ch(f"SELECT concatWithSeparator('|', toString(id), "
         f"toString(toInt64(round(amount*100)))) FROM {TC}__current ORDER BY id")
src = ma(f"SELECT CONCAT_WS('|', id, CAST(ROUND(amount*100) AS SIGNED)) "
         f"FROM bench.{TC} ORDER BY id")
if cur == src:
    print("   ✓ __current view equals the MariaDB table")
else:
    ok = False
    print(f"   ✗ __current mismatch\n   src:\n{src}\n   dst:\n{cur}")

print("\n" + ("MARIADB CDC E2E: ALL GREEN" if ok else "MARIADB CDC E2E: FAILED"))
raise SystemExit(0 if ok else 1)
