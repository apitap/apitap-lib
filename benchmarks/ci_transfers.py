"""The routes CI can prove in minutes: every row, exactly, on every lane.

This is the suite the release gate runs at scale, shrunk to what a GitHub
runner can host. Same code paths, same checksums, small tables — so a broken
lane cannot reach a tag just because nobody ran the bench box by hand.
"""
import os
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5432/apitap_ci"
MY = "mysql://root:bench@127.0.0.1:3306/apitap_ci"
CH = "clickhouse://default:bench@127.0.0.1:8123/default"

failures = []


def psql(sql):
    env = dict(os.environ, PGPASSWORD="bench")
    return subprocess.run(["psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "apitap_ci",
                           "-Atc", sql], capture_output=True, text=True,
                          env=env).stdout.strip()


def mysql(sql):
    return subprocess.run(["mysql", "-h", "127.0.0.1", "-uroot", "-pbench",
                           "-D", "apitap_ci", "-N", "-e", sql],
                          capture_output=True, text=True).stdout.strip()


def ch(sql):
    import urllib.request
    req = urllib.request.Request("http://127.0.0.1:8123/", data=sql.encode())
    req.add_header("X-ClickHouse-User", "default")
    req.add_header("X-ClickHouse-Key", "bench")
    return urllib.request.urlopen(req, timeout=30).read().decode().strip()


def check(name, got, want):
    if got == want:
        print(f"   ✓ {name}: {got}")
    else:
        failures.append(name)
        print(f"   ✗ {name}: got {got}, want {want}")


truth_n = psql("SELECT count(*) FROM ci_src")
truth_s = psql("SELECT sum(id) FROM ci_src")
my_n = mysql("SELECT count(*) FROM ci_src_my")
my_s = mysql("SELECT sum(id) FROM ci_src_my")

print("== postgres → postgres (raw binary COPY relay) ==")
psql("DROP TABLE IF EXISTS ci_pg_pg")
r = apitap.transfer(PG, PG, table="ci_src", dest_table="ci_pg_pg", mode="replace")
check("rows reported", str(r.rows), truth_n)
check("rows landed", psql("SELECT count(*) FROM ci_pg_pg"), truth_n)
check("sum(id)", psql("SELECT sum(id) FROM ci_pg_pg"), truth_s)

print("== postgres → clickhouse (binary → RowBinary transcode) ==")
ch("DROP TABLE IF EXISTS ci_pg_ch")
r = apitap.transfer(PG, CH, table="ci_src", dest_table="ci_pg_ch", mode="replace")
check("rows reported", str(r.rows), truth_n)
check("rows landed", ch("SELECT count() FROM ci_pg_ch"), truth_n)
check("sum(id)", ch("SELECT toString(sum(id)) FROM ci_pg_ch"), truth_s)

print("== mysql → clickhouse (wire decode → RowBinary) ==")
ch("DROP TABLE IF EXISTS ci_my_ch")
r = apitap.transfer(MY, CH, table="ci_src_my", dest_table="ci_my_ch", mode="replace")
check("rows reported", str(r.rows), my_n)
check("rows landed", ch("SELECT count() FROM ci_my_ch"), my_n)
check("sum(id)", ch("SELECT toString(sum(id)) FROM ci_my_ch"), my_s)

print("== mysql → postgres (wire decode → binary COPY) ==")
psql("DROP TABLE IF EXISTS ci_my_pg")
r = apitap.transfer(MY, PG, table="ci_src_my", dest_table="ci_my_pg", mode="replace")
check("rows landed", psql("SELECT count(*) FROM ci_my_pg"), my_n)
check("sum(id)", psql("SELECT sum(id) FROM ci_my_pg"), my_s)

print("== incremental append moves only what is new ==")
psql("INSERT INTO ci_src SELECT g, 'late-'||g, 1.0, true, now(), NULL, NULL "
     f"FROM generate_series({int(truth_n)+1}, {int(truth_n)+500}) g")
r = apitap.transfer(PG, PG, table="ci_src", dest_table="ci_pg_pg", mode="append")
check("appended exactly the new rows", str(r.rows), "500")
check("total after append", psql("SELECT count(*) FROM ci_pg_pg"), str(int(truth_n) + 500))

print("== log_based CDC captures every operation ==")
ch("DROP TABLE IF EXISTS ci_cdc")
apitap.transfer(PG, CH, table="ci_src", dest_table="ci_cdc", mode="log_based")
psql("UPDATE ci_src SET name = 'changed' WHERE id <= 10")
psql("DELETE FROM ci_src WHERE id > 20000")
psql("INSERT INTO ci_src VALUES (999999, 'fresh', 1.0, false, now(), NULL, NULL)")
apitap.transfer(PG, CH, table="ci_src", dest_table="ci_cdc", mode="log_based")
check("CDC row count matches the source", ch("SELECT count() FROM ci_cdc"),
      psql("SELECT count(*) FROM ci_src"))
check("CDC applied the updates", ch("SELECT count() FROM ci_cdc WHERE name = 'changed'"), "10")
check("CDC applied the insert", ch("SELECT count() FROM ci_cdc WHERE id = 999999"), "1")

print("== a failed transfer never touches the destination ==")
before = psql("SELECT count(*) FROM ci_pg_pg")
try:
    apitap.transfer(PG, PG, table="does_not_exist", dest_table="ci_pg_pg", mode="replace")
    failures.append("missing source table should have raised")
    print("   ✗ transferring a nonexistent table succeeded")
except Exception:
    print("   ✓ refused a nonexistent source table")
check("destination untouched", psql("SELECT count(*) FROM ci_pg_pg"), before)

print()
if failures:
    print(f"CI TRANSFERS: {len(failures)} FAILED — {failures}")
    sys.exit(1)
print("CI TRANSFERS: ALL GREEN")
