"""Per-table truth for the MySQL CDC rig: engine-neutral aggregates."""
import sys
import urllib.request

import pymysql

TABLES = [f"my_cdc_t{i:02d}" for i in range(1, 11)]
CH = "http://127.0.0.1:8124/?user=default&password=bench"

conn = pymysql.connect(host="127.0.0.1", port=3307, user="root", password="bench", database="bench")
cur = conn.cursor()


def ch(sql):
    return urllib.request.urlopen(urllib.request.Request(CH, data=sql.encode()), timeout=600).read().decode().strip()


bad = 0
tot_my = tot_ch = 0
for t in TABLES:
    cur.execute(f"SELECT COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(cust_id),0), COALESCE(SUM(amount),0) FROM {t}")
    a = cur.fetchone()
    b = ch(f"SELECT count(), sum(id), sum(cust_id), sum(amount) FROM {t} FORMAT TSV").split("\t")
    a_s = [str(int(a[0])), str(int(a[1])), str(int(a[2])), f"{float(a[3]):.2f}"]
    b_s = [b[0], b[1], b[2], f"{float(b[3]):.2f}"]
    tot_my += int(a[0])
    tot_ch += int(b[0])
    ok = a_s == b_s
    bad += not ok
    print(f"  {t}: my={a_s[0]:>9} ch={b_s[0]:>9}  {'MATCH' if ok else 'MISMATCH ' + str(a_s) + ' vs ' + str(b_s)}")
print(f"  TOTAL my={tot_my:,}  ch={tot_ch:,}  tables mismatched: {bad}")
sys.exit(1 if bad else 0)
