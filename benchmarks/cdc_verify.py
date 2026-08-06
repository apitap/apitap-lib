"""Per-table truth: engine-neutral aggregates on both sides."""
import sys
import urllib.request

import psycopg2

TABLES = [f"cdc_t{i:02d}" for i in range(1, 11)]
CH = "http://127.0.0.1:8124/?user=default&password=bench"

pg = psycopg2.connect("postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
cur = pg.cursor()


def ch(sql):
    return urllib.request.urlopen(urllib.request.Request(CH, data=sql.encode()), timeout=600).read().decode().strip()


bad = 0
tot_pg = tot_ch = 0
for t in TABLES:
    cur.execute(f"SELECT count(*), coalesce(sum(id),0), coalesce(sum(cust_id),0), coalesce(sum(amount),0) FROM {t}")
    a = cur.fetchone()
    b = ch(f"SELECT count(), sum(id), sum(cust_id), sum(amount) FROM {t} FORMAT TSV").split("\t")
    a_s = [str(int(a[0])), str(int(a[1])), str(int(a[2])), f"{float(a[3]):.2f}"]
    b_s = [b[0], b[1], b[2], f"{float(b[3]):.2f}"]
    tot_pg += int(a[0])
    tot_ch += int(b[0])
    ok = a_s == b_s
    bad += not ok
    print(f"  {t}: pg={a_s[0]:>9} ch={b_s[0]:>9}  {'MATCH' if ok else 'MISMATCH ' + str(a_s) + ' vs ' + str(b_s)}")
print(f"  TOTAL pg={tot_pg:,}  ch={tot_ch:,}  tables mismatched: {bad}")
sys.exit(1 if bad else 0)
