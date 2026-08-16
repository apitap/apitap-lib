"""pgoutput binary-mode A/B — seed / gen / verify / dropdest / cleanup.

The 15-column schema mirrors benchmarks/gcp-cdc-100tables.md (bigint PK,
varchars, small/int/bigint, numeric(12,2), numeric(6,3), bool, two
timestamptz with real microseconds, ~220 B payload, nullable notes, jsonb).
Changes are 60/30/10 insert/update/delete in 10,000-row transactions.

Every count printed by `gen` is the SUM OF rowcount — never the intended
number (the ledger's rule: rates come from what was actually applied).
"""
import json
import sys
import time
import urllib.request

import psycopg2

TABLES = [f"bin_t{i:02d}" for i in range(1, 11)]
PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "http://127.0.0.1:8124/?user=default&password=bench"
BATCH = 10_000


def ch(sql):
    return (
        urllib.request.urlopen(urllib.request.Request(CH, data=sql.encode()), timeout=600)
        .read()
        .decode()
        .strip()
    )


def connect():
    c = psycopg2.connect(PG)
    c.autocommit = False
    return c


DDL = """CREATE TABLE {t} (
  id         BIGINT PRIMARY KEY,
  ref        VARCHAR(32)    NOT NULL,
  s2         SMALLINT       NOT NULL,
  i4         INT            NOT NULL,
  qty        INT            NOT NULL,
  i8         BIGINT         NOT NULL,
  amount     NUMERIC(12,2)  NOT NULL,
  rate       NUMERIC(6,3)   NOT NULL,
  flag       BOOLEAN        NOT NULL,
  created_at TIMESTAMPTZ    NOT NULL,
  updated_at TIMESTAMPTZ    NOT NULL,
  payload    TEXT           NOT NULL,
  notes      TEXT,
  ref2       VARCHAR(64)    NOT NULL,
  meta       JSONB          NOT NULL
)"""

INSERT = """INSERT INTO {t}
SELECT g,
       'ref-' || (g %% 100000),
       (g %% 30000)::smallint,
       (g %% 1000000)::int,
       (g %% 97)::int,
       g::bigint * 1000003,
       ((g %% 1000000)::numeric) / 100,
       ((g %% 100000)::numeric) / 1000,
       (g %% 2 = 0),
       timestamptz '2026-01-01 00:00:00+00'
         + (g %% 86400) * interval '1 second'
         + (g %% 999983) * interval '1 microsecond',
       clock_timestamp(),
       rpad('payload-' || md5(g::text), 220, 'x'),
       CASE WHEN g %% 10 = 0 THEN NULL ELSE 'note-' || (g %% 777) END,
       'ref2-' || md5((g + 7)::text),
       jsonb_build_object('g', g %% 1000, 's', md5(g::text), 'f', (g %% 2 = 0))
FROM generate_series(%s, %s) g"""

UPDATE = """UPDATE {t} SET qty = qty + 1,
       amount = amount + 0.01,
       s2 = ((s2::int + 1) %% 30000)::smallint,
       updated_at = clock_timestamp()
WHERE id BETWEEN %s AND %s"""

DELETE = "DELETE FROM {t} WHERE id BETWEEN %s AND %s"


def seed():
    conn = connect()
    cur = conn.cursor()
    for t in TABLES:
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        cur.execute(DDL.format(t=t))
        cur.execute(INSERT.format(t=t), (1, 100))
    conn.commit()
    print(f"seeded {len(TABLES)} tables x 100 rows (15 cols, GCP-campaign shape)")


def gen(total):
    """~`total` changes across the group, exact count from rowcounts."""
    per_table = total // len(TABLES)
    blocks = max(1, per_table // (BATCH * 10))  # one block = 6 INS + 3 UPD + 1 DEL
    conn = connect()
    cur = conn.cursor()
    t0 = time.time()
    changes = txns = 0
    for t in TABLES:
        cur.execute(f"SELECT coalesce(max(id), 0), coalesce(min(id), 1) FROM {t}")
        next_id, live_lo = cur.fetchone()
        next_id += 1
        upd_cursor = live_lo
        for _ in range(blocks):
            for _ in range(6):
                cur.execute(INSERT.format(t=t), (next_id, next_id + BATCH - 1))
                changes += cur.rowcount
                next_id += BATCH
                conn.commit()
                txns += 1
            for _ in range(3):
                # March the update window; never fold onto the same ids twice
                # in a row (the collapser must not get a flattering stream).
                if upd_cursor + BATCH > next_id:
                    upd_cursor = live_lo
                cur.execute(UPDATE.format(t=t), (upd_cursor, upd_cursor + BATCH - 1))
                changes += cur.rowcount
                upd_cursor += BATCH
                conn.commit()
                txns += 1
            cur.execute(DELETE.format(t=t), (live_lo, live_lo + BATCH - 1))
            changes += cur.rowcount
            live_lo += BATCH
            if upd_cursor < live_lo:
                upd_cursor = live_lo
            conn.commit()
            txns += 1
    wall = time.time() - t0
    print(
        f"generated {changes:,} changes in {txns:,} txns of {BATCH:,}, {wall:.1f}s"
    )
    # Machine-readable line the orchestrator parses.
    print(f"GEN changes={changes}")


def verify():
    conn = connect()
    cur = conn.cursor()
    bad = 0
    tot_pg = tot_ch = 0
    for t in TABLES:
        cur.execute(
            f"SELECT count(*), coalesce(sum(id),0), coalesce(sum(qty),0), "
            f"coalesce(sum(amount),0) FROM {t}"
        )
        conn.commit()
        a = cur.fetchone()
        b = ch(f"SELECT count(), sum(id), sum(qty), sum(amount) FROM {t} FORMAT TSV").split("\t")
        a_s = [str(int(a[0])), str(int(a[1])), str(int(a[2])), f"{float(a[3]):.2f}"]
        b_s = [b[0], b[1], b[2], f"{float(b[3]):.2f}"]
        tot_pg += int(a[0])
        tot_ch += int(b[0])
        ok = a_s == b_s
        bad += not ok
        if not ok:
            print(f"  {t}: MISMATCH pg={a_s} ch={b_s}")
    state = "ALL MATCH" if not bad else f"{bad} MISMATCHED"
    print(f"  verify: pg={tot_pg:,} ch={tot_ch:,} over {len(TABLES)} tables — {state}")
    sys.exit(1 if bad else 0)


def dropdest():
    for t in TABLES:
        ch(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}__apitap_cdc_del")  # CDC delete-staging twin
    ch("ALTER TABLE _apitap_state DELETE WHERE 1=1 SETTINGS mutations_sync=1")
    print("  dropped ClickHouse dest tables + staging twins + state rows")


def cleanup():
    dropdest()
    conn = connect()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'apitap_%' "
        "OR slot_name LIKE 'ab_%'"
    )
    for (s,) in cur.fetchall():
        cur.execute("SELECT pg_drop_replication_slot(%s)", (s,))
    cur.execute("SELECT pubname FROM pg_publication WHERE pubname LIKE 'apitap_%' OR pubname LIKE 'ab_%'")
    for (p,) in cur.fetchall():
        cur.execute(f'DROP PUBLICATION "{p}"')
    print("  dropped replication slots + publications (WAL released)")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "seed":
        seed()
    elif cmd == "gen":
        gen(int(sys.argv[2]))
    elif cmd == "verify":
        verify()
    elif cmd == "dropdest":
        dropdest()
    elif cmd == "cleanup":
        cleanup()
    else:
        sys.exit(f"unknown subcommand {cmd}")
