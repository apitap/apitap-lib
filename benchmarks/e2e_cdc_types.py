"""The types a MySQL binlog does not send as text: ENUM, SET, BIT, TIME, JSON.

A bootstrap reads those columns through a SELECT and gets the text a human
would see. The binlog sends something else entirely — an index for ENUM, a
bitmask for SET, raw bytes for BIT, a sign-biased integer for TIME, a binary
envelope for MySQL's JSON. Nothing in either path knows the other exists, so
when they disagree the column just quietly holds two different values
depending on which one wrote it last. Measured before the fix, on a live
server:

    ✗ DIFFERS  status: bootstrap='shipped' cdc='3'
    ✗ DIFFERS  perms:  bootstrap='read,write' cdc='3'
    ✗ DIFFERS  flags:  bootstrap='05' cdc='35'
    ✗ DIFFERS  neg:    bootstrap='-01:00:00' cdc='1023:00:00'

The method is the only one worth trusting for this class of bug: seed two
identical rows, bootstrap them both, then UPDATE the second row to the exact
values the first one already holds. If the two paths agree the rows are now
byte-identical; any divergence is the decoder writing something the source
never contained. Row 1 is the bulk lane's answer, row 2 is CDC's.

Runs against BOTH dialects, because they differ here: MariaDB stores JSON as
LONGTEXT (it comes through as text), MySQL 8 stores a binary envelope apitap
cannot yet render — so on MySQL the run must REFUSE the table rather than
write the envelope. A refusal that does not happen is as much a failure as a
wrong value.

Rig: `apitap-bench-my` on :3307 (MySQL 8.0), `apitap-bench-mariadb` on :3309
(MariaDB 10.6), `apitap-bench-ch` on :8124.
"""
import subprocess
import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "cdc_types"

SERVERS = [
    # (label, container, client binary, port, JSON is binary here)
    ("MySQL 8.0", "apitap-bench-my", "mysql", 3307, True),
    ("MariaDB 10.6", "apitap-bench-mariadb", "mariadb", 3309, False),
]

ok = True


def sh(container, client, sql, check=True):
    o = subprocess.run(
        [
            "docker", "exec", "-i", container, client,
            "-uroot", "-pbench", "-N", "-D", "bench", "-e", sql,
        ],
        capture_output=True, text=True,
    )
    if check and o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql, check=True):
    o = subprocess.run(
        ["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
         "--user", "default", "--password", "bench", "-q", sql],
        capture_output=True, text=True,
    )
    if check and o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def check(label, cond):
    global ok
    print(f"   {'✓' if cond else '✗'} {label}")
    ok = ok and bool(cond)


def clean(src, table):
    ch(f"DROP TABLE IF EXISTS {table}")
    ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{table}' "
       "SETTINGS mutations_sync=1", check=False)


for label, container, client, port, json_is_binary in SERVERS:
    src = f"mysql://root:bench@127.0.0.1:{port}/bench"
    print(f"\n──────── {label} ────────")

    # ── the columns that travel as text on one path and not the other ──
    sh(container, client, f"DROP TABLE IF EXISTS {T}")
    sh(container, client, f"""CREATE TABLE {T} (
      id INT PRIMARY KEY,
      status ENUM('new','paid','shipped') NULL,
      perms SET('read','write','admin') NULL,
      flags BIT(8) NULL,
      wide BIT(12) NULL,
      dur TIME NULL,
      neg TIME NULL,
      zero TIME NULL,
      maybe ENUM('a','b') NULL)""")
    # Both rows carry the SAME values, so the two write paths have nowhere
    # to hide: row 1 is written by the bootstrap, row 2 by a CDC update.
    vals = ("'shipped','read,write',b'00000101',b'101010101010',"
            "'10:20:30','-01:00:00','00:00:00',NULL")
    sh(container, client, f"INSERT INTO {T} VALUES (1,{vals}),(2,{vals})")
    clean(src, T)

    apitap.transfer(src, CH, table=T, mode="log_based")
    # Rewrite row 2 to what it already holds — the values arrive through the
    # binlog this time.
    sh(container, client, f"""UPDATE {T} SET
      status='shipped', perms='read,write', flags=b'00000101',
      wide=b'101010101010', dur='10:20:30', neg='-01:00:00',
      zero='00:00:00', maybe=NULL WHERE id=2""")
    apitap.transfer(src, CH, table=T, mode="log_based")

    cols = ["status", "perms", "flags", "wide", "dur", "neg", "zero", "maybe"]
    # BIT lands as raw bytes, which is not text — read it as hex so the
    # comparison is over the bytes themselves and not over an encoding.
    sel = ["hex(flags)" if c == "flags" else "hex(wide)" if c == "wide" else c
           for c in cols]
    got = ch(f"SELECT {','.join(sel)} FROM {T} ORDER BY id FORMAT TSV")
    rows = [r.split("\t") for r in got.splitlines()]
    if len(rows) != 2:
        check(f"two rows landed (got {len(rows)})", False)
        continue
    boot, cdc = rows
    for name, b, c in zip(cols, boot, cdc):
        check(f"{name}: bulk={b!r} cdc={c!r}", b == c)
    # Agreeing on the WRONG value would also pass the line above, so pin the
    # two that a numeric renderer would have gotten wrong to their real text.
    check(f"status is the label, not the index ({boot[0]!r})",
          boot[0] == "shipped")
    check(f"perms is the member list, not the mask ({boot[1]!r})",
          boot[1] == "read,write")
    check(f"neg keeps its sign ({cdc[5]!r})", cdc[5].startswith("-01:00:00"))

    # ── a member added under a running stream ──
    # The catalog is re-read per drain, so a value from a NEW member must
    # arrive as its label, never as the index that member happens to have.
    sh(container, client,
       f"ALTER TABLE {T} MODIFY status ENUM('new','paid','shipped','void')")
    sh(container, client, f"UPDATE {T} SET status='void' WHERE id=2")
    apitap.transfer(src, CH, table=T, mode="log_based")
    after = ch(f"SELECT status FROM {T} WHERE id=2")
    check(f"ENUM member added mid-stream reads as its label ({after!r})",
          after == "void")

    sh(container, client, f"DROP TABLE IF EXISTS {T}")
    clean(src, T)

    # ── JSON: rendered on MariaDB, refused on MySQL ──
    JT = T + "_json"
    sh(container, client, f"DROP TABLE IF EXISTS {JT}")
    sh(container, client,
       f"CREATE TABLE {JT} (id INT PRIMARY KEY, doc JSON NULL)")
    sh(container, client,
       f"""INSERT INTO {JT} VALUES (1,'{{"a": 1}}'),(2,'{{"a": 1}}')""")
    clean(src, JT)
    if json_is_binary:
        # MySQL's binlog ships the binary envelope; before the refusal existed
        # this wrote '\\0\\x01\\0\\f\\0' where the bootstrap wrote {"a": 1}.
        try:
            apitap.transfer(src, CH, table=JT, mode="log_based")
            check("MySQL JSON refused at precheck", False)
        except Exception as e:
            msg = str(e)
            check(f"MySQL JSON refused, and the message names the column: "
                  f"{msg[:90]}...",
                  "JSON" in msg and f"bench.{JT}" in msg and "doc" in msg)
    else:
        # MariaDB's JSON is LONGTEXT and comes through as the text it is.
        apitap.transfer(src, CH, table=JT, mode="log_based")
        sh(container, client,
           f"""UPDATE {JT} SET doc='{{"a": 1}}' WHERE id=2""")
        apitap.transfer(src, CH, table=JT, mode="log_based")
        docs = ch(f"SELECT doc FROM {JT} ORDER BY id FORMAT TSV").splitlines()
        check(f"MariaDB JSON round-trips as text ({docs!r})",
              len(docs) == 2 and docs[0] == docs[1] and "a" in docs[0])
    sh(container, client, f"DROP TABLE IF EXISTS {JT}")
    clean(src, JT)

print("\n" + ("CDC TYPE E2E: ALL GREEN" if ok else "CDC TYPE E2E: FAILED"))
raise SystemExit(0 if ok else 1)
