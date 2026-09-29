"""A key-changing UPDATE on a row with an unchanged TOAST column.

Postgres does not re-send a large (externally TOASTed) value when an UPDATE does
not touch it — the new tuple carries `UnchangedToast` in that slot. The
collapser handles that with a residue `MaskedUpdate`: an `UPDATE ... SET <the
columns it does have> WHERE <key>`.

That is right when the key did not move. When it DID move, the same `update()`
call had already queued a DELETE for the OLD key, and the MaskedUpdate targeted
the NEW key — which never existed at the destination. Measured before the fix,
pg -> pg:

    source: 3,101,102
    dest:   3,102          <- the row is gone
    run:    changes=3, exit 0

Postgres and MySQL matched zero rows and checked nothing. ClickHouse read the
row back first and raised, so it failed loudly instead of losing data. Neither
is acceptable and both came from the same place, so the fix is one op:
`ResidueOp::Rekey` carries BOTH keys and every destination MOVES the row rather
than deleting and rewriting it — which carries the untouched value across
without anyone having to know what it is.

Runs against every destination whose applier that change touched, because a fix
proven on one of four is a fix proven on one of four.

  case 1  re-key, TOAST untouched     — the bug
  case 2  re-key, TOAST rewritten     — control: ordinary upsert path
  case 3  masked update, key unmoved  — control: what the residue tail was for
  case A  re-key, then a NEW row takes the vacated key in the same window
  case B  insert and re-key the same row inside one window

Cases 2 and 3 are what make case 1 mean something: without them a failure reads
as "cannot re-key at all" or "cannot do masked updates at all", and the fix gets
aimed at the wrong place. A and B exist because moving the row instead of
deleting it puts the operation in the residue tail, AFTER the set phase — so a
later INSERT reusing the old key could be dragged along with it. Both keys go
sticky to prevent that, and reasoning is not evidence.

One more destination is a changelog, `chlog` (T5a): the same re-key with
`changelog=True` into ClickHouse. A changelog never patches a row in place —
every record is read whole — so the apply rebuilds a masked cell from the
window, else from the destination's row at that key. The `U` half of a re-key
sits at a key the destination never held, and 0.56.0 looked THERE: every retry
failed "the window is torn" and the table never moved again. The cell is at
the OLD key. The window also carries a chain (2 -> 20 -> 5), whose second move
can only find the body in what the first one resolved.

Rig: `apitap-bench-pg-src` on :5544 (source, always), and as destinations
`apitap-bench-pg-dst` on :5545, `apitap-bench-my-dst` on :3308, ClickHouse on
:8124. Pass DESTS to narrow it, e.g. DESTS=ch or DESTS=chlog.
"""
import os
import subprocess
import sys

SRC = os.environ.get("PG_URL", "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src")
SRC_C = "apitap-bench-pg-src"
T = "toast_rekey"

ok = True


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def src(sql):
    o = sh(["docker", "exec", "-i", SRC_C, "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr[-400:])
    return o.stdout.strip()


def _slots_now():
    return set(src("SELECT slot_name FROM pg_replication_slots").split())


_SLOTS_BEFORE = _slots_now()


def drop_our_slots():
    for s in sorted(_slots_now() - _SLOTS_BEFORE):
        src(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots "
            f"WHERE slot_name='{s}' AND NOT active")
        # A drain names its publication after its slot.
        src(f'DROP PUBLICATION IF EXISTS "{s}_pub"')


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + detail if detail else ''}")
    ok = ok and bool(good)


# ── destinations ───────────────────────────────────────────────────────────
# Each one answers the same three questions in its own dialect: which ids are
# here, what does one row look like, and please forget everything about this
# table. Nothing else about them differs from this leg's point of view.
def pg_dest():
    def q(sql):
        o = sh(["docker", "exec", "-i", "apitap-bench-pg-dst", "psql", "-U", "postgres",
                "-d", "apitap_bench_dst", "-Atc", sql])
        if o.returncode:
            raise RuntimeError(o.stderr[-400:])
        return o.stdout.strip()
    return dict(
        name="postgres",
        url=os.environ.get("PGD_URL",
                           "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"),
        ids=lambda: q(f"SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM {T}"),
        row=lambda i: q(f"SELECT note || '/' || length(blob) FROM {T} WHERE id = {i}"),
        reset=lambda: (q(f"DROP TABLE IF EXISTS {T}"),
                       q(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")),
    )


def my_dest():
    def q(sql):
        o = sh(["docker", "exec", "-i", "apitap-bench-my-dst", "mysql", "-uroot",
                "-pbench", "-N", "-D", "bench", "-e", sql])
        if o.returncode and "Unknown table" not in o.stderr:
            # stdout too: mysql puts the password warning on stderr for every
            # invocation, so stderr alone shows that and never the real cause.
            raise RuntimeError(f"{sql[:120]} -> {o.stdout.strip()[-200:]} {o.stderr.strip()[-300:]}")
        return o.stdout.strip()
    return dict(
        name="mysql",
        url=os.environ.get("MYD_URL", "mysql://root:bench@127.0.0.1:3308/bench"),
        ids=lambda: q(f"SELECT coalesce(group_concat(id ORDER BY id), '') FROM {T}"),
        # `blob` is a MySQL type keyword; unquoted it is a syntax error.
        row=lambda i: q(f"SELECT concat(note,'/',length(`blob`)) FROM {T} WHERE id = {i}"),
        reset=lambda: (q(f"DROP TABLE IF EXISTS {T}"),
                       q(f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")),
    )


def ch_dest():
    def q(sql):
        return sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
                   "--user", "default", "--password", "bench", "-q", sql]).stdout.strip()
    return dict(
        name="clickhouse",
        url=os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default"),
        ids=lambda: q(f"SELECT arrayStringConcat(arraySort(groupArray(id)), ',') FROM {T}"),
        row=lambda i: q(f"SELECT concat(note,'/',toString(length(blob))) FROM {T} WHERE id = {i}"),
        reset=lambda: (q(f"DROP TABLE IF EXISTS {T}"),
                       q(f"DROP TABLE IF EXISTS `{T}__apitap_cdc_del`"),
                       q(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' "
                         f"SETTINGS mutations_sync=1")),
    )


ALL = {"pg": pg_dest, "my": my_dest, "ch": ch_dest}
WANT = os.environ.get("DESTS", "pg,my,ch,chlog").split(",")


def seed(rows):
    src(f"DROP TABLE IF EXISTS {T}")
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, note text, blob text)")
    # EXTERNAL storage forces the value out of line and uncompressed, so an
    # UPDATE that does not touch it really does produce UnchangedToast rather
    # than an inline value. Without this the leg passes while testing nothing.
    src(f"ALTER TABLE {T} ALTER COLUMN blob SET STORAGE EXTERNAL")
    src(f"INSERT INTO {T} SELECT g, 'note'||g, repeat('z', 40000) "
        f"FROM generate_series(1,{rows}) g")


def run_for(d):
    global ok
    print(f"\n════════ destination: {d['name']} ════════")

    def drain():
        code = ("import apitap\n"
                f"r = apitap.transfer({SRC!r}, {d['url']!r}, table={T!r}, mode='log_based')\n"
                "print('ROWS', r.rows, flush=True)\n")
        return sh([sys.executable, "-c", code])

    # ── the bug and its two controls ───────────────────────────────────────
    seed(3)
    stored = src(f"SELECT pg_column_size(blob) FROM {T} WHERE id = 1")
    case("the big column is stored out of line", int(stored) > 2000, f"{stored} bytes")
    d["reset"]()
    drop_our_slots()
    r = drain()
    if r.returncode:
        case("bootstrap", False, r.stderr.strip()[-400:])
        return
    case("bootstrap landed all three rows", d["ids"]() == "1,2,3", d["ids"]())

    src(f"""
BEGIN;
UPDATE {T} SET id = 101, note = 'rekeyed-masked' WHERE id = 1;
UPDATE {T} SET id = 102, note = 'rekeyed-full', blob = repeat('y', 40000) WHERE id = 2;
UPDATE {T} SET note = 'same-key-masked' WHERE id = 3;
COMMIT;
""")
    want = src(f"SELECT string_agg(id::text, ',' ORDER BY id) FROM {T}")
    r = drain()
    case("the drain succeeds", r.returncode == 0, r.stderr.strip()[-250:])
    got = d["ids"]()
    print(f"   source: {want}\n   dest:   {got}")
    case("control: re-key with the TOAST column REWRITTEN survives", "102" in got.split(","))
    case("control: masked update on a key that did NOT move survives", "3" in got.split(","))
    case("re-key with the TOAST column UNTOUCHED survives", "101" in got.split(","), got)
    if "101" in got.split(","):
        case("and it carries the new note AND the untouched blob",
             d["row"](101) == "rekeyed-masked/40000", d["row"](101))
    case("the destination agrees with the source exactly", got == want, f"{want} vs {got}")

    # ── the two ordering cases the fix's stickiness has to get right ───────
    seed(2)
    d["reset"]()
    drop_our_slots()
    r = drain()
    case("bootstrap for the ordering cases", r.returncode == 0 and d["ids"]() == "1,2")
    src(f"""
BEGIN;
UPDATE {T} SET id = 201, note = 'moved' WHERE id = 1;
INSERT INTO {T} VALUES (1, 'brand-new', repeat('w', 40000));
INSERT INTO {T} VALUES (300, 'fresh', repeat('v', 40000));
UPDATE {T} SET id = 301, note = 'fresh-moved' WHERE id = 300;
COMMIT;
""")
    want = src(f"SELECT string_agg(id::text, ',' ORDER BY id) FROM {T}")
    r = drain()
    case("the ordering drain succeeds", r.returncode == 0, r.stderr.strip()[-250:])
    got = d["ids"]()
    print(f"   source: {want}\n   dest:   {got}")
    case("A: the key a re-key vacated can be reused in the same window",
         got == want, f"{want} vs {got}")
    if got == want:
        case("A: the new row stayed put and the moved row kept its blob",
             d["row"](1) == "brand-new/40000" and d["row"](201) == "moved/40000",
             f"id=1 {d['row'](1)}  id=201 {d['row'](201)}")
        case("B: insert-then-re-key in one window lands once, with its blob",
             d["row"](301) == "fresh-moved/40000", d["row"](301))

    src(f"DROP TABLE IF EXISTS {T}")
    d["reset"]()
    drop_our_slots()


CL = "toast_rekey_cl"
CL_BODY = 102400        # 100 KiB: out of line, EXTERNAL keeps it uncompressed


def run_changelog_ch():
    """T5a: a re-key whose TOASTed column must be carried from the old key."""
    print("\n════════ destination: clickhouse, changelog=True ════════")
    url = os.environ.get("CH_URL", "clickhouse://default:bench@127.0.0.1:8124/default")

    def q(sql):
        o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
                "--user", "default", "--password", "bench", "-q", sql])
        if o.returncode:
            raise RuntimeError(f"{sql[:120]} -> {o.stderr.strip()[-300:]}")
        return o.stdout.strip()

    def reset():
        q(f"DROP VIEW IF EXISTS `{CL}__current`")
        q(f"DROP TABLE IF EXISTS `{CL}`")
        for t in ("_apitap_state", "_apitap_cdc_pending"):
            if q(f"SELECT count() FROM system.tables WHERE database = currentDatabase() "
                 f"AND name = '{t}'") != "0":
                q(f"ALTER TABLE `{t}` DELETE WHERE dest_table = '{CL}' SETTINGS mutations_sync = 1")

    def drain():
        code = ("import apitap\n"
                f"r = apitap.transfer({SRC!r}, {url!r}, table={CL!r}, mode='log_based', "
                "changelog=True)\n"
                "print('ROWS', r.rows, flush=True)\n")
        return sh([sys.executable, "-c", code])

    def watermark():
        # The drain's own row; a Postgres source writes no other for this table.
        return q(f"SELECT watermark FROM `_apitap_state` FINAL "
                 f"WHERE dest_table = '{CL}' AND mode = 'log_based'")

    def current(i):
        return q(f"SELECT concat(lower(hex(MD5(ifNull(body, '')))), '/', toString(length(ifNull(body, '')))) "
                 f"FROM `{CL}__current` WHERE id = {i}")

    def source(i):
        return src(f"SELECT md5(body) || '/' || length(body) FROM {CL} WHERE id = {i}")

    src(f"DROP TABLE IF EXISTS {CL}")
    src(f"CREATE TABLE {CL} (id int PRIMARY KEY, title text, body text)")
    src(f"ALTER TABLE {CL} ALTER COLUMN body SET STORAGE EXTERNAL")
    # A different body per row, so a value carried from the wrong key shows.
    src(f"INSERT INTO {CL} SELECT g, 'title' || g, repeat(md5(g::text), {CL_BODY // 32}) "
        f"FROM generate_series(1, 3) g")
    try:
        stored = src(f"SELECT pg_column_size(body) FROM {CL} WHERE id = 1")
        case("the body is stored out of line, uncompressed", int(stored) >= CL_BODY, f"{stored} bytes")
        reset()
        drop_our_slots()
        r = drain()
        if r.returncode:
            case("bootstrap", False, r.stderr.strip()[-400:])
            return
        case("bootstrap landed three baseline rows",
             q(f"SELECT count() FROM `{CL}` WHERE _apitap_op = 'B'") == "3")
        before = watermark()

        src(f"""
BEGIN;
UPDATE {CL} SET id = 9 WHERE id = 1;
UPDATE {CL} SET id = 20 WHERE id = 2;
UPDATE {CL} SET id = 5 WHERE id = 20;
COMMIT;
""")
        r = drain()
        case("the drain succeeds", r.returncode == 0, r.stderr.strip()[-300:])
        after = watermark()
        print(f"   watermark {before} -> {after}")
        case("the watermark advanced",
             before.isdigit() and after.isdigit() and int(after) > int(before))
        want9, got9 = source(9), current(9)
        case("T5a: __current at id=9 carries the source body",
             got9 == want9 and want9.endswith(f"/{CL_BODY}"), f"source {want9}  ch {got9!r}")
        case("T5a: id=1 is gone from __current",
             q(f"SELECT count() FROM `{CL}__current` WHERE id = 1") == "0")
        newest = q(f"SELECT length(ifNull(body, '')) FROM `{CL}` WHERE id = 9 AND _apitap_op = 'U' "
                   f"ORDER BY _apitap_lsn DESC, _apitap_seq DESC LIMIT 1")
        case("T5a: the U record itself carries the body, not NULL", newest == str(CL_BODY), newest)
        want5, got5 = source(5), current(5)
        case("chain 2 -> 20 -> 5: __current at id=5 carries the source body",
             got5 == want5 and want5.endswith(f"/{CL_BODY}"), f"source {want5}  ch {got5!r}")
        ids = q(f"SELECT arrayStringConcat(arraySort(groupArray(toString(id))), ',') "
                f"FROM `{CL}__current`")
        want = src(f"SELECT string_agg(id::text, ',' ORDER BY id::text) FROM {CL}")
        case("__current holds exactly the source's keys", ids == want, f"{want} vs {ids}")
    finally:
        src(f"DROP TABLE IF EXISTS {CL}")
        reset()
        drop_our_slots()


for k in WANT:
    k = k.strip()
    if k == "chlog":
        run_changelog_ch()
    elif k in ALL:
        run_for(ALL[k]())
    else:
        print(f"   .. unknown destination {k!r}, skipped")

print("\nTOAST REKEY E2E: " + ("PASSED" if ok else "FAILED"))
raise SystemExit(0 if ok else 1)
