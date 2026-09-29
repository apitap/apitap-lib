"""T1 / N6: a TRUNCATE with nothing after it in its window, from a MySQL-family
binlog source into ClickHouse. Shared by e2e_mariadb_cdc.py and e2e_mysql84.py
(claim `truncate.only-window`).

MySQL and MariaDB write TRUNCATE as a QUERY event, not a rows event, so a
window that holds a table's TRUNCATE and no row of it never decoded that
table's column layout. Until 0.57.0 the window then carried the truncate with
no column list, and every destination's apply failed "missing WAL column list"
— on every run, since a re-drain reads the same window: the watermark never
moved again, and in a group the sibling tables stopped with it (audit §3.4).
0.57.0 resolves the layout at the TRUNCATE itself and cuts the window there.

  a  one table: bootstrap 3 rows, TRUNCATE, drain — ClickHouse holds 0 rows,
     the watermark moved, and a second drain succeeds
  b  a group [A, B]: B gets rows on both sides of A's TRUNCATE — A holds 0,
     B equals the source, and both watermarks moved to the same place
  c  changelog=True: the TRUNCATE lands as ONE `T` record and `__current` is
     empty

Every transfer runs in a child of THIS interpreter (the wheel under test), so a
failure is reported with its text instead of ending the leg. Every answer is
asked of ClickHouse.
"""
import subprocess
import sys


def _transfer(src, dst, deadline, **kw):
    args = ", ".join(f"{k}={v!r}" for k, v in kw.items())
    code = f"import apitap\napitap.transfer({src!r}, {dst!r}, mode='log_based', {args})\n"
    try:
        o = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           timeout=deadline)
    except subprocess.TimeoutExpired:
        return -1, f"no answer in {deadline}s"
    tail = [l for l in o.stderr.strip().splitlines() if l.strip()][-1:] or [""]
    return o.returncode, tail[0][-400:]


def run(src_url, ch_url, src_sql, ch_sql, prefix, deadline=180):
    """src_sql / ch_sql: run one statement on the source / ClickHouse, return
    its text output. `prefix` names this leg's tables (`<prefix>_trunc_*`)."""
    one, ga, gb, cl = (f"{prefix}_trunc_{s}" for s in ("only", "ga", "gb", "cl"))
    mine = (one, ga, gb, cl)
    ok = True

    def case(label, good, detail=""):
        nonlocal ok
        print(f"   {'✓' if good else '✗'} {label}{': ' + detail if detail and not good else ''}")
        ok = ok and bool(good)

    def mark(t):
        return ch_sql(f"SELECT watermark FROM _apitap_state FINAL WHERE dest_table = '{t}' "
                      f"AND source_id NOT LIKE 'server-identity:%'")

    def clean():
        for t in mine:
            src_sql(f"DROP TABLE IF EXISTS {t}")
            ch_sql(f"DROP VIEW IF EXISTS `{t}__current`")
            ch_sql(f"DROP TABLE IF EXISTS `{t}`")
        names = ", ".join(f"'{t}'" for t in mine)
        for meta in ("_apitap_state", "_apitap_cdc_pending"):
            if ch_sql(f"SELECT count() FROM system.tables WHERE database = currentDatabase() "
                      f"AND name = '{meta}'") == "1":
                ch_sql(f"ALTER TABLE {meta} DELETE WHERE dest_table IN ({names}) "
                       f"SETTINGS mutations_sync = 1")

    def boot(label, **kw):
        rc, err = _transfer(src_url, ch_url, deadline, **kw)
        if rc:
            # Not the claim under test: without a bootstrap there is no drain.
            print(f"   (rig) {label} bootstrap failed rc={rc}: {err} — FAILED")
        return rc == 0

    ddl = "(id BIGINT PRIMARY KEY, v VARCHAR(32))"
    clean()
    try:
        print("== T1a: one table, a TRUNCATE and nothing after it ==")
        src_sql(f"CREATE TABLE {one} {ddl}")
        src_sql(f"INSERT INTO {one} VALUES (1,'a'),(2,'b'),(3,'c')")
        if boot("T1a", table=one):
            w0 = mark(one)
            src_sql(f"TRUNCATE TABLE {one}")
            rc, err = _transfer(src_url, ch_url, deadline, table=one)
            case("the drain succeeds", rc == 0, f"rc={rc} {err}")
            n = ch_sql(f"SELECT count() FROM `{one}`")
            case(f"ClickHouse holds 0 rows (was 3, source {src_sql(f'SELECT COUNT(*) FROM {one}')})",
                 n == "0", f"count() = {n}")
            w1 = mark(one)
            case(f"the watermark moved ({w0} -> {w1})", w1.isdigit() and w0.isdigit() and int(w1) > int(w0))
            rc, err = _transfer(src_url, ch_url, deadline, table=one)
            case("a second drain succeeds", rc == 0, f"rc={rc} {err}")
        else:
            ok = False

        print("== T1b: a group, one member truncated, its sibling written around it ==")
        src_sql(f"CREATE TABLE {ga} {ddl}")
        src_sql(f"CREATE TABLE {gb} {ddl}")
        src_sql(f"INSERT INTO {ga} VALUES (1,'a'),(2,'b'),(3,'c')")
        src_sql(f"INSERT INTO {gb} VALUES (1,'x')")
        if boot("T1b", tables=[ga, gb]):
            w0 = (mark(ga), mark(gb))
            # B's rows on BOTH sides of A's TRUNCATE: the window before the cut
            # carries B's rows beside A's wipe, the one after carries B alone.
            src_sql(f"INSERT INTO {gb} VALUES (2,'x'),(3,'x'),(4,'x')")
            src_sql(f"TRUNCATE TABLE {ga}")
            src_sql(f"INSERT INTO {gb} VALUES (5,'x'),(6,'x')")
            rc, err = _transfer(src_url, ch_url, deadline, tables=[ga, gb])
            case("the group drain succeeds", rc == 0, f"rc={rc} {err}")
            na = ch_sql(f"SELECT count() FROM `{ga}`")
            case("A holds 0 rows", na == "0", f"count() = {na}")
            nb, sb = ch_sql(f"SELECT count() FROM `{gb}`"), src_sql(f"SELECT COUNT(*) FROM {gb}")
            case(f"B equals the source ({sb} rows)", nb == sb, f"ClickHouse {nb}, source {sb}")
            w1 = (mark(ga), mark(gb))
            moved = all(a.isdigit() and b.isdigit() and int(b) > int(a) for a, b in zip(w0, w1))
            case(f"both watermarks moved, to one place ({w0} -> {w1})", moved and w1[0] == w1[1])
        else:
            ok = False

        print("== T1c: changelog=True, a TRUNCATE and nothing after it ==")
        src_sql(f"CREATE TABLE {cl} {ddl}")
        src_sql(f"INSERT INTO {cl} VALUES (1,'a'),(2,'b'),(3,'c')")
        if boot("T1c", table=cl, changelog=True):
            w0 = mark(cl)
            src_sql(f"TRUNCATE TABLE {cl}")
            rc, err = _transfer(src_url, ch_url, deadline, table=cl, changelog=True)
            case("the drain succeeds", rc == 0, f"rc={rc} {err}")
            nt = ch_sql(f"SELECT count() FROM `{cl}` WHERE _apitap_op = 'T'")
            case("one T record in the log", nt == "1", f"count() = {nt}")
            nc = ch_sql(f"SELECT count() FROM `{cl}__current`")
            case("__current is empty", nc == "0", f"count() = {nc}")
            w1 = mark(cl)
            case(f"the watermark moved ({w0} -> {w1})", w1.isdigit() and w0.isdigit() and int(w1) > int(w0))
        else:
            ok = False
    finally:
        clean()
        print("   cleaned up the T1 tables, views and state rows")
    return ok
