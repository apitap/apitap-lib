"""PG→CH CDC on a table binary pgoutput cannot render stays on text.

Since 0.61.0 the ClickHouse lane asks the walsender for binary pgoutput by
default, but `pgbindec` only renders scalars. A table with an array, enum,
domain, interval or inet column failed EVERY window (MASTER review M-7).
The lane now checks the catalog before START_REPLICATION and keeps text
when any tracked column has no binary renderer.

Proof: two identical source tables — one drained on the default, one with
APITAP_PG_BINARY=0 (the text reference). Both must succeed through a
bootstrap and a change round, and the two ClickHouse tables must be
row-for-row identical. A scalar-only table still drains on the default.
"""
import os
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
DEF, TXT, SCALAR = "bt_default", "bt_text", "bt_scalar"
COLS = "id, tags, mood, span, addr, pos"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def wipe():
    # Our slot is the one whose publication covers our tables — asked of the
    # server BEFORE the tables go (a dropped table leaves its publication
    # empty and the slot unfindable, holding WAL for ever).
    for pub in set(pg("SELECT pubname FROM pg_publication_tables "
                       f"WHERE tablename IN ({', '.join(repr(t) for t in (DEF, TXT, SCALAR))})").split()):
        pg(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        pg(f"DROP PUBLICATION IF EXISTS {pub}")
    for t in (DEF, TXT, SCALAR):
        pg(f"DROP TABLE IF EXISTS {t}")
        ch(f"DROP TABLE IF EXISTS {t}")
        if ch("SELECT count() FROM system.tables WHERE database='default' "
              "AND name='_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{t}' "
               "SETTINGS mutations_sync=1")
    pg("DROP TYPE IF EXISTS bt_mood; DROP DOMAIN IF EXISTS bt_pos")


def run(t, binary=None):
    env = dict(os.environ)
    env.pop("APITAP_PG_BINARY", None)
    if binary is not None:
        env["APITAP_PG_BINARY"] = binary
    code = ("import apitap\n"
            f"r = apitap.transfer({PG!r}, {CH!r}, table={t!r}, mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=240, env=env)


def rows(t, cols=COLS):
    return ch(f"SELECT {cols} FROM {t} ORDER BY id FORMAT TSV")


def main():
    wipe()
    pg("CREATE TYPE bt_mood AS ENUM ('sad', 'ok', 'happy');"
       "CREATE DOMAIN bt_pos AS int CHECK (VALUE > 0)")
    for t in (DEF, TXT):
        pg(f"CREATE TABLE {t} (id int PRIMARY KEY, tags text[], mood bt_mood, "
           f"span interval, addr inet, pos bt_pos);"
           f"INSERT INTO {t} SELECT g, ARRAY['a'||g, 'b'], "
           f"(ARRAY['sad','ok','happy']::bt_mood[])[1 + g % 3], "
           f"make_interval(hours => g), ('10.0.0.' || (g % 250))::inet, g "
           f"FROM generate_series(1, 300) g")
    pg(f"CREATE TABLE {SCALAR} (id int PRIMARY KEY, v text, n numeric(12,3), ts timestamptz);"
       f"INSERT INTO {SCALAR} SELECT g, 'v'||g, g * 1.5, '2026-01-01'::timestamptz "
       f"+ make_interval(secs => g) FROM generate_series(1, 300) g")

    for rnd in ("bootstrap", "changes"):
        if rnd == "changes":
            for t in (DEF, TXT):
                pg(f"UPDATE {t} SET tags = array_append(tags, 'z'), mood = 'happy', span = span * 2 "
                   f"WHERE id % 7 = 0; DELETE FROM {t} WHERE id % 11 = 0;"
                   f"INSERT INTO {t} VALUES (1000, '{{x,y}}', 'ok', '3 days', '::1', 5)")
            pg(f"UPDATE {SCALAR} SET v = v || '!' WHERE id % 5 = 0")
        d, x, s = run(DEF), run(TXT, "0"), run(SCALAR)
        assert d.returncode == 0, f"{rnd}: default lane on non-scalar types failed:\n{d.stderr[-2000:]}"
        assert x.returncode == 0, f"{rnd}: text reference failed:\n{x.stderr[-2000:]}"
        assert s.returncode == 0, f"{rnd}: scalar lane failed:\n{s.stderr[-2000:]}"
        a, b = rows(DEF), rows(TXT)
        assert a and a == b, f"{rnd}: default vs text reference differ"
        want = pg(f"SELECT count(*) FROM {DEF}")
        assert ch(f"SELECT count() FROM {DEF}") == want, f"{rnd}: row count vs source"
        assert ch(f"SELECT count() FROM {SCALAR}") == pg(f"SELECT count(*) FROM {SCALAR}")
        print(f"  {rnd}: {want} rows, default == text reference")

    wipe()
    print("✓ PG→CH default lane: non-scalar types stream as text, identical to the text reference")


if __name__ == "__main__":
    main()
