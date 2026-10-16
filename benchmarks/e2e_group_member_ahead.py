"""A group member already past a window does not apply it again.

One slot serves a group; each member commits its window in its own unit. When
member A committed window W and member B failed, the next run drains from the
group MINIMUM and re-applied W to A (MASTER review M-55). A TOAST-masked
re-key whose old key was reused (`UPDATE id 1→2` then `INSERT 1`) is then
`UPDATE … SET id = 2 WHERE id = 1` over the NEW row 1 — a duplicate key on
every rerun: the whole group wedged.

The drain now stops a pass at the next member watermark above the minimum,
so no window straddles one, and a member at or past a window's end skips it
without writing a mark.

Proof (Postgres → Postgres): A gets the masked re-key + reuse, B gets a row a
destination trigger refuses. The first drain fails on B. With the trigger
gone, the next drain must succeed and both destinations equal their sources
(id, value, md5 of the TOASTed column).
"""
import subprocess
import sys

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
A, B = "gm55_a", "gm55_b"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def q(container, db, sql):
    o = sh(["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", db,
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return q("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return q("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def wipe():
    # Our slot is the one whose publication covers our tables — found from
    # the server, so a run that died earlier is cleaned up too.
    for pub in set(src("SELECT pubname FROM pg_publication_tables "
                       f"WHERE tablename IN ('{A}', '{B}')").split()):
        slot = pub.removesuffix("_pub")
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{slot}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    for t in (A, B):
        src(f"DROP TABLE IF EXISTS {t}")
        dst(f"DROP TABLE IF EXISTS {t}")
    dst("DROP FUNCTION IF EXISTS gm55_boom() CASCADE")
    if dst("SELECT to_regclass('_apitap_state') IS NOT NULL") == "t":
        dst(f"DELETE FROM _apitap_state WHERE dest_table IN ('{A}', '{B}', 'public.{A}', 'public.{B}')")


def run():
    code = ("import apitap\n"
            f"r = apitap.transfer({SRC!r}, {DST!r}, tables=[{A!r}, {B!r}], mode='log_based')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def digest(f, t):
    return f(f"SELECT coalesce(string_agg(id || ':' || v || ':' || md5(big), ',' ORDER BY id), '') FROM {t}")


def main():
    wipe()
    for t in (A, B):
        src(f"CREATE TABLE {t} (id int PRIMARY KEY, v text, big text);"
            f"ALTER TABLE {t} ALTER COLUMN big SET STORAGE EXTERNAL;"
            f"INSERT INTO {t} SELECT g, 'v' || g, repeat(md5(g::text), 300) FROM generate_series(1, 50) g")
    r = run()
    assert r.returncode == 0, f"bootstrap failed:\n{r.stderr[-1200:]}"
    dst("CREATE FUNCTION gm55_boom() RETURNS trigger LANGUAGE plpgsql AS "
        "$$BEGIN IF NEW.v = 'boom' THEN RAISE EXCEPTION 'gm55 refused'; END IF; RETURN NEW; END$$")
    dst(f"CREATE TRIGGER gm55_t BEFORE INSERT OR UPDATE ON {B} FOR EACH ROW EXECUTE FUNCTION gm55_boom()")

    # One transaction, both members: A's masked re-key with the old key reused,
    # B's refused row.
    src(f"BEGIN;"
        f"UPDATE {A} SET id = 1000 WHERE id = 1;"
        f"INSERT INTO {A} VALUES (1, 'reused', repeat('X', 9000));"
        f"UPDATE {A} SET v = 'masked' WHERE id = 2;"
        f"INSERT INTO {B} VALUES (999, 'boom', 'b');"
        f"COMMIT")
    r = run()
    assert r.returncode != 0 and "gm55 refused" in r.stderr, \
        f"the first drain must fail on B (rc={r.returncode}):\n{r.stderr[-800:]}"
    assert digest(dst, A) == digest(src, A), "A must have committed the window before B failed"
    print("  first drain: A committed, B refused")

    dst("DROP FUNCTION gm55_boom() CASCADE")
    r = run()
    assert r.returncode == 0, f"the rerun failed — the group is wedged:\n{r.stderr[-1200:]}"
    for t in (A, B):
        assert digest(dst, t) == digest(src, t), f"{t}: destination differs from source"
    print("  rerun: A skipped the window it had, B caught up, both exact")

    # The group is level again: an ordinary window lands on both.
    src(f"UPDATE {A} SET v = 'after' WHERE id = 3; UPDATE {B} SET v = 'after' WHERE id = 3")
    r = run()
    assert r.returncode == 0, r.stderr[-800:]
    for t in (A, B):
        assert digest(dst, t) == digest(src, t), f"{t}: differs after the next window"
    wipe()
    print("✓ group replay: a member already past a window skips it, the group never wedges")


if __name__ == "__main__":
    main()
