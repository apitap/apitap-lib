"""A destination foreign key into a replicated group is refused before any row moves.

The drain applies a changed key as delete-then-insert. When a destination
table is the REFERENCED side of a foreign key, deleting its rows fires the
reference: CASCADE / SET NULL / SET DEFAULT silently rewrites or removes child
rows that were never part of the change — rows outside the group included — and
NO ACTION / RESTRICT aborts the apply mid-run. apitap knows the destination's
catalog before it copies a row, and must refuse there (system review
2026-10-07, P4).

This leg proves the refusal names the constraint's two tables and the remedy,
that nothing moved, and that the same shape WITHOUT the reference — a child in
the group pointing at a parent outside it — still drains: the rule is about
the referenced side, not about foreign keys as such.
"""
import os
import subprocess
import sys

import apitap

PG_SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
PG_DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
GROUP = ["fk_parent", "fk_child"]


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def sql(container, db, q):
    o = sh(["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", db, "-Atc", q])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(q):
    return sql("apitap-bench-pg-src", "apitap_bench_src", q)


def dst(q):
    return sql("apitap-bench-pg-dst", "apitap_bench_dst", q)


def drain(extra_env=None):
    """Run a CDC drain in a child so the refusal's stderr can be inspected."""
    code = (
        "import apitap\n"
        f"r = apitap.transfer({PG_SRC!r}, {PG_DST!r}, tables={GROUP!r}, mode='log_based')\n"
        "print('ROWS', r.rows)\n"
    )
    env = dict(os.environ)
    env.update(extra_env or {})
    return sh([sys.executable, "-c", code], env=env)


def main():
    src("DROP TABLE IF EXISTS fk_child CASCADE; DROP TABLE IF EXISTS fk_parent CASCADE;")
    dst("DROP TABLE IF EXISTS fk_child CASCADE; DROP TABLE IF EXISTS fk_parent CASCADE;"
        "DROP TABLE IF EXISTS fk_out CASCADE;")
    src("CREATE TABLE fk_parent (id int PRIMARY KEY, v text);"
        "CREATE TABLE fk_child (id int PRIMARY KEY, pid int, v text);")
    src("INSERT INTO fk_parent VALUES (1, 'a'), (2, 'b');"
        "INSERT INTO fk_child VALUES (10, 1, 'x'), (11, 2, 'y');")

    r = drain()
    assert r.returncode == 0, r.stderr
    assert dst("SELECT count(*) FROM fk_parent") == "2", "bootstrap landed both tables"
    assert dst("SELECT count(*) FROM fk_child") == "2", "bootstrap landed both tables"

    # The dangerous shape: the referenced side is a member of the group.
    dst("ALTER TABLE fk_child ADD CONSTRAINT fk_c_p FOREIGN KEY (pid) "
        "REFERENCES fk_parent(id) ON DELETE CASCADE;")
    src("UPDATE fk_parent SET v = 'a2' WHERE id = 1;")
    r = drain()
    assert r.returncode != 0, "a destination FK into the group must refuse"
    out = (r.stderr or "") + (r.stdout or "")
    for want in ("REFERENCED by a foreign key", "fk_parent", "fk_child", "Drop the foreign key"):
        assert want in out, f"refusal must name {want!r}; got:\n{out}"
    assert dst("SELECT v FROM fk_parent WHERE id = 1") == "a", \
        "the refused run must not have applied the change"

    # The remedy the refusal names: drop the FK, same change lands.
    dst("ALTER TABLE fk_child DROP CONSTRAINT fk_c_p;")
    r = drain()
    assert r.returncode == 0, r.stderr
    assert dst("SELECT v FROM fk_parent WHERE id = 1") == "a2", "the change lands once the FK is gone"

    # The allowed shape: a child in the group pointing OUTSIDE it. The parent
    # is never written by this run, so no reference can fire.
    dst("CREATE TABLE fk_out (id int PRIMARY KEY); INSERT INTO fk_out VALUES (1), (2);")
    dst("ALTER TABLE fk_child ADD CONSTRAINT fk_c_out FOREIGN KEY (pid) "
        "REFERENCES fk_out(id) ON DELETE CASCADE;")
    src("UPDATE fk_child SET v = 'x2' WHERE id = 10;")
    r = drain()
    assert r.returncode == 0, f"a child-side FK to an outside parent must drain:\n{r.stderr}"
    assert dst("SELECT v FROM fk_child WHERE id = 10") == "x2", "the change lands"

    dst("DROP TABLE IF EXISTS fk_child CASCADE; DROP TABLE IF EXISTS fk_parent CASCADE;"
        "DROP TABLE IF EXISTS fk_out CASCADE;")
    src("DROP TABLE IF EXISTS fk_child CASCADE; DROP TABLE IF EXISTS fk_parent CASCADE;")
    print("✓ FK into a group member refused at admission with its remedy; outside-parent FK drains")


if __name__ == "__main__":
    main()
