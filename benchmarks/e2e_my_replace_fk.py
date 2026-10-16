"""A MySQL replace into a table that foreign keys reference is refused before the load.

A replace RENAMEs the live table to a parked name and drops it after the
swap — and a child table's FOREIGN KEY follows the RENAME. The DROP then
failed after the new table was already live: the run reported failure on
published data, the parked table could never be dropped, every later run was
refused as "locked", and the child's FK silently pointed at the STALE rows
(reproduced; MASTER review M-62). The replace is now refused before anything
is copied, naming the constraint.

Proof: replace once, add a child FK, replace again — refused, destination
untouched, nothing left behind; with the FK gone the replace lands.
"""
import subprocess
import sys

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
MY = "mysql://root:bench@127.0.0.1:3307/bench"
T = "myfk62"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, **k)


def pg(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
            "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def run():
    code = (f"import apitap\nr = apitap.transfer({PG!r}, {MY!r}, table={T!r}, mode='replace')\n"
            "print('ROWS', r.rows)\n")
    return sh([sys.executable, "-c", code], timeout=300)


def ours():
    return my(f"SELECT COALESCE(GROUP_CONCAT(table_name ORDER BY table_name), '') FROM information_schema.tables "
              f"WHERE table_schema = 'bench' AND table_name LIKE '{T}%' AND table_name != '{T}_child'")


def wipe():
    my(f"DROP TABLE IF EXISTS {T}_child")
    for n in my(f"SELECT table_name FROM information_schema.tables WHERE table_schema='bench' "
                f"AND table_name LIKE '{T}%'").split():
        my(f"SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS `{n}`")
    pg(f"DROP TABLE IF EXISTS {T}")


def main():
    wipe()
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, v text); INSERT INTO {T} SELECT g, 'a' FROM generate_series(1, 50) g")
    r = run()
    assert r.returncode == 0, r.stderr[-600:]
    my(f"CREATE TABLE {T}_child (cid int PRIMARY KEY, pid int, "
       f"FOREIGN KEY (pid) REFERENCES {T}(id)) ENGINE=InnoDB; INSERT INTO {T}_child VALUES (1, 1)")

    pg(f"UPDATE {T} SET v = 'b'")
    r = run()
    assert r.returncode != 0 and "foreign key" in r.stderr and f"{T}_child" in r.stderr, \
        f"the replace must be refused up front, naming the FK:\n{r.stderr[-600:]}"
    assert my(f"SELECT CONCAT(COUNT(*), ':', MAX(v)) FROM {T}") == "50:a", "the destination must be untouched"
    assert ours() == T, f"nothing may be left behind, found {ours()}"
    print("  replace with a referencing FK: refused before the load, destination untouched")

    my(f"DROP TABLE {T}_child")
    r = run()
    assert r.returncode == 0, r.stderr[-600:]
    assert my(f"SELECT CONCAT(COUNT(*), ':', MAX(v)) FROM {T}") == "50:b", "the replace lands once the FK is gone"
    assert ours() == T, f"nothing may be left behind, found {ours()}"
    print("  without the FK: replaced, nothing left behind")
    wipe()
    print("✓ MySQL replace refuses a table that foreign keys reference, before any data moves")


if __name__ == "__main__":
    main()
