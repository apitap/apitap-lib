"""An append without cursor= never auto-picks part of a composite key.

MySQL reports COLUMN_KEY='PRI' for every column of a composite primary key.
With PRIMARY KEY (tenant_id INT, sku VARCHAR) only tenant_id is an integer,
so it was the "single integer PK" and became the incremental cursor: run 2
read `tenant_id > max(tenant_id)` and a new sku under an existing tenant was
never copied, silently (deep review D6).

Proof (mysql -> postgres): bootstrap with mode='append' and no cursor=, add a
row for an existing tenant, append again. The run must refuse and ask for a
cursor; a run that succeeds must have copied the row. A green run that
skipped it fails the leg. With cursor= given explicitly the same table works.
"""
import subprocess

import apitap

MY = "mysql://root:bench@127.0.0.1:3307/bench"
PG = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "my_composite_cursor"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def run(a):
    o = sh(a)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def my(sql):
    return run(["docker", "exec", "apitap-bench-my", "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])


def pg(sql):
    return run(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d", "apitap_bench_dst",
                "-v", "ON_ERROR_STOP=1", "-Atc", sql])


def wipe():
    my(f"DROP TABLE IF EXISTS {T}")
    pg(f"DROP TABLE IF EXISTS {T}")
    pg(f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
       f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")


def main():
    wipe()
    my(f"CREATE TABLE {T} (tenant_id INT NOT NULL, sku VARCHAR(32) NOT NULL, n INT, "
       f"PRIMARY KEY (tenant_id, sku))")
    my(f"INSERT INTO {T} VALUES (1, 'a', 1), (3, 'a', 2), (9, 'a', 3)")
    try:
        refused = None
        try:
            apitap.transfer(MY, PG, table=T, mode="append")
            my(f"INSERT INTO {T} VALUES (3, 'b', 4)")
            apitap.transfer(MY, PG, table=T, mode="append")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            refused = str(e)
        if refused is not None:
            assert "cursor" in refused, f"refused for the wrong reason: {refused}"
            print(f"  append without cursor= on a composite key: refused ({refused[:80]}...)")
        else:
            n = pg(f"SELECT count(*) FROM {T} WHERE tenant_id = 3 AND sku = 'b'")
            assert n == "1", "a green append skipped a new sku of an existing tenant (cursor = part of the key)"
            print("  append without cursor= copied the new row")

        # The explicit cursor still works on the same shape.
        wipe()
        my(f"CREATE TABLE {T} (tenant_id INT NOT NULL, sku VARCHAR(32) NOT NULL, n INT, "
           f"PRIMARY KEY (tenant_id, sku))")
        my(f"INSERT INTO {T} VALUES (1, 'a', 1), (3, 'a', 2)")
        apitap.transfer(MY, PG, table=T, mode="append", cursor="n")
        my(f"INSERT INTO {T} VALUES (3, 'b', 3)")
        apitap.transfer(MY, PG, table=T, mode="append", cursor="n")
        assert pg(f"SELECT count(*) FROM {T}") == "3", "explicit cursor= append lost a row"
        print("  append with cursor=n: every row")
    finally:
        wipe()
    print("✓ an append never auto-picks part of a composite key")


if __name__ == "__main__":
    main()
