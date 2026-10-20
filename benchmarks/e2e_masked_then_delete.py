"""A masked update whose key the same window deletes never wedges the lane.

The Postgres and MySQL appliers refuse a masked update (an unchanged TOAST
cell) that matches no row: nothing can rebuild the unsent value (0ed9158).
When the same window then deletes that key, the missing row has lost
nothing. A re-applied part of a window, from a group straddling a member's
floor (K3), meets exactly that: the row is already gone, and the window
holding "masked update k, delete k" was refused on every rerun (deep review
D12). A later move away from k takes the row too: "masked update k, move
k -> j" and a move chain were refused the same way (deep review 2, N3).

Proof, per destination (pg, my): bootstrap, remove row k at the destination
(the state a re-applied window meets), then one source transaction updates k
without touching its TOAST column and deletes it. The drain must succeed and
the destination must equal the source. The refusal itself is kept: a masked
update of a missing row that the window does NOT delete still fails.
"""
import subprocess
import sys

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DSTS = {"pg": "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst",
        "my": "mysql://root:bench@127.0.0.1:3308/bench"}
T = "masked_then_delete"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def run(a):
    o = sh(a)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres", "-d",
                "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql])


def dst(which, sql):
    if which == "pg":
        return run(["docker", "exec", "apitap-bench-pg-dst", "psql", "-U", "postgres", "-d",
                    "apitap_bench_dst", "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    return run(["docker", "exec", "apitap-bench-my-dst", "mysql", "-uroot", "-pbench", "-N",
                "bench", "-e", sql])


def fp_src():
    return src(f"SELECT coalesce(string_agg(id || ':' || v || ':' || md5(body), ',' ORDER BY id), '') FROM {T}")


def fp_dst(which):
    if which == "pg":
        return dst(which, f"SELECT coalesce(string_agg(id || ':' || v || ':' || md5(body), ',' ORDER BY id), '') FROM {T}")
    return dst(which, f"SELECT COALESCE(GROUP_CONCAT(CONCAT(id, ':', v, ':', MD5(body)) "
                      f"ORDER BY id SEPARATOR ','), '') FROM {T}")


def wipe(which):
    for pub in set(src(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{T}'").split()):
        src(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        src(f"DROP PUBLICATION IF EXISTS {pub}")
    src(f"DROP TABLE IF EXISTS {T}")
    dst(which, f"DROP TABLE IF EXISTS {T}")
    if which == "pg":
        dst(which, f"DO $$ BEGIN IF to_regclass('public._apitap_state') IS NOT NULL THEN "
                   f"DELETE FROM _apitap_state WHERE dest_table IN ('{T}', 'public.{T}'); END IF; END $$")
    elif dst(which, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'bench' "
                    "AND table_name = '_apitap_state'") != "0":
        dst(which, f"DELETE FROM _apitap_state WHERE dest_table = '{T}'")


def leg(which):
    D = DSTS[which]
    wipe(which)
    src(f"CREATE TABLE {T} (id int PRIMARY KEY, v int, body text)")
    src(f"ALTER TABLE {T} ALTER COLUMN body SET STORAGE EXTERNAL")
    src(f"INSERT INTO {T} SELECT g, 0, repeat(md5(g::text), 200) FROM generate_series(1, 5) g")
    assert src(f"SELECT pg_relation_size(reltoastrelid) > 0 FROM pg_class WHERE relname = '{T}'") == "t", \
        "(rig) body is not stored out of line; the masked path would not run"
    try:
        apitap.transfer(SRC, D, table=T, mode="log_based")
        assert fp_src() == fp_dst(which), "bootstrap differs"
        dst(which, f"DELETE FROM {T} WHERE id = 2")
        src(f"BEGIN; UPDATE {T} SET v = 1 WHERE id = 2; DELETE FROM {T} WHERE id = 2; COMMIT")
        apitap.transfer(SRC, D, table=T, mode="log_based")
        s, d = fp_src(), fp_dst(which)
        assert s == d, f"[{which}] after masked-then-delete: source {s!r} vs destination {d!r}"
        print(f"  [{which}] masked update then delete of a row already gone: applied, equal")

        # N3: the row already moved on, as a re-applied window finds it.
        dst(which, f"UPDATE {T} SET id = 9, v = 1 WHERE id = 1")
        src(f"BEGIN; UPDATE {T} SET v = 1 WHERE id = 1; UPDATE {T} SET id = 9 WHERE id = 1; COMMIT")
        dst(which, f"UPDATE {T} SET id = 8, v = 0 WHERE id = 4")
        src(f"BEGIN; UPDATE {T} SET id = 7 WHERE id = 4; UPDATE {T} SET id = 8 WHERE id = 7; COMMIT")
        apitap.transfer(SRC, D, table=T, mode="log_based")
        s, d = fp_src(), fp_dst(which)
        assert s == d, f"[{which}] after masked-then-move and a move chain: source {s!r} vs destination {d!r}"
        print(f"  [{which}] masked update then a move, and a move chain, over rows already moved: applied, equal")

        # The refusal stays for the case it exists for.
        dst(which, f"DELETE FROM {T} WHERE id = 3")
        src(f"UPDATE {T} SET v = 1 WHERE id = 3")
        try:
            apitap.transfer(SRC, D, table=T, mode="log_based")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            assert "found no row" in str(e), f"[{which}] wrong refusal: {e}"
            print(f"  [{which}] a masked update of a missing row the window keeps: still refused")
        else:
            raise AssertionError(f"[{which}] a masked update of a missing row was applied silently")
    finally:
        wipe(which)


def main():
    for which in (sys.argv[1:] or ["pg", "my"]):
        leg(which)
    print("✓ a masked update whose key the window deletes never wedges the lane")


if __name__ == "__main__":
    main()
