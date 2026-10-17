"""A parallel Postgres read sees ONE table state, whatever commits while it runs.

A PK-less table is read in ctid spans, more spans than pipes, each its own
COPY statement — and each took its own snapshot. An UPDATE committing after
the first span started moved a tuple out of a span not yet read (its new
version past the planned page range, or into a span already read): the row
was lost or read twice, and a replace published that (MASTER review M-18).
Every span now pins one exported snapshot.

Proof: 1000 rows over ~200 heap pages, each with a 100 KB out-of-line value
so the read takes long enough; once the first span is running, UPDATE every
row of the last span's pages (no free space there: the new versions go past
the planned range) and commit. The destination must hold exactly the 1000
rows as they were when the read began.
"""
import subprocess
import sys
import time

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
DST = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
T = "pg_par_snap"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


def psql(container, db, sql):
    o = sh(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return psql("apitap-bench-pg-src", "apitap_bench_src", sql)


def dst(sql):
    return psql("apitap-bench-pg-dst", "apitap_bench_dst", sql)


def main():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    src(f"CREATE TABLE {T} (n int, v int, pad char(1500), body text)")
    src(f"ALTER TABLE {T} ALTER pad SET STORAGE PLAIN, ALTER body SET STORAGE EXTERNAL")
    src(f"INSERT INTO {T} SELECT g, 0, 'p', repeat(md5(g::text), 3200) FROM generate_series(1, 1000) g")
    src(f"VACUUM ANALYZE {T}")
    pages = int(src(f"SELECT pg_relation_size('{T}') / 8192"))
    # The last span of 2 x 6 = 12: its pages are read last.
    last_lo = pages - pages // 12
    victims = int(src(f"SELECT count(*) FROM {T} WHERE ctid >= '({last_lo},0)'::tid"))
    print(f"  {pages} heap pages; {victims} rows live in the last span's pages")

    code = (f"import apitap\napitap.transfer({PG!r}, {DST!r}, table={T!r}, mode='replace', parallel=2)\n")
    run = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.time() + 60
    while time.time() < deadline:
        busy = src("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'COPY (SELECT%' "
                   f"AND query LIKE '%{T}%' AND state = 'active'")
        if busy != "0":
            break
        time.sleep(0.02)
    else:
        raise AssertionError("the read never started")
    src(f"UPDATE {T} SET v = 1 WHERE ctid >= '({last_lo},0)'::tid")
    out, err = run.communicate(timeout=300)
    assert run.returncode == 0, err[-800:]

    got = dst(f"SELECT count(*) || ':' || count(DISTINCT n) || ':' || sum(v) FROM {T}")
    assert got == "1000:1000:0", (f"the destination must hold the 1000 rows as the read began "
                                  f"(count:distinct:updated), got {got}")
    print("  an UPDATE moving the last span's rows mid-read: 1000 rows, each once, as of the start")
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")
    print("✓ a parallel Postgres read sees one table state")


if __name__ == "__main__":
    main()
