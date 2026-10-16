"""Every Postgres plane dials the server the URL resolves to, not its authority.

The sqlx pool honours `?host/hostaddr/port/dbname/user/password/options`, the
PG* environment and .pgpass (last wins). The raw COPY plane and the walsender
read only the URL authority, so `read()` planned against one server and
streamed rows from another (MASTER review M-6, reproduced in round 3), and
`source_origin` dropped the query so two `?host=` servers shared a watermark.

Proof: the authority names a DECOY server (bench-pg-dst) holding a table of
the same name and shape with different values; `?port=&dbname=` point at the
real source. read() and a log_based drain into ClickHouse must see only the
real rows. A PGPORT/PGDATABASE-only URL (no port, no db) must do the same.
"""
import os
import subprocess
import sys

REAL = ("apitap-bench-pg-src", "apitap_bench_src", 5544)
DECOY = ("apitap-bench-pg-dst", "apitap_bench_dst", 5545)
URL = ("postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
       "?port=5544&dbname=apitap_bench_src")
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "m6_probe"


def sh(args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


def pg(server, sql):
    c, db, _ = server
    o = sh(["docker", "exec", "-i", c, "psql", "-U", "postgres", "-d", db,
            "-v", "ON_ERROR_STOP=1", "-Atc", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def ch(sql):
    o = sh(["docker", "exec", "-i", "apitap-bench-ch", "clickhouse-client",
            "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def real(sql):
    return pg(REAL, sql)


def wipe():
    # Our slot is the one whose publication covers our tables — asked of the
    # server BEFORE the tables go (a dropped table leaves its publication
    # empty and the slot unfindable, holding WAL for ever).
    for pub in set(real("SELECT pubname FROM pg_publication_tables "
                       f"WHERE tablename IN ({', '.join(repr(t) for t in (T,))})").split()):
        real(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE slot_name = '{pub.removesuffix('_pub')}' AND NOT active")
        real(f"DROP PUBLICATION IF EXISTS {pub}")
    for s in (REAL, DECOY):
        pg(s, f"DROP TABLE IF EXISTS {T}")
    ch(f"DROP TABLE IF EXISTS {T}")
    if ch("SELECT count() FROM system.tables WHERE database='default' "
          "AND name='_apitap_state'") != "0":
        ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table='{T}' SETTINGS mutations_sync=1")


def py(code, env=None):
    e = dict(os.environ)
    e.update(env or {})
    return sh([sys.executable, "-c", "import apitap\n" + code], timeout=240, env=e)


def main():
    wipe()
    for s, who in ((REAL, "real"), (DECOY, "decoy")):
        pg(s, f"CREATE TABLE {T} (id int PRIMARY KEY, who text);"
              f"INSERT INTO {T} SELECT g, '{who}' FROM generate_series(1, 200) g")

    for label, url, env in (
        ("query overrides", URL, {}),
        ("PG* environment", "postgres://postgres:bench@127.0.0.1",
         {"PGPORT": "5544", "PGDATABASE": "apitap_bench_src"}),
    ):
        r = py(f"t = apitap.read({url!r}, table={T!r}).to_polars()\n"
               "print(sorted(set(t['who'])), t.height)", env)
        assert r.returncode == 0, f"{label}: read failed:\n{r.stderr[-1500:]}"
        assert r.stdout.strip() == "['real'] 200", f"{label}: read() saw {r.stdout.strip()!r}"
        print(f"  read() via {label}: 200 real rows, no decoy")

    run = f"r = apitap.transfer({URL!r}, {CH!r}, table={T!r}, mode='log_based')\nprint(r.rows)"
    r = py(run)
    assert r.returncode == 0, f"log_based bootstrap failed:\n{r.stderr[-1500:]}"
    pg(REAL, f"UPDATE {T} SET who = 'real2' WHERE id <= 10")
    pg(DECOY, f"UPDATE {T} SET who = 'decoy2' WHERE id <= 20")
    r = py(run)
    assert r.returncode == 0, f"log_based drain failed:\n{r.stderr[-1500:]}"
    got = ch(f"SELECT who, count() FROM {T} GROUP BY who ORDER BY who FORMAT TSV")
    assert got == "real\t190\nreal2\t10", f"ClickHouse holds {got!r}"
    slots = pg(DECOY, "SELECT count(*) FROM pg_replication_slots")
    assert slots == "0", "no slot may exist on the decoy"
    print("  log_based: bootstrap + drain from the real server, decoy untouched")

    wipe()
    print("✓ PG URL planes: read() and the walsender dial the resolved server, not the authority")


if __name__ == "__main__":
    main()
