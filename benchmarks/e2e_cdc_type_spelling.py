"""log_based: a value spells the same in the bootstrap and in the stream, for every type, under any role settings.

QA, round P p20/p21 (pre-existing on 0.67.2): the bootstrap rendered a
column as `col::text` under the session's own formats, the stream as the
type's output function under the walsender's pinned formats. inet kept a
'/32' the stream drops, and a role with IntervalStyle = postgres_verbose
bootstrapped '@ 1 day' where the stream sent '1 day'. As a PRIMARY KEY that
is a silent wrong answer: an UPDATE lands a second row next to the bootstrap
row, a DELETE misses it (pg->ch: 2 source rows, 4 destination rows). p22
renders text with the output function where `::text` is a cast of its own
(catalog-driven) and pins the subscriber formats on every read connection.

For each role (the default; one with IntervalStyle postgres_verbose,
DateStyle 'SQL, DMY', bytea_output escape, extra_float_digits 0, TimeZone
Asia/Kolkata) and each destination (ClickHouse, Postgres):
  S  one column per type: bootstrap, UPDATE every row's other column, drain:
     each typed value in the destination is byte-identical before and after
  K  each type as the PRIMARY KEY (inet, cidr, uuid, enum, numeric, char(4),
     "char", name, date, timestamptz, interval, bytea): UPDATE and DELETE hit
     the bootstrap row — the destination holds exactly the source's keys

RED (p21): inet (both roles) and interval (the verbose role) as keys.
Usage: e2e_cdc_type_spelling.py
"""
import subprocess
import sys

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
WS_ROLE, WS_PW = "tsp_ws", "tsp_ws_pw"
PG_WS = f"postgres://{WS_ROLE}:{WS_PW}@127.0.0.1:5544/apitap_bench_src"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
PGD = "postgres://postgres:bench@127.0.0.1:5545/apitap_bench_dst"
ok = True


def case(label, good, detail=""):
    global ok
    print(f"   {'OK' if good else 'XX'} {label}{': ' + str(detail)[:240] if detail else ''}", flush=True)
    ok = ok and bool(good)


def psql(sql, container="apitap-bench-pg-src", db="apitap_bench_src"):
    o = subprocess.run(["docker", "exec", container, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1",
                        "-AtF", "|", "-c", sql], capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr[-300:])
    return o.stdout.strip()


def ch(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench",
                        "-q", sql], capture_output=True, text=True)
    return o.stdout.strip() if o.returncode == 0 else "ERR " + o.stderr.strip()[-200:]


def dst_rows(dest, t, cols):
    if dest == "ch":
        r = ch(f"SELECT {', '.join(cols)} FROM default.{t} ORDER BY 1 FORMAT TSV")
        return r.splitlines() if not r.startswith("ERR") else [r]
    return psql(f"SELECT {', '.join(c + '::text' for c in cols)} FROM {t} ORDER BY 1", "apitap-bench-pg-dst",
                "apitap_bench_dst").replace("|", "\t").splitlines()


SLOTS0 = set(psql("SELECT slot_name FROM pg_replication_slots").split())


def drop_slot_of(t):
    """The slot and publication log_based made for table t (its publication names it), and t's state rows."""
    for pub in psql(f"SELECT pubname FROM pg_publication_tables WHERE tablename = '{t}'").split():
        slot = pub[:-4] if pub.endswith("_pub") else pub
        psql(f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name = '{slot}' AND NOT active")
        psql(f'DROP PUBLICATION IF EXISTS "{pub}"')


def cleanup(tables):
    for s in set(psql("SELECT slot_name FROM pg_replication_slots").split()) - SLOTS0:
        psql(f"SELECT pg_drop_replication_slot('{s}') FROM pg_replication_slots WHERE slot_name = '{s}' AND NOT active")
        psql(f'DROP PUBLICATION IF EXISTS "{s}_pub"')
    for t in tables:
        psql(f"DROP TABLE IF EXISTS {t}")
        psql(f"DROP TABLE IF EXISTS {t}", "apitap-bench-pg-dst", "apitap_bench_dst")
        ch(f"DROP TABLE IF EXISTS default.{t}")
    psql("DELETE FROM _apitap_state WHERE dest_table LIKE 'tsp\\_%'", "apitap-bench-pg-dst", "apitap_bench_dst") \
        if psql("SELECT to_regclass('_apitap_state') IS NOT NULL", "apitap-bench-pg-dst", "apitap_bench_dst") == "t" else None
    ch("ALTER TABLE default._apitap_state DELETE WHERE dest_table LIKE 'tsp\\_%' SETTINGS mutations_sync = 1")


TYPES = {  # column -> (type, three values)
    "ip": ("inet", ["'10.0.0.2'", "'10.0.0.10/8'", "'::1'"]),
    "cd": ("cidr", ["'10.0.0.0/8'", "'192.168.0.0/16'", "'::/0'"]),
    "iv": ("interval", ["'1 day 1 hour'", "'1 mon 2 days'", "'-4 hours'"]),
    "u": ("uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'00000000-0000-0000-0000-00000000000a'", "'ffffffff-0000-0000-0000-000000000000'"]),
    "nu": ("numeric", ["1.50", "-0.000", "123456789012345678901234567890.1"]),
    "tm": ("time", ["'09:00'", "'23:59:59.5'", "'24:00'"]),
    "ttz": ("timetz", ["'10:00+02'", "'23:00-05'", "'00:00+00'"]),
    "jb": ("jsonb", ["'{\"b\":1,\"a\":2}'", "'[1,2.50]'", "'\"x\"'"]),
    "ta": ("text[]", ["'{a,\"b c\"}'", "'{}'", "'{NULL}'"]),
    "ipa": ("inet[]", ["'{10.0.0.2,::1}'", "'{10.0.0.10/8}'", "'{}'"]),
    "mac": ("macaddr", ["'08:00:2b:01:02:03'", "'0a-00-00-00-00-00'", "'ff:ff:ff:ff:ff:ff'"]),
    "vb": ("varbit", ["B'101'", "B''", "B'1'"]),
    "ts": ("timestamp", ["'2020-01-01 10:00:00.5'", "'1999-12-31 23:59:59'", "'2038-01-19 03:14:07'"]),
    "tz": ("timestamptz", ["'2020-01-01 10:00+02'", "'1999-12-31 23:59:59+00'", "'2038-01-19 03:14:07-05'"]),
    "f8": ("float8", ["0.1", "-0", "1e300"]),
    "by": ("bytea", ["'\\x0041'", "'\\x'", "'\\xff00'"]),
}
KEYS = {
    "inet": ["'10.0.0.2'", "'10.0.0.10/32'", "'::1'"], "cidr": ["'10.0.0.0/8'", "'10.1.0.0/16'", "'::/0'"],
    "uuid": ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'00000000-0000-0000-0000-00000000000a'", "'ffffffff-0000-0000-0000-000000000000'"],
    "tsp_enum": ["'zeta'", "'alpha'", "'Mid'"], "numeric": ["1.50", "2", "-3.000"], "char(4)": ["'ab'", "'abcd'", "'x'"],
    '"char"': ["'a'", "'B'", "'z'"], "name": ["'alpha'", "'Beta'", "'gamma'"], "date": ["'2020-01-01'", "'1999-12-31'", "'2038-01-19'"],
    "timestamptz": ["'2020-01-01 10:00+02'", "'1999-12-31 23:59:59+00'", "'2038-01-19 03:14:07-05'"],
    "interval": ["'1 day 1 hour'", "'2 hours'", "'1 mon'"], "bytea": ["'\\x0041'", "'\\x'", "'\\xff00'"],
}


def main():
    psql(f"DROP TYPE IF EXISTS tsp_enum; CREATE TYPE tsp_enum AS ENUM ('zeta','alpha','Mid')")
    # pg->pg mirrors the enum by name: the destination has the type.
    psql("DROP TYPE IF EXISTS tsp_enum; CREATE TYPE tsp_enum AS ENUM ('zeta','alpha','Mid')", "apitap-bench-pg-dst", "apitap_bench_dst")
    psql(f"DROP ROLE IF EXISTS {WS_ROLE}")
    psql(f"CREATE ROLE {WS_ROLE} LOGIN SUPERUSER REPLICATION PASSWORD '{WS_PW}'; "
         f"ALTER ROLE {WS_ROLE} SET IntervalStyle = 'postgres_verbose'; ALTER ROLE {WS_ROLE} SET DateStyle = 'SQL, DMY'; "
         f"ALTER ROLE {WS_ROLE} SET bytea_output = 'escape'; ALTER ROLE {WS_ROLE} SET extra_float_digits = 0; "
         f"ALTER ROLE {WS_ROLE} SET TimeZone = 'Asia/Kolkata'")
    tables = []
    try:
        print(f"apitap {apitap.__version__}", flush=True)
        for rname, src in (("default role", PG), ("verbose role", PG_WS)):
            for dname, dest in (("ch", CH), ("pg", PGD)):
                tag = f"{'d' if src == PG else 'w'}{dname}"
                print(f"════════ {rname} -> {dname} ════════", flush=True)
                t = f"tsp_s_{tag}"
                tables.append(t)
                cols = ", ".join(f"{c} {ty}" for c, (ty, _) in TYPES.items())
                psql(f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id int PRIMARY KEY, v int, {cols})")
                for i in range(3):
                    vals = ", ".join(v[i] for _, (_, v) in TYPES.items())
                    psql(f"INSERT INTO {t} VALUES ({i + 1}, 0, {vals})")
                names = ["id"] + list(TYPES)
                apitap.transfer(src, dest, table=t, mode="log_based")
                before = dst_rows(dname, t, names)
                psql(f"UPDATE {t} SET v = v + 1")
                apitap.transfer(src, dest, table=t, mode="log_based")
                after = dst_rows(dname, t, names)
                drop_slot_of(t)
                case(f"S {rname} -> {dname}: every typed value spells as its bootstrap after an UPDATE",
                     before == after and len(after) == 3,
                     next((f"bootstrap {b!r} stream {a!r}" for b, a in zip(before, after) if b != a), f"{len(after)} rows"))
                for ty, vals in KEYS.items():
                    k = f"tsp_k{len(tables)}_{tag}"
                    tables.append(k)
                    psql(f"DROP TABLE IF EXISTS {k}; CREATE TABLE {k} (k {ty} PRIMARY KEY, v int)")
                    psql(f"INSERT INTO {k} VALUES " + ", ".join(f"({x}, 1)" for x in vals))
                    try:
                        apitap.transfer(src, dest, table=k, mode="log_based")
                        psql(f"UPDATE {k} SET v = 2; DELETE FROM {k} WHERE k = {vals[2]}")
                        apitap.transfer(src, dest, table=k, mode="log_based")
                        got = dst_rows(dname, k, ["k", "v"])
                        n_src = int(psql(f"SELECT count(*) FROM {k}"))
                        good = len(got) == n_src and all(r.endswith("\t2") for r in got)
                        case(f"K {rname} -> {dname}: {ty} key: UPDATE and DELETE hit the bootstrap row", good,
                             f"destination {got} vs {n_src} source rows")
                    except Exception as e:  # noqa: BLE001
                        case(f"K {rname} -> {dname}: {ty} key runs", False, str(e)[:200])
                    drop_slot_of(k)
    finally:
        cleanup(tables)
        psql(f"DROP ROLE IF EXISTS {WS_ROLE}")
        psql("DROP TYPE IF EXISTS tsp_enum")
        psql("DROP TYPE IF EXISTS tsp_enum CASCADE", "apitap-bench-pg-dst", "apitap_bench_dst")
    print("CDC TYPE SPELLING E2E: " + ("PASSED" if ok else "FAILED"), flush=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
