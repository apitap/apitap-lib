"""read() over Postgres types the Arrow builder has no send-format decoder for.

read() pulls COPY BINARY. Only udts the builder decodes may skip the server
cast; before the fix every other type (time, interval, inet, money, arrays,
enums, domains, oid, …) put send-format bytes into a Utf8 column, often
invalid UTF-8 (MASTER review M-8). Unconstrained NUMERIC failed outright and
NUMERIC(3,5) truncated every value (M-45).

Proof: every column read() returns, rendered as text, must equal what the
server's own `::text` says for the same row — the expectation comes from the
server, not from this file. NUMERIC(3,5) must keep all five digits.
"""
import decimal
import subprocess

import apitap

SRC = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
T = "rt_types"
COLS = ["t", "tz", "span", "addr", "cash", "ints", "mood", "pos", "o", "num", "tiny"]


def pg(sql):
    o = subprocess.run(["docker", "exec", "-i", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-AtF", "\t", "-c", sql],
                       capture_output=True, text=True)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.rstrip("\n")


def wipe():
    pg(f"DROP TABLE IF EXISTS {T}; DROP TYPE IF EXISTS rt_mood; DROP DOMAIN IF EXISTS rt_pos")


def main():
    wipe()
    pg("CREATE TYPE rt_mood AS ENUM ('sad','ok','happy'); "
       "CREATE DOMAIN rt_pos AS int CHECK (VALUE > 0)")
    pg(f"CREATE TABLE {T} (id int PRIMARY KEY, t time, tz timetz, span interval, addr inet, "
       f"cash money, ints int4[], mood rt_mood, pos rt_pos, o oid, num numeric, tiny numeric(3,5))")
    pg(f"INSERT INTO {T} SELECT g, make_time(g % 24, g % 60, 1.5), '10:11:12+02', "
       f"make_interval(days => g, secs => 0.25), ('192.168.' || (g % 250) || '.1/24')::inet, "
       f"g * 1.25, ARRAY[g, -g], (ARRAY['sad','ok','happy']::rt_mood[])[1 + g % 3], g, g, "
       f"CASE WHEN g % 10 = 0 THEN 'NaN'::numeric ELSE g * 1.000000000123 END, g * 0.00001 "
       f"FROM generate_series(1, 500) g")
    pg(f"INSERT INTO {T} (id) VALUES (9999)")  # one all-NULL row

    want = {}
    for line in pg(f"SELECT id, {', '.join(c + '::text' for c in COLS)} FROM {T}").split("\n"):
        f = line.split("\t")
        want[int(f[0])] = [None if v == "" else v for v in f[1:]]

    df = apitap.read(SRC, table=T).to_polars()
    assert df.height == len(want), f"rows {df.height} vs {len(want)}"
    bad = []
    for row in df.iter_rows(named=True):
        exp = want[row["id"]]
        for c, e in zip(COLS, exp):
            v = row[c]
            if c == "tiny" and v is not None:
                v = format(decimal.Decimal(v), "f")
                e = None if e is None else format(decimal.Decimal(e), "f")
            if c == "tiny" and v is not None and e is not None:
                ok = decimal.Decimal(v) == decimal.Decimal(e)
            else:
                ok = (None if v is None else str(v)) == e
            if not ok:
                bad.append((row["id"], c, v, e))
    assert not bad, f"{len(bad)} cells differ from the server's ::text, first: {bad[:5]}"
    assert want[1][COLS.index("tiny")] == "0.00001", "fixture sanity"
    wipe()
    print(f"✓ read(): {len(want)} rows × {len(COLS)} exotic columns equal the server's ::text")


if __name__ == "__main__":
    main()
