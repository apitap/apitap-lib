"""A CSV from GitHub lands in ClickHouse with order_by= on one of its columns.

The text sources (github CSV, Google Sheets) framed every RowBinary cell as
Nullable(String). The ClickHouse sink makes an order_by, engine or cursor
column non-Nullable, so the flag byte in front of each of its cells was read
as the string's length: the stream shifted for the rest of the insert, and
the load failed with "Cannot read all data", or committed garbage rows when
the lengths lined up (deep review D32).

Proof: this repository's own tests/data/people.csv (public, read over the
network) into ClickHouse with mode='replace' and order_by='id'. Every row
must land with the file's values.
"""
import csv
import io
import subprocess

import apitap

SRC = "github://apitap/apitap-lib/tests/data?ref=main"
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "text_source_order_by"


def ch(sql):
    # Bytes, not text=True: universal newlines would turn the file's quoted
    # CRLF into LF and fail a value that landed exact.
    o = subprocess.run(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
                        "--password", "bench", "-q", sql], capture_output=True,
                       stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr.decode())
    return o.stdout.decode().strip()


def main():
    # As the CSV source documents (and the file holds one of each): a blank
    # line is skipped, an empty field is NULL, a short row pads with NULLs.
    # NULL reads back as \N in ClickHouse's CSV.
    with open("tests/data/people.csv", newline="") as f:
        rows = [r for r in csv.reader(f) if r]
    width = len(rows[0])
    want = sorted(([c or "\\N" for c in r] + ["\\N"] * (width - len(r)) for r in rows[1:]),
                  key=lambda r: int(r[0]))
    ch(f"DROP TABLE IF EXISTS {T}")
    try:
        apitap.transfer(SRC, CH, table="people", dest_table=T, mode="replace", order_by="id")
        got = list(csv.reader(io.StringIO(ch(f"SELECT * FROM {T} ORDER BY toInt64(id) FORMAT CSV"),
                                          newline="")))
        assert len(got) == len(want), f"{len(got)} rows landed, the file has {len(want)}"
        bad = [(g, w) for g, w in zip(got, want) if g != w]
        assert not bad, f"rows landed with other values, first: {bad[0]}"
        print(f"  people.csv with order_by=id: {len(got)} rows, every value as in the file")
    finally:
        ch(f"DROP TABLE IF EXISTS {T}")
    print("✓ a CSV from GitHub lands in ClickHouse with order_by= on one of its columns")


if __name__ == "__main__":
    main()
