"""MySQL/MariaDB CDC carries a negative TIME with a fraction as the server holds it.

TIME2 stores the integer part and the fraction as ONE biased number: a
negative value with a fraction borrows a second and stores the fraction's
complement (fsp 1-4), and at fsp 5-6 the six bytes are one 48-bit number.
The decoder read the two halves separately: -00:00:01.5 landed as
"-00:00:02.D", -00:00:00.000001 as bytes that are not even UTF-8 (P2-10,
reproduced on MySQL 8).

Proof, MySQL 8 and MariaDB 10.6: TIME(0/1/3/6) values around zero, at the
range edge and with every fraction width, inserted after a bootstrap and
drained into ClickHouse — each equals the server's own CAST(... AS CHAR).
Usage: e2e_my_time_fraction.py my|maria
"""
import subprocess
import sys

import apitap

SRC = {"my": ("apitap-bench-my", "mysql", 3307), "maria": ("apitap-bench-mariadb", "mariadb", 3309)}
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "my_time_fraction"
VALUES = ["-00:00:01.5", "-01:02:03.25", "-00:00:00.5", "00:00:00.5", "-838:59:59", "838:59:59",
          "-00:00:00.000001", "-00:00:00.999999", "-12:00:00.000500", "12:34:56.789", "00:00:00"]


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, errors="backslashreplace",
                          stdin=subprocess.DEVNULL)


def main():
    which = (sys.argv[1:] or [""])[0]
    assert which in SRC, "usage: e2e_my_time_fraction.py my|maria"
    box, client, port = SRC[which]
    url = f"mysql://root:bench@127.0.0.1:{port}/bench"

    def my(sql):
        o = sh(["docker", "exec", box, client, "-uroot", "-pbench", "-N", "bench", "-e", sql])
        if o.returncode:
            raise RuntimeError(o.stderr)
        return o.stdout.strip()

    def ch(sql):
        o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default",
                "--password", "bench", "-q", sql])
        if o.returncode:
            raise RuntimeError(o.stderr)
        return o.stdout.strip()

    def wipe():
        my(f"DROP TABLE IF EXISTS {T}")
        ch(f"DROP TABLE IF EXISTS {T}")
        if ch("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
            ch(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")

    wipe()
    my(f"CREATE TABLE {T} (id int PRIMARY KEY, t0 time, t1 time(1), t3 time(3), t6 time(6))")
    my(f"INSERT INTO {T} VALUES (0, '00:00:00', '00:00:00', '00:00:00', '00:00:00')")
    apitap.transfer(url, CH, table=T, mode="log_based")
    rows = ", ".join(f"({i}, '{v}', '{v}', '{v}', '{v}')" for i, v in enumerate(VALUES, 1))
    my(f"INSERT INTO {T} VALUES {rows}")
    apitap.transfer(url, CH, table=T, mode="log_based")
    src = my(f"SELECT id, CAST(t0 AS CHAR), CAST(t1 AS CHAR), CAST(t3 AS CHAR), CAST(t6 AS CHAR) "
             f"FROM {T} ORDER BY id").splitlines()
    dst = ch(f"SELECT id, t0, t1, t3, t6 FROM {T} ORDER BY id FORMAT TSV").splitlines()
    bad = [f"{s!r} vs {d!r}" for s, d in zip(src, dst) if s != d]
    wipe()
    assert len(src) == len(dst) == len(VALUES) + 1, f"{len(src)} source rows, {len(dst)} destination rows"
    assert not bad, f"{which}: TIME values differ:\n  " + "\n  ".join(bad)
    print(f"  {which}: {len(src)} rows x 4 TIME widths equal the server's text")
    print(f"✓ {which} CDC carries negative fractional TIME as the server holds it")


if __name__ == "__main__":
    main()
