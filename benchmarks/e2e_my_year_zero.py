"""A mysql -> mysql copy keeps the zero YEAR as 0000, not 2000.

YEAR arrives over the binary protocol as a 2-byte integer, and the text the
MySQL sink loads was written with no padding: 0000 became the field `0`, and
LOAD DATA stores the one-digit string `0` as 2000, with no warning (deep
review D29). Four digits keep 0000 a zero.

Proof: a source YEAR column holding 0000, 1901, 2026 and 2155, copied with
mode='replace'; the destination must hold the same four values.
"""
import subprocess

import apitap

MYSRC = "mysql://root:bench@127.0.0.1:3307/bench"
MY = "mysql://root:bench@127.0.0.1:3308/bench"
T = "my_year_zero"


def sh(a):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def mysql(container, sql):
    o = sh(["docker", "exec", container, "mysql", "-uroot", "-pbench", "-N", "bench", "-e", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def src(sql):
    return mysql("apitap-bench-my", sql)


def dst(sql):
    return mysql("apitap-bench-my-dst", sql)


def years(q):
    return q(f"SELECT GROUP_CONCAT(CONCAT(id, ':', y) ORDER BY id) FROM {T}")


def wipe():
    src(f"DROP TABLE IF EXISTS {T}")
    dst(f"DROP TABLE IF EXISTS {T}")


def main():
    wipe()
    try:
        src(f"CREATE TABLE {T} (id int PRIMARY KEY, y YEAR); "
            f"INSERT INTO {T} VALUES (1, 0000), (2, 1901), (3, 2026), (4, 2155)")
        assert years(src) == "1:0000,2:1901,3:2026,4:2155", f"(rig) source years: {years(src)}"
        apitap.transfer(MYSRC, MY, table=T, mode="replace")
        got = years(dst)
        assert got == years(src), f"destination years {got!r}, source {years(src)!r}"
        print(f"  YEAR copied exact: {got}")
    finally:
        wipe()
    print("✓ a mysql -> mysql copy keeps the zero YEAR")


if __name__ == "__main__":
    main()
