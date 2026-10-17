"""A MySQL user on an auth plugin apitap does not speak gets told what to do.

MariaDB's ed25519 (and parsec, sha256_password) are spoken by neither of
apitap's MySQL clients; both lanes failed with the driver's bare "unknown
authentication plugin: client_ed25519" (MASTER review M-42). The error now
names the plugins apitap speaks and the ALTER USER that switches to one.

Proof: a throwaway MariaDB ed25519 user (dropped after): a bulk replace and a
log_based run both fail at connect naming mysql_native_password and the
ALTER USER remedy.
"""
import subprocess

import apitap

CH = "clickhouse://default:bench@127.0.0.1:8124/default"
SRC = "mysql://cdc_ed:edpw@127.0.0.1:3309/bench"


def ma(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-mariadb", "mariadb", "-uroot", "-pbench", "-N", "-e", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def main():
    if ma("SELECT COUNT(*) FROM information_schema.plugins WHERE plugin_name = 'ed25519'") == "0":
        ma("INSTALL SONAME 'auth_ed25519'")
    ma("DROP USER IF EXISTS cdc_ed@'%'")
    ma("CREATE USER cdc_ed@'%' IDENTIFIED VIA ed25519 USING PASSWORD('edpw')")
    try:
        for mode in ("replace", "log_based"):
            try:
                apitap.transfer(SRC, CH, table="no_such_table", mode=mode)
                m = None
            except Exception as e:  # the binding maps connect errors to its own types
                m = str(e)
            assert m is not None, f"{mode}: an ed25519 user cannot connect"
            assert "client_ed25519" in m and "mysql_native_password" in m and "ALTER USER" in m, \
                f"{mode}: the error must name the plugin and the remedy: {m}"
            print(f"  {mode}: refused at connect, naming the plugin and the ALTER USER remedy")
    finally:
        ma("DROP USER IF EXISTS cdc_ed@'%'")
    print("✓ a MySQL user on an unspoken auth plugin is told how to fix it")


if __name__ == "__main__":
    main()
