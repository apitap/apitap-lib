"""The fast MySQL plane verifies against a private CA given as ssl-ca=.

P2-9 (docs/review/2026-10-18-p2-triage.md): the raw plane (bulk read and the
binlog) ignored `ssl-ca`, verifying only against the bundled public roots,
while sqlx (the control pool) honoured it on the same URL. Against RDS,
Cloud SQL or any private CA, verify_identity failed UnknownIssuer and
`required` (no verification at all) was the only way through.

Proof: a throwaway MySQL 8.0 (port 18313) serving a certificate for
127.0.0.1 signed by a CA made here. A log_based run (its binlog drain is the
raw plane) with ssl-mode=verify_identity&ssl-ca=<that CA> bootstraps and
drains a change; the same URL without ssl-ca is refused (the CA is not
public), so the verification is real.
"""
import os
import subprocess
import sys
import tempfile
import time

import apitap

NAME, PORT = "apitap-qa-my-tls-ca", 18313
CH = "clickhouse://default:bench@127.0.0.1:8124/default"
T = "my_tls_ca_cdc"


def sh(a, **k):
    return subprocess.run(a, capture_output=True, text=True, stdin=subprocess.DEVNULL, **k)


def ch_q(sql):
    o = sh(["docker", "exec", "apitap-bench-ch", "clickhouse-client", "--user", "default", "--password", "bench", "-q", sql])
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def certs(d):
    run = lambda *a: subprocess.run(a, cwd=d, check=True, capture_output=True)  # noqa: E731
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-keyout", "ca-key.pem", "-out", "ca.pem", "-subj", "/CN=apitap test CA")
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server-key.pem",
        "-out", "server.csr", "-subj", "/CN=127.0.0.1")
    with open(os.path.join(d, "san.ext"), "w") as f:
        f.write("subjectAltName=IP:127.0.0.1\n")
    run("openssl", "x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca-key.pem",
        "-CAcreateserial", "-out", "server-cert.pem", "-days", "2", "-extfile", "san.ext")
    # mysqld runs as another uid inside the container: mkdtemp's 0700
    # directory hid the files from it ("SSL_CTX_set_default_verify_paths failed").
    os.chmod(d, 0o755)
    for f in os.listdir(d):
        os.chmod(os.path.join(d, f), 0o644)


def main():
    d = tempfile.mkdtemp(prefix="apitap-tls-ca-")
    certs(d)
    sh(["docker", "rm", "-f", NAME])
    r = sh(["docker", "run", "-d", "--rm", "--name", NAME, "-p", f"127.0.0.1:{PORT}:3306",
            "-v", f"{d}:/certs:ro", "-e", "MYSQL_ROOT_PASSWORD=bench", "-e", "MYSQL_DATABASE=bench",
            "mysql:8.0", "--ssl-ca=/certs/ca.pem", "--ssl-cert=/certs/server-cert.pem",
            "--ssl-key=/certs/server-key.pem", "--require-secure-transport=ON"])
    assert r.returncode == 0, r.stderr
    try:
        for _ in range(120):
            # Over TCP: the image's init runs a socket-only temporary server
            # first, which a socket login would accept too early.
            if sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "-h127.0.0.1", "--protocol=TCP",
                   "-e", "SELECT 1"]).returncode == 0:
                break
            time.sleep(1)
        sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "bench", "-e",
            "CREATE TABLE t (id int PRIMARY KEY); INSERT INTO t VALUES (1), (2), (3)"])
        base = f"mysql://root:bench@127.0.0.1:{PORT}/bench?ssl-mode=verify_identity"
        ca = os.path.join(d, "ca.pem")
        ch_q(f"DROP TABLE IF EXISTS {T}")
        # The binlog drain rides the raw plane (read() here does not): the
        # bootstrap, then a change drained through the verified channel.
        apitap.transfer(f"{base}&ssl-ca={ca}", CH, table="bench.t", dest_table=T, mode="log_based")
        sh(["docker", "exec", NAME, "mysql", "-uroot", "-pbench", "bench", "-e", "INSERT INTO t VALUES (4)"])
        apitap.transfer(f"{base}&ssl-ca={ca}", CH, table="bench.t", dest_table=T, mode="log_based")
        n = ch_q(f"SELECT count() FROM {T}")
        assert n == "4", f"four rows through the verified channel, got {n}"
        try:
            apitap.transfer(base, CH, table="bench.t", dest_table=T, mode="log_based")
            raise AssertionError("without ssl-ca a private CA must not verify")
        except AssertionError:
            raise
        except Exception as e:  # noqa: BLE001
            refused = str(e)
        print(f"  binlog CDC with verify_identity + ssl-ca: {n} rows; without it: refused ({refused[:60]}...)")
    finally:
        sh(["docker", "rm", "-f", NAME])
        ch_q(f"DROP TABLE IF EXISTS {T}")
        if ch_q("SELECT count() FROM system.tables WHERE database = 'default' AND name = '_apitap_state'") != "0":
            ch_q(f"ALTER TABLE _apitap_state DELETE WHERE dest_table = '{T}' SETTINGS mutations_sync = 1")
    print("✓ the fast MySQL plane verifies against a private CA given as ssl-ca=")


if __name__ == "__main__":
    main()
