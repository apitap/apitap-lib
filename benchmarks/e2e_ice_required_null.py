"""A NULL never lands in a REQUIRED field of an existing Iceberg table.

apitap writes into Iceberg tables it did not create (Spark, Trino,
pyiceberg). Such a table can declare a field REQUIRED (NOT NULL); the
schema check compared names and types only, and the encoder wrote every
column as optional, so a source NULL landed under a NOT NULL on a green
run: Spark then folds `amount IS NULL` to false, and stricter readers fail
on the file (deep review 2, N27).

Proof (pg -> iceberg append into a table created through the REST catalog
with `amount` required): one source row holds NULL there. The run must
refuse and name the column; nothing may be committed.
"""
import json
import subprocess
import urllib.request

import apitap

PG = "postgres://postgres:bench@127.0.0.1:5544/apitap_bench_src"
NS = "cdc_e2e"
CAT = "http://127.0.0.1:8181"
ICE = (f"iceberg://127.0.0.1:8181/{NS}?endpoint=http://127.0.0.1:9100"
       "&access_key_id=bench&secret_access_key=benchbench")
T = "ice_required_null"


def pg(sql):
    o = subprocess.run(["docker", "exec", "apitap-bench-pg-src", "psql", "-U", "postgres",
                        "-d", "apitap_bench_src", "-v", "ON_ERROR_STOP=1", "-Atc", sql],
                       capture_output=True, text=True, stdin=subprocess.DEVNULL)
    if o.returncode:
        raise RuntimeError(o.stderr)
    return o.stdout.strip()


def catalog(method, path, body=None):
    req = urllib.request.Request(f"{CAT}/v1/namespaces/{NS}/tables{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def drop():
    try:
        catalog("DELETE", f"/{T}?purgeRequested=true")
    except Exception:  # noqa: BLE001 — absent is fine
        pass


def main():
    drop()
    pg(f"DROP TABLE IF EXISTS {T}")
    try:
        catalog("POST", "", {"name": T, "schema": {"type": "struct", "schema-id": 0, "fields": [
            {"id": 1, "name": "id", "required": True, "type": "long"},
            {"id": 2, "name": "amount", "required": True, "type": "string"}]}})
        pg(f"CREATE TABLE {T} (id int8 PRIMARY KEY, amount text); "
           f"INSERT INTO {T} VALUES (1, 'a'), (2, NULL), (3, 'c')")
        try:
            apitap.transfer(PG, ICE, table=T, mode="append", cursor="id")
        except Exception as e:  # noqa: BLE001 — the message is what is checked
            msg = str(e)
            assert "'amount' is REQUIRED" in msg, f"refused for the wrong reason: {msg}"
            meta = catalog("GET", f"/{T}")["metadata"]
            assert meta.get("current-snapshot-id") in (None, -1), "a refused load committed a snapshot"
            print(f"  a NULL into a REQUIRED field is refused, nothing committed: {msg[:90]}...")
        else:
            raise AssertionError("a green append wrote a NULL under the table's REQUIRED 'amount'")
    finally:
        drop()
        pg(f"DROP TABLE IF EXISTS {T}")
    print("✓ a NULL never lands in a REQUIRED Iceberg field")


if __name__ == "__main__":
    main()
