"""Drop every apitap artifact beside a destination table — tokenized or not.

Since 0.55.0 an artifact's name carries the run's token, so `<table>__apitap_staging`
is only the PRE-TOKEN spelling and matches nothing a real run makes. And nothing
collects a crashed run's leftover on a timer any more (the token records when the
run STARTED, so age cannot prove the object is dead — `naming::classify` has the
argument). Put together: a leg that deliberately kills or fails a run leaves a
permanent blocker, and the NEXT leg to touch that table is refused.

That is correct behaviour and it is what several legs were failing on. The fix is
not to teach each leg its own spelling — that is how the sinks ended up with six
different copies of one rule — but to discover the artifacts and drop them, once,
here.

    from _artifacts import drop_ch, drop_pg, drop_my

    drop_ch(ch, "orders")                       # ch(sql) -> str
    drop_ch(ch, "orders", on_cluster="benchcluster")
    drop_pg(dst, "orders")
    drop_my(my, "orders", db="bench")

Each takes the leg's own query callable, so nothing here needs credentials or a
driver of its own.
"""

# Every artifact suffix apitap can park beside a table. Kept as a literal rather
# than imported, because these legs run against an INSTALLED wheel and must not
# depend on the source tree they were built from.
SUFFIXES = ("__apitap_staging", "__apitap_new", "__apitap_cl", "__current")


def _names(rows):
    return [n for n in (rows or "").split() if n]


def drop_ch(q, table, on_cluster=None):
    """ClickHouse. `q` runs a statement and returns its output as text."""
    oc = f" ON CLUSTER {on_cluster}" if on_cluster else ""
    # LIKE with the literal suffix would need every `_` escaped; matching on the
    # marker apitap actually reserves is both simpler and wider — it catches a
    # suffix added after this file was written.
    listed = _names(q(
        "SELECT name FROM system.tables WHERE database = currentDatabase() "
        f"AND position(name, '__apitap_') > 0 AND startsWith(name, '{table}')"
    ))
    for n in listed:
        q(f"DROP TABLE IF EXISTS {n}{oc} SYNC")
    q(f"DROP TABLE IF EXISTS {table}{oc} SYNC")
    return listed


def drop_pg(q, table, schema="public"):
    """Postgres. `q` runs a statement and returns its output as text."""
    listed = _names(q(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{schema}' AND c.relkind = 'r' "
        f"AND c.relname LIKE '{table}%' AND position('__apitap_' in c.relname) > 0"
    ))
    for n in listed:
        q(f'DROP TABLE IF EXISTS "{schema}"."{n}" CASCADE')
    q(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
    return listed


def drop_my(q, table, db="bench"):
    """MySQL / MariaDB. `q` runs a statement and returns its output as text."""
    listed = _names(q(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{db}' AND table_type = 'BASE TABLE' "
        f"AND table_name LIKE '{table}%' AND INSTR(table_name, '__apitap_') > 0"
    ))
    for n in listed:
        q(f"DROP TABLE IF EXISTS `{db}`.`{n}`")
    q(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    return listed
