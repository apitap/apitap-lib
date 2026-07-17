"""apitap — move whole tables between databases at wire speed, in bounded memory.

The engine is Rust (see https://apitap.dev); this package is a thin binding.

    import apitap

    report = apitap.transfer(
        "postgres://user:pass@src-host/db",
        "postgres://user:pass@dst-host/db",
        table="public.events",
    )
    print(report.rows, report.elapsed_ms)

    # Many tables — or a whole schema — through ONE resource budget:
    report = apitap.transfer(src, dst, tables=["public.events", "public.users"])
    report = apitap.transfer(src, dst, schema="public")
"""

from dataclasses import dataclass

from apitap._apitap import (
    __version__,
    transfer as _transfer,
    transfer_many as _transfer_many,
)

__all__ = [
    "transfer",
    "TransferReport",
    "TableResult",
    "MultiTransferError",
    "__version__",
]


@dataclass(frozen=True)
class TableResult:
    """One table's outcome inside a multi-table transfer."""

    table: str
    """Source table (the destination table has the same name)."""
    rows: int
    """Rows landed (0 on error — a failed table commits nothing)."""
    elapsed_ms: int
    """Wall-clock for this table, from the moment it got its pipes."""
    parallel: int
    """Pipes this table ran with (its slice of the shared budget)."""
    error: str | None
    """``None`` = success. A failed table never poisons its siblings: each keeps
    the single-table atomicity, so its destination holds either the previous
    table or the complete new one — never a partial."""


@dataclass(frozen=True)
class TransferReport:
    """What a transfer did."""

    rows: int
    """Rows landed in the destination (multi-table: sum over successful tables)."""
    elapsed_ms: int
    """Wall-clock duration of the whole transfer."""
    parallel: int
    """Single table: concurrent pipes actually used (0 = empty source).
    Multi-table: the shared pipe budget the tables drew from."""
    tables: tuple[TableResult, ...] | None = None
    """Per-table outcomes for a multi-table run; ``None`` for a single table."""


class MultiTransferError(RuntimeError):
    """Some tables of a multi-table run failed. The tables that succeeded ARE
    committed (each table lands atomically and independently); ``report`` holds
    the full per-table detail, ``report.tables`` includes every error."""

    def __init__(self, message: str, report: TransferReport):
        super().__init__(message)
        self.report = report

    def __reduce__(self):
        # Exceptions pickle-reconstruct via cls(*args); args holds only the
        # message, so spell out both — a worker-process failure must arrive
        # intact, report included, not die re-raising with a TypeError.
        return (MultiTransferError, (self.args[0], self.report))


def transfer(
    src: str,
    dst: str,
    table: str | None = None,
    *,
    tables: list[str] | None = None,
    schema: str | None = None,
    dest_table: str | None = None,
    parallel: int | None = None,
    cursor: str | None = None,
    chunk_bytes: int | None = None,
    durable: bool = True,
    mode: str = "replace",
    engine: str | None = None,
    order_by: str | None = None,
    on_cluster: str | None = None,
) -> TransferReport:
    """Copy one table, a list of tables, or a whole schema from ``src`` to ``dst``.

    Exactly one of ``table``, ``tables``, ``schema`` picks the scope. The URL
    schemes pick the route — ``postgres://``/``postgresql://``, ``mysql://``
    sources; ``postgres://``, ``clickhouse://`` (``clickhouse+https://`` for TLS),
    ``bigquery://<project>/<dataset>?credentials=/path/key.json`` destinations — and
    each pair negotiates its fastest wire format (raw binary COPY passthrough,
    in-flight RowBinary transcode, raw wire decode, or gzipped parallel load jobs).
    N concurrent range pipes feed a staging table that is swapped in atomically.
    Atomic (readers never see a partial load), 0-row-guarded (an empty source never
    wipes a good table), and memory-bounded (streams with TCP backpressure).

    Multi-table runs share ONE pipe budget — the same number a single-table run
    gets, so peak memory stays at the single-table ceiling no matter how many
    tables move. Tables run largest-first over shared connection pools: big tables
    take many pipes, small ones take one and overlap. Destination tables keep
    their source names. If some tables fail, the rest keep going and a
    :class:`MultiTransferError` is raised at the end — its ``report`` lists every
    table's outcome, and the successful ones are already committed.

    Full guide: https://github.com/apitap/apitap-lib/blob/main/docs/usage.md

    Args:
        src: Source URL, e.g. ``postgres://user:pass@host:5432/db`` or
            ``mysql://user:pass@host:3306/db``.
        dst: Destination URL (Postgres or ClickHouse).
        table: One source table, optionally schema-qualified (``public.events``).
        tables: A list of source tables — moved in one call through one budget.
        schema: Move EVERY base table of this schema — pass the name explicitly
            (Postgres: ``schema="public"``; MySQL: the database, e.g.
            ``schema="mydb"``). Postgres also brings materialized views, and
            skips partition/INHERITS children whose parent is in the same schema
            (the parent's scan covers their rows). apitap's own
            ``*__apitap_staging``/``_apitap_state`` artifacts never travel.
        dest_table: Destination table; defaults to ``table``. Single-table only.
        parallel: Concurrent range pipes (multi-table: the shared budget); default
            auto — a route-specific CPU heuristic capped by the cgroup's memory
            limit. An explicit value is never overridden.
        cursor: Numeric column to range-split on; default auto-detects the integer
            primary key. PK-less Postgres tables fall back to TID ranges; other
            sources to a single stream. Multi-table: applies to every table, so
            leave it auto unless all tables share the column.
        chunk_bytes: Bytes coalesced per send (default 4 MiB).
        mode: ``"replace"`` (default, full refresh + atomic swap), ``"append"``
            (incremental: only rows with cursor past the destination's current
            ``max(cursor)`` are loaded — stateless, the watermark lives in the data;
            bootstraps as replace when the table doesn't exist), or ``"merge"``
            (Postgres destinations: incremental upsert by the destination's
            PRIMARY KEY). Incremental modes require a cursor (integer or
            timestamp column). Append assumes the cursor is monotonic with
            COMMIT order — for update-prone or concurrently-written tables use
            merge with an ``updated_at`` cursor. See docs/usage.md.
        engine: ClickHouse destinations only. Engine of the table apitap creates —
            any MergeTree-family spelling, Replicated included: ``"MergeTree"``
            (default), ``"ReplacingMergeTree(ins_dt)"``,
            ``"ReplicatedReplacingMergeTree(ins_dt)"`` (path-less: requires
            ``on_cluster``, ClickHouse mints the ``{uuid}`` ZooKeeper path only
            for ON CLUSTER DDL), … Columns named in the engine arguments are
            declared non-nullable. With ``mode="append"``, an existing
            destination is the structural authority: apitap appends into it
            as-is and only checks that the engine family, arguments, and
            ``order_by`` agree. With ``mode="replace"`` the table is rebuilt
            with this engine (an existing explicit-ZooKeeper-path Replicated
            table can't be replaced — the shadow copy would collide; use append
            or drop it first).
        order_by: ClickHouse destinations only. ORDER BY of the created table,
            e.g. ``"id"`` or ``"client_id, id"``; default = the cursor column.
            Strongly recommended with Replacing engines (it is the dedup key).
        on_cluster: ClickHouse destinations only. Run the table DDL
            ``ON CLUSTER`` this cluster. Requires a ``Replicated*`` engine so the
            data reaches the other replicas through replication.
        durable: Postgres destinations only. ``False`` loads through an UNLOGGED
            table — skipping WAL roughly halves the destination's write cost — and the
            swapped-in table REMAINS unlogged: Postgres truncates it during crash
            recovery until you run ``ALTER TABLE … SET LOGGED``. Leave ``True`` unless
            the destination is rebuildable scratch data. Other destinations ignore it.
    """
    picked = sum(x is not None for x in (table, tables, schema))
    if picked != 1:
        raise ValueError(
            "pass exactly one of table=…, tables=[…], schema=… "
            f"(got {picked} of them)"
        )

    if table is not None:
        rows, elapsed_ms, used = _transfer(
            src,
            dst,
            table,
            dest_table=dest_table,
            parallel=parallel,
            cursor=cursor,
            chunk_bytes=chunk_bytes,
            durable=durable,
            mode=mode,
            engine=engine,
            order_by=order_by,
            on_cluster=on_cluster,
        )
        return TransferReport(rows=rows, elapsed_ms=elapsed_ms, parallel=used)

    if dest_table is not None:
        raise ValueError(
            "dest_table applies to single-table transfers — multi-table runs "
            "keep the source names"
        )
    elapsed_ms, budget, raw = _transfer_many(
        src,
        dst,
        tables=tables,
        schema=schema,
        parallel=parallel,
        cursor=cursor,
        chunk_bytes=chunk_bytes,
        durable=durable,
        mode=mode,
        engine=engine,
        order_by=order_by,
        on_cluster=on_cluster,
    )
    results = tuple(
        TableResult(table=t, rows=r, elapsed_ms=e, parallel=p, error=err)
        for (t, r, e, p, err) in raw
    )
    report = TransferReport(
        rows=sum(t.rows for t in results if t.error is None),
        elapsed_ms=elapsed_ms,
        parallel=budget,
        tables=results,
    )
    failed = [t for t in results if t.error is not None]
    if failed:
        ok = len(results) - len(failed)
        lines = "\n".join(f"  {t.table}: {t.error}" for t in failed)
        raise MultiTransferError(
            f"{len(failed)} of {len(results)} tables failed "
            f"({ok} succeeded and ARE committed — see .report):\n{lines}",
            report,
        )
    return report
