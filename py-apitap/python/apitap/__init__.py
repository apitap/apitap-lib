"""apitap — move whole tables between databases at wire speed, in bounded memory.

The engine is Rust (see https://apitap.dev); this package is a thin binding.

    import apitap

    report = apitap.transfer(
        "postgres://user:pass@src-host/db",
        "postgres://user:pass@dst-host/db",
        table="public.events",
    )
    print(report.rows, report.elapsed_ms)
"""

from dataclasses import dataclass

from apitap._apitap import __version__, transfer as _transfer

__all__ = ["transfer", "TransferReport", "__version__"]


@dataclass(frozen=True)
class TransferReport:
    """What a transfer did."""

    rows: int
    """Rows landed in the destination."""
    elapsed_ms: int
    """Wall-clock duration of the whole transfer."""
    parallel: int
    """Concurrent pipes actually used (0 = empty source, 1 = single stream)."""


def transfer(
    src: str,
    dst: str,
    table: str,
    *,
    dest_table: str | None = None,
    parallel: int | None = None,
    cursor: str | None = None,
    chunk_bytes: int | None = None,
    durable: bool = True,
    mode: str = "replace",
) -> TransferReport:
    """Copy ``table`` from ``src`` to ``dst``, atomically replacing the destination table.

    The URL schemes pick the route — ``postgres://``/``postgresql://``, ``mysql://``
    sources; ``postgres://``, ``clickhouse://`` (``clickhouse+https://`` for TLS)
    destinations — and each pair negotiates its fastest wire format (raw binary COPY
    passthrough, in-flight RowBinary transcode, or raw wire decode). N concurrent
    range pipes feed a staging table that is swapped in atomically. Atomic (readers
    never see a partial load), 0-row-guarded (an empty source never wipes a good
    table), and memory-bounded (streams with TCP backpressure).

    Full guide: https://github.com/apitap/apitap-lib/blob/main/docs/usage.md

    Args:
        src: Source URL, e.g. ``postgres://user:pass@host:5432/db`` or
            ``mysql://user:pass@host:3306/db``.
        dst: Destination URL (Postgres or ClickHouse).
        table: Source table, optionally schema-qualified (``public.events``).
        dest_table: Destination table; defaults to ``table``.
        parallel: Concurrent range pipes; default auto — a route-specific CPU
            heuristic capped by the cgroup's memory limit. An explicit value is
            never overridden.
        cursor: Numeric column to range-split on; default auto-detects the integer
            primary key. PK-less Postgres tables fall back to TID ranges; other
            sources to a single stream.
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
        durable: Postgres destinations only. ``False`` loads through an UNLOGGED
            table — skipping WAL roughly halves the destination's write cost — and the
            swapped-in table REMAINS unlogged: Postgres truncates it during crash
            recovery until you run ``ALTER TABLE … SET LOGGED``. Leave ``True`` unless
            the destination is rebuildable scratch data. Other destinations ignore it.
    """
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
    )
    return TransferReport(rows=rows, elapsed_ms=elapsed_ms, parallel=used)
