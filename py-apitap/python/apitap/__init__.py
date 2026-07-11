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
) -> TransferReport:
    """Copy ``table`` from ``src`` to ``dst``, atomically replacing the destination table.

    Raw ``COPY (FORMAT binary)`` passthrough — no per-row decode — through N concurrent
    range pipes, staged and swapped in a single transaction. Atomic (readers never see a
    partial load), 0-row-guarded (an empty source never wipes a good table), and
    memory-bounded (streams with TCP backpressure).

    Args:
        src: Source Postgres DSN, e.g. ``postgres://user:pass@host:5432/db``.
        dst: Destination Postgres DSN.
        table: Source table, optionally schema-qualified (``public.events``).
        dest_table: Destination table; defaults to ``table``.
        parallel: Concurrent range pipes; default auto (CPU count, clamped 1–8).
        cursor: Numeric column to range-split on; default auto-detects the integer
            primary key. With no usable cursor the copy runs single-stream.
        chunk_bytes: Bytes coalesced per COPY send (default 4 MiB).
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
    )
    return TransferReport(rows=rows, elapsed_ms=elapsed_ms, parallel=used)
