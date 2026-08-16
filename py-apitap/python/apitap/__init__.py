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

from __future__ import annotations

from dataclasses import dataclass

from apitap._apitap import (
    __version__,
    read as _read,
    read_schema as _read_schema,
    transfer as _transfer,
    transfer_many as _transfer_many,
)

__all__ = [
    "read",
    "Reader",
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


class Reader:
    """A running parallel read. Consume it ONCE, whichever way you like:

    - ``reader.to_polars()`` — the one-liner (needs ``pip install polars``)
    - ``reader.lazy()`` — a polars LazyFrame: write ordinary polars
      (filter/group_by/agg) and ``.collect(engine="streaming")`` pulls the
      table through in constant memory — 10M rows fit a 256 MB container
    - ``for df in reader.batches():`` — small polars DataFrames, one per
      Arrow batch, constant memory
    - ``reader.to_parquet(path)`` — table → Parquet file, constant memory
    - ``pl.DataFrame(reader)`` / ``pa.table(reader)`` / DuckDB — via the
      Arrow PyCapsule protocol, zero-copy
    - ``reader.to_arrow()`` / ``reader.to_pandas()``

    Batches stream on demand: memory holds the batches in flight, never the
    table. With ``parallel > 1`` row order across batches is
    nondeterministic — pass ``parallel=1`` for source order, or sort the
    frame (cheaper than giving up parallel read bandwidth).
    """

    def __init__(self, src, table, cursor, parallel, query, columns=None):
        self._args = (src, table, cursor, parallel, query, columns)
        self._native = None

    def _start(self, materialize: bool):
        # The engine starts on FIRST consumption and picks its strategy
        # from how you consume: to_polars()/to_arrow() build one giant
        # batch per pipe in Rust (fewest FFI crossings, no rechunk);
        # streaming consumers get cgroup-sized batches, memory bounded.
        if self._native is None:
            src, table, cursor, parallel, query, columns = self._args
            self._native = _read(src, table, cursor=cursor, parallel=parallel,
                                 query=query, materialize=materialize,
                                 columns=columns)
        return self._native

    def __arrow_c_stream__(self, requested_schema=None):
        return self._start(False).__arrow_c_stream__(requested_schema)

    @property
    def columns(self):
        return self._start(False).columns()

    def to_polars(self):
        """The primary path: ``apitap.read(...).to_polars()``."""
        try:
            import polars as pl
        except ImportError as e:
            raise ImportError(
                "to_polars() needs polars — pip install polars "
                "(or use to_arrow() / to_pandas())"
            ) from e
        self._start(True)
        return pl.DataFrame(self)

    def to_arrow(self):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("to_arrow() needs pyarrow — pip install pyarrow") from e
        self._start(True)
        return pa.table(self)

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def lazy(self):
        """A polars LazyFrame over the STREAM — write ordinary polars and let
        ``collect(engine="streaming")`` pull batches through constant memory::

            top = (apitap.read(src, table="events")
                   .lazy()
                   .filter(pl.col("amount") > 100)
                   .group_by("status").agg(pl.len())
                   .collect(engine="streaming"))

        Ten million rows aggregate in a 256 MB container this way — the full
        DataFrame never exists. The query's COLUMN PROJECTION is pushed all
        the way into the SQL: a query touching 2 of 15 columns makes the
        server serialize and this side decode only those 2. The FILTER
        pushes too when a conservative SQL translation exists (arithmetic,
        comparisons, AND/OR — see ``_predicate_sql``): the server then
        skips serializing the dropped rows entirely. Predicates always
        re-run client-side, and head() prunes per batch. One-shot: collect
        once.
        """
        try:
            import polars as pl
            from polars.io.plugins import register_io_source
        except ImportError as e:
            raise ImportError(
                "lazy() needs polars >= 1.0 and pyarrow — "
                "pip install polars pyarrow"
            ) from e
        if self._native is not None:
            raise RuntimeError(
                "lazy() must be this Reader's first consumption — make a "
                "fresh apitap.read(...) for it"
            )
        src, table, cursor, parallel, query, _ = self._args

        def dtype(tag):
            if tag.startswith("decimal:"):
                _, p, s = tag.split(":")
                return pl.Decimal(int(p), int(s))
            return {
                "i16": pl.Int16, "i32": pl.Int32, "i64": pl.Int64,
                "f32": pl.Float32, "f64": pl.Float64, "bool": pl.Boolean,
                "date": pl.Date, "ts:utc": pl.Datetime("us", "UTC"),
                "ts:naive": pl.Datetime("us"), "str": pl.String,
                "bin": pl.Binary,
            }[tag]

        # Cheap schema-only probe — the engine starts LATER, inside the
        # collect, once polars has told us which columns the query needs.
        schema = pl.Schema(
            {name: dtype(tag) for name, tag, _ in _read_schema(src, table)}
        )

        str_cols = {
            n for n, dt in schema.items() if dt in (pl.String, pl.Binary)
        }
        float_cols = {
            n for n, dt in schema.items() if dt in (pl.Float32, pl.Float64)
        }
        is_my = src.split("://", 1)[0].lower().startswith("mysql")

        def source(with_columns, predicate, n_rows, batch_size):
            import pyarrow as pa
            cols = list(with_columns) if with_columns is not None else None
            # FILTER pushdown: a conservative subset of the predicate is
            # rendered as SQL and ANDed into every span statement — the
            # server filters, so the wire and the decoders only carry
            # survivors. The polars filter below still runs on whatever
            # arrives: the pushdown is bandwidth, never correctness.
            where = (
                _predicate_sql(
                    predicate, set(schema.names()), str_cols, float_cols, is_my
                )
                if predicate is not None
                else None
            )
            native = _read(src, table, cursor=cursor, parallel=parallel,
                           query=query, materialize=False, columns=cols,
                           push_where=where)
            taken = 0
            # The engine emits exactly the requested columns in the
            # requested order — no per-batch select needed.
            for batch in pa.RecordBatchReader.from_stream(native):
                df = pl.from_arrow(batch)
                if predicate is not None:
                    df = df.filter(predicate)
                if n_rows is not None:
                    remaining = n_rows - taken
                    if remaining <= 0:
                        return
                    if df.height > remaining:
                        df = df.head(remaining)
                taken += df.height
                yield df

        return register_io_source(source, schema=schema)

    def batches(self):
        """Iterate the table as SMALL polars DataFrames — one per Arrow
        batch, constant memory, no pyarrow ceremony::

            for df in apitap.read(src, table="events").batches():
                ...   # df is a plain polars DataFrame, a few MB each
        """
        try:
            import polars as pl
            import pyarrow as pa
        except ImportError as e:
            raise ImportError(
                "batches() needs polars and pyarrow — pip install polars pyarrow"
            ) from e
        for batch in pa.RecordBatchReader.from_stream(self):
            yield pl.from_arrow(batch)

    def to_parquet(self, path, *, compression="zstd", row_group_bytes=None,
                   **writer_kwargs):
        """Stream the table into a Parquet file at constant memory; returns
        the row count. ``apitap.read(src, table="t").to_parquet("t.parquet")``
        moves a 10M-row table through a 256 MB container.

        Batches accumulate to ~``row_group_bytes`` of Arrow data before each
        row group is written. Measured on the 15-col 10M bench: 128 MB groups
        beat per-batch flushing on every axis at once (25.7 s → 21.3 s,
        497 MB → 366 MB, lower peak RSS) — but 16 MB groups LOSE density to
        per-batch flushing (dictionary pages blow their 1 MB limit mid-group
        and the remainder goes plain), so the default only buys big groups
        where the cgroup limit affords real ones; capped containers keep the
        per-batch path (``row_group_bytes=0``), whose memory profile is the
        proven one. Pass an explicit value to override either way."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise ImportError("to_parquet() needs pyarrow — pip install pyarrow") from e
        if row_group_bytes is None:
            try:
                limit = int(open("/sys/fs/cgroup/memory.max").read().strip())
                cand = min(128 << 20, limit // 8)
                row_group_bytes = cand if cand >= 64 << 20 else 0
            except (OSError, ValueError):  # no cgroup v2 limit ("max" or absent)
                row_group_bytes = 128 << 20
        reader = pa.RecordBatchReader.from_stream(self)
        rows = 0
        buf, buf_bytes = [], 0
        with pq.ParquetWriter(path, reader.schema, compression=compression,
                              **writer_kwargs) as w:
            def flush():
                nonlocal buf, buf_bytes
                w.write_table(pa.Table.from_batches(buf, schema=reader.schema))
                buf, buf_bytes = [], 0
            for batch in reader:
                buf.append(batch)
                buf_bytes += batch.nbytes
                rows += batch.num_rows
                if buf_bytes >= row_group_bytes:
                    flush()
            if buf:
                flush()
        return rows


def _predicate_sql(predicate, allowed, str_cols, float_cols, is_my):
    """Render a polars predicate as a SQL WHERE fragment, or None.

    Only a conservative subset crosses the wire: arithmetic (+ - * %) and
    comparisons over non-string columns, equality/inequality on strings
    (range comparisons on text differ between server collations and
    polars byte order), AND/OR, boolean and numeric literals. Casts are
    elided only around literals (polars' automatic widening) — a cast over
    a COLUMN changes values and must not be dropped. `!=` between two
    float subtrees is refused (Postgres defines NaN = NaN as true; IEEE
    says false). Backslashes in string literals are refused (their meaning
    depends on server escape modes this session never pinned). Column
    names must exist in the schema; anything else — including any
    surprise in the serialized expression tree, which is not a stable
    format across polars versions — returns None and the filter simply
    stays client-side. The client-side filter runs regardless, so this
    function is a bandwidth optimization and never a correctness
    dependency.
    """
    import json

    try:
        raw = predicate.meta.serialize(format="json")
        ir = json.loads(raw)
    except Exception:
        return None

    CMP = {"Eq": "=", "NotEq": "<>", "Lt": "<", "LtEq": "<=",
           "Gt": ">", "GtEq": ">="}
    STR_OK = {"Eq", "NotEq"}
    ARITH = {"Plus": "+", "Minus": "-", "Multiply": "*", "Modulus": "%"}
    LOGIC = {"And": "AND", "Or": "OR", "LogicalAnd": "AND", "LogicalOr": "OR"}
    INT_T = {"Int", "Int8", "Int16", "Int32", "Int64",
             "UInt8", "UInt16", "UInt32", "UInt64"}
    FLOAT_T = {"Float", "Float32", "Float64"}

    def quote(name):
        if is_my:
            return "`" + name.replace("`", "``") + "`"
        return '"' + name.replace('"', '""') + '"'

    def lit(v):
        # -> (sql, stringy, floaty)
        if isinstance(v, dict) and len(v) == 1:
            (t, x), = v.items()
            if t in ("Dyn", "Scalar"):
                return lit(x)
            if t in INT_T:
                if isinstance(x, bool) or not isinstance(x, int):
                    raise ValueError("non-int payload")
                return str(x), False, False
            if t in FLOAT_T:
                if not isinstance(x, (int, float)) or isinstance(x, bool):
                    raise ValueError("non-float payload")
                f = float(x)
                if f != f or f in (float("inf"), float("-inf")):
                    raise ValueError("non-finite literal")
                return repr(f), False, True
            if t == "Boolean":
                if not isinstance(x, bool):
                    raise ValueError("non-bool payload")
                return ("TRUE" if x else "FALSE"), False, False
            if t in ("String", "StrOwned", "Str"):
                if not isinstance(x, str):
                    raise ValueError("non-str payload")
                if "\x00" in x or "\\" in x:
                    raise ValueError("unsafe byte in literal")
                return "'" + x.replace("'", "''") + "'", True, False
            if t == "Date":
                # Days since the epoch — both dialects take DATE 'Y-M-D', and
                # a DATE column compares to it with identical ordering on
                # either side of the wire (no collation, no timezone).
                if isinstance(x, bool) or not isinstance(x, int):
                    raise ValueError("non-int date payload")
                import datetime

                d = datetime.date(1970, 1, 1) + datetime.timedelta(days=x)
                return "DATE '" + d.isoformat() + "'", False, False
        raise ValueError(f"literal {v!r}")

    def walk(n):
        # -> (sql, stringy, floaty)
        if not isinstance(n, dict) or len(n) != 1:
            raise ValueError(f"node {n!r}")
        (k, v), = n.items()
        if k == "Column":
            name = v if isinstance(v, str) else v.get("name")
            if name not in allowed:
                raise ValueError(f"unknown column {name!r}")
            return quote(name), name in str_cols, name in float_cols
        if k == "Literal":
            return lit(v)
        if k == "Cast":
            # Elide ONLY the widening casts polars wraps around literals.
            # A cast over a column changes the values being compared —
            # dropping it would make the server filter on different data
            # than polars sees (rows lost silently, the client filter
            # cannot resurrect what never arrived).
            inner = v.get("expr")
            if not (isinstance(inner, dict) and set(inner) == {"Literal"}):
                raise ValueError("cast over non-literal")
            dt = json.dumps(v.get("dtype", ""))
            if not any(t in dt for t in ("Int", "Float", "UInt")):
                raise ValueError("non-numeric cast")
            return walk(inner)
        if k == "BinaryExpr":
            op = v["op"]
            if not isinstance(op, str):
                raise ValueError(f"op shape {op!r}")
            left, ls, lf = walk(v["left"])
            right, rs, rf = walk(v["right"])
            floaty = lf or rf
            if op in LOGIC:
                return f"({left} {LOGIC[op]} {right})", False, False
            if op in CMP:
                if (ls or rs) and op not in STR_OK:
                    raise ValueError("string range comparison")
                if op == "NotEq" and lf and rf:
                    raise ValueError("float != float (NaN semantics differ)")
                return f"({left} {CMP[op]} {right})", False, False
            if op in ARITH:
                if ls or rs:
                    raise ValueError("string arithmetic")
                return f"({left} {ARITH[op]} {right})", False, floaty
            raise ValueError(f"op {op!r}")
        raise ValueError(f"node kind {k!r}")

    try:
        sql, stringy, _ = walk(ir)
        return None if stringy else sql
    except Exception:
        return None


def read(
    src: str,
    table: str | None = None,
    *,
    cursor: str | None = None,
    parallel: int | None = None,
    query: str | None = None,
    columns: list[str] | None = None,
) -> Reader:
    """Read a Postgres or MySQL table straight into a DataFrame, at wire speed.

    One line, no knobs required::

        df = apitap.read("postgres://user:pass@host/db", table="public.orders").to_polars()
        df = apitap.read("mysql://user:pass@host/db", table="orders").to_polars()

    The engine runs the same parallel binary range pipes the transfer
    routes use and decodes them into Arrow batches in Rust — Python only
    ever receives buffer pointers. Memory stays bounded (batches in
    flight, sized off the cgroup limit), so big tables read fine from
    small containers.

    Args:
        src: ``postgres://`` or ``mysql://`` source URL.
        table: Source table, optionally schema-qualified.
        cursor: Integer column to range-split on (default: the integer PK;
            PK-less Postgres tables fall back to TID ranges, PK-less MySQL
            tables read as one stream).
        parallel: Concurrent range pipes; default auto. ``1`` = source order.
        query: Raw SQL instead of a table (coming next — refused loudly today).
        columns: Read only these columns, in this order (default: all).
            ``.lazy()`` fills this automatically from the query's projection.
    """
    if (table is None) == (query is None):
        raise ValueError("pass exactly one of table=…, query=…")
    if query is not None:
        raise ValueError(
            "read: query= lands next — pass table= (and optionally cursor=) today"
        )
    return Reader(src, table, cursor, parallel, query, columns)


def _split_ddl(value, name):
    """`partition_by`/`order_by` as either one clause for every table, or a dict
    keyed by table name for a group where each table wants its own.

    Returns (global_clause, per_table_map). A dict is NOT collapsed into a
    global value: a table missing from the dict falls back to the engine
    default, which is what "give me a custom column for these three tables"
    should mean.
    """
    if value is None:
        return None, None
    if isinstance(value, str):
        return value, None
    if isinstance(value, dict):
        if not value:
            return None, None
        bad = [k for k, v in value.items() if not isinstance(k, str) or not isinstance(v, str)]
        if bad:
            raise ValueError(
                f"{name}={value!r}: a dict must map table name -> clause, both strings"
            )
        return None, dict(value)
    raise ValueError(
        f"{name}={value!r}: pass a string for every table, or a dict "
        f"{{table: clause}} for per-table clauses"
    )


def transfer(
    src: str,
    dst: str,
    table: str | None = None,
    *,
    tables: list[str] | dict[str, str] | None = None,
    schema: str | None = None,
    dest_table: str | None = None,
    parallel: int | None = None,
    cursor: str | None = None,
    chunk_bytes: int | None = None,
    durable: bool = True,
    mode: str = "replace",
    engine: str | None = None,
    order_by: str | dict[str, str] | None = None,
    on_cluster: str | None = None,
    partition_by: str | dict[str, str] | None = None,
    changelog: bool = False,
    slots: int | None = None,
) -> TransferReport:
    """Copy one table, a list of tables, or a whole schema from ``src`` to ``dst``.

    Exactly one of ``table``, ``tables``, ``schema`` picks the scope. The URL
    schemes pick the route — ``postgres://``/``postgresql://``, ``mysql://``,
    ``gsheets://<spreadsheet_id>?credentials=/path/key.json`` (tabs are the
    tables, all-text, replace only), ``github://owner/repo[/dir]?ref=main``
    (CSV files are the tables, all-text, replace only),
    ``github+api://owner/repo`` (issues/PRs/commits/stars… as typed tables,
    incremental where the API allows) sources — Postgres (logical replication)
    and MySQL/MariaDB (binlog) sources additionally support ``mode="log_based"`` batch
    CDC (every change, drained per scheduled run, snapshot-pinned bootstrap) —
    ``postgres://``,
    ``clickhouse://`` (``clickhouse+https://`` for TLS),
    ``bigquery://<project>/<dataset>?credentials=/path/key.json``,
    ``gcs://<bucket>[/prefix]?format=csv|parquet&credentials=/path/key.json``
    (files: one composed .csv.gz per table, or a directory of Parquet parts),
    ``s3://<bucket>[/prefix]?format=parquet&endpoint=…`` (S3/MinIO/R2, Parquet
    files), ``iceberg://<catalog-host:port>/<namespace>?warehouse=…&endpoint=…``
    (Apache Iceberg via any REST catalog; replace/append/merge are real
    snapshot commits) destinations — and
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
        tables: A list of source tables — moved in one call through one budget —
            or a ``{table: mode}`` dict giving EACH table its own mode in one
            call (e.g. ``{"orders": "log_based", "dim_date": "replace"}``).
            The dict form partitions by mode: bulk modes run through the
            shared-budget pipeline; all ``log_based`` tables share ONE
            replication slot (one publication, one drain pass, one snapshot-
            pinned group bootstrap, one watermark — a CDC group fails as a
            unit). A plain list with ``mode="log_based"`` is the all-CDC
            version of the same group.
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
            bootstraps as replace when the table doesn't exist), ``"merge"``
            (Postgres destinations: incremental upsert by the destination's
            PRIMARY KEY), or ``"log_based"`` (batch CDC from a Postgres
            (logical replication) or MySQL/MariaDB (binlog) source — every change incl.
            deletes and TRUNCATEs, drained per scheduled run into Postgres,
            ClickHouse, MySQL, BigQuery or Iceberg; first run bootstraps
            snapshot-pinned, the watermark commits atomically with the data, and
            a table list shares ONE replication slot. BigQuery needs a billed
            project — CDC applies row-level DML the free tier rejects). Incremental modes require a cursor
            (integer or timestamp column). Append assumes the cursor is
            monotonic with COMMIT order — for update-prone or
            concurrently-written tables use merge with an ``updated_at``
            cursor. See docs/usage.md.
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
        partition_by: Analytical destinations (ClickHouse, BigQuery). PARTITION
            BY of the table apitap creates. Default = MONTHLY on the changelog's
            own ``_apitap_at``, because **BigQuery caps the number of partitions
            per table**: daily partitions run out in roughly a decade or three
            depending on the cap in force, monthly in centuries — and a
            changelog is meant to live a long time.
            Partitioning does not speed up the ``__current`` view (it scans
            every version per key by design); it buys RETENTION (drop partitions
            older than N months) and time-range audit queries. ClickHouse takes
            the expression verbatim; BigQuery takes a COLUMN and can only
            partition on DATE/TIMESTAMP/DATETIME — never a STRING, so
            ``_apitap_op`` is refused there and belongs in the cluster key
            instead. The emitted DDL always lands MONTHLY
            (``DATE_TRUNC``/``TIMESTAMP_TRUNC``/``DATETIME_TRUNC``); a DATE
            column is not used bare, because bare is daily. Ignored when the
            table already exists.

            Give it a COLUMN NAME and it means monthly on that column, the
            same way on both engines::

                partition_by="created_at"

            For a MULTI-TABLE run where each table has its own time column,
            pass a dict — same spelling, one entry per table::

                apitap.transfer(src, dst,
                    tables=["orders", "events", "audit"],
                    mode="log_based", changelog=True,
                    partition_by={"orders": "created_at",
                                  "events": "occurred_at"})
                    # "audit" is not listed -> monthly on _apitap_at

            Keys are matched against the name you passed, the resolved
            ``schema.table``, and the bare name, so ``"orders"`` and
            ``"public.orders"`` both work. Before anything is written, every
            member's clause is checked against that table's real columns — a
            column only some tables own is refused with nothing bootstrapped,
            rather than tearing the group.

            Anything that is NOT a plain column name is treated as a verbatim
            ClickHouse expression (``"toStartOfWeek(ts)"``,
            ``"(toYYYYMM(ts), region)"``) — the escape hatch for a granularity
            other than a month. BigQuery takes column names only.
        changelog: ``mode="log_based"`` into an ANALYTICAL destination
            (ClickHouse, BigQuery). ``False`` (default) keeps the destination a
            REPLICA of the source — the window is applied with delete+insert or
            one MERGE, so the table holds current state only. ``True`` makes it a
            CHANGELOG: every change is APPENDED with ``_apitap_op``
            (``I``/``U``/``D``), ``_apitap_lsn``, ``_apitap_seq`` and
            ``_apitap_at``, nothing is ever updated or deleted, and a companion
            ``<table>__current`` view derives the current state. Analytical
            stores are built for appends, so this is both faster and gentler:
            BigQuery loses its per-window MERGE job floor (a window becomes a
            load job plus one ``INSERT … SELECT``) and ClickHouse stops writing
            mutations entirely. BigQuery still needs a **billed** project — an
            ``INSERT`` is row-level DML, which sandbox projects reject. On
            replay ``<table>__current`` stays correct, but the LOG is
            at-least-once: on ClickHouse the append and the watermark are two
            statements, and window boundaries are not reproducible, so a crash
            between them leaves duplicate history under a NEW ``_apitap_lsn``.
            Row stores (Postgres,
            MySQL) and Iceberg REFUSE it loudly rather than quietly hand back a
            replica; every bulk mode ignores it.
        slots: ``mode="log_based"``, multi-table, Postgres sources only —
            split the tables across N replication slots and drain them
            CONCURRENTLY. Postgres decodes each slot in ONE walsender process
            that saturates a core long before apitap does; on the measured
            100-table / 100M-change rig, ``slots=4`` took the drain from
            121,789 to 278,947 changes/s (2.29x, verified per table). Not
            linear: every slot decodes the whole WAL and keeps only its own
            tables, so gains flatten past ~4-16 slots. The source pays one
            busy core per slot, each slot holds WAL independently, and
            ``max_replication_slots`` must cover N. Groups are cut
            deterministically from the sorted table list, so re-runs resume
            the same slots; CHANGING ``slots`` renames them all, which is
            refused loudly until the old state is cleared. Rejected for bulk
            modes, single-table runs, and MySQL sources (one binlog stream).
    """
    _pb_global, _pb_map = _split_ddl(partition_by, "partition_by")
    _ob_global, _ob_map = _split_ddl(order_by, "order_by")
    if (_pb_map or _ob_map) and table is not None:
        raise ValueError(
            "partition_by/order_by as a dict is for multi-table runs — a "
            "single table=… run takes a plain string"
        )
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
            order_by=_ob_global,
            on_cluster=on_cluster,
            partition_by=_pb_global,
            partition_by_per_table=_pb_map,
            order_by_per_table=_ob_map,
            changelog=changelog,
            slots=slots,
        )
        return TransferReport(rows=rows, elapsed_ms=elapsed_ms, parallel=used)

    if dest_table is not None:
        raise ValueError(
            "dest_table applies to single-table transfers — multi-table runs "
            "keep the source names"
        )
    specs = None
    if isinstance(tables, dict):
        specs, tables = [(t, m) for t, m in tables.items()], None
    elapsed_ms, budget, raw = _transfer_many(
        src,
        dst,
        tables=tables,
        schema=schema,
        specs=specs,
        parallel=parallel,
        cursor=cursor,
        chunk_bytes=chunk_bytes,
        durable=durable,
        mode=mode,
        engine=engine,
        order_by=_ob_global,
        on_cluster=on_cluster,
        partition_by=_pb_global,
        partition_by_per_table=_pb_map,
        order_by_per_table=_ob_map,
        changelog=changelog,
        slots=slots,
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
