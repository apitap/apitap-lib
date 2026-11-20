# SQL across databases: `apitap.sql()`

One SQL statement over tables that live in **different databases** — a Postgres
table joined to a MySQL table, aggregated, filtered, grouped — answered by
apitap's own Rust engine. The servers only scan and send their rows; the join,
the aggregates and every staged subquery run inside apitap, exact, in a
container as small as 0.5 vCPU / 256 MB.

```python
import apitap

SRC = {"pg": "postgres://user:pass@pg-host/db", "my": "mysql://user:pass@my-host/db"}

r = apitap.sql("""
    WITH early AS (SELECT o_orderkey, o_orderpriority FROM my.orders
                   WHERE o_orderdate < DATE '1995-03-15')
    SELECT o_orderpriority, SUM(l_extendedprice * (1 - l_discount)) AS revenue
    FROM pg.lineitem l JOIN early o ON l.l_orderkey = o.o_orderkey
    GROUP BY 1 ORDER BY 1""", sources=SRC)
df = r.to_polars()          # or hand the Arrow stream to DuckDB / pyarrow

# land the answer in any destination (typed, staged, atomic swap)
apitap.sql(QUERY, sources=SRC, dest="clickhouse://…", table="revenue")

# a join + SUM/COUNT kept up to date by the changes since the last run
apitap.sql(JOIN_SUM, sources=SRC, dest="postgres://…", table="rev", mode="log_based")
```

Try it in the browser, no install: **[apitap.dev/extract](https://apitap.dev/extract)**,
SQL tab — each query runs in its own 256 MB · 0.5 vCPU container against
sandbox Postgres and MySQL databases.

## The call

```python
apitap.sql(query, *, sources, dest=None, table=None, mode=None) -> SqlResult | TransferReport
```

`sources` names each database; the query calls tables `name.table`. Without
`dest=` the answer is a `SqlResult`, an Arrow stream read a chunk at a time
(`.to_polars()`, or hand it to DuckDB / pyarrow). With `dest=` and `table=` it
lands in any destination through the ordinary transfer path and a
`TransferReport` comes back. With `mode="log_based"` as well it is an
**incremental view**.

## What runs, and what is refused

One table, two tables joined on an integer key, or a star of
three or more: one aggregated (fact) table, every other table joined by its
key — single or two-column (`partsupp` on `(ps_partkey, ps_suppkey)`) — to a
table already joined. Then:

| piece | supported |
|---|---|
| joins | `INNER JOIN … ON a.k = b.k` or the comma form with the equalities in `WHERE`; the key is an integer column on a database, while a sheet's, CSV's or GitHub API table's text key is read as an integer (`'010'` equals `'10'`, spaces are trimmed, an empty cell is NULL, a value that is not an integer is refused); a text column from a database, directly or through a CTE or derived table, is refused as a key |
| `WHERE` | per-table conditions (comparisons, `BETWEEN`, `IN`, `LIKE`, `IS NULL`, `AND`/`OR`/`NOT`, date literals, folded constants) are pushed to that table's server; an `OR` across tables (TPC-H Q19) is checked here per matched row, its single-table parts implied to each server too; a comparison of a column of each table is checked on each joined row when both are numbers, both dates or both timestamps of one kind (`l.shipdate > o.orderdate`) |
| subqueries | `IN` / `EXISTS` / `NOT IN` / `NOT EXISTS (SELECT …)`, including `IN (SELECT k … GROUP BY k HAVING SUM/COUNT/AVG(…) > x)` (TPC-H Q18) |
| expressions | arithmetic in aggregates, `CASE WHEN … THEN … END` over one table, `EXTRACT(field FROM col)` (YEAR, QUARTER, MONTH, WEEK, DAY, DOW, ISODOW, DOY, HOUR, MINUTE; each column's own server's definitions) and `date_trunc('unit', col)` as keys, derived tables `FROM (SELECT … ) AS t` without GROUP BY |
| aggregates | `SUM`, `COUNT(*)`, `COUNT(col)`, `MIN`, `MAX` (numbers, dates and timestamps, and text under a byte collation), `AVG`; decimals exact (i128, digit for digit), integer overflow widened, floats f64 |
| result | `GROUP BY` (columns of any joined table), `ORDER BY` (exact for decimals), `LIMIT`, `HAVING`, `COUNT(DISTINCT col)`, a plain `SELECT` with no aggregate |
| staged | `WITH` (CTEs, nested), derived tables with GROUP BY, scalar subqueries, correlated scalar subqueries used as a NULL-rejecting comparison (TPC-H Q2, Q17, Q20, Q22), `EXISTS` comparing two tables' columns (Q21), `LEFT JOIN … GROUP BY` the preserved table's key with `COUNT(*)` or aggregates of a joined-table column (Q13) |

**Refused** before a row is read, naming the piece: `SELECT *`, `UNION`, window
functions, `WITH RECURSIVE`, `OFFSET`/`FETCH`/`LIMIT BY` at any depth, right
and full outer joins, a `LEFT JOIN` grouped coarser than its key or with
`HAVING`/`DISTINCT` or an aggregate over an expression of the joined table, a
correlated subquery in any other place, a CTE shadowing another, a timestamp
scalar subquery used as a literal, a `WHERE` expression over two tables'
columns (`l.a * 2 > o.b`), a comparison of two tables' text columns (their
collations may differ) or of unlike kinds (a date against a timestamp or a
number), joins on non-integer keys (a database's text column as a join key included).

### Refused today, and what to write instead

Each of these is refused by name before a row is read. Where a rewrite runs
today, it is given; it was checked against the servers.

| you wrote | why it is refused | what runs instead |
|---|---|---|
| `GROUP BY id % 3`, `GROUP BY a + b`, `GROUP BY CASE … END` (arithmetic or CASE as a group key) | the key would be computed here, and only column keys, `EXTRACT(field FROM col)` and `date_trunc('unit', col)` are | a view in the source that computes the key, read as a column. A CTE does **not** work: `WITH x AS (SELECT id % 3 AS r, …)` is refused the same way |
| aggregates that read both tables of a join (`SELECT m.grp, MIN(p.tc), MAX(m.tb) FROM pg.p JOIN my.m … GROUP BY m.grp`) | a join's aggregates must all read one table | join first in a CTE that selects the columns, then aggregate the CTE: `WITH j AS (SELECT m.grp AS g, p.tc AS x, m.tb AS y FROM pg.p p JOIN my.m m ON p.id = m.id) SELECT g, MIN(x), MAX(y), COUNT(*) FROM j GROUP BY g`. The same as a derived table in `FROM` is still refused (it is inlined) |
| `MIN`/`MAX` of text under a linguistic or case-insensitive collation (Postgres `en_US`, ICU, `citext`; MySQL `_ci`, `_ai`) | the order is the collation's, which apitap does not apply | MIN/MAX of text under a byte collation (Postgres `COLLATE "C"`/`POSIX`, MySQL `_bin`) runs, padded `CHAR` compared as its server does. Otherwise a view with `COLLATE "C"` / a `_bin` collation |
| `EXTRACT` / `date_trunc` of a `timestamptz` (or MySQL `TIMESTAMP`) whose server's default time zone is not UTC | the server answers in its own zone, apitap reads the instant in UTC | a view converting it (`col AT TIME ZONE 'Europe/Vilnius'`, `CONVERT_TZ(col, '+00:00', …)`), or a UTC default zone |
| `EXTRACT(DOW/ISODOW/DOY …)` or `date_trunc('week', …)` of a MySQL column | not MySQL's fields; MySQL's weeks depend on a mode | a view with `DAYOFWEEK()`, `WEEKDAY()`, `DAYOFYEAR()` or the week start you mean; `EXTRACT(WEEK …)` of a MySQL column is MySQL's `WEEK(d, 0)` |
| `ORDER BY`, `MIN`/`MAX` of an enum (Postgres enum or a domain over one; MySQL `ENUM`) | an enum orders by its declaration, not by its labels, and apitap holds the labels | expose the position as an integer in a source view (`array_position(enum_range(NULL::t), col)`; MySQL `col+0`). `GROUP BY`, `DISTINCT`, and `= / <> / IN 'label'` run |
| `ORDER BY`, `MIN`/`MAX` of a type held as its text form whose text does not order as the type (inet, cidr, oid, pg_lsn, `"char"`, arrays, MySQL `SET` and `TIME`); `MIN`/`MAX` where the source has none (uuid, macaddr, bit, varbit) | the text's byte order is not the value's (`'10.0.0.10' < '9.0.0.1'`, `'-01:00' < '-02:00'`) | a source view exposing an orderable column (an integer, a timestamp, text). `GROUP BY` and `DISTINCT` run (one text per value); uuid, macaddr, bit, varbit and Postgres `time` also `ORDER BY` |
| any ordering or grouping of interval, json/jsonb, money, timetz, ranges, composite types, tsvector, xml, geometric types, any type not listed here | two different texts can be one value (`'1 day'` = `'24:00:00'`, `1` = `1.0` in jsonb), or the text has no order | projection runs; compute an orderable column in a source view |
| a comparison with a literal that apitap evaluates (over a CTE or a stage) on a type held as its text form, other than an enum's `= / <> / IN 'label'` and a numeric | the literal's text need not be the value's own (`'10.0.0.10'` vs `10.0.0.10/32`, an uppercase uuid) | filter in the base table (pushed to the source), or in a source view |
| `ORDER BY` a text column under a linguistic collation, where the server orders a pair otherwise than its bytes | apitap sorts bytes, and asks the server about each pair it cannot vouch for; a disagreement is refused, naming the pair | `ORDER BY col COLLATE "C"` in a source view, or a `_bin` collation |
| `SELECT … ORDER BY col LIMIT n` on a big table, `col` text under a glibc or ICU collation (or MySQL `_ci`) | the first n rows cannot be kept as the read streams (a byte cut could drop a row the collation keeps), so every row would be held; past the memory budget it is refused, before the read when the server's statistics already say so | `ORDER BY col COLLATE "C"` in a source view, or a `_bin` collation. On a Postgres server built against musl (the official Alpine images) a libc collation such as `en_US.utf8` orders by bytes, so this streams and answers |
| `… LIMIT n OFFSET m` (or `FETCH`, `LIMIT BY`), at any depth | not implemented: a skipped prefix of an ordered answer | `LIMIT m + n` here and drop the first `m` rows in the caller (`.slice(m)` on the frame) |
| `UNION` / `UNION ALL` of two `SELECT`s | set operations are not implemented | one `sql()` call a branch, concatenated in the caller (`pl.concat([...])`); for `UNION`, de-duplicate there (`.unique()`) |
| `SELECT *`, also inside a subquery, CTE or derived table (`SELECT … FROM (SELECT * FROM pg.t) x`) | the columns to read are named by the statement, so each server reads only those | name the columns: `SELECT a, b FROM (SELECT a, b FROM pg.t) x` |
| `MIN`/`MAX`/`DISTINCT` under a Postgres nondeterministic collation | equality is the collation's, not bytes | a deterministic collation in a view |
| two `sql()` statements reading a role with one connection free, each reading a source twice | its reads need one snapshot: one connection to hold it, one to read | raise the role's `CONNECTION LIMIT` by one, or run fewer statements at once |

## How it runs

Each server only scans and sends: its table's conditions and
columns go into its own `SELECT`, never a join, aggregate or another table's
keys (opt-in exceptions below). The join key's range is asked of both sides
and intersected; the smaller side becomes a direct-address key map (dimension
tables load concurrently into their own maps); the large side streams past it
over parallel pipes, and the decoder drops rows whose key the map lacks before
decoding their other columns. Aggregates fold as rows arrive — no joined row is
ever built. A build past its memory budget (40% of the cgroup limit) splits into
key-range passes; on a Postgres table stored in key order each pass scans only
the pages its keys lie in, and a MySQL table without a leading index on the key
is read once and its rows spilled to disk by range (grace) instead of re-read.
Groups past an eighth of the limit are answered in parts of a `GROUP BY`
column. A query the engine cannot answer in one pass (CTEs, subqueries,
`HAVING`, `COUNT(DISTINCT)`, `LEFT JOIN`) runs as stages: each inner answer is a
stage table, held in memory up to a sixteenth of the limit for all stages
together, then written to disk in chunks of at most 8 MiB and read back a chunk
at a time. The final answer leaves the same way, so a 5.7M-row answer reaches
DuckDB or a `dest=` table at 244 MB peak.

## Consistency and semantics

Every read of one Postgres server in a query —
passes, page spans, dimension and late reads, subqueries, every stage — runs
in **one exported snapshot** for the whole statement, so an UPDATE committing
mid-query never loses or doubles a row. MySQL sources take no snapshot: a
statement reading one MySQL table twice can see two states.

Text compares follow the column's collation: Postgres `CHAR(n)` ignores
trailing spaces, VARCHAR/TEXT do not; a MySQL `_ci` column compares
case-insensitively, `_ai_ci` / `general_ci` also accent-insensitively
(`'Brésil' = 'bresil'`), PAD SPACE collations ignore trailing spaces, and a
`GROUP BY` on such a column makes the server's groups (shown as the first value
met). Not emulated: the full Unicode collation algorithm beyond accents and the
common Latin base letters; text join keys compare bytewise.

## Incremental views

`mode="log_based"` (needs `dest=` and `table=`): the first
run reads the tables once at a replication snapshot (Postgres slot snapshot;
MySQL `FLUSH TABLES WITH READ LOCK` + consistent snapshot + binlog position),
every later run moves the answer by the changes since — no re-read. One
Postgres table, or a Postgres × MySQL join; `SUM`/`COUNT`/`AVG`
(`MIN`/`MAX` refused: a delete needs the rest of the group); Postgres tables
need `REPLICA IDENTITY FULL`. State lives in `APITAP_STATE_DIR` (default
`~/.apitap/views`), bucketed on disk with a SHA-256 per block. Dropping the
destination table does not reset a view: its state file and its replication
slot stay, and the next run writes the table again from that state moved by
the new changes (not from a fresh read). To start over, remove the view's state file (errors name its path), drop its
replication slot and run again: a first run that finds a slot under the view's own name and no state for it refuses,
naming the slot, because another state dir or host may own that view (it never drops it); a
`transfer()` destination keeps its `_apitap_state` row the same way (see the
usage guide, "Incremental semantics you must know").

## Measured: all 22 TPC-H queries across two databases

A non-standard TPC-H seed of about 29 GB (not SF10): lineitem 60.3M, partsupp
52.8M, customer 9.9M rows in PostgreSQL; orders 69.9M, part 13.1M, supplier
0.65M, nation and region in MySQL. Each query ran once in a 0.5 vCPU / 256 MB
container (`--cpus=0.5 --memory=256m`, swap off) on the 0.67.0 release wheel;
all 22 answers equal DuckDB's (decimals digit for digit; Q1's float `AVG`
columns to the last digits of summation order). 0.67.1, run interleaved with
0.67.0 on the same host, gave the same answers in 928 s of wall against 941 s.

| query | wall | CPU | peak | | query | wall | CPU | peak |
|---|---|---|---|---|---|---|---|---|
| Q1 | 62.2 s | 31.1 s | 200 MB | | Q12 | 12.8 s | 3.5 s | 98 MB |
| Q2 | 30.7 s | 11.8 s | 130 MB | | Q13 | 171.6 s | 71.8 s | 259 MB |
| Q3 | 39.8 s | 12.8 s | 162 MB | | Q14 | 12.6 s | 5.2 s | 165 MB |
| Q4 | 13.2 s | 3.9 s | 116 MB | | Q15 | 20.2 s | 6.1 s | 113 MB |
| Q5 | 33.4 s | 12.0 s | 123 MB | | Q16 | 82.7 s | 30.4 s | 150 MB |
| Q6 | 3.8 s | 1.0 s | 50 MB | | Q17 | 29.4 s | 10.9 s | 179 MB |
| Q7 | 24.7 s | 10.2 s | 176 MB | | Q18 | 55.7 s | 19.4 s | 219 MB |
| Q8 | 36.8 s | 14.4 s | 148 MB | | Q19 | 11.3 s | 3.3 s | 53 MB |
| Q9 | 77.4 s | 26.7 s | 189 MB | | Q20 | 16.7 s | 6.5 s | 149 MB |
| Q10 | 24.9 s | 8.1 s | 156 MB | | Q21 | 103.2 s | 32.0 s | 175 MB |
| Q11 | 55.0 s | 22.7 s | 147 MB | | Q22 | 16.8 s | 7.6 s | 90 MB |

Total 934.9 s of wall and 351.4 s of CPU for all 22; no query passed 259 MB
(the cgroup peak, page cache included). The twelve 0.66.0 ran (Q3–Q10, Q12, Q14,
Q18, Q19) take 346.4 s against 0.66.0's 342.7 s; side by side on one host the
two releases are within each query's run-to-run spread. Q13 (a LEFT JOIN of
9.9M customers to 69.9M orders, grouped twice) is the slowest at 171.6 s. A
plain SELECT of 5.7M rows streamed into DuckDB finishes in 225 s inside the same
cage. *What these do not show:* no other engine was run under the same cap
(DuckDB, the truth, ran with more memory); the servers did no join or aggregate
work for apitap; this is not standard SF10, and no bounded-memory claim is made
at larger scales (Q9 at SF50 was not run to completion).

## Limits

An answer read into a Python list (`.to_polars()`, `fetchall`) is
bounded by Python's objects, not the engine: past roughly 500K wide rows in
256 MB, stream it to DuckDB/pyarrow or land it with `dest=`. A Postgres view or
partitioned table as the large side is re-read per key-range pass (no page
spans). MySQL sources take no snapshot. A process killed mid-query (SIGKILL) or
a stream held open to interpreter exit can leave stage files in the temp
directory. `dest=` with a 0-row answer creates no table.

## Opt-in levers

Off by default, because they move work onto the source:
`APITAP_SQL_KEY_FILTER=1` sends a selective build's keys to Postgres as a
bitmap filter; `APITAP_SQL_PUSH_ARGS=1` has Postgres compute an aggregate's
argument as one scaled int8 (rerun without it on overflow).
