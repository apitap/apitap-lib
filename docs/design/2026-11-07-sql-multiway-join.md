# apitap.sql(): joins of three and more tables (B2), TPC-H as written

Status: design, 2026-11-07. Goal (user): 0.66 runs the TPC-H join queries as written,
across sources, at 0.5 core / 256 MB, exact; then production hardening.

## Shape v1 keeps

The 2-table engine: the GROUP BY side is built into a key map (key → group id,
direct-address when it can), the other side streams past it and folds its aggregates;
key-range passes split a build past the budget. It stays the core.

## Shape B2 adds: one fact, the rest as lookups

- **P (probe/fact):** the table the aggregate arguments name (`lineitem` in Q3/Q5/Q10).
- **B (main build):** a table joined to P whose part holds every GROUP BY column — the
  largest candidate (it gets the multi-pass machinery). May repeat its key (Q12).
- **Dims:** every other table, joined by its *unique* key (declared PK/unique index; else
  its first column, checked at load: a repeated key fails naming the table). Each is
  loaded once into a map key → payload id (direct array over its key range when dense,
  else a hash), payload = its encoded fields that flow up + its own dims' payloads
  (snowflake composed at load: a row whose FK misses a child map is dropped there).
- **Attach:** equalities form column equivalence classes (union-find over ON and WHERE
  `a.x = b.y`, comma joins included). Each dim attaches under an attached table it shares
  a class with through its unique key, preferring B's part (where GROUP BY can use it).
  Dims under B's part act at build (filter the row, append payload to the group key);
  dims under P's part act at probe (filter the row, give check values).
- **Checks:** a class member no tree edge used must equal the rest. Within a part: compared
  on the row. Across parts (Q5's `s_nationkey = c_nationkey`): the build-side value is
  appended to the group key as a hidden field, the probe compares against the group's
  field, and the result merges groups that differ only in hidden fields (SUM/COUNT add,
  MIN/MAX fold, AVG merges sum and count).
- **GROUP BY substitution:** a GROUP BY column of P that is P's main join key is replaced by
  B's key (Q3's `l_orderkey` = `o_orderkey`).
- Semi-joins happen here, never on the server (no-heavy-pushdown): servers get only their
  own table's WHERE and projection.

## Refused in B2 (named, before any read)

GROUP BY columns from P's part other than its join key (Q7/Q8/Q9 need probe-side groups:
a later step keys accumulators by (build group, probe code)); a table joined by no
equality; OUTER joins; aggregates naming two tables.

## Next (B3)

`EXISTS` / `IN (subquery)` as a semi-join map (Q4, Q18, Q21), `HAVING`, `LIKE` (Q14, Q9),
`EXTRACT(year)` (Q7–Q9), then probe-side groups.

## Order of work

1. planner (tables, classes, attach, checks, substitution, uniqueness probe) + plan tests
   for Q3/Q5/Q10 shapes;
2. dim load + build/probe hooks + hidden-field merge + result kinds;
3. e2e on the 10g seeds (customer in PG, nation/region/supplier/part in MySQL) against
   DuckDB uncapped as truth; Q3, Q5, Q10, Q12 in the gate.

## Later (user 2026-11-07: "fokus saja yang sekarang, sql live nanti")

- `mode="log_based"` for Q12's shape (a GROUP BY side with repeated keys) and CASE in a
  view; for three or more tables (refused by name today).

## Optimization queue (after every TPC-H join query passes)

- **Scale with cores** (8c/8g measured 2026-11-07: apitap used 0.4–1.6 of 8 cores; Q3 22 s CPU
  in 19 s wall): the fold/build is one task. Shard probe batches over N workers with their
  own accumulators merged at the end (`merge_acc`), build per key range in parallel, load
  dimensions concurrently, size pipes from cores and memory. At 0.5 core N = 1: no change.
- **256 MB margin**: Q3 peaked 252 MB, Q10 255 MB in the cage; price the probe/build so every
  TPC-H query peaks ≤ ~220 MB.
- T0 regression from the probe CPU pipe cap (17 s → 24 s), ORDER BY on decimals compared as
  f64, `transfer()` carrying PG `CHAR(n)` padding into MySQL `VARCHAR`.

## B4: TPC-H Q7, Q8, Q9 (started 2026-11-07)

Status (0.5 core / 256 MB, exact vs DuckDB): Q1, Q3, Q4, Q5, Q6, Q7, Q8, Q10, Q12, Q14, Q18, Q19 pass
(S1–S5 done, plus OR-branch matching across build/probe, row estimates choose B). Next: Q9 (S6:
composite keys from catalog PKs, dimension value fields in aggregates, partsupp_10g seeded in PG
52.8M rows), then the optimization phase (−70% TPC-H wall, multi-core).

- **S1 derived table in FROM**: a `FROM (SELECT exprs AS a … FROM … WHERE …) AS t` with no
  GROUP BY/DISTINCT/LIMIT is inlined: outer items take the inner expressions (aliased by the
  outer names), WHEREs are ANDed. GROUP BY / ORDER BY name a SELECT alias (resolved to the item).
- **S2 `EXTRACT(YEAR FROM col)`**: a virtual column of the column's table, appended to each
  batch as it is read (`Side.virt`, read in `read` order, never asked of the server); a Date32's
  civil year, as Int32. Views refuse virtual columns.
- **S3 probe-side GROUP BY**: a star plan's probe part may hold GROUP BY fields; the fold keys
  accumulators by (build group, probe fields) in a second dictionary; the final key is the
  build key's kept fields then the probe's, `gpos` over both.
- **S4 residual predicates over several tables** whose columns are all GROUP BY columns: a
  filter of the final groups (Q7's nation pair).
- **S5 CASE over a probe-part dimension**: the condition evaluated where its table is loaded,
  carried as a flag field in the payload, the arm picked per probe row (Q8's `nation = 'BRAZIL'`).
- **S6 composite join keys** (`partsupp` on `(ps_partkey, ps_suppkey)`) and seeding partsupp.

## Production readiness: found while building B/B4

Status 2026-11-08 (p69/p70):

- **One snapshot for multi-pass reads — done.** `execute` exports one snapshot per Postgres URL
  (`HeldSnapshot`, held until the answer is made) and pins every side of that URL to it through
  `__apitap_snapshot=`: the key-range passes, `ctid` spans, dimensions, late reads, subqueries, and
  the MIN/MAX page parts themselves (`PgSource::fetch_one_pinned`). `APITAP_SQL_SNAPSHOT=0` turns it
  off. MySQL sides keep a statement per key range (each row read once by its key). Leg
  `e2e_sql_snapshot`: 600K lines in key order, ~60 passes, another session updating random rows in a
  loop once the parts were asked: exact; `--control` (off) lost 7,300–10,000 of 600K rows (RED, twice). The open-ended last part no longer
  needs to be taken in every pass.
- **Memory margin — done** by the optimization phase: 0.5c/256 MB peaks ≤ 212 MB (Q18) in p68.
- **`transfer()` PG `character(n)` → MySQL `CHAR(n)`** (n ≤ 255; VARCHAR past that), not
  `VARCHAR(n)` carrying the padding.
- **Text compares:** trailing spaces are ignored only for fixed-width text
  (`ArrowField::padded`: PG `bpchar`, MySQL `CHAR`; `Lit::Pad`, LIKE's pad flag); VARCHAR/TEXT
  compare exactly. Open: MySQL `_ci` and PAD SPACE collations on VARCHAR are still compared
  bytewise where apitap evaluates a condition itself (cross-table ORs, CASE flags).
- **ORDER BY on decimals — exact** (scaled i128 keys; f64 only for floats and expressions).
- **T0 regression — fixed** (p72): the probe pipe cap now tiers by the tightest gate's density
  (≤ 1/16: 8 pipes, ≤ 1/8: 6, held ≤ 96 MB both; `GATED_SCAN_PIPES`). T0 gates to 0.121 and got 2
  pipes (24.7 s); now 6: 14.5–15.7 s, peak 143 MB. TPC-H 0.5c unchanged (354.9 s, 12/12 exact,
  peak ≤ 210 MB). `APITAP_SQL_PROBE_PIPES=n` forces the probe's pipes for measuring.
- **Probe pipe floor (p75).** Without debug, forcing 4 probe pipes showed the memory model gave
  gated probes 2-3 pipes (Q3/Q8 3, Q10 2) and an ungated MySQL probe beside a small map 2 (Q12, 25%
  CPU). Now: at least 4 when the tightest gate is ≤ 1/8 or the map ≤ 32 MiB (`PROBE_MIN_PIPES`),
  and a gated (≤ 1/8) probe beside ≤ 112 MiB gets 4 even where the memory model said fewer. A heavy
  decode beside a large map keeps 2 (T2 at 4 pipes peaked 243 MB). TPC-H 0.5c 355.7 → 337.7 s, all
  exact, peak ≤ 205 MB; T0 14.4 s, T2 59.6 s / 203 MB, T3 13.6 s.
- **MySQL collations.** `ArrowField::ci` (`_ci`) and PAD SPACE (`padded`, all but 0900/nopad) come
  from the probed `COLLATE`; locally evaluated compares and LIKE fold case (ASCII fast path,
  Unicode lowercase otherwise). e2e_sql_join case 11: an OR across tables on a utf8mb4_0900_ai_ci
  column — RED on p72 (1,818 vs 6,365 rows), GREEN now.
- **Accents and GROUP BY under a collation (p76/p77).** `ArrowField::ai` (every `_ci` but
  `_as_ci`): `Coll::fold` = padding off, NFKD, combining marks dropped, lowercase, and the letters
  UCA weighs as base letters (`ß`→ss, `æ`→ae, `œ`→oe, `ø`→o, `đ`→d, `ł`→l, `ħ`→h). GROUP BY on
  such a column: groups equal under the fold are merged after the passes (`collate`, over groups,
  not rows), shown as the first value met; ORDER BY compares folded; live views merge the same way
  in `answer`. e2e_sql_join case 12 (MySQL decides each value's group): 'Brazil' = 'BRAZIL',
  'Brésil' = 'bresil', 'brazil ' apart (0900 is NO PAD) — 6 groups on p75 (RED), MySQL's 4 now.
  TPC-H 0.5c 342.7 s, 12/12 exact. Not emulated: the full UCA beyond those letters (e.g.
  ligatures other than æ/œ, Hangul/CJK weights), and text join keys compare bytewise.
- **Flaky `the_cause_wins_even_when_its_abort_is_slowest`** — 400 spans (16 let the spinning
  pipes drain the queue before the cancel reached them on a loaded host).
