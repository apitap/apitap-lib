# apitap.sql(mode="log_based") over three or more tables

Status 2026-11-08: design. 0.66 keeps an incremental view over one table or a
pair (`sql/view.rs`, `sql/live.rs`); a third table is refused ("an incremental
view keeps two").

## What the pair view keeps, and why a third table breaks it

The pair view is the eager-aggregate layout: `G[k]` (the GROUP BY side's group
per join key, one row per key), `S[k]` (the other side's rows and accumulators
per key), `R[g]` (the answer). A change on either side is a signed image that
moves `R` through `G`/`S` of its key alone. That works because each row's
fate (its WHERE, its group) is decided by its own columns and one key.

With a dimension (`customer` under `orders` in TPC-H Q3, `nation` under
`supplier` in Q5), a row's fate also depends on another table's row. A change
of that dimension row changes the fate of every row that points at it, and
neither `G` nor `S` knows which rows those are.

## The shape v1 keeps

The batch planner's star tree already names it: a fact `P`, a build table `B`
joined to it by key `k`, and every other table a **dimension** joined by its
primary key (its first column) to a table already in the tree. v1 keeps:

- dimensions under `B` may give GROUP BY columns and WHERE conditions;
- dimensions under `P` may give WHERE conditions only (no GROUP BY column, no
  aggregate argument, no CASE over them), so `S[k]` stays one entry per key;
- dimension **labels** fit the view's memory budget: per primary key, whether
  the dimension's own WHERE (and its chain's) passes, and its GROUP BY values.
  A filter-only dimension is a key set (Q3's `c_mktsegment = 'BUILDING'`
  customers); a dimension with text GROUP BY columns (Q10's `c_name`,
  `c_address`) needs its text in memory, so it scales with the cage, and past
  the budget the run fails naming the dimension and the bytes it needed.

Refused by name, as today: OR across tables, MIN/MAX, EXISTS/IN, CASE over
another table, GROUP BY or aggregate arguments from a dimension under `P`.

## State

`G`, `S`, `R` unchanged. Added: each dimension's label map (sorted keys +
label column, persisted in the state file under the same SHA-256 trailer) and
each dimension's change-stream position (one slot per Postgres dimension, as
each side has today; MySQL dimensions share the server's binlog position
logic per table, as sides do).

## Runs

**First run.** Every table pinned as sides are now (slot snapshot / consistent
snapshot + binlog position). Dimensions are read first, leaves up, into their
label maps (a child's label folds into its parent's: `supplier` passes iff
its row passes and its `nation` passes). Then `B` and `P` stream as today,
each row extended by its dimensions' labels before `bulk_g` / `bulk_s_rows`
(a row whose chain fails is not applied).

**Later runs.** Drain every table from its position. Images of `B` and `P`
are extended by the label maps, as at the first run, and move the state as
today. An image of a **dimension** that changes a column the view reads (its
key, a WHERE column, a GROUP BY column) means the fate of rows behind it
changed: v1 **rebuilds** the view by a full first run, logged with the
dimension and the key. Exact by construction: labels only ever reflect a
dimension at the same positions the state reflects. Dimensions are the
slow-changing tables of a star schema; a later version can move the rows
behind a changed key with an inverted index `fk -> keys` per dimension under
`B` (the rows under `P` would need `S` keyed by `(k, fk)`).

## Mechanism: virtual sides

No second view engine. The N-table query is rewritten into the pair query
the 0.66 view already keeps, over two **virtual sides**:

- `B'` = `B` plus, per dimension under it, the columns the query reads from
  that dimension, named `<alias>__<column>` (TPC-H Q7 reads `nation` twice;
  the alias keeps `n1.n_name` and `n2.n_name` apart);
- `P'` likewise for the dimensions under `P`.

Every reference to a dimension column is rewritten to its `B'`/`P'` name, a
condition between a dimension and its own side becomes a condition of the
virtual side (re-checked per image as every WHERE conjunct is), and the pair
view is planned over `B'` and `P'` with their extended schemas. A row of `B`
(read at the first run, or a change image later) becomes a row of `B'` by one
step, `extend`: gather each dimension's label columns by the row's foreign key
from the dimension's label map (sorted keys + Arrow columns, a dimension's own
WHERE already applied, its chain already folded in); a key with no label
drops the row (the inner join). The same `extend` serves the bootstrap's
batches and the windows' image batches, so the first run and a later window
cannot disagree.

## Order of work

1. Plan: accept the star tree under the rules above; refuse the rest by name.
   Unit tests for every refusal and acceptance (Q3, Q10, Q12+customer, a chain
   `supplier -> nation -> region` under `P` as filters).
2. Label maps: build from a pinned read, fold chains, persist, load; budget
   check with the named failure.
3. Extend images of `B` and `P` by labels in `apply_batch` and the bulk
   bootstrap.
4. Dimension change ⇒ rebuild.
5. Leg `e2e_sql_live_star.py`: Q3-shaped and Q10-shaped views over Postgres +
   MySQL, rounds of random traffic on every table (fact inserts/deletes/
   updates, build-table moves, dimension changes that cross the filter), each
   round equal to a recompute; RED on 0.66 (refused) and on a planted bug (a
   dimension change not rebuilt).
