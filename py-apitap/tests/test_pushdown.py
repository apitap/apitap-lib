"""The lazy reader's predicate pushdown refuses what would change the answer.

`_predicate_sql` renders a polars predicate as a SQL WHERE fragment or
returns None (the filter stays client-side). A pushed fragment may only ever
match MORE rows than polars keeps — never fewer. These are the shapes that
matched fewer (MASTER review M-46); each must come back None, and the plain
ones must still be pushed.
"""

import pytest

pl = pytest.importorskip("polars")
from apitap import _predicate_sql  # noqa: E402

COLS = {"s", "i", "n", "d", "f", "b", "t", "j"}


def sql(pred, is_my=False):
    return _predicate_sql(
        pred,
        COLS,
        str_cols={"t", "j"},
        float_cols={"d", "f"},
        float32_cols={"f"},
        bool_cols={"b"},
        is_my=is_my,
        text_cols={"j"},
        narrow_cols={"s", "i"},
    )


@pytest.mark.parametrize(
    "name,pred",
    [
        ("arithmetic on an Int16 wraps in polars", (pl.col("s") + 1) < 0),
        ("arithmetic on an Int32 wraps in polars", (pl.col("i") * 2) < 0),
        ("a Float32 column", pl.col("f") > 0.5),
        ("a Float32 literal", pl.col("d") > pl.lit(0.1, dtype=pl.Float32)),
        ("a boolean cast to a number", pl.col("n") > pl.lit(True).cast(pl.Int64)),
        ("3.7 cast to an int is 3 in polars", pl.col("n") >= pl.lit(3.7).cast(pl.Int64)),
        ("a column read as text (json)", pl.col("j") == '{"k": 1}'),
    ],
)
def test_refused(name, pred):
    assert sql(pred) is None, name


@pytest.mark.parametrize(
    "pred,frag",
    [
        (pl.col("n") > 500, '"n"'),
        (pl.col("t") == "row 7", "'row 7'"),
        ((pl.col("n") + 1) > 10, '"n"'),  # Int64 arithmetic: an overflow is an error on both servers
    ],
)
def test_still_pushed(pred, frag):
    got = sql(pred)
    assert got is not None and frag in got, got
