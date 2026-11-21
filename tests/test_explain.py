import polars as pl

import rowgate as rg
from rowgate import checks as ck

C = rg.Contract(columns={
    "order_id": rg.Column(pl.Int64, required=True, unique=True),
    "amount": rg.Column(pl.Float64, min=0, max=1000),
    "currency": rg.Column(pl.Utf8, accepted=["USD", "EUR"]),
}, rules=[rg.Rule.from_sql("small_refund", "amount < 900")])


def test_explain_failed_batch():
    df = pl.DataFrame({"order_id": [1, 2, 2, 4, 5], "amount": [1.0, 2.0, 2.0, -5.0, 50_000.0],
                       "currency": ["USD", "usd", "USD", "EUR", "EUR"]})
    res = C.split(df)
    rep = rg.Gate([ck.reconciles(res), ck.quarantine_rate(res, 10)]).run(res.valid)
    e = rep.explain(res)
    assert e.headline.startswith("Blocked: 1 blocking check failed")
    text = e.text
    assert "1 row broke amount:min: value is below the minimum of 0" in text
    assert "Examples: order_id=4, amount=-5" in text
    assert "'usd' -> 'USD'" in text  # case-only mismatch hint
    assert "50 times the maximum of 1,000" in text
    assert "rule amount < 900 is false" in text
    assert "broke more than one rule" in text
    assert "### Why rows were quarantined" in e.markdown


def test_explain_passing_batch():
    df = pl.DataFrame({"order_id": [1], "amount": [1.0], "currency": ["USD"]})
    res = C.split(df)
    e = rg.explain(rg.Gate([ck.reconciles(res)]).run(res.valid), res)
    assert e.headline.startswith("Passed") and e.sections == []
