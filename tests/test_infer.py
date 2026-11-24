import tomllib
from datetime import date

import polars as pl

import rowgate as rg
from rowgate.infer import infer, learn_pattern


def frame() -> pl.DataFrame:
    n = 60
    return pl.DataFrame({
        "order_id": list(range(1000, 1000 + n)),
        "status": ["open", "closed", "void"] * (n // 3),
        "zip": [f"981{i % 90:02d}" for i in range(n)],
        "amount": [float(i) for i in range(n)],
        "note": [None if i % 2 else "x" for i in range(n)],
        "ordered": [date(2026, 1, 1 + i % 28) for i in range(n)],
        "shipped": [date(2026, 2, 1 + i % 28) for i in range(n)],
    })


def test_infer_proposes_a_contract_the_data_passes():
    df = frame()
    inf = infer(df)
    c = inf.contract
    assert c.columns["order_id"].unique and c.columns["order_id"].required
    assert c.key == ["order_id"]
    assert c.columns["status"].accepted == ["closed", "open", "void"]
    assert c.columns["zip"].pattern == r"^\d{5}$"
    assert c.columns["amount"].min == 0
    assert not c.columns["note"].required
    assert c.split(df).quarantine.is_empty()


def test_infer_toml_round_trips_and_suggests_rules():
    text = infer(frame()).to_toml("orders.parquet")
    assert "# Draft contract written by `rowgate infer` from orders.parquet" in text
    assert '# sql = "shipped >= ordered"' in text
    c = rg.Contract.from_dict(tomllib.loads(text))
    assert c.columns["zip"].pattern == r"^\d{5}$"
    assert c.split(frame()).quarantine.is_empty()


def test_learn_pattern_reports_outliers():
    vals = pl.Series([f"{6000000 + i}-CN" for i in range(400)] + ["6112685-EX-002"])
    pattern, misses, examples = learn_pattern(vals)
    assert pattern == r"^\d{7}-[A-Z]{2}$"
    assert misses == 1 and examples == ["6112685-EX-002"]
    assert learn_pattern(pl.Series(["a b", "1-2", "Q"])) is None


def test_infer_accepts_pandas():
    inf = rg.infer(frame().to_pandas())
    assert "order_id" in inf.contract.columns
