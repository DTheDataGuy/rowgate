import re
from datetime import date

import polars as pl
import pytest

import rowgate as rg


@pytest.fixture
def permits() -> pl.DataFrame:
    return pl.DataFrame({
        "permit_num": ["A1", "A2", "A2", "A4", None, "A6"],
        "applied": [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 3),
                    date(2024, 2, 1), date(2024, 2, 2), date(2024, 3, 1)],
        "issued": [date(2024, 2, 1), None, None, date(2024, 1, 1), date(2024, 3, 1), date(2024, 3, 5)],
        "cost": [1200.0, 5e4, 5e4, -10.0, 300.0, 2.5e10],
        "zip": ["98101", "98122", "98122", "981", None, "98105"],
        "kind": ["Residential", "Non-Residential", "Non-Residential", "Residential", "Residential", "Industrial"],
    })


@pytest.fixture
def contract() -> rg.Contract:
    return rg.Contract(
        columns={
            "permit_num": rg.Column(pl.Utf8, required=True, unique=True),
            "applied": rg.Column(pl.Date, required=True),
            "issued": rg.Column(pl.Date),
            "cost": rg.Column(pl.Float64, min=0, max=1e9),
            "zip": rg.Column(pl.Utf8, pattern=r"^\d{5}$"),
            "kind": rg.Column(pl.Utf8, accepted=["Residential", "Non-Residential"]),
        },
        rules=[rg.Rule("issued_after_applied", pl.col("issued") >= pl.col("applied"))],
    )


def test_split_keeps_every_row(permits, contract):
    r = contract.split(permits)
    assert r.valid.height + r.quarantine.height == permits.height
    assert r.valid["permit_num"].to_list() == ["A1", "A2"]


def test_reasons_name_every_broken_rule(permits, contract):
    q = contract.split(permits).quarantine
    by_id = {row["permit_num"]: set(row[rg.REASONS]) for row in q.iter_rows(named=True)}
    assert by_id["A4"] == {"cost:min", "zip:pattern", "issued_after_applied"}
    assert by_id["A6"] == {"cost:max", "kind:accepted"}
    assert by_id[None] == {"permit_num:required"}


def test_duplicates_keep_first_occurrence(permits, contract):
    q = contract.split(permits).quarantine
    dup_rows = q.filter(pl.col(rg.REASONS).list.contains("permit_num:duplicate"))
    assert dup_rows.height == 1


def test_null_rule_result_is_a_pass(permits, contract):
    # A2 has no issued date; the ordering rule must not reject it.
    valid = contract.split(permits).valid
    assert "A2" in valid["permit_num"].to_list()


def test_empty_string_fails_required():
    c = rg.Contract({"id": rg.Column(pl.Utf8, required=True)})
    r = c.split(pl.DataFrame({"id": ["x", "  ", ""]}))
    assert r.valid.height == 1 and r.quarantine.height == 2


def test_reason_counts(permits, contract):
    counts = dict(contract.split(permits).reason_counts().iter_rows())
    assert counts["cost:min"] == 1 and counts["permit_num:duplicate"] == 1


def test_schema_drift(permits, contract):
    assert contract.schema_drift(permits) == {}
    drifted = permits.with_columns(pl.col("cost").cast(pl.Int64)).drop("zip")
    assert contract.schema_drift(drifted) == {"cost": ("Float64", "Int64"), "zip": ("String", "missing")}


def test_extra_columns_can_be_rejected(permits, contract):
    strict = rg.Contract(contract.columns, contract.rules, allow_extra_columns=False)
    assert strict.schema_drift(permits.with_columns(pl.lit(1).alias("extra"))) == {"extra": ("absent", "Int32")}


def test_missing_required_column_raises(contract, permits):
    with pytest.raises(KeyError, match="permit_num"):
        contract.split(permits.drop("permit_num"))


def test_contract_without_checks_passes_everything():
    df = pl.DataFrame({"a": [1, 2]})
    r = rg.Contract({"a": rg.Column(pl.Int64)}).split(df)
    assert r.valid.height == 2 and r.quarantine.height == 0 and rg.REASONS in r.quarantine.columns


def test_from_toml(tmp_path, permits):
    p = tmp_path / "c.toml"
    p.write_text('''
[columns.permit_num]
dtype = "str"
required = true
unique = true

[columns.cost]
dtype = "float"
min = 0

[[rules]]
name = "issued_after_applied"
sql = "issued >= applied"
''')
    c = rg.Contract.from_toml(p)
    q = c.split(permits).quarantine
    assert set(q["permit_num"].drop_nulls().to_list()) == {"A2", "A4"}


def test_bad_dtype_in_toml(tmp_path):
    p = tmp_path / "c.toml"
    p.write_text('[columns.a]\ndtype = "text"\n')
    with pytest.raises(ValueError, match="unknown dtype"):
        rg.Contract.from_toml(p)


def test_invalid_column_definitions():
    with pytest.raises(ValueError):
        rg.Column(min=5, max=1)
    with pytest.raises(re.error):
        rg.Column(pattern="(")
