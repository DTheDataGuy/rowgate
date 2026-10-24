import json
from datetime import date

import polars as pl
import pytest

import rowgate as rg
from rowgate import checks as ck


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({
        "id": [1, 2, 3, 3],
        "zip": ["98101", None, None, "98105"],
        "status": ["open", "closed", "open", "void"],
        "updated": [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3), date(2026, 10, 3)],
    })


def test_gate_reports_each_check(df):
    report = rg.Gate([
        ck.unique("id"),
        ck.not_null("id"),
        ck.null_rate("zip", max_pct=10),
        ck.accepted_values("status", ["open", "closed"]),
        ck.freshness("updated", max_age_days=30, as_of=date(2026, 10, 20)),
        ck.row_count(min_rows=1),
    ]).run(df)
    res = {r.name: r for r in report.results}
    assert not res["unique:id"].passed and res["unique:id"].observed == "1 duplicate keys"
    assert res["not_null:id"].passed
    assert not res["null_rate:zip"].passed and res["null_rate:zip"].severity == "warn"
    assert res["accepted_values:status"].observed == "unexpected: void"
    assert res["freshness:updated"].passed
    assert len(report.errors) == 2 and len(report.warnings) == 1 and not report.passed


def test_raise_for_errors(df):
    with pytest.raises(rg.GateFailed, match="unique:id"):
        rg.Gate([ck.unique("id")]).run(df).raise_for_errors()
    assert rg.Gate([ck.not_null("id")]).run(df).raise_for_errors().passed


def test_warnings_do_not_block(df):
    assert rg.Gate([ck.null_rate("zip", max_pct=0)]).run(df).passed


def test_broken_check_is_recorded_not_raised(df):
    report = rg.Gate([ck.not_null("missing_column"), ck.row_count()]).run(df)
    assert not report.results[0].passed and "raised" in report.results[0].observed
    assert report.results[1].passed


def test_split_aware_checks():
    c = rg.Contract({"v": rg.Column(pl.Int64, min=0)})
    result = c.split(pl.DataFrame({"v": [1, -1, 2, 3]}))
    report = rg.Gate([ck.reconciles(result), ck.quarantine_rate(result, max_pct=10)]).run(result.valid)
    assert report.results[0].passed
    assert not report.results[1].passed and report.results[1].observed == "25.0%"


def test_expr_check_allows_tolerance(df):
    strict = rg.Gate([ck.expr("id_small", pl.col("id") < 3)]).run(df)
    loose = rg.Gate([ck.expr("id_small", pl.col("id") < 3, max_failing=2)]).run(df)
    assert not strict.passed and loose.passed


def test_reports_render(df, tmp_path):
    report = rg.Gate([ck.unique("id"), ck.null_rate("zip", 10)]).run(df, source="unit-test")
    data = json.loads(report.to_json(tmp_path / "r.json"))
    assert data["passed"] is False and data["meta"] == {"source": "unit-test"} and len(data["results"]) == 2
    md = report.to_markdown(tmp_path / "r.md")
    assert md.startswith("# Data-quality report") and "**FAIL**" in md
    assert "FAIL" in str(report)


def test_duplicate_check_names_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        rg.Gate([ck.unique("a"), ck.unique("a")])


def test_bad_severity_rejected():
    with pytest.raises(ValueError):
        ck.unique("a", severity="info")
