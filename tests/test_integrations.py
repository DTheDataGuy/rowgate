import polars as pl
import pytest

import rowgate as rg
from rowgate.integrations import airflow as rg_airflow
from rowgate.integrations.dbt import check_model, to_schema_yml

C = rg.Contract(columns={"id": rg.Column(pl.Int64, required=True, unique=True),
                         "amt": rg.Column(pl.Float64, min=0, max=10),
                         "s": rg.Column(pl.Utf8, accepted=["a", "b"], pattern="^[ab]$")},
                rules=[rg.Rule.from_sql("amt_small", "amt < 9")])
GOOD = pl.DataFrame({"id": [1, 2], "amt": [1.0, 2.0], "s": ["a", "b"]})
BAD = pl.DataFrame({"id": [1, 2], "amt": [1.0, -2.0], "s": ["a", "b"]})


def test_dbt_schema_yml():
    y = to_schema_yml(C, "orders")
    assert "  - name: orders" in y and "- not_null" in y and "- unique" in y
    assert 'values: ["a", "b"]' in y and "dbt_utils.accepted_range" in y and "max_value: 10" in y
    assert 'expression: "amt < 9"' in y and "# pattern ^[ab]$ is enforced by rowgate" in y


def test_dbt_check_model(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    db = tmp_path / "dev.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create table orders as select 1::bigint id, 1.0::double amt, 'a' s")
    con.close()
    assert check_model(db, "orders", C).passed


def test_airflow_helpers_without_airflow(tmp_path):
    assert rg_airflow.gate_frame(GOOD, C)["passed"] is True
    with pytest.raises(rg.GateFailed):
        rg_airflow.gate_frame(BAD, C, max_quarantine_pct=1)
    p = tmp_path / "x.parquet"
    GOOD.write_parquet(p)
    assert rg_airflow.gate_file(p, C, report_dir=tmp_path / "r")["valid_rows"] == 2


def test_dagster_asset_check():
    dg = pytest.importorskip("dagster")
    from rowgate.integrations.dagster import contract_check

    @dg.asset
    def orders() -> pl.DataFrame:
        return BAD

    check = contract_check(orders, C)
    res = dg.materialize([orders, check], raise_on_error=False)
    ev = res.get_asset_check_evaluations()[0]
    assert ev.passed is False and "Blocked" in ev.metadata["report"].value


def test_prefect_task():
    pytest.importorskip("prefect")
    from prefect import flow

    from rowgate.integrations.prefect import rowgate_task

    @flow
    def good_flow():
        return rowgate_task(GOOD, C)

    assert good_flow().height == 2
