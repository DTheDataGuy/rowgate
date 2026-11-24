import json
import tomllib

import duckdb
import polars as pl
import pytest

import rowgate as rg
from rowgate.cli import main

CONTRACT = '''
[columns.id]
dtype = "int"
required = true
unique = true

[columns.amount]
dtype = "float"
min = 0
'''


def test_validate_end_to_end(tmp_path):
    (tmp_path / "c.toml").write_text(CONTRACT)
    out = rg.validate(pl.DataFrame({"id": [1, 2, 2], "amount": [1.0, -2.0, 3.0]}), tmp_path / "c.toml",
                      history=tmp_path / "h", report_dir=tmp_path / "r")
    assert not out.passed and out.run.new == 2
    md = (tmp_path / "r" / "report.md").read_text()
    assert "## Why" in md and "## History" in md
    assert rg.KEY in pl.read_parquet(tmp_path / "r" / "quarantine.parquet").columns


def test_cli_infer_check_replay_history_dbt(tmp_path, capsys, monkeypatch):
    data = tmp_path / "orders.parquet"
    pl.DataFrame({"order_id": list(range(1, 41)), "amount": [float(i) for i in range(40)]}).write_parquet(data)
    contract = tmp_path / "orders.toml"
    assert main(["infer", str(data), "-o", str(contract)]) == 0
    assert tomllib.loads(contract.read_text())["columns"]["order_id"]["unique"] is True
    with pytest.raises(SystemExit):  # never overwrite a reviewed contract by accident
        main(["infer", str(data), "-o", str(contract)])

    bad = tmp_path / "bad.parquet"
    pl.DataFrame({"order_id": [1, 2, 3], "amount": [1.0, -2.0, 3.0]}).write_parquet(bad)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    hist = tmp_path / "hist"
    assert main(["check", str(bad), "--contract", str(contract), "--history", str(hist), "--github-summary"]) == 1
    assert "amount:min" in summary.read_text()

    q = rg.QuarantineStore(hist).quarantined_rows()
    fixed = tmp_path / "fixed.parquet"
    q.with_columns(pl.col("amount").abs()).write_parquet(fixed)
    assert main(["replay", str(fixed), "--contract", str(contract), "--history", str(hist),
                 "--out", str(tmp_path / "ok.parquet")]) == 0
    assert "Nothing is in quarantine" in capsys.readouterr().out

    assert main(["history", str(hist), "--html", str(tmp_path / "h.html")]) == 0
    assert (tmp_path / "h.html").read_text().startswith("<!doctype html>")
    assert main(["dbt", str(contract), "--model", "orders"]) == 0
    assert "- unique" in capsys.readouterr().out


def test_cli_reads_duckdb(tmp_path, capsys):
    db = tmp_path / "dev.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create table orders as select * from (values (1, 2.0::double), (2, 3.0::double)) t(id, amount)")
    con.close()
    (tmp_path / "c.toml").write_text(CONTRACT.replace('dtype = "int"', 'dtype = "int32"'))
    assert main(["check", str(db), "--table", "orders", "--contract", str(tmp_path / "c.toml")]) == 0
    assert json.dumps(capsys.readouterr().out)
