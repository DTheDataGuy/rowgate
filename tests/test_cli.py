import json

import polars as pl

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


def test_cli_pass_and_fail(tmp_path, capsys):
    (tmp_path / "c.toml").write_text(CONTRACT)
    good = tmp_path / "good.parquet"
    pl.DataFrame({"id": [1, 2, 3], "amount": [1.0, 2.0, 3.0]}).write_parquet(good)
    assert main(["check", str(good), "--contract", str(tmp_path / "c.toml")]) == 0

    bad = tmp_path / "bad.csv"
    pl.DataFrame({"id": [1, 2, 2], "amount": [1.0, -2.0, 3.0]}).write_csv(bad)
    out = tmp_path / "out"
    code = main(["check", str(bad), "--contract", str(tmp_path / "c.toml"), "--report-dir", str(out)])
    assert code == 1
    text = capsys.readouterr().out
    assert "amount:min" in text and "id:duplicate" in text
    assert json.loads((out / "report.json").read_text())["passed"] is False
    assert pl.read_parquet(out / "quarantine.parquet").height == 2
