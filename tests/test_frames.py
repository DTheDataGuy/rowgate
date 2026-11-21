import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import rowgate as rg

C = rg.Contract(columns={"id": rg.Column(pl.Int64, required=True, unique=True), "amt": rg.Column(pl.Float64, min=0)})
DATA = {"id": [1, 2, 2], "amt": [1.0, -1.0, 3.0]}


@pytest.mark.parametrize("make", [
    lambda: pl.DataFrame(DATA), lambda: pl.LazyFrame(DATA), lambda: pd.DataFrame(DATA), lambda: pa.table(DATA),
    lambda: duckdb.sql("select * from (values (1, 1.0), (2, -1.0), (2, 3.0)) t(id, amt)"),
])
def test_split_accepts_common_frames(make):
    res = C.split(make())
    assert (res.valid.height, res.quarantine.height) == (1, 2)
    valid, quarantine = res.to_pandas()
    assert isinstance(valid, pd.DataFrame) and len(quarantine) == 2


def test_read_duckdb_table_and_sql(tmp_path):
    db = tmp_path / "w.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create schema main2; create table main2.orders as select * from (values (1, 2.0)) t(id, amt)")
    con.close()
    assert rg.read_duckdb(db, table="main2.orders").height == 1
    assert rg.read_file(db, sql="select 1 as x")["x"][0] == 1
    with pytest.raises(ValueError):
        rg.read_file(db)


def test_unsupported_type():
    with pytest.raises(TypeError):
        rg.to_polars(object())
