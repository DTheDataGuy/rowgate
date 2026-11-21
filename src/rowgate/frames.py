"""Accept the DataFrames people already have.

rowgate evaluates everything in Polars. These helpers turn pandas, PyArrow, DuckDB and
lazy Polars inputs into a Polars DataFrame, and read tables straight out of DuckDB.
pandas and DuckDB are optional: install ``rowgate[pandas]`` or ``rowgate[duckdb]``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl


def to_polars(data: Any) -> pl.DataFrame:
    """Convert a supported frame to a Polars DataFrame without copying when possible.

    Supported: Polars DataFrame or LazyFrame, pandas DataFrame, PyArrow Table or
    RecordBatch, DuckDB relation, and anything that exposes the Arrow PyCapsule stream
    interface (``__arrow_c_stream__``).
    """
    if isinstance(data, pl.DataFrame):
        return data
    if isinstance(data, pl.LazyFrame):
        return data.collect()
    mod = type(data).__module__.split(".")[0]
    if mod == "duckdb" and hasattr(data, "pl"):
        out = data.pl()
        return out.collect() if isinstance(out, pl.LazyFrame) else out
    if mod == "pandas":
        return pl.from_pandas(data)
    if mod == "pyarrow":
        return pl.from_arrow(data)  # type: ignore[return-value]
    if hasattr(data, "__arrow_c_stream__"):
        return pl.DataFrame(data)
    raise TypeError(f"rowgate can't read {type(data).__name__}; pass a Polars, pandas, PyArrow or DuckDB frame")


def read_duckdb(database: str | Path | Any, table: str | None = None, sql: str | None = None) -> pl.DataFrame:
    """Read a table or query result from DuckDB.

    ``database`` is a path to a ``.duckdb`` file or an open ``duckdb`` connection. Pass
    either ``table`` or ``sql``. This is how rowgate checks the output of a dbt-duckdb model.
    """
    if (table is None) == (sql is None):
        raise ValueError("pass exactly one of table= or sql=")
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError("reading DuckDB needs the duckdb package: pip install 'rowgate[duckdb]'") from exc
    con = database if hasattr(database, "sql") else duckdb.connect(str(database), read_only=True)
    try:
        query = sql if sql is not None else f'select * from {_quote_ident(table)}'  # type: ignore[arg-type]
        return to_polars(con.sql(query))
    finally:
        if con is not database:
            con.close()


def _quote_ident(name: str) -> str:
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def read_file(path: str | Path, table: str | None = None, sql: str | None = None) -> pl.DataFrame:
    """Read Parquet, CSV, NDJSON, JSON or a DuckDB database file into Polars."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in (".duckdb", ".ddb", ".db"):
        if table is None and sql is None:
            raise ValueError(f"{path.name} is a DuckDB file; pass --table or --sql to choose what to check")
        return read_duckdb(path, table=table, sql=sql)
    if table is not None or sql is not None:
        raise ValueError("--table and --sql only apply to DuckDB files")
    if suffix == ".parquet":
        return pl.read_parquet(path)
    if suffix == ".csv":
        return pl.read_csv(path, try_parse_dates=True)
    if suffix in (".ndjson", ".jsonl"):
        return pl.read_ndjson(path)
    if suffix == ".json":
        return pl.read_json(path)
    raise ValueError(f"unsupported file type {suffix!r} (use parquet, csv, ndjson, json or duckdb)")
