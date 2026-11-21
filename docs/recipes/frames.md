# pandas, DuckDB and Arrow

rowgate evaluates everything in Polars, which is why it's fast and light. It accepts the frames you already have and converts them on the way in, without copying when Arrow allows it.

```python
import pandas as pd, duckdb, pyarrow as pa
import rowgate as rg

contract.split(pd.read_csv("orders.csv"))                 # pandas
contract.split(duckdb.sql("select * from 'orders/*.parquet'"))  # a DuckDB relation
contract.split(pa.parquet.read_table("orders.parquet"))   # PyArrow
contract.split(pl.scan_parquet("orders/*.parquet"))       # a Polars LazyFrame
```

Results are Polars frames. Need pandas back?

```python
valid, quarantine = result.to_pandas()
```

## Read DuckDB tables

```python
df = rg.read_duckdb("warehouse.duckdb", table="main.orders")
df = rg.read_duckdb(con, sql="select * from orders where day = current_date")
```

```bash
rowgate check warehouse.duckdb --table main.orders --contract orders.toml
```

Install with `pip install "rowgate[pandas]"` or `pip install "rowgate[duckdb]"`, or `rowgate[all]` for both.
