# Airflow

```python
from airflow.decorators import dag, task
from rowgate.integrations.airflow import gate_file

@dag(schedule="@daily", catchup=False)
def orders():
    @task
    def check(path: str) -> dict:
        return gate_file(path, "contracts/orders.toml",
                         report_dir="/data/quality/orders/latest",
                         history="/data/quality/orders/history")

    check("/data/raw/orders/{{ ds }}.parquet")

orders()
```

`gate_file` reads the file (Parquet, CSV, NDJSON, JSON or DuckDB with `table=`), prints the report and explanation to the task log, writes the report files, and returns a small JSON-safe summary for XCom. A blocking failure raises `AirflowFailException`, which fails the task **without retries**: retrying won't fix bad data.

Already have a DataFrame? Use `gate_frame(df, contract, ...)`. Outside Airflow both helpers raise `rowgate.GateFailed`, so they're easy to unit test.
