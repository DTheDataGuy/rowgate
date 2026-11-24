# Prefect

```python
from prefect import flow
from rowgate.integrations.prefect import rowgate_task

@flow
def load_orders():
    df = extract()
    valid = rowgate_task(df, "contracts/orders.toml", history="quality/orders")
    load(valid)
```

`rowgate_task` returns the valid rows. It publishes the Markdown report as a Prefect artifact (key `rowgate-report`) and raises `rowgate.GateFailed` when a blocking check fails, which marks the task and flow as failed. To keep the quarantine, pass `quarantine_path="quarantine/orders.parquet"`.

Tested with Prefect 3.
