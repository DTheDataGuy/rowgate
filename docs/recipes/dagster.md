# Dagster

A contract becomes a blocking [asset check](https://docs.dagster.io/guides/test/asset-checks). Downstream assets don't materialise when it fails.

```python
import dagster as dg
import polars as pl
from rowgate.integrations.dagster import contract_check

@dg.asset
def orders() -> pl.DataFrame:
    return pl.read_parquet("raw/orders.parquet")

orders_contract = contract_check(orders, "contracts/orders.toml",
                                 max_quarantine_pct=1, history="quality/orders")

defs = dg.Definitions(assets=[orders], asset_checks=[orders_contract])
```

The check records `input_rows`, `valid_rows`, `quarantined_rows`, `quarantine_pct` and a Markdown `report` (with the explanation) as metadata, so the reason a batch was blocked is visible in the Dagster UI. Pass `blocking=False` to report without stopping downstream assets.

Tested with Dagster 1.13.
