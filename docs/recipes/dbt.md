# dbt

One contract, two places to enforce it.

## Generate dbt tests from the contract

```bash
rowgate dbt contracts/orders.toml --model orders > models/orders_rowgate.yml
```

```yaml
models:
  - name: orders
    data_tests:
      - dbt_utils.expression_is_true:
          name: "orders_shipped_on_or_after_ordered"
          arguments:
            expression: "shipped_date >= ordered_date"
    columns:
      - name: order_id
        data_tests:
          - not_null
          - unique
      - name: amount
        data_tests:
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
```

`required`, `unique` and `accepted` map to dbt's built-in tests. Ranges and row rules use [`dbt_utils`](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/), so add it to `packages.yml`. Regex patterns have no portable dbt test and are listed as comments.

## Check the model after `dbt build`

dbt tests tell you a test failed. rowgate tells you **which rows, why, and since when**:

```bash
dbt build
rowgate check target/dev.duckdb --table main.orders --contract contracts/orders.toml --history quality/orders
```

```python
from rowgate.integrations.dbt import check_model
out = check_model("target/dev.duckdb", "main.orders", "contracts/orders.toml", history="quality/orders")
```

Reading DuckDB needs `pip install "rowgate[duckdb]"`. For other warehouses, read the model into Polars (for example with `pl.read_database`) and call `rg.validate`.
