# Getting started

## Install

```bash
pip install rowgate               # Polars is the only dependency
pip install "rowgate[pandas]"     # also accept pandas DataFrames
pip install "rowgate[duckdb]"     # also read DuckDB tables, for example dbt-duckdb models
```

rowgate supports Python 3.11 to 3.14 and Polars 1.0 or newer.

## 1. Draft a contract from data you trust

Writing a contract by hand is the step where most people give up on validation. Start from a file that's known to be good and let rowgate write the first draft:

```bash
rowgate infer orders.parquet -o contracts/orders.toml
```

Each column gets a comment explaining what was observed, so you can review the draft quickly:

```toml
key = ['order_id']

[columns.order_id]
# 0% null, 50,000 distinct
dtype = "int"
required = true
unique = true

[columns.zip]
# 4.63% null, 28 distinct
dtype = "str"
pattern = '^\d{5}$'
```

Tighten anything that describes how the data *is* rather than how it *should be*. See [Draft a contract from data](recipes/infer.md) for what `infer` looks for.

## 2. Check a batch

```bash
rowgate check new_orders.parquet --contract contracts/orders.toml --report-dir out/
```

The exit code is `0` when the batch can load and `1` when a blocking check failed, so the same command works in cron, CI and orchestrators. `out/` gets `report.json`, `report.md` and `quarantine.parquet`.

## 3. Use it in Python

```python
import rowgate as rg

out = rg.validate(df, "contracts/orders.toml", max_quarantine_pct=2, history="quality/orders")
out.quarantine.write_parquet("quarantine/orders.parquet")   # keep the evidence
out.raise_for_errors()                                       # stop here if the batch is blocked
out.valid.write_parquet("silver/orders.parquet")
```

`validate` is the whole flow in one call. When you want finer control, use the pieces directly:

```python
result = contract.split(df)                   # valid + quarantine
report = rg.Gate([...checks...]).run(result.valid)
print(report.explain(result))                 # the plain-language summary
```

## 4. Put it in CI

```yaml
- uses: DTheDataGuy/rowgate@v0.2.0
  with:
    data: data/orders.parquet
    contract: contracts/orders.toml
```

The report and explanation land in the job summary. More in [GitHub Actions](recipes/github-actions.md).

## Next

* [Concepts](concepts.md): contracts, the split, the gate and history
* [Quarantine, fix and replay](recipes/replay.md)
* [Track failures across runs](recipes/history.md)
