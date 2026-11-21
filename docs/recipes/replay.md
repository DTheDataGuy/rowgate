# Quarantine, fix and replay

Quarantined rows aren't a dead end. Fix them and put them back through the same contract.

## In Python

```python
store = rg.QuarantineStore("quality/orders")
out = rg.validate(df, contract, history=store)

q = store.quarantined_rows()                      # the latest run's quarantine, with _rowgate_key
fixed = q.with_columns(pl.col("currency").str.to_uppercase())
result = rg.replay(contract, fixed, store)

result.valid        # rows that now pass: load these
result.quarantine   # rows that still fail, with updated reasons
```

Rows that now pass are marked **fixed (by replay)** in the store.

## From the command line

```bash
rowgate check orders.parquet --contract orders.toml --history quality/orders --report-dir out/
# edit out/quarantine.parquet in your tool of choice, keeping the _rowgate_key column
rowgate replay fixed.parquet --contract orders.toml --history quality/orders --out recovered.parquet
```

`replay` exits with `0` when every row passes and `1` when some still fail, so it can gate a backfill job too.

## Why keep `_rowgate_key`

The key is how the store knows which open issue a fixed row belongs to. With a contract key such as `order_id`, it's just the key value. Without one, it's a hash of the original row content, which changes when you edit the row, so keep the column.
