# Draft a contract from data

```bash
rowgate infer orders.parquet -o contracts/orders.toml
```

```python
draft = rg.infer(df)          # an Inferred object
draft.contract                # a Contract the data passes today, mostly
print(draft.to_toml("orders.parquet"))
```

## What it looks for

| Finding | Becomes |
|---|---|
| No nulls or blanks | `required = true` |
| ID-like name (`id`, `*_id`, `*_num`, `*_code`, `sku`...) with no repeats | `unique = true` and the contract `key` |
| Text with 20 or fewer distinct values, each used at least twice on average | `accepted = [...]` |
| Numbers that are never negative | `min = 0` |
| Text where at least 99% of values share one shape | `pattern`, for example `^\d{7}-[A-Z]{2}$` |
| Two date columns where one is on or after the other for at least 99% of rows | a suggested rule, commented out |

Every column also gets a comment with its null share, distinct count and range, so you can tighten bounds without opening the data.

## Outliers are reported, not hidden

When a pattern covers 99% of values, the rest are named in the comment:

```toml
[columns.permit_num]
# 0% null, 454 distinct, 3 value(s) don't match the pattern, e.g. 6112685-EX-002, 6544003-EX-002, 6478457-EX-002
pattern = '^\d{7}-[A-Z]{2}$'
```

That's a draft contract that already found something worth asking about. Decide whether those values are errors or whether the pattern should be wider.

## Things to review

* `accepted` lists describe values seen so far. Add the ones you expect.
* `min = 0` is a guess for counts and money. Add a `max` from domain knowledge; the comment shows the observed range.
* Suggested rules are commented out on purpose. Uncomment the ones that are real business rules.
* Large files are sampled (200,000 rows by default) to keep inference fast.
