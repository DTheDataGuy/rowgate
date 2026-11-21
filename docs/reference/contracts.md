# Contracts

## Columns

| Option | Meaning | Nulls |
|---|---|---|
| `dtype` | Expected Polars dtype, checked by `schema_matches` | n/a |
| `required` | Value must be present. Blank strings count as missing | fail |
| `unique` | First occurrence kept; later duplicates quarantined | skipped |
| `accepted` | Value must be in the list | skipped |
| `min`, `max` | Inclusive bounds | skipped |
| `pattern` | Regex the value must match | skipped |

Contract options: `allow_extra_columns = false` treats unexpected columns as schema drift, and `key = ["col", ...]` names the columns that identify a row across runs (defaults to the first `unique` column).

## Rules

Row-level rules are true for valid rows. A rule that evaluates to null passes.

```python
rg.Rule("issued_on_or_after_applied", pl.col("issued_date") >= pl.col("applied_date"))
rg.Rule.from_sql("issued_on_or_after_applied", "issued_date >= applied_date")
```

## TOML

```toml
key = ["permit_num"]

[columns.permit_num]
dtype = "str"
required = true
unique = true
pattern = '^\d{6,7}-[A-Z]{2}$'

[columns.est_cost]
dtype = "float"
min = 0
max = 5_000_000_000

[[rules]]
name = "issued_on_or_after_applied"
sql = "issued_date >= applied_date"
description = "a permit can't be issued before it was applied for"
```

```python
contract = rg.Contract.from_toml("permits_contract.toml")
print(contract.to_toml())     # write it back out; SQL rules round-trip
```

Dtype names: `str`, `int`, `int32`, `float`, `float32`, `bool`, `date`, `datetime`, `decimal`. `datetime` and `decimal` without parameters match any time unit, time zone or precision.

## Quarantine reasons

| Reason | Meaning |
|---|---|
| `col:required` | value is missing or blank |
| `col:accepted` | value is not in the list |
| `col:min`, `col:max` | value is outside the bound |
| `col:pattern` | value doesn't match the regex |
| `col:duplicate` | key repeats a value seen earlier in the batch |
| `rule_name` | the rule is false for this row |
