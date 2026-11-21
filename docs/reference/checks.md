# Checks

Dataset-level checks for a `Gate`. Every check takes `severity="error"` (blocks the batch) or `severity="warn"` (reported only).

| Check | Default severity | Passes when |
|---|---|---|
| `schema_matches(contract)` | error | Every contract column is present with the right dtype |
| `unique(cols)` | error | No duplicate keys (one column or a composite) |
| `not_null(cols)` | error | No nulls in the listed columns |
| `null_rate(col, max_pct)` | warn | Null share is at or below the limit |
| `accepted_values(col, values)` | error | No values outside the set |
| `row_count(min_rows, max_rows)` | error | Row count is within bounds |
| `reconciles(result)` | error | Valid + quarantined equals input |
| `quarantine_rate(result, max_pct)` | error | Quarantined share is below the limit |
| `freshness(col, max_age_days, as_of)` | warn | Newest date is recent enough |
| `expr(name, condition, max_failing)` | error | At most `max_failing` rows break the condition |
| `custom(name, fn, expectation)` | error | `fn(df)` returns `(True, observed)` |

If a check itself raises, for example because a column is missing, it's recorded as a failure with the exception message, and the other checks still run.

`rg.validate` always runs `schema_matches`, `reconciles`, `quarantine_rate` and `unique` for every unique column; pass `checks=[...]` to add more.
