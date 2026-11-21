# Concepts

rowgate has four ideas. Each one exists because of a way pipelines usually go wrong.

## Contract

A contract describes a valid row: column types, which values are required, unique, in a list, inside a range or matching a pattern, plus row-level rules such as `issued_date >= applied_date`. Write it in Python or keep it next to the pipeline as TOML. See [Contracts](reference/contracts.md).

Nulls only fail `required`. Range, list and pattern checks skip nulls, the same way SQL constraints behave, and a rule that evaluates to null passes. A permit that hasn't been issued yet isn't rejected by a date-ordering rule.

## Split

`contract.split(df)` separates rows that meet the contract from rows that don't.

* **Nothing is dropped.** `valid + quarantine = input`, always. A pipeline that silently deletes rows makes its own totals impossible to audit.
* **Every reason is listed.** A row that breaks three rules carries all three in `_rowgate_reasons`, so fixing one doesn't reveal the next problem a day later.
* **Duplicates keep the first occurrence.** Later repeats are quarantined with `column:duplicate`.

## Gate

A `Gate` runs dataset-level checks on the batch: schema drift, uniqueness, null rate, row count, freshness, reconciliation and quarantine rate. `error` checks block the load and `warn` checks are reported. The result is a `Report` you can print, save as JSON or Markdown, or turn into an exception with `raise_for_errors()`.

`report.explain(result)` turns the report and the quarantine into a short note: what blocked the load, which rules rows broke with real examples, and where to start. It recognises common causes, such as values that differ from the accepted list only by case, a number far past its maximum that's probably a typo, and pattern failures that all share one shape (which usually means the pattern is too narrow).

## History

A `QuarantineStore` is a folder of Parquet files that remembers quarantined rows across runs. Each run is compared with the last:

* **new**: rows failing for the first time
* **still failing**: rows that were already open
* **fixed**: open rows that now pass, either in a later batch or through `replay`

Rows are matched by the contract `key`, or by the first `unique` column. Without a key they're matched by content, which means a corrected row can only be marked fixed through `replay`.

```text
quarantine history: 3 runs since 2026-11-18. 3 rows are in quarantine; 3 have been failing for 3+ days
(oldest: 3 days, first seen 2026-11-18). Most common open reasons: issued_on_or_after_applied (3).
Last run: 0 new, 3 still failing, 3 fixed.
```
