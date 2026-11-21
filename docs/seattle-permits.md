# Case study: Seattle building permits

[`examples/seattle_permits.py`](https://github.com/DTheDataGuy/rowgate/blob/main/examples/seattle_permits.py) runs a contract over a 454-row sample of the City of Seattle's public [Building Permits](https://data.seattle.gov/Built-Environment/Building-Permits/76t5-zqzr) dataset.

```text
rowgate: PASS (447 rows)
  ok    schema_matches                    no drift
  ok    rows_reconcile                    447 valid + 7 quarantined = 454 vs 454 input
  ok    quarantine_rate                   1.54%
  ok    unique:permit_num                 0 duplicate keys
  ok    not_null:permit_num+permit_class  0 nulls
  ok    null_rate:applied_date            9.62% null
  ok    null_rate:zip                     4.7% null
  warn  freshness:applied_date            newest 2026-07-02 (110 days old)
```

The 7 quarantined rows are real problems in the source data:

| Rows | Reason | Example |
|---|---|---|
| 3 | `issued_on_or_after_applied` | `6069714-CN` was applied for on 2003-08-21 and issued on 2003-05-21 |
| 3 | `permit_num:pattern` | sub-permit numbers such as `6112685-EX-002` |
| 1 | `est_cost:max` | `6973063-CN` lists an estimated cost of $25,000,000,000 |

## What the explanation says

```text
Where to start:
  - Every permit_num value that failed the pattern has the same shape (9999999-AA-999).
    If these are valid, widen the pattern instead of fixing the rows.
```

rowgate noticed that all three pattern failures look alike, which usually means the pattern is too narrow rather than the data being wrong. That's the right call here: sub-permits are a real part of the city's numbering.

## `infer` finds the same problems without being told

Run `rowgate infer` on the same sample and the draft contract already points at two of the three issues:

```toml
[columns.permit_num]
# 0% null, 454 distinct, 3 value(s) don't match the pattern, e.g. 6112685-EX-002, 6544003-EX-002, 6478457-EX-002

[columns.est_cost]
# 13.44% null, 232 distinct, range 0 to 25,000,000,000

# [[rules]]  # held for all but 3 of 405 rows
# name = "issued_on_or_after_applied"
# sql = "issued_date >= applied_date"
```

## Three days of history

[`examples/quarantine_history.py`](https://github.com/DTheDataGuy/rowgate/blob/main/examples/quarantine_history.py) replays a small story:

1. **Day 1.** 7 rows are quarantined.
2. **Day 2.** The $25 billion typo is corrected and replayed. 6 rows are still open.
3. **Day 3.** The team decides sub-permits are valid and widens the pattern. 3 rows are fixed.

```text
quarantine history: 3 runs since 2026-11-18. 3 rows are in quarantine; 3 have been failing for 3+ days
(oldest: 3 days, first seen 2026-11-18). Most common open reasons: issued_on_or_after_applied (3).
Last run: 0 new, 3 still failing, 3 fixed.
```

The three permits issued before they were applied for are the ones that need a conversation with the data owner, and history makes that obvious. [See the dashboard](assets/history-demo.html).

## At full scale

The same approach runs in the [Seattle Permits Data Platform](https://github.com/DTheDataGuy/the-data-guys-portfolio/tree/main/data-engineering): 193,747 rows through a Polars pipeline, dbt, PySpark and Scala Spark.
