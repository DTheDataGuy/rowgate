# rowgate

**Data contracts and quality gates for [Polars](https://pola.rs/).** Quarantine bad rows with reasons, block bad batches, and write reports people can read.

```bash
pip install rowgate
```

Most validation tools answer one question: did this DataFrame pass? A production pipeline needs three more answers. **Which rows failed, why did each one fail, and is it safe to load the rest?** rowgate is built around those questions.

<div class="grid cards" markdown>

-   **Split, don't drop**

    `contract.split(df)` returns valid rows and a quarantine frame. Every quarantined row lists every rule it broke, and `valid + quarantined = input` always holds.

-   **Gate the batch**

    Schema drift, duplicate keys, null rates, freshness and quarantine rate. `error` checks block the load, `warn` checks are reported.

-   **Explain the failure**

    A plain-language note with real example rows and where to start fixing, in the terminal, in Markdown and in your CI summary.

-   **Remember across runs**

    Quarantine history tells you which rows have been failing for days, which were fixed, and lets you replay corrected rows.

</div>

## Thirty seconds

```bash
rowgate infer orders.parquet -o orders.toml     # draft a contract from data you trust
rowgate check orders.parquet --contract orders.toml --max-quarantine-pct 10
```

```text
rowgate: FAIL (2 rows)
  ok    schema_matches   no drift
  ok    rows_reconcile   2 valid + 3 quarantined = 5 vs 5 input
  FAIL  quarantine_rate  60.0%
  ok    unique:order_id  0 duplicate keys

Blocked: 1 blocking check failed, so this batch should not be loaded (3 of 5 rows quarantined).

Why rows were quarantined:
  - 1 row broke amount:min: value is below the minimum of 0. Examples: order_id=1004, amount=-20
  - 1 row broke currency:accepted: value is not one of 'USD', 'EUR', 'GBP'. Examples: order_id=1005, currency=usd
  - 1 row broke customer:required: value is missing or blank. Examples: order_id=1004, customer=null
  - 1 row broke order_id:duplicate: key repeats a value seen earlier in the batch. Examples: order_id=1002

Where to start:
  - currency values differ from the accepted list only by case or spacing ('usd' -> 'USD'). Normalising upstream would clear these rows.
  - order_id repeats. The first occurrence was loaded; check whether the source sent the batch twice or whether the key is not unique.
```

## Where it runs

rowgate's only required dependency is Polars. It reads pandas, PyArrow and DuckDB input, and ships ready-made pieces for [GitHub Actions](recipes/github-actions.md), [dbt](recipes/dbt.md), [Dagster](recipes/dagster.md), [Airflow](recipes/airflow.md) and [Prefect](recipes/prefect.md).

## On real data

Run against the City of Seattle's public building permits, rowgate found [permits issued before they were applied for, sub-permit numbers outside the documented format, and an estimated cost of $25,000,000,000](seattle-permits.md).

---

Built by [Dei'Marlon Scisney](https://github.com/DTheDataGuy) at [The Data Guys™](https://www.thedataguys.org/). If rowgate saved you from writing another validation script, a [star on GitHub](https://github.com/DTheDataGuy/rowgate) helps other people find it.
