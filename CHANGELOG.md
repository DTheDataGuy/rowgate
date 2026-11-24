# Changelog

## 0.2.0 (2026-11-21)

- `rowgate infer` and `rg.infer()` draft a contract from existing data: dtypes, nullability, keys, ranges, accepted values, learned patterns with the values that don't fit, and suggested date-order rules.
- `QuarantineStore` keeps quarantine history across runs. It tracks when each row was first seen, how long it has been failing and when it was fixed. `rowgate history` prints a summary, exports open rows and writes a single-file HTML dashboard.
- `rowgate replay` and `rg.replay()` re-check corrected rows and mark them resolved.
- `Report.explain()` writes a plain-language "why did this batch fail" summary with hints such as case mismatches, values far past a limit, failing values that share a shape, duplicates and schema drift.
- `rg.validate()` runs split, gate, history and explanation in one call.
- Integrations: Dagster asset checks, Prefect tasks, Airflow helpers and dbt `schema.yml` generation (`rowgate dbt`).
- A GitHub Action (`uses: DTheDataGuy/rowgate@v0.2.0`) that posts the report and explanation to the job summary and uploads the quarantine.
- pandas, DuckDB and Arrow inputs. The CLI reads DuckDB files with `--table` or `--sql`.
- `Contract.key`, `Contract.to_toml()` and SQL round-tripping for rules.
- `split` is about 13 times faster on large batches: rows are flagged lazily and reasons are built only for failing rows.
- A public benchmark against Pandera and Pointblank, and a documentation site.
- Tested on Python 3.11 to 3.14 and Polars 1.0 to 2.1.


## 0.1.0 (2026-10-23)

First release.

- `Contract` with per-column dtype, required, unique, accepted values, min/max and regex checks, plus row-level rules written as Polars expressions or SQL.
- `Contract.split()` separates valid rows from a quarantine frame. Each rejected row lists every rule it broke, and nothing is dropped.
- `Gate` with dataset-level checks: schema drift, uniqueness, nulls, null rate, accepted values, row count, reconciliation, quarantine rate, freshness and custom expressions. Checks are `error` (blocking) or `warn`.
- Reports as JSON, Markdown and plain text, plus `raise_for_errors()` for pipelines.
- Contracts can be loaded from TOML.
- `rowgate check` CLI for Parquet, CSV, NDJSON and JSON files, with exit codes for CI.
- Tested on Python 3.11 to 3.13 and Polars 1.0 through 2.0.
