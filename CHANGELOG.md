# Changelog

## 0.1.0 (2026-10-23)

First release.

- `Contract` with per-column dtype, required, unique, accepted values, min/max and regex checks, plus row-level rules written as Polars expressions or SQL.
- `Contract.split()` separates valid rows from a quarantine frame. Each rejected row lists every rule it broke, and nothing is dropped.
- `Gate` with dataset-level checks: schema drift, uniqueness, nulls, null rate, accepted values, row count, reconciliation, quarantine rate, freshness and custom expressions. Checks are `error` (blocking) or `warn`.
- Reports as JSON, Markdown and plain text, plus `raise_for_errors()` for pipelines.
- Contracts can be loaded from TOML.
- `rowgate check` CLI for Parquet, CSV, NDJSON and JSON files, with exit codes for CI.
- Tested on Python 3.11 to 3.13 and Polars 1.0 through 2.0.
