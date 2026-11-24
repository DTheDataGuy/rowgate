# GitHub Actions

## The action

```yaml
name: data-quality
on: [push, pull_request, workflow_dispatch]

jobs:
  orders:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: DTheDataGuy/rowgate@v0.2.0
        with:
          data: data/orders.parquet
          contract: contracts/orders.toml
          max-quarantine-pct: "1"
```

The job fails when a blocking check fails. The Markdown report and the plain-language explanation are added to the job summary, and the report folder (including `quarantine.parquet`) is uploaded as an artifact.

| Input | Default | Meaning |
|---|---|---|
| `data` | required | Parquet, CSV, NDJSON, JSON or DuckDB file |
| `contract` | required | contract TOML |
| `table`, `sql` | | what to read from a DuckDB file |
| `max-quarantine-pct` | `1` | block when more than this share of rows is quarantined |
| `report-dir` | `rowgate-report` | where reports are written |
| `history` | | folder for quarantine history; cache it to keep it between runs |
| `strict` | `false` | treat warnings as failures |
| `upload-artifact` | `true` | upload the report folder |
| `version` | latest | rowgate version from PyPI |

Outputs: `passed` (`true` or `false`) and `quarantined-rows`.

## Keep history between runs

```yaml
      - uses: actions/cache@v4
        with:
          path: quality/orders
          key: rowgate-orders-${{ github.run_id }}
          restore-keys: rowgate-orders-
      - uses: DTheDataGuy/rowgate@v0.2.0
        with:
          data: data/orders.parquet
          contract: contracts/orders.toml
          history: quality/orders
```

## Without the action

```yaml
      - run: pip install rowgate
      - run: rowgate check data/orders.parquet --contract contracts/orders.toml --report-dir out/ --github-summary
```
