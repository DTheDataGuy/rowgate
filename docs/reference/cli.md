# Command line

```text
rowgate check   DATA --contract C.toml [--history DIR] [--report-dir DIR] [--max-quarantine-pct 1]
                     [--strict] [--github-summary] [--table T | --sql Q]
rowgate infer   DATA [-o C.toml] [--force] [--max-categories 20] [--table T | --sql Q]
rowgate replay  DATA --contract C.toml [--history DIR] [--out OK.parquet] [--still-failing BAD.parquet]
rowgate history DIR [--html PAGE.html] [--open-csv OPEN.csv]
rowgate dbt     C.toml --model NAME
rowgate --version
```

`DATA` is a Parquet, CSV, NDJSON, JSON or DuckDB file. DuckDB files need `--table` or `--sql`.

| Command | Exit code |
|---|---|
| `check` | `0` pass, `1` a blocking check failed (or any check with `--strict`) |
| `replay` | `0` every row now passes, `1` some still fail |
| others | `0` on success |

Usage errors exit with `2`.

`check --report-dir` writes `report.json`, `report.md` (with the explanation and history) and `quarantine.parquet` (with `_rowgate_key`, ready to fix and replay). `--github-summary` appends the Markdown report to `$GITHUB_STEP_SUMMARY`.
