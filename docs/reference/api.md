# Python API

```python
import rowgate as rg
```

## The whole flow

| Call | Returns |
|---|---|
| `rg.validate(data, contract, *, max_quarantine_pct=1.0, checks=(), history=None, source=None, report_dir=None)` | `Outcome` with `.valid`, `.quarantine`, `.report`, `.explanation`, `.run`, `.passed`, `.raise_for_errors()`, `.summary()`, `.markdown()`, `.write(dir)` |

## Contracts

| Call | Returns |
|---|---|
| `rg.Contract(columns, rules=[], allow_extra_columns=True, key=None)` | a contract |
| `rg.Contract.from_toml(path)`, `.from_dict(d)`, `.to_toml()` | load and save |
| `contract.split(data)` | `SplitResult` with `.valid`, `.quarantine`, `.input_rows`, `.quarantine_rate`, `.reason_counts()`, `.to_pandas()` |
| `contract.schema_drift(data)` | `{column: (expected, actual)}` |
| `contract.describe_reason("amount:min")` | plain-English meaning |
| `rg.Column(dtype, required, unique, accepted, min, max, pattern)` | column expectations |
| `rg.Rule(name, expr, description)`, `rg.Rule.from_sql(name, sql)` | row rules |
| `rg.infer(data, max_categories=20)` | `Inferred` with `.contract` and `.to_toml(source)` |

## Gates and reports

| Call | Returns |
|---|---|
| `rg.Gate(checks).run(data, **meta)` | `Report` |
| `report.passed`, `.errors`, `.warnings`, `.raise_for_errors()` | status |
| `report.to_json(path)`, `.to_markdown(path, explanation=None)`, `str(report)` | output |
| `report.explain(result)`, `rg.explain(report, result, store=None)` | `Explanation` with `.headline`, `.text`, `.markdown` |

## History

| Call | Returns |
|---|---|
| `rg.QuarantineStore(path, key=None)` | a store |
| `store.record(result, at=None, source=None, passed=None)` | `RunRecord` with `new`, `recurring`, `resolved`, `open_after` |
| `store.summary()`, `.open_issues()`, `.aging()`, `.runs()`, `.issues()` | history views |
| `store.quarantined_rows(run_id=None)` | the full quarantined rows of a run |
| `store.resolve(keys, by="manual")` | number of rows marked fixed |
| `rg.replay(contract, fixed, store=None)` | `SplitResult` for the corrected rows |

## Frames

| Call | Returns |
|---|---|
| `rg.to_polars(data)` | Polars DataFrame from Polars, pandas, PyArrow, DuckDB or any Arrow stream |
| `rg.read_duckdb(db, table=None, sql=None)` | Polars DataFrame |
| `rg.read_file(path, table=None, sql=None)` | Polars DataFrame from any supported file |

Constants: `rg.REASONS` (`"_rowgate_reasons"`) and `rg.KEY` (`"_rowgate_key"`).
