"""Command line: ``rowgate check data.parquet --contract contract.toml``.

Exit codes: 0 pass, 1 blocking failure (or any failure with ``--strict``), 2 usage error.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

from . import checks as ck
from .contract import Contract
from .gate import Gate


def _read(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pl.read_parquet(path)
    if suffix == ".csv":
        return pl.read_csv(path, try_parse_dates=True)
    if suffix in (".ndjson", ".jsonl"):
        return pl.read_ndjson(path)
    if suffix == ".json":
        return pl.read_json(path)
    raise SystemExit(f"rowgate: unsupported file type {suffix!r} (use parquet, csv, ndjson or json)")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="rowgate", description="Data contracts and quality gates for Polars.")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="validate a file against a contract")
    c.add_argument("data", type=Path)
    c.add_argument("--contract", type=Path, required=True)
    c.add_argument("--max-quarantine-pct", type=float, default=1.0)
    c.add_argument("--report-dir", type=Path, help="write report.json, report.md and quarantine.parquet here")
    c.add_argument("--strict", action="store_true", help="treat warnings as failures")
    a = p.parse_args(argv)

    contract = Contract.from_toml(a.contract)
    df = _read(a.data)
    result = contract.split(df)
    gate_checks = [ck.schema_matches(contract), ck.reconciles(result),
                   ck.quarantine_rate(result, a.max_quarantine_pct)]
    gate_checks += [ck.unique(n) for n, col in contract.columns.items() if col.unique and n in df.columns]
    report = Gate(gate_checks).run(result.valid, source=str(a.data), contract=str(a.contract))

    print(report)
    if not result.quarantine.is_empty():
        print("\nquarantine reasons:")
        for reason, rows in result.reason_counts().iter_rows():
            print(f"  {rows:>8,}  {reason}")
    if a.report_dir:
        a.report_dir.mkdir(parents=True, exist_ok=True)
        report.to_json(a.report_dir / "report.json")
        report.to_markdown(a.report_dir / "report.md")
        result.quarantine.write_parquet(a.report_dir / "quarantine.parquet")
    failed = not report.passed or (a.strict and report.warnings)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
