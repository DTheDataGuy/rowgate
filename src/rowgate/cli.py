"""Command line.

    rowgate check   data.parquet --contract contract.toml [--history DIR] [--report-dir DIR]
    rowgate infer   data.parquet [-o contract.toml]
    rowgate replay  fixed.parquet --contract contract.toml [--history DIR] [--out valid.parquet]
    rowgate history DIR [--html page.html]
    rowgate dbt     contract.toml --model orders

Exit codes: 0 pass, 1 blocking failure (or any failure with ``--strict``), 2 usage error.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import __version__
from .contract import Contract
from .frames import read_file
from .history import QuarantineStore, replay
from .pipeline import validate


def _source_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("data", type=Path, help="Parquet, CSV, NDJSON, JSON or DuckDB file")
    p.add_argument("--table", help="table to read from a DuckDB file, e.g. main.orders")
    p.add_argument("--sql", help="query to run against a DuckDB file")


def _read(a: argparse.Namespace):
    try:
        return read_file(a.data, table=a.table, sql=a.sql)
    except (ValueError, FileNotFoundError) as exc:
        raise SystemExit(f"rowgate: {exc}") from exc


def _summary_target() -> Path | None:
    p = os.environ.get("GITHUB_STEP_SUMMARY")
    return Path(p) if p else None


def cmd_check(a: argparse.Namespace) -> int:
    out = validate(_read(a), a.contract, max_quarantine_pct=a.max_quarantine_pct, history=a.history,
                   source=str(a.data), report_dir=a.report_dir)
    print(out.summary())
    if not out.quarantine.is_empty():
        print("\nquarantine reasons:")
        for reason, rows in out.result.reason_counts().iter_rows():
            print(f"  {rows:>8,}  {reason}")
    if a.github_summary and (target := _summary_target()):
        with target.open("a") as fh:
            fh.write(out.markdown() + "\n")
    return 1 if (not out.passed or (a.strict and out.report.warnings)) else 0


def cmd_infer(a: argparse.Namespace) -> int:
    from .infer import infer
    text = infer(_read(a), max_categories=a.max_categories).to_toml(a.data.name)
    if a.output:
        if a.output.exists() and not a.force:
            raise SystemExit(f"rowgate: {a.output} exists; pass --force to overwrite")
        a.output.write_text(text)
        print(f"wrote draft contract to {a.output}. Review it before relying on it.", file=sys.stderr)
    else:
        print(text, end="")
    return 0


def cmd_replay(a: argparse.Namespace) -> int:
    contract = Contract.from_toml(a.contract)
    store = QuarantineStore(a.history) if a.history else None
    result = replay(contract, _read(a), store)
    print(f"replay: {result.valid.height:,} row(s) now pass, {result.quarantine.height:,} still fail")
    for reason, rows in result.reason_counts().iter_rows():
        print(f"  {rows:>8,}  {reason}")
    if a.out:
        result.valid.write_parquet(a.out)
        print(f"wrote {result.valid.height:,} fixed row(s) to {a.out}")
    if a.still_failing:
        result.quarantine.write_parquet(a.still_failing)
    if store:
        print(store.summary())
    return 0 if result.quarantine.is_empty() else 1


def cmd_history(a: argparse.Namespace) -> int:
    store = QuarantineStore(a.store)
    if store.runs().is_empty():
        raise SystemExit(f"rowgate: no history in {a.store}")
    print(store.summary())
    print()
    print(store.aging())
    if a.html:
        from .dashboard import render
        a.html.write_text(render(store))
        print(f"wrote {a.html}")
    if a.open_csv:
        store.open_issues().with_columns(__import__("polars").col("reasons").list.join(";")).write_csv(a.open_csv)
    return 0


def cmd_dbt(a: argparse.Namespace) -> int:
    from .integrations.dbt import to_schema_yml
    print(to_schema_yml(a.contract, a.model), end="")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="rowgate", description="Data contracts and quality gates for Polars.")
    p.add_argument("--version", action="version", version=f"rowgate {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("check", help="validate data against a contract")
    _source_args(c)
    c.add_argument("--contract", type=Path, required=True)
    c.add_argument("--max-quarantine-pct", type=float, default=1.0)
    c.add_argument("--report-dir", type=Path, help="write report.json, report.md and quarantine.parquet here")
    c.add_argument("--history", type=Path, help="folder that keeps quarantine history across runs")
    c.add_argument("--strict", action="store_true", help="treat warnings as failures")
    c.add_argument("--github-summary", action="store_true",
                   help="append the Markdown report to $GITHUB_STEP_SUMMARY")
    c.set_defaults(fn=cmd_check)

    i = sub.add_parser("infer", help="draft a contract from existing data")
    _source_args(i)
    i.add_argument("-o", "--output", type=Path, help="write the TOML here instead of stdout")
    i.add_argument("--force", action="store_true", help="overwrite --output if it exists")
    i.add_argument("--max-categories", type=int, default=20, help="largest value set to list as accepted")
    i.set_defaults(fn=cmd_infer)

    r = sub.add_parser("replay", help="re-check corrected quarantine rows")
    _source_args(r)
    r.add_argument("--contract", type=Path, required=True)
    r.add_argument("--history", type=Path, help="mark rows that now pass as resolved here")
    r.add_argument("--out", type=Path, help="write rows that now pass to this Parquet file")
    r.add_argument("--still-failing", type=Path, help="write rows that still fail to this Parquet file")
    r.set_defaults(fn=cmd_replay)

    h = sub.add_parser("history", help="summarise quarantine history")
    h.add_argument("store", type=Path)
    h.add_argument("--html", type=Path, help="write a single-file dashboard")
    h.add_argument("--open-csv", type=Path, help="export open rows to CSV")
    h.set_defaults(fn=cmd_history)

    d = sub.add_parser("dbt", help="print dbt tests generated from a contract")
    d.add_argument("contract", type=Path)
    d.add_argument("--model", required=True)
    d.set_defaults(fn=cmd_dbt)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
