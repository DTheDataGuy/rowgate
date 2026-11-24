"""One call for the whole flow: split, gate, remember, explain and write reports.

Integrations (CLI, GitHub Action, Airflow, Dagster, Prefect) all go through
:func:`validate`, so a contract behaves the same wherever it runs.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from . import checks as ck
from .contract import Contract, SplitResult
from .explain import Explanation, explain
from .frames import to_polars
from .gate import Gate, GateFailed, Report
from .history import QuarantineStore, RunRecord


@dataclass
class Outcome:
    """Everything :func:`validate` produced."""

    result: SplitResult
    report: Report
    explanation: Explanation
    run: RunRecord | None = None
    store: QuarantineStore | None = None

    @property
    def passed(self) -> bool:
        return self.report.passed

    @property
    def valid(self) -> pl.DataFrame:
        return self.result.valid

    @property
    def quarantine(self) -> pl.DataFrame:
        return self.result.quarantine

    def raise_for_errors(self) -> Outcome:
        if not self.report.passed:
            raise GateFailed(self.report)
        return self

    def summary(self) -> str:
        """Terminal output: the report, then the explanation and history when relevant."""
        parts = [str(self.report)]
        if self.result.quarantine.height or self.report.errors or self.report.warnings:
            parts.append(self.explanation.text)
        if self.run is not None:
            parts.append(str(self.run))
        return "\n\n".join(parts)

    def markdown(self, title: str = "rowgate data-quality report") -> str:
        md = self.report.to_markdown(title=title, explanation=self.explanation)
        if self.store is not None and self.run is not None:
            md += "\n## History\n\n" + self.store.summary() + "\n"
        return md

    def write(self, directory: str | Path) -> Path:
        """Write ``report.json``, ``report.md`` and ``quarantine.parquet``."""
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)
        self.report.to_json(d / "report.json")
        (d / "report.md").write_text(self.markdown())
        q = self.result.quarantine
        if self.store is not None or (self.result.contract and self.result.contract.key_columns):
            q = (self.store or QuarantineStore(d)).with_keys(q, self.result.contract)
        q.write_parquet(d / "quarantine.parquet")
        return d


def validate(data: Any, contract: Contract | str | Path, *, max_quarantine_pct: float = 1.0,
             checks: Sequence[ck.Check] = (), history: QuarantineStore | str | Path | None = None,
             source: str | None = None, at: datetime | date | None = None,
             report_dir: str | Path | None = None) -> Outcome:
    """Split ``data`` with ``contract``, run the standard gate plus ``checks``, and explain the result.

    The standard gate is: schema matches the contract, valid + quarantined = input,
    quarantine rate below ``max_quarantine_pct``, and no duplicate keys in the valid rows.
    Pass ``history`` (a folder or store) to keep quarantine history across runs.
    """
    if not isinstance(contract, Contract):
        contract = Contract.from_toml(contract)
    df = to_polars(data)
    result = contract.split(df)
    gate_checks = [ck.schema_matches(contract), ck.reconciles(result), ck.quarantine_rate(result, max_quarantine_pct)]
    gate_checks += [ck.unique(n) for n, c in contract.columns.items() if c.unique and n in df.columns]
    names = {c.name for c in gate_checks}
    gate_checks += [c for c in checks if c.name not in names]
    meta = {"source": source} if source else {}
    report = Gate(gate_checks).run(result.valid, **meta)

    store = QuarantineStore(history) if isinstance(history, (str, Path)) else history
    run = store.record(result, at=at, source=source, passed=report.passed) if store is not None else None
    outcome = Outcome(result, report, explain(report, result, store=store, as_of=at), run, store)
    if report_dir is not None:
        outcome.write(report_dir)
    return outcome
