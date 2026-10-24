"""Run a list of checks and produce a report that machines and people can both read."""
from __future__ import annotations

import json
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .checks import Check


class GateFailed(RuntimeError):
    def __init__(self, report: Report) -> None:
        self.report = report
        names = ", ".join(r.name for r in report.errors)
        super().__init__(f"{len(report.errors)} blocking check(s) failed: {names}")


@dataclass
class CheckResult:
    name: str
    severity: str
    passed: bool
    observed: str
    expectation: str
    seconds: float


@dataclass
class Report:
    results: list[CheckResult]
    rows: int
    created_at: str
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == "error" and not r.passed]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == "warn" and not r.passed]

    @property
    def passed(self) -> bool:
        return not self.errors

    def raise_for_errors(self) -> Report:
        if self.errors:
            raise GateFailed(self)
        return self

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "rows": self.rows, "created_at": self.created_at,
                "meta": self.meta, "errors": len(self.errors), "warnings": len(self.warnings),
                "results": [asdict(r) for r in self.results]}

    def to_json(self, path: str | Path | None = None) -> str:
        text = json.dumps(self.to_dict(), indent=2, default=str)
        if path is not None:
            Path(path).write_text(text + "\n")
        return text

    def to_markdown(self, path: str | Path | None = None, title: str = "Data-quality report") -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"# {title}", "",
                 f"**{status}** · {_rows(self.rows)} · {len(self.results)} checks · "
                 f"{len(self.errors)} blocking failures · {len(self.warnings)} warnings · {self.created_at}", "",
                 "| Check | Severity | Result | Observed | Expectation |", "|---|---|---|---|---|"]
        for r in self.results:
            res = "pass" if r.passed else ("**FAIL**" if r.severity == "error" else "warn")
            lines.append(f"| `{r.name}` | {r.severity} | {res} | {_md(r.observed)} | {_md(r.expectation)} |")
        text = "\n".join(lines) + "\n"
        if path is not None:
            Path(path).write_text(text)
        return text

    def __str__(self) -> str:
        width = max((len(r.name) for r in self.results), default=10)
        out = [f"rowgate: {'PASS' if self.passed else 'FAIL'} ({_rows(self.rows)})"]
        for r in self.results:
            mark = "ok  " if r.passed else ("FAIL" if r.severity == "error" else "warn")
            out.append(f"  {mark}  {r.name:<{width}}  {r.observed}")
        return "\n".join(out)


def _rows(n: int) -> str:
    return f"{n:,} row" + ("" if n == 1 else "s")


def _md(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


class Gate:
    """An ordered set of checks. A check that raises is recorded as a failure, not a crash."""

    def __init__(self, checks: Sequence[Check]) -> None:
        names = [c.name for c in checks]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate check names: {sorted(dupes)}")
        self.checks = list(checks)

    def run(self, df: pl.DataFrame, **meta: Any) -> Report:
        results = []
        for c in self.checks:
            t0 = time.perf_counter()
            try:
                passed, observed = c.fn(df)
            except Exception as exc:  # noqa: BLE001 - a broken check must not hide the others
                passed, observed = False, f"check raised {type(exc).__name__}: {exc}"
            results.append(CheckResult(c.name, c.severity, bool(passed), observed, c.expectation,
                                       round(time.perf_counter() - t0, 4)))
        created = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return Report(results, df.height, created, meta)
