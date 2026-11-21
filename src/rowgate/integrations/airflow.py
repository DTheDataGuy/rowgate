"""Airflow: a callable for ``@task`` or ``PythonOperator``.

    from airflow.decorators import task
    from rowgate.integrations.airflow import gate_file

    @task
    def check_orders(path: str) -> dict:
        return gate_file(path, "contracts/orders.toml", report_dir="/data/quality/orders",
                         history="/data/quality/orders/history")

``gate_file`` reads the file, writes the reports, and returns a small JSON-safe
summary for XCom. A blocking failure raises ``AirflowFailException`` (no retries:
retrying won't fix bad data). Outside Airflow it raises :class:`rowgate.GateFailed`.
"""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ..checks import Check
from ..contract import Contract
from ..frames import read_file
from ..history import QuarantineStore
from ..pipeline import Outcome, validate


def summary(out: Outcome) -> dict[str, Any]:
    """A JSON-safe dict for XCom."""
    r = out.result
    return {"passed": out.passed, "input_rows": r.input_rows, "valid_rows": r.valid.height,
            "quarantined_rows": r.quarantine.height, "quarantine_pct": round(r.quarantine_rate, 4),
            "errors": [c.name for c in out.report.errors], "warnings": [c.name for c in out.report.warnings],
            "explanation": out.explanation.text}


def fail(out: Outcome) -> None:
    if out.passed:
        return
    try:
        from airflow.exceptions import AirflowFailException
    except ImportError:
        out.raise_for_errors()
    else:
        raise AirflowFailException(out.explanation.text)


def gate_frame(data: Any, contract: Contract | str | Path, *, max_quarantine_pct: float = 1.0,
               checks: Sequence[Check] = (), history: QuarantineStore | str | Path | None = None,
               report_dir: str | Path | None = None) -> dict[str, Any]:
    out = validate(data, contract, max_quarantine_pct=max_quarantine_pct, checks=checks, history=history,
                   report_dir=report_dir)
    print(out.summary())  # lands in the task log
    fail(out)
    return summary(out)


def gate_file(path: str | Path, contract: Contract | str | Path, **kw: Any) -> dict[str, Any]:
    table, sql = kw.pop("table", None), kw.pop("sql", None)
    return gate_frame(read_file(path, table=table, sql=sql), contract, **kw)
