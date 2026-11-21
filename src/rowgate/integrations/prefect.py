"""Prefect: a task that validates a frame and publishes the report as an artifact.

    from prefect import flow
    from rowgate.integrations.prefect import rowgate_task

    @flow
    def load_orders():
        df = extract()
        valid = rowgate_task(df, "contracts/orders.toml", history="quality/orders")
        load(valid)

The task returns the valid rows. When a blocking check fails it raises
:class:`rowgate.GateFailed`, so the task and the flow are marked failed, after the
Markdown report has been attached to the run.
"""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ..checks import Check
from ..contract import Contract
from ..history import QuarantineStore
from ..pipeline import Outcome, validate


def run_and_publish(data: Any, contract: Contract | str | Path, *, max_quarantine_pct: float = 1.0,
                    checks: Sequence[Check] = (), history: QuarantineStore | str | Path | None = None,
                    artifact_key: str | None = "rowgate-report", quarantine_path: str | Path | None = None) -> Outcome:
    out = validate(data, contract, max_quarantine_pct=max_quarantine_pct, checks=checks, history=history)
    if quarantine_path and out.quarantine.height:
        out.quarantine.write_parquet(quarantine_path)
    try:
        from prefect.artifacts import create_markdown_artifact
        create_markdown_artifact(markdown=out.markdown(), key=artifact_key,
                                 description="rowgate data-quality report")
    except Exception:  # noqa: BLE001 - outside a flow run there is nowhere to publish
        pass
    return out


def _task_fn(data: Any, contract: Contract | str | Path, **kw: Any) -> Any:
    return run_and_publish(data, contract, **kw).raise_for_errors().valid


def __getattr__(name: str) -> Any:
    if name == "rowgate_task":
        from prefect import task
        return task(_task_fn, name="rowgate", log_prints=True)
    raise AttributeError(name)
