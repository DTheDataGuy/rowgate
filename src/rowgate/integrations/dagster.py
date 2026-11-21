"""Dagster: a contract as a blocking asset check.

    import dagster as dg
    from rowgate.integrations.dagster import contract_check

    @dg.asset
    def orders() -> pl.DataFrame: ...

    orders_contract = contract_check(orders, "contracts/orders.toml", history="quality/orders")
    defs = dg.Definitions(assets=[orders], asset_checks=[orders_contract])

With ``blocking=True`` (the default) downstream assets don't materialise when the
check fails. The check's metadata carries the Markdown report and explanation, so the
reason shows up in the Dagster UI.
"""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ..checks import Check
from ..contract import Contract
from ..history import QuarantineStore
from ..pipeline import validate


def contract_check(asset: Any, contract: Contract | str | Path, *, name: str = "rowgate_contract",
                   max_quarantine_pct: float = 1.0, checks: Sequence[Check] = (),
                   history: QuarantineStore | str | Path | None = None, blocking: bool = True,
                   description: str | None = None) -> Any:
    import dagster as dg

    if not isinstance(contract, Contract):
        contract = Contract.from_toml(contract)
    desc = description or "rowgate contract: quarantine bad rows, block the batch when gate checks fail"

    @dg.asset_check(asset=asset, name=name, blocking=blocking, description=desc)
    def _rowgate_check(context, data):  # noqa: ANN001 - Dagster inspects these parameters
        out = validate(data, contract, max_quarantine_pct=max_quarantine_pct, checks=checks, history=history,
                       source=context.op.name if hasattr(context, "op") else None)
        res = out.result
        meta = {
            "input_rows": dg.MetadataValue.int(res.input_rows),
            "valid_rows": dg.MetadataValue.int(res.valid.height),
            "quarantined_rows": dg.MetadataValue.int(res.quarantine.height),
            "quarantine_pct": dg.MetadataValue.float(round(res.quarantine_rate, 4)),
            "report": dg.MetadataValue.md(out.markdown()),
        }
        if out.run is not None:
            meta["history"] = dg.MetadataValue.text(out.store.summary() if out.store else str(out.run))
        severity = dg.AssetCheckSeverity.ERROR if out.report.errors else dg.AssetCheckSeverity.WARN
        return dg.AssetCheckResult(passed=out.passed, metadata=meta, severity=severity)

    return _rowgate_check
