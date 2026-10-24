"""Dataset-level checks for a :class:`rowgate.Gate`.

Each factory returns a :class:`Check`: a name, a severity and a function that looks at
a DataFrame and returns ``(passed, observed)``. ``error`` checks block the batch,
``warn`` checks are reported but don't block it.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from .contract import Contract, SplitResult

Severity = str  # "error" | "warn"


@dataclass(frozen=True)
class Check:
    name: str
    severity: Severity
    expectation: str
    fn: Callable[[pl.DataFrame], tuple[bool, str]]

    def __post_init__(self) -> None:
        if self.severity not in ("error", "warn"):
            raise ValueError(f"severity must be 'error' or 'warn', got {self.severity!r}")


def _pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 2) if d else 0.0


def schema_matches(contract: Contract, severity: Severity = "error") -> Check:
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        drift = contract.schema_drift(df)
        if not drift:
            return True, "no drift"
        return False, "; ".join(f"{k}: expected {e}, got {a}" for k, (e, a) in sorted(drift.items()))
    return Check("schema_matches", severity, "columns and dtypes match the contract", fn)


def unique(cols: str | Sequence[str], severity: Severity = "error") -> Check:
    keys = [cols] if isinstance(cols, str) else list(cols)
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        dup = df.height - df.select(keys).unique().height
        return dup == 0, f"{dup:,} duplicate keys"
    return Check(f"unique:{'+'.join(keys)}", severity, "0 duplicate keys", fn)


def not_null(cols: str | Sequence[str], severity: Severity = "error") -> Check:
    names = [cols] if isinstance(cols, str) else list(cols)
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        bad = {c: n for c in names if (n := df[c].null_count())}
        return not bad, ", ".join(f"{c}: {n:,}" for c, n in bad.items()) or "0 nulls"
    return Check(f"not_null:{'+'.join(names)}", severity, f"no nulls in {len(names)} column(s)", fn)


def null_rate(col: str, max_pct: float, severity: Severity = "warn") -> Check:
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        p = _pct(df[col].null_count(), df.height)
        return p <= max_pct, f"{p}% null"
    return Check(f"null_rate:{col}", severity, f"<= {max_pct}% null", fn)


def accepted_values(col: str, values: Sequence[object], severity: Severity = "error") -> Check:
    allowed = set(values)
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        seen = set(df[col].drop_nulls().unique().to_list())
        extra = sorted(map(str, seen - allowed))
        return not extra, "unexpected: " + ", ".join(extra) if extra else "all values accepted"
    return Check(f"accepted_values:{col}", severity, f"subset of {len(allowed)} accepted values", fn)


def row_count(min_rows: int = 1, max_rows: int | None = None, severity: Severity = "error") -> Check:
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        ok = df.height >= min_rows and (max_rows is None or df.height <= max_rows)
        return ok, f"{df.height:,} rows"
    bound = f">= {min_rows:,}" + (f" and <= {max_rows:,}" if max_rows is not None else "")
    return Check("row_count", severity, f"{bound} rows", fn)


def reconciles(result: SplitResult, severity: Severity = "error") -> Check:
    """Every input row is either valid or quarantined."""
    def fn(_: pl.DataFrame) -> tuple[bool, str]:
        total = result.valid.height + result.quarantine.height
        return total == result.input_rows, (
            f"{result.valid.height:,} valid + {result.quarantine.height:,} quarantined = {total:,} "
            f"vs {result.input_rows:,} input")
    return Check("rows_reconcile", severity, "valid + quarantined = input", fn)


def quarantine_rate(result: SplitResult, max_pct: float, severity: Severity = "error") -> Check:
    def fn(_: pl.DataFrame) -> tuple[bool, str]:
        return result.quarantine_rate < max_pct, f"{round(result.quarantine_rate, 2)}%"
    return Check("quarantine_rate", severity, f"< {max_pct}% of input quarantined", fn)


def freshness(col: str, max_age_days: int, as_of: date | None = None, severity: Severity = "warn") -> Check:
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        newest = df[col].max()
        if newest is None:
            return False, "no values"
        ref = as_of or date.today()
        newest_d = newest.date() if isinstance(newest, datetime) else newest
        age = (ref - newest_d).days
        return age <= max_age_days, f"newest {newest_d} ({age} days old)"
    return Check(f"freshness:{col}", severity, f"<= {max_age_days} days old", fn)


def expr(name: str, condition: pl.Expr, severity: Severity = "error", max_failing: int = 0) -> Check:
    """Custom row condition that must hold for (almost) every row."""
    def fn(df: pl.DataFrame) -> tuple[bool, str]:
        failing = df.select(condition.not_().fill_null(False).sum()).item()
        return failing <= max_failing, f"{failing:,} rows fail"
    return Check(name, severity, f"<= {max_failing:,} failing rows", fn)


def custom(name: str, fn: Callable[[pl.DataFrame], tuple[bool, str]], expectation: str,
           severity: Severity = "error") -> Check:
    return Check(name, severity, expectation, fn)
