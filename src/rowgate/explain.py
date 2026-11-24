"""Explain a failed batch in plain language.

:func:`explain` reads a :class:`~rowgate.Report`, the :class:`~rowgate.SplitResult` and,
optionally, a :class:`~rowgate.QuarantineStore`, and writes the note a person would
want at 2 a.m.: what blocked the load, which rules rows broke with real examples,
and where to start fixing.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import polars as pl

from .contract import KEY, REASONS, SplitResult, _num

if TYPE_CHECKING:
    from .gate import Report
    from .history import QuarantineStore


@dataclass
class Explanation:
    headline: str
    sections: list[tuple[str, list[str]]] = field(default_factory=list)

    @property
    def markdown(self) -> str:
        out = [f"**{self.headline}**"]
        for title, items in self.sections:
            out += ["", f"### {title}", ""] + [f"- {i}" for i in items]
        return "\n".join(out) + "\n"

    @property
    def text(self) -> str:
        out = [_plain(self.headline)]
        for title, items in self.sections:
            out += ["", title + ":"] + [f"  - {_plain(i)}" for i in items]
        return "\n".join(out)

    def __str__(self) -> str:
        return self.text


def explain(report: Report | None = None, result: SplitResult | None = None, *,
            store: QuarantineStore | None = None, examples: int = 3, as_of: Any = None) -> Explanation:
    """Summarise why a batch failed (or passed) and what to fix first."""
    sections: list[tuple[str, list[str]]] = []
    headline = _headline(report, result)

    if report is not None and (report.errors or report.warnings):
        items = [f"`{r.name}` observed {r.observed}; expected {r.expectation}" for r in report.errors]
        items += [f"warning: `{r.name}` observed {r.observed}; expected {r.expectation}" for r in report.warnings]
        sections.append(("What failed", items))

    hints: list[str] = []
    if result is not None and result.quarantine.height:
        counts = result.reason_counts()
        q = result.quarantine
        items = []
        for reason, n in counts.iter_rows():
            meaning = result.contract.describe_reason(reason) if result.contract else reason
            rows = q.filter(pl.col(REASONS).list.contains(reason))
            items.append(f"{_n(n, 'row')} broke `{reason}`: {meaning}. "
                         f"{_examples(rows, reason, result, examples)}")
            hint = _hint(reason, rows, result)
            if hint:
                hints.append(hint)
        sections.append(("Why rows were quarantined", items))

        top_reason, top_n = counts.row(0)
        share = 100 * top_n / q.height
        if counts.height > 1 and share >= 50:
            hints.insert(0, f"Start with `{top_reason}`: it affects {share:.0f}% of quarantined rows.")
        multi = int((q[REASONS].list.len() > 1).sum())
        if multi:
            hints.append(f"{_n(multi, 'row')} broke more than one rule; every reason is listed in "
                         f"`{REASONS}`, so one pass over the source can fix them all.")

    if report is not None and any(r.name == "schema_matches" and not r.passed for r in report.results):
        hints.insert(0, "The source schema changed. Check whether an upstream export renamed or retyped a "
                        "column before fixing individual rows.")

    if store is not None:
        open_ = store.open_issues(as_of)
        if open_.height:
            stale = open_.filter(pl.col("days_open") >= 3).height
            line = f"{_n(open_.height, 'row')} {'is' if open_.height == 1 else 'are'} open in quarantine history"
            if stale:
                line += f", and {stale:,} {'has' if stale == 1 else 'have'} been failing for 3 or more days"
            hints.append(line + ". Fix and replay them with `rowgate replay`.")

    if hints:
        sections.append(("Where to start", hints))
    return Explanation(headline, sections)


def _headline(report: Report | None, result: SplitResult | None) -> str:
    q = f"{result.quarantine.height:,} of {result.input_rows:,} rows quarantined" if result else ""
    if report is None:
        return f"Contract check: {q}." if q else "Contract check."
    if report.errors:
        k = len(report.errors)
        msg = f"Blocked: {k} blocking check{'s' if k != 1 else ''} failed, so this batch should not be loaded"
    elif report.warnings:
        k = len(report.warnings)
        msg = f"Passed with {k} warning{'s' if k != 1 else ''}"
    else:
        msg = "Passed: every check is within its limit"
    return msg + (f" ({q})." if q else ".")


def _involved(reason: str, result: SplitResult) -> list[str]:
    c = result.contract
    if c is not None:
        for rule in c.rules:
            if rule.name == reason:
                try:
                    return list(dict.fromkeys(rule.check.meta.root_names()))
                except Exception:  # noqa: BLE001
                    return []
    col = reason.rpartition(":")[0]
    return [col] if col in result.quarantine.columns else []


def _examples(rows: pl.DataFrame, reason: str, result: SplitResult, k: int) -> str:
    key = [c for c in (result.contract.key_columns if result.contract else []) if c in rows.columns]
    cols = list(dict.fromkeys(key + [c for c in _involved(reason, result) if c in rows.columns]))
    cols = [c for c in cols if c not in (REASONS, KEY)]
    if not cols:
        return ""
    shown = []
    for r in rows.select(cols).head(k).iter_rows(named=True):
        shown.append(", ".join(f"{c}={_v(v)}" for c, v in r.items()))
    more = f" (+{rows.height - k:,} more)" if rows.height > k else ""
    return "Examples: " + "; ".join(f"`{s}`" for s in shown) + more


def _hint(reason: str, rows: pl.DataFrame, result: SplitResult) -> str | None:
    c = result.contract
    col_name, _, kind = reason.rpartition(":")
    if c is None or col_name not in c.columns or col_name not in rows.columns:
        return None
    col = c.columns[col_name]
    vals = rows[col_name].drop_nulls()
    if kind == "accepted" and col.accepted and vals.dtype == pl.Utf8:
        lookup = {str(a).casefold(): a for a in col.accepted}
        fixable = [v for v in vals.unique().to_list() if v.strip().casefold() in lookup]
        if fixable:
            pairs = ", ".join(f"'{v}' -> '{lookup[v.strip().casefold()]}'" for v in fixable[:3])
            return (f"`{col_name}` values differ from the accepted list only by case or spacing ({pairs}). "
                    "Normalising upstream would clear these rows.")
    if kind == "max" and len(vals) and col.max:
        top = vals.max()
        if isinstance(top, (int, float)) and col.max > 0 and top >= 5 * col.max:
            return (f"`{col_name}` has a value of {_num(top)}, {top / col.max:,.0f} times the maximum of "
                    f"{_num(col.max)}. That usually means a unit error or a typo rather than a real value.")
    if kind == "pattern" and len(vals):
        common = _common_suffix_shape(vals.cast(pl.Utf8).to_list())
        if common:
            return (f"Every `{col_name}` value that failed the pattern has the same shape ({common}). If these are "
                    f"valid, widen the pattern instead of fixing the rows.")
    if kind == "duplicate":
        return (f"`{col_name}` repeats. The first occurrence was loaded; check whether the source sent the "
                "batch twice or whether the key is not unique.")
    return None


def _common_suffix_shape(vals: list[str]) -> str | None:
    shapes = {re.sub(r"[A-Z]", "A", re.sub(r"\d", "9", v)) for v in vals}
    return next(iter(shapes)) if len(shapes) == 1 and len(vals) > 1 else None


def _v(v: Any) -> str:
    if isinstance(v, float):
        return f"{int(v):,}" if v.is_integer() and abs(v) < 1e18 else f"{v:,.6g}"
    if isinstance(v, int) and not isinstance(v, bool):
        return f"{v:,}" if abs(v) >= 10_000 else str(v)
    return "null" if v is None else str(v)


def _n(k: int, word: str) -> str:
    return f"{k:,} {word}" + ("" if k == 1 else "s")


def _plain(s: str) -> str:
    return s.replace("**", "").replace("`", "")
