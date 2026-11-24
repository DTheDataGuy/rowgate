"""Draft a contract from data you already trust.

``infer`` looks at a DataFrame and proposes a :class:`~rowgate.Contract` that the data
passes today. Treat the result as a first draft: it describes what the data *is*, and
only you know what it *should* be. ``infer_toml`` writes the draft with a comment on
every column explaining what was observed, so reviewing it takes minutes.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import polars as pl

from .contract import Column, Contract, Rule
from .frames import to_polars
from .toml import contract_to_toml, dtype_name

_ID_NAME = re.compile(r"(^|_)(id|key|uuid|guid|num|number|code|sku)$|^(id|pk)$", re.I)
_INT_TYPES = (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)


@dataclass
class Inferred:
    contract: Contract
    notes: dict[str, str]
    suggested_rules: list[tuple[Rule, int, int]]  # rule, violations, comparable rows
    rows: int

    def to_toml(self, source: str = "") -> str:
        header = ["# Draft contract written by `rowgate infer`" + (f" from {source}" if source else ""),
                  f"# Based on {self.rows:,} rows. Review every column: this describes what the data is today,",
                  "# not necessarily what it should be. Tighten bounds and lists before relying on it."]
        text = contract_to_toml(self.contract, self.notes, "\n".join(header))
        if self.suggested_rules:
            text += "\n# Suggested row rules. Each held for almost every row; uncomment the ones that are real.\n"
            for rule, bad, n in self.suggested_rules:
                held = "every row" if bad == 0 else f"all but {bad:,} of {n:,} rows"
                text += (f"\n# [[rules]]  # held for {held}\n# name = \"{rule.name}\"\n# sql = \"{rule.sql}\"\n")
        return text


def infer(data: Any, *, max_categories: int = 20, sample: int | None = 200_000) -> Inferred:
    """Propose a contract that ``data`` passes.

    For each column it records the dtype and marks the column ``required`` when it has
    no nulls or blanks. It marks ``unique`` for ID-like columns with no repeats, lists
    ``accepted`` values for low-cardinality text, sets ``min = 0`` for numbers that are
    never negative, and learns a regex ``pattern`` when every text value has the same
    shape (``98101`` gives ``^\\d{5}$``). It also suggests date-ordering rules.
    """
    df = to_polars(data)
    rows = df.height
    if sample and rows > sample:
        df = df.sample(sample, seed=0)
    cols: dict[str, Column] = {}
    notes: dict[str, str] = {}
    for name, dtype in df.schema.items():
        cols[name], notes[name] = _infer_column(df[name], dtype, max_categories)
    key = next((n for n, c in cols.items() if c.unique), None)
    contract = Contract(cols, key=[key] if key else None)
    return Inferred(contract, notes, _suggest_rules(df), rows)


def infer_toml(data: Any, source: str = "", **kw: Any) -> str:
    return infer(data, **kw).to_toml(source)


def _infer_column(s: pl.Series, dtype: Any, max_categories: int) -> tuple[Column, str]:
    n = s.len()
    nulls = s.null_count()
    is_str = dtype == pl.Utf8
    blanks = int((s.str.strip_chars() == "").sum()) if is_str and n else 0
    nonnull = s.drop_nulls()
    distinct = nonnull.n_unique() if len(nonnull) else 0
    dt = dtype if dtype_name(dtype) else None
    if dt is None and dtype in _INT_TYPES:
        dt = pl.Int64 if dtype != pl.UInt64 else None
    kw: dict[str, Any] = {"dtype": dt}
    note = [f"{_pct(nulls, n)} null", f"{distinct:,} distinct"]

    if n and nulls == 0 and blanks == 0:
        kw["required"] = True
    if (n >= 2 and nulls == 0 and distinct == n and (is_str or dtype in _INT_TYPES)
            and (_ID_NAME.search(s.name) or s.name == "")):
        kw["unique"] = True
    if dtype.is_numeric() and len(nonnull):
        lo, hi = nonnull.min(), nonnull.max()
        note.append(f"range {_fmt(lo)} to {_fmt(hi)}")
        if lo >= 0 and not kw.get("unique"):
            kw["min"] = 0
    elif dtype in (pl.Date, pl.Datetime) or getattr(dtype, "base_type", lambda: None)() == pl.Datetime:
        if len(nonnull):
            note.append(f"range {nonnull.min()} to {nonnull.max()}")
    if is_str and len(nonnull):
        if 0 < distinct <= max_categories and distinct * 2 <= len(nonnull) and not kw.get("unique"):
            kw["accepted"] = sorted(nonnull.unique().to_list())
        else:
            learned = learn_pattern(nonnull)
            if learned:
                kw["pattern"], misses, examples = learned
                if misses:
                    note.append(f"{misses:,} value(s) don't match the pattern, e.g. {', '.join(examples)}")
        if blanks:
            note.append(f"{blanks:,} blank")
    return Column(**kw), ", ".join(note)


# ----------------------------------------------------------------------------- patterns
def _tokens(v: str) -> tuple[tuple[str, int], ...]:
    out: list[list[Any]] = []
    for ch in v:
        cls = "d" if ch.isdigit() else "U" if ch.isupper() and ch.isascii() else "L" if ch.islower() and ch.isascii() \
            else ch
        if out and out[-1][0] == cls and cls in "dUL":
            out[-1][1] += 1
        else:
            out.append([cls, 1])
    return tuple((c, k) for c, k in out)


def learn_pattern(values: pl.Series, max_len: int = 64, limit: int = 50_000,
                  min_share: float = 0.99) -> tuple[str, int, list[str]] | None:
    """Learn a regex for text values that share one shape.

    Returns ``(pattern, mismatches, examples)``: the pattern covers at least ``min_share``
    of the values, and the few that don't fit are reported so you can decide whether
    they are errors or the pattern needs widening. Returns None when there's no
    dominant shape.
    """
    counts = values.value_counts().head(limit)
    vals, ns = counts[:, 0].to_list(), counts[:, 1].to_list()
    total = sum(ns)
    if not vals or total < 3:
        return None
    shapes: dict[tuple[str, ...], list[Any]] = {}  # sig -> [rows, bounds, values]
    for v, k in zip(vals, ns, strict=True):
        if not v or len(v) > max_len:
            continue
        toks = _tokens(v)
        sig = tuple(c for c, _ in toks)
        entry = shapes.setdefault(sig, [0, [(n, n) for _, n in toks], []])
        entry[0] += k
        entry[1] = [(min(a, n), max(b, n)) for (a, b), (_, n) in zip(entry[1], toks, strict=True)]
        entry[2].append(v)
    if not shapes:
        return None
    sig, (rows, bounds, members) = max(shapes.items(), key=lambda kv: kv[1][0])
    if rows < total * min_share or (len(members) < 3 and all(c in "dUL" for c in sig)):
        return None
    parts = []
    for c, (a, b) in zip(sig, bounds, strict=True):
        if c in "dUL":
            rep = "" if a == b == 1 else f"{{{a}}}" if a == b else f"{{{a},{b}}}"
            parts.append({"d": r"\d", "U": "[A-Z]", "L": "[a-z]"}[c] + rep)
        else:
            parts.append(_esc(c) * a if a == b else f"(?:{_esc(c)}){{{a},{b}}}")
    pattern = "^" + "".join(parts) + "$"
    others = [v for v in vals if not v or not re.fullmatch(pattern, v)]
    return pattern, total - rows, others[:3]


def _esc(c: str) -> str:
    return "\\" + c if c in r".^$*+?{}[]\|()" else c


# ----------------------------------------------------------------------------- rules
def _suggest_rules(df: pl.DataFrame, min_rows: int = 20, min_share: float = 0.99) -> list[tuple[Rule, int, int]]:
    dates = [n for n, t in df.schema.items() if t == pl.Date or getattr(t, "base_type", lambda: None)() == pl.Datetime]
    out = []
    for i, a in enumerate(dates):
        for b in dates[i + 1:]:
            both = df.select(pl.col(a), pl.col(b)).drop_nulls()
            if both.height < min_rows:
                continue
            fwd = int((both[b] < both[a]).sum())  # violations of b >= a
            rev = int((both[a] < both[b]).sum())  # violations of a >= b
            bad, (lo, hi) = (fwd, (a, b)) if fwd <= rev else (rev, (b, a))
            if bad <= both.height * (1 - min_share):
                name = f"{_stem(hi)}_on_or_after_{_stem(lo)}"
                out.append((Rule.from_sql(name, f"{hi} >= {lo}"), bad, both.height))
    return out


def _stem(col: str) -> str:
    return re.sub(r"(_date|_at|_on|_time|_ts|date)$", "", col) or col


def _pct(k: int, n: int) -> str:
    p = round(100 * k / n, 2) if n else 0
    return f"{p:g}%"


def _fmt(v: Any) -> str:
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e18:
        return f"{int(v):,}"
    return f"{v:,}" if isinstance(v, int) else f"{v:,.6g}" if isinstance(v, float) else str(v)
