"""Column contracts and row-level rules.

A :class:`Contract` describes what a valid row looks like. :meth:`Contract.split`
evaluates it against a DataFrame and returns the rows that pass, plus a quarantine
frame where every rejected row carries the names of the rules it broke.
"""
from __future__ import annotations

import re
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

REASONS = "_rowgate_reasons"

_DTYPES: dict[str, Any] = {
    "str": pl.Utf8, "string": pl.Utf8, "utf8": pl.Utf8,
    "int": pl.Int64, "int64": pl.Int64, "int32": pl.Int32,
    "float": pl.Float64, "float64": pl.Float64, "float32": pl.Float32,
    "bool": pl.Boolean, "boolean": pl.Boolean,
    "date": pl.Date, "datetime": pl.Datetime, "decimal": pl.Decimal,
}


@dataclass(frozen=True)
class Column:
    """Expectations for a single column.

    ``None`` values only fail ``required``. Range, pattern and accepted-value checks
    skip nulls, the same way SQL constraints do.
    """

    dtype: Any = None
    required: bool = False
    unique: bool = False
    accepted: Sequence[Any] | None = None
    min: Any = None
    max: Any = None
    pattern: str | None = None

    def __post_init__(self) -> None:
        if self.pattern is not None:
            re.compile(self.pattern)  # fail early on a bad regex
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(f"min ({self.min}) is greater than max ({self.max})")


@dataclass(frozen=True)
class Rule:
    """A row-level rule. ``check`` must evaluate to True for valid rows.

    A null result counts as a pass, so a rule like ``issued >= applied`` does not
    reject rows that simply haven't been issued yet.
    """

    name: str
    check: pl.Expr
    description: str = ""


@dataclass
class SplitResult:
    valid: pl.DataFrame
    quarantine: pl.DataFrame
    input_rows: int

    @property
    def quarantine_rate(self) -> float:
        return 100.0 * self.quarantine.height / self.input_rows if self.input_rows else 0.0

    def reason_counts(self) -> pl.DataFrame:
        """How many quarantined rows broke each rule, most common first."""
        if self.quarantine.is_empty():
            return pl.DataFrame({"reason": [], "rows": []}, schema={"reason": pl.Utf8, "rows": pl.UInt32})
        return (self.quarantine.select(pl.col(REASONS).explode().alias("reason"))
                .group_by("reason").len(name="rows").sort(["rows", "reason"], descending=[True, False]))


@dataclass
class Contract:
    columns: Mapping[str, Column]
    rules: Sequence[Rule] = field(default_factory=list)
    allow_extra_columns: bool = True

    # ------------------------------------------------------------------ loading
    @classmethod
    def from_toml(cls, path: str | Path) -> Contract:
        """Load a contract from TOML. See ``examples/permits_contract.toml``."""
        raw = tomllib.loads(Path(path).read_text())
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> Contract:
        cols: dict[str, Column] = {}
        for name, spec in raw.get("columns", {}).items():
            spec = dict(spec)
            dt = spec.pop("dtype", None)
            if dt is not None:
                key = str(dt).lower()
                if key not in _DTYPES:
                    raise ValueError(f"column {name!r}: unknown dtype {dt!r}; use one of {sorted(_DTYPES)}")
                spec["dtype"] = _DTYPES[key]
            cols[name] = Column(**spec)
        rules = [Rule(r["name"], pl.sql_expr(r["sql"]), r.get("description", ""))
                 for r in raw.get("rules", [])]
        return cls(cols, rules, raw.get("allow_extra_columns", True))

    # ---------------------------------------------------------------- inspection
    def schema_drift(self, df: pl.DataFrame) -> dict[str, tuple[str, str]]:
        """Columns whose presence or dtype differs from the contract: ``{name: (expected, actual)}``."""
        actual = dict(df.schema)
        drift: dict[str, tuple[str, str]] = {}
        for name, col in self.columns.items():
            if name not in actual:
                drift[name] = (str(col.dtype or "any"), "missing")
            elif col.dtype is not None and not _dtype_matches(actual[name], col.dtype):
                drift[name] = (str(col.dtype), str(actual[name]))
        if not self.allow_extra_columns:
            for name in actual.keys() - self.columns.keys():
                drift[name] = ("absent", str(actual[name]))
        return drift

    def failure_expressions(self, df: pl.DataFrame) -> list[tuple[str, pl.Expr]]:
        """``(reason, expr)`` pairs where ``expr`` is True for rows that fail."""
        out: list[tuple[str, pl.Expr]] = []
        present = set(df.columns)
        for name, col in self.columns.items():
            if name not in present:
                continue
            c = pl.col(name)
            if col.required:
                fail = c.is_null()
                if df.schema[name] == pl.Utf8:
                    fail = fail | (c.str.strip_chars() == "")
                out.append((f"{name}:required", fail))
            if col.accepted is not None:
                out.append((f"{name}:accepted", c.is_not_null() & ~c.is_in(list(col.accepted))))
            if col.min is not None:
                out.append((f"{name}:min", c < col.min))
            if col.max is not None:
                out.append((f"{name}:max", c > col.max))
            if col.pattern is not None:
                out.append((f"{name}:pattern", c.is_not_null() & ~c.cast(pl.Utf8).str.contains(col.pattern)))
            if col.unique:
                # keep the first occurrence, quarantine later ones
                out.append((f"{name}:duplicate", c.is_not_null() & c.is_duplicated() & ~c.is_first_distinct()))
        for rule in self.rules:
            out.append((rule.name, rule.check.not_().fill_null(False)))
        return out

    # -------------------------------------------------------------------- split
    def split(self, df: pl.DataFrame) -> SplitResult:
        """Separate valid rows from rows that break the contract.

        Nothing is dropped: ``valid.height + quarantine.height == df.height``.
        """
        missing = [n for n, c in self.columns.items() if c.required and n not in df.columns]
        if missing:
            raise KeyError(f"required columns missing from input: {missing}")
        fails = self.failure_expressions(df)
        if not fails:
            return SplitResult(df, df.clear().with_columns(pl.lit([], pl.List(pl.Utf8)).alias(REASONS)), df.height)
        reasons = pl.concat_list(
            [pl.when(expr.fill_null(False)).then(pl.lit(name)).otherwise(pl.lit(None, pl.Utf8)) for name, expr in fails]
        ).list.drop_nulls().alias(REASONS)
        tagged = df.with_columns(reasons)
        bad = tagged[REASONS].list.len() > 0
        return SplitResult(valid=tagged.filter(~bad).drop(REASONS), quarantine=tagged.filter(bad), input_rows=df.height)


def _dtype_matches(actual: Any, expected: Any) -> bool:
    if actual == expected:
        return True
    # Datetime/Decimal declared without parameters match any time unit, zone or precision.
    base = getattr(expected, "base_type", lambda: expected)()
    return actual.base_type() == base if hasattr(actual, "base_type") else False


def columns(names: Iterable[str], **kw: Any) -> dict[str, Column]:
    """Shorthand for several columns with the same expectations."""
    return {n: Column(**kw) for n in names}
