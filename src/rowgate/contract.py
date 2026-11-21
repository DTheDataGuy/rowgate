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

from .frames import to_polars

REASONS = "_rowgate_reasons"
KEY = "_rowgate_key"

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
    sql: str | None = None

    @classmethod
    def from_sql(cls, name: str, sql: str, description: str = "") -> Rule:
        return cls(name, pl.sql_expr(sql), description, sql)


@dataclass
class SplitResult:
    valid: pl.DataFrame
    quarantine: pl.DataFrame
    input_rows: int
    contract: Contract | None = field(default=None, repr=False)

    @property
    def reconciles(self) -> bool:
        return self.valid.height + self.quarantine.height == self.input_rows

    @property
    def quarantine_rate(self) -> float:
        return 100.0 * self.quarantine.height / self.input_rows if self.input_rows else 0.0

    def reason_counts(self) -> pl.DataFrame:
        """How many quarantined rows broke each rule, most common first."""
        if self.quarantine.is_empty():
            return pl.DataFrame({"reason": [], "rows": []}, schema={"reason": pl.Utf8, "rows": pl.UInt32})
        return (self.quarantine.select(pl.col(REASONS).explode().alias("reason"))
                .group_by("reason").len(name="rows").sort(["rows", "reason"], descending=[True, False]))

    def to_pandas(self) -> tuple[Any, Any]:
        """``(valid, quarantine)`` as pandas DataFrames. Needs ``rowgate[pandas]``."""
        return self.valid.to_pandas(), self.quarantine.to_pandas()


@dataclass
class Contract:
    columns: Mapping[str, Column]
    rules: Sequence[Rule] = field(default_factory=list)
    allow_extra_columns: bool = True
    key: Sequence[str] | None = None
    """Columns that identify a row across runs. Defaults to the first ``unique`` column.
    Used by :class:`rowgate.QuarantineStore` to tell when a quarantined row has been fixed."""

    def __post_init__(self) -> None:
        if isinstance(self.key, str):
            self.key = [self.key]

    @property
    def key_columns(self) -> list[str]:
        if self.key:
            return list(self.key)
        return next(([n] for n, c in self.columns.items() if c.unique), [])

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
        rules = [Rule(r["name"], pl.sql_expr(r["sql"]), r.get("description", ""), r["sql"])
                 for r in raw.get("rules", [])]
        return cls(cols, rules, raw.get("allow_extra_columns", True), raw.get("key"))

    def to_toml(self) -> str:
        """Serialise to TOML. Rules round-trip only if they were written in SQL."""
        from .toml import contract_to_toml
        return contract_to_toml(self)

    # ---------------------------------------------------------------- inspection
    def schema_drift(self, df: Any) -> dict[str, tuple[str, str]]:
        """Columns whose presence or dtype differs from the contract: ``{name: (expected, actual)}``."""
        actual = dict(to_polars(df).schema)
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
                out.append((f"{name}:duplicate", c.is_not_null() & ~c.is_first_distinct()))
        for rule in self.rules:
            out.append((rule.name, rule.check.not_().fill_null(False)))
        return out

    # -------------------------------------------------------------------- split
    def split(self, df: Any) -> SplitResult:
        """Separate valid rows from rows that break the contract.

        Accepts Polars, pandas, PyArrow or DuckDB input and returns Polars frames.
        Nothing is dropped: ``valid.height + quarantine.height == df.height``.
        """
        df = to_polars(df)
        df = df.drop([c for c in (REASONS, KEY) if c in df.columns])
        missing = [n for n, c in self.columns.items() if c.required and n not in df.columns]
        if missing:
            raise KeyError(f"required columns missing from input: {missing}")
        fails = self._fast_duplicates(df, self.failure_expressions(df))
        if not fails:
            empty = df.clear().with_columns(pl.lit([], pl.List(pl.Utf8)).alias(REASONS))
            return SplitResult(df, empty, df.height, self)
        # Evaluate every rule once as a boolean column, then build reason lists only for the
        # rows that failed. Building lists for every row would dominate the cost on clean data.
        flags = (df.lazy().select([expr.fill_null(False).alias(f"__rowgate_{i}") for i, (_, expr) in enumerate(fails)])
                 .collect())
        bad = flags.select(pl.any_horizontal(pl.all()).alias("bad")).to_series()
        if not bad.any():
            empty = df.clear().with_columns(pl.lit([], pl.List(pl.Utf8)).alias(REASONS))
            return SplitResult(df, empty, df.height, self)
        qflags = flags.filter(bad)
        reasons = qflags.select(pl.concat_list(
            [pl.when(pl.col(f"__rowgate_{i}")).then(pl.lit(name)).otherwise(pl.lit(None, pl.Utf8))
             for i, (name, _) in enumerate(fails)]).list.drop_nulls().alias(REASONS)).to_series()
        quarantine = df.filter(bad).with_columns(reasons)
        return SplitResult(valid=df.filter(~bad), quarantine=quarantine, input_rows=df.height, contract=self)

    def _fast_duplicates(self, df: pl.DataFrame, fails: list[tuple[str, pl.Expr]]) -> list[tuple[str, pl.Expr]]:
        """Swap the generic duplicate expression for a faster equivalent.

        A distinct count is cheap, so a column with no repeats skips the check entirely.
        When there are repeats, keeping the first index per value beats ``is_first_distinct``.
        """
        out = []
        for reason, expr in fails:
            name, _, kind = reason.rpartition(":")
            if kind != "duplicate" or name not in self.columns or not self.columns[name].unique:
                out.append((reason, expr))
                continue
            c = pl.col(name)
            distinct, present = df.select(c.drop_nulls().n_unique().alias("d"), c.count().alias("n")).row(0)
            if distinct == present:
                continue
            first = (df.select(c).with_row_index("__rowgate_i").filter(c.is_not_null())
                     .unique(name, keep="first", maintain_order=False)["__rowgate_i"])
            base = pl.repeat(True, df.height, dtype=pl.Boolean, eager=True)
            try:
                later = base.scatter(first, False)
            except pl.exceptions.ComputeError:  # older Polars needs sorted indices
                later = base.scatter(first.sort(), False)
            later = later & df[name].is_not_null()
            out.append((reason, pl.lit(later)))
        return out

    def describe_reason(self, reason: str) -> str:
        """Plain-English meaning of a quarantine reason such as ``amount:min``."""
        for rule in self.rules:
            if rule.name == reason:
                return rule.description or (f"rule `{rule.sql}` is false" if rule.sql else "custom rule is false")
        col_name, _, kind = reason.rpartition(":")
        col = self.columns.get(col_name)
        if col is None:
            return reason
        return {
            "required": "value is missing or blank",
            "accepted": f"value is not one of {_short_list(col.accepted)}",
            "min": f"value is below the minimum of {_num(col.min)}",
            "max": f"value is above the maximum of {_num(col.max)}",
            "pattern": f"value does not match the pattern {col.pattern}",
            "duplicate": "key repeats a value seen earlier in the batch",
        }.get(kind, reason)


def _num(v: Any) -> str:
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int) or (isinstance(v, float) and v.is_integer() and abs(v) < 1e18):
        return f"{int(v):,}"
    return str(v)


def _short_list(values: Sequence[Any] | None, limit: int = 6) -> str:
    vals = list(values or [])
    shown = ", ".join(repr(v) for v in vals[:limit])
    return shown + (f" and {len(vals) - limit} more" if len(vals) > limit else "")


def _dtype_matches(actual: Any, expected: Any) -> bool:
    if actual == expected:
        return True
    # Datetime/Decimal declared without parameters match any time unit, zone or precision.
    base = getattr(expected, "base_type", lambda: expected)()
    return actual.base_type() == base if hasattr(actual, "base_type") else False


def columns(names: Iterable[str], **kw: Any) -> dict[str, Column]:
    """Shorthand for several columns with the same expectations."""
    return {n: Column(**kw) for n in names}
