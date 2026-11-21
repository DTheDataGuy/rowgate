"""Write contracts back out as TOML (the standard library only reads TOML)."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from .contract import Contract

_NAMES = [(pl.Utf8, "str"), (pl.Int64, "int"), (pl.Int32, "int32"), (pl.Float64, "float"), (pl.Float32, "float32"),
          (pl.Boolean, "bool"), (pl.Date, "date"), (pl.Datetime, "datetime"), (pl.Decimal, "decimal")]


def dtype_name(dtype: Any) -> str | None:
    """TOML name for a Polars dtype, or None when the contract can't express it."""
    if dtype is None:
        return None
    base = dtype.base_type() if hasattr(dtype, "base_type") else dtype
    for pl_type, name in _NAMES:
        if dtype == pl_type or base == pl_type:
            return name
    if dtype in (pl.Int8, pl.Int16, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return None
    return None


def value(v: Any) -> str:
    """A TOML literal for a scalar or list."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v:_}" if abs(v) >= 100_000 else str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(value(x) for x in v) + "]"
    s = str(v)
    if "'" not in s and "\n" not in s:
        return f"'{s}'"  # literal string: regex backslashes stay readable
    return json.dumps(s)


def key(name: str) -> str:
    return name if name.replace("_", "").replace("-", "").isalnum() else json.dumps(name)


def contract_to_toml(contract: Contract, comments: dict[str, str] | None = None, header: str = "") -> str:
    comments = comments or {}
    lines: list[str] = [header.rstrip(), ""] if header else []
    if contract.key:
        lines += [f"key = {value(list(contract.key))}"]
    if not contract.allow_extra_columns:
        lines += ["allow_extra_columns = false"]
    if lines and lines[-1] != "":
        lines.append("")
    for name, col in contract.columns.items():
        lines.append(f"[columns.{key(name)}]")
        if name in comments:
            lines.append(f"# {comments[name]}")
        dt = dtype_name(col.dtype)
        if dt:
            lines.append(f'dtype = "{dt}"')
        for attr in ("required", "unique"):
            if getattr(col, attr):
                lines.append(f"{attr} = true")
        for attr in ("accepted", "min", "max", "pattern"):
            v = getattr(col, attr)
            if v is not None:
                lines.append(f"{attr} = {value(list(v) if attr == 'accepted' else v)}")
        lines.append("")
    for rule in contract.rules:
        if rule.sql is None:
            lines.append(f"# rule {rule.name!r} is a Polars expression and can't be written as TOML")
            continue
        lines += ["[[rules]]", f"name = {value(rule.name)}", f"sql = {value(rule.sql)}"]
        if rule.description:
            lines.append(f"description = {value(rule.description)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
