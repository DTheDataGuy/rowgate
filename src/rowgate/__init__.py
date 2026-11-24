"""rowgate: data contracts and quality gates for Polars."""
__version__ = "0.2.0"

from . import checks
from .checks import Check
from .contract import KEY, REASONS, Column, Contract, Rule, SplitResult, columns
from .explain import Explanation, explain
from .frames import read_duckdb, read_file, to_polars
from .gate import CheckResult, Gate, GateFailed, Report
from .history import QuarantineStore, RunRecord, replay
from .infer import infer, infer_toml
from .pipeline import Outcome, validate

__all__ = ["KEY", "REASONS", "Check", "CheckResult", "Column", "Contract", "Explanation", "Gate", "GateFailed",
           "Outcome", "QuarantineStore", "Report", "Rule", "RunRecord", "SplitResult", "checks", "columns",
           "explain", "infer", "infer_toml", "read_duckdb", "read_file", "replay", "to_polars", "validate"]
