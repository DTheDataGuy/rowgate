"""Remember quarantined rows across runs.

A :class:`QuarantineStore` is a folder of Parquet files. Each time you record a
:class:`~rowgate.SplitResult` it notes which quarantined rows are new, which are back
again, and which earlier rows now pass. That lets a pipeline say "40 rows have been
failing for 3 days" instead of starting from zero every run.

Rows are matched across runs by the contract key (``Contract.key``, or the first
``unique`` column). Without a key, rows are matched by their content, so a fixed row
can only be marked resolved through :func:`replay`.

The store expects one writer at a time, which is how a scheduled pipeline runs.
"""
from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from .contract import KEY, REASONS, Contract, SplitResult
from .frames import to_polars

_ISSUES_SCHEMA = {
    KEY: pl.Utf8, "reasons": pl.List(pl.Utf8), "status": pl.Utf8,
    "first_seen": pl.Datetime("us", "UTC"), "last_seen": pl.Datetime("us", "UTC"),
    "first_run": pl.Utf8, "last_run": pl.Utf8, "times_seen": pl.Int64,
    "resolved_at": pl.Datetime("us", "UTC"), "resolved_by": pl.Utf8, "row": pl.Utf8,
}
_RUNS_SCHEMA = {
    "run_id": pl.Utf8, "at": pl.Datetime("us", "UTC"), "source": pl.Utf8, "input_rows": pl.Int64,
    "valid_rows": pl.Int64, "quarantined_rows": pl.Int64, "new": pl.Int64, "recurring": pl.Int64,
    "resolved": pl.Int64, "open_after": pl.Int64, "passed": pl.Boolean, "top_reason": pl.Utf8,
}


@dataclass
class RunRecord:
    """What changed in the quarantine during one run."""

    run_id: str
    at: datetime
    input_rows: int
    quarantined_rows: int
    new: int
    recurring: int
    resolved: int
    open_after: int

    def __str__(self) -> str:
        return (f"quarantine history: {self.new:,} new, {self.recurring:,} still failing, "
                f"{self.resolved:,} fixed, {self.open_after:,} open")


class QuarantineStore:
    """A folder that keeps quarantine history across pipeline runs.

    >>> store = rg.QuarantineStore("quality/orders")
    >>> store.record(contract.split(df))
    >>> print(store.summary())
    """

    def __init__(self, path: str | Path, key: str | Sequence[str] | None = None) -> None:
        self.path = Path(path)
        self.key = [key] if isinstance(key, str) else list(key) if key else None
        self._issues_path = self.path / "issues.parquet"
        self._runs_path = self.path / "runs.parquet"
        self._rows_dir = self.path / "quarantine"

    # ----------------------------------------------------------------- reading
    def issues(self) -> pl.DataFrame:
        """Every row the store has seen in quarantine, open and resolved."""
        if not self._issues_path.exists():
            return pl.DataFrame(schema=_ISSUES_SCHEMA)
        return pl.read_parquet(self._issues_path)

    def runs(self) -> pl.DataFrame:
        """One row per recorded run, oldest first."""
        if not self._runs_path.exists():
            return pl.DataFrame(schema=_RUNS_SCHEMA)
        return pl.read_parquet(self._runs_path)

    def open_issues(self, as_of: datetime | date | None = None) -> pl.DataFrame:
        """Rows still in quarantine, oldest first, with ``days_open``."""
        now = _as_dt(as_of)
        return (self.issues().filter(pl.col("status") == "open")
                .with_columns(((pl.lit(now) - pl.col("first_seen")).dt.total_hours() // 24).cast(pl.Int64)
                              .alias("days_open"))
                .sort("first_seen"))

    def quarantined_rows(self, run_id: str | None = None) -> pl.DataFrame:
        """The full quarantined rows of a run (the latest run by default), ready to fix and replay."""
        runs = self.runs()
        if runs.is_empty():
            raise LookupError(f"no runs recorded in {self.path}")
        rid = run_id or runs["run_id"][-1]
        return pl.read_parquet(self._rows_dir / f"{rid}.parquet")

    def summary(self, as_of: datetime | date | None = None, stale_days: int = 3) -> str:
        """A few plain sentences for a log, a Slack message or a run summary."""
        runs, open_ = self.runs(), self.open_issues(as_of)
        if runs.is_empty():
            return "quarantine history: no runs recorded yet"
        first = runs["at"].min()
        lines = [f"quarantine history: {_n(runs.height, 'run')} since {first:%Y-%m-%d}."]
        if open_.is_empty():
            lines.append("Nothing is in quarantine.")
        else:
            stale = open_.filter(pl.col("days_open") >= stale_days)
            oldest = open_.row(0, named=True)
            verb = "is" if open_.height == 1 else "are"
            msg = f"{_n(open_.height, 'row')} {verb} in quarantine"
            if stale.height:
                msg += (f"; {stale.height:,} {'has' if stale.height == 1 else 'have'} "
                        f"been failing for {stale_days}+ days")
            msg += (f" (oldest: {_n(oldest['days_open'], 'day')}, "
                    f"first seen {oldest['first_seen']:%Y-%m-%d}).")
            lines.append(msg)
            top = (open_.select(pl.col("reasons").explode().alias("reason")).group_by("reason").len()
                   .sort(["len", "reason"], descending=[True, False]).head(3))
            lines.append("Most common open reasons: " + ", ".join(f"{r} ({n:,})" for r, n in top.iter_rows()) + ".")
        last = runs.row(-1, named=True)
        lines.append(f"Last run: {last['new']:,} new, {last['recurring']:,} still failing, "
                     f"{last['resolved']:,} fixed.")
        return " ".join(lines)

    def aging(self, as_of: datetime | date | None = None) -> pl.DataFrame:
        """Open rows bucketed by how long they've been failing."""
        buckets = ["< 1 day", "1-2 days", "3-6 days", "7-29 days", "30+ days"]
        open_ = self.open_issues(as_of)
        b = (pl.when(pl.col("days_open") < 1).then(pl.lit(buckets[0]))
             .when(pl.col("days_open") < 3).then(pl.lit(buckets[1]))
             .when(pl.col("days_open") < 7).then(pl.lit(buckets[2]))
             .when(pl.col("days_open") < 30).then(pl.lit(buckets[3])).otherwise(pl.lit(buckets[4])))
        counts = dict(open_.group_by(b.alias("age")).len().iter_rows()) if open_.height else {}
        return pl.DataFrame({"age": buckets, "rows": [counts.get(x, 0) for x in buckets]})

    # ----------------------------------------------------------------- writing
    def record(self, result: SplitResult, *, at: datetime | date | None = None, run_id: str | None = None,
               source: str | None = None, passed: bool | None = None) -> RunRecord:
        """Add a run. Returns what changed: new, recurring and fixed rows."""
        now = _as_dt(at)
        rid = run_id or f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        key_cols = self._key_columns(result.contract, result.quarantine)
        q = self.with_keys(result.quarantine, result.contract)
        cur = (q.select(KEY, pl.col(REASONS).alias("reasons"),
                        pl.struct([c for c in q.columns if c not in (KEY, REASONS)]).struct.json_encode().alias("row"))
               .unique(KEY, keep="first", maintain_order=True))
        valid_keys: set[str] = set()
        if key_cols and all(c in result.valid.columns for c in key_cols):
            valid_keys = set(result.valid.select(_key_expr(key_cols)).to_series().to_list())

        old = self.issues()
        old_status = dict(zip(old[KEY].to_list(), old["status"].to_list(), strict=True))
        cur_keys = cur[KEY].to_list()
        recurring_keys = {k for k in cur_keys if old_status.get(k) == "open"}
        fixed_keys = {k for k, s in old_status.items()
                      if s == "open" and k.split("#dup:")[0] in valid_keys} - set(cur_keys)

        ts = pl.lit(now, pl.Datetime("us", "UTC"))
        prior = old.filter(pl.col(KEY).is_in(list(recurring_keys)))
        seen = cur.join(prior.select(KEY, "first_seen", "first_run", "times_seen"), on=KEY, how="left").select(
            KEY, "reasons", pl.lit("open").alias("status"),
            pl.col("first_seen").fill_null(ts), ts.alias("last_seen"),
            pl.col("first_run").fill_null(pl.lit(rid)), pl.lit(rid).alias("last_run"),
            (pl.col("times_seen").fill_null(0) + 1).cast(pl.Int64),
            pl.lit(None, pl.Datetime("us", "UTC")).alias("resolved_at"), pl.lit(None, pl.Utf8).alias("resolved_by"),
            "row")
        rest = old.filter(~pl.col(KEY).is_in(cur_keys)).with_columns(
            pl.when(pl.col(KEY).is_in(list(fixed_keys))).then(pl.lit("resolved")).otherwise("status").alias("status"),
            pl.when(pl.col(KEY).is_in(list(fixed_keys))).then(ts).otherwise("resolved_at").alias("resolved_at"),
            pl.when(pl.col(KEY).is_in(list(fixed_keys))).then(pl.lit("batch")).otherwise("resolved_by")
            .alias("resolved_by"))
        issues = pl.concat([rest, seen.cast(_ISSUES_SCHEMA)], how="vertical")  # type: ignore[arg-type]
        open_after = int((issues["status"] == "open").sum())

        top = result.reason_counts()
        rec = RunRecord(rid, now, result.input_rows, result.quarantine.height,
                        len(cur_keys) - len(recurring_keys), len(recurring_keys), len(fixed_keys), open_after)
        run_row = {**asdict(rec), "source": source, "valid_rows": result.valid.height, "passed": passed,
                   "top_reason": top["reason"][0] if top.height else None}
        runs = pl.concat([self.runs(), pl.DataFrame([run_row], schema=_RUNS_SCHEMA)], how="vertical")

        self._rows_dir.mkdir(parents=True, exist_ok=True)
        _atomic_parquet(q, self._rows_dir / f"{rid}.parquet")
        _atomic_parquet(issues, self._issues_path)
        _atomic_parquet(runs, self._runs_path)
        return rec

    def resolve(self, keys: Iterable[str] | pl.DataFrame, *, by: str = "manual",
                at: datetime | date | None = None) -> int:
        """Mark rows as fixed. ``keys`` are ``_rowgate_key`` values or a frame that has that column."""
        if isinstance(keys, pl.DataFrame):
            keys = keys[KEY].to_list()
        wanted = list(set(keys))
        issues = self.issues()
        hit = (pl.col(KEY).is_in(wanted)) & (pl.col("status") == "open")
        n = int(issues.select(hit.sum()).item())
        if n:
            ts = pl.lit(_as_dt(at), pl.Datetime("us", "UTC"))
            issues = issues.with_columns(
                pl.when(hit).then(pl.lit("resolved")).otherwise("status").alias("status"),
                pl.when(hit).then(ts).otherwise("resolved_at").alias("resolved_at"),
                pl.when(hit).then(pl.lit(by)).otherwise("resolved_by").alias("resolved_by"))
            _atomic_parquet(issues, self._issues_path)
        return n

    # ----------------------------------------------------------------- keys
    def _key_columns(self, contract: Contract | None, df: pl.DataFrame) -> list[str]:
        cols = self.key or (contract.key_columns if contract else [])
        return [c for c in cols if c in df.columns] if cols and all(c in df.columns for c in cols) else []

    def with_keys(self, df: pl.DataFrame, contract: Contract | None = None) -> pl.DataFrame:
        """Add the ``_rowgate_key`` column the store uses to match rows across runs."""
        if KEY in df.columns:
            return df
        cols = self._key_columns(contract, df)
        data_cols = [c for c in df.columns if c not in (REASONS, KEY)]
        if cols:
            out = df.with_columns(_key_expr(cols))
            if REASONS in df.columns:  # a repeated key is its own issue, separate from the row that was kept
                dup = pl.col(REASONS).list.eval(pl.element().str.ends_with(":duplicate")).list.any()
                out = out.with_columns(pl.when(dup).then(pl.col(KEY) + "#dup:" + _content_hash(out, data_cols))
                                       .otherwise(pl.col(KEY)).alias(KEY))
            return out
        return df.with_columns(("row:" + _content_hash(df, data_cols)).alias(KEY))


def replay(contract: Contract, fixed: Any, store: QuarantineStore | None = None,
           at: datetime | date | None = None) -> SplitResult:
    """Re-check rows after they've been corrected.

    ``fixed`` is usually the quarantine frame with values edited. Rows that now pass come
    back in ``valid`` and are marked resolved in ``store``; rows that still fail stay in
    quarantine with their updated reasons.
    """
    df = to_polars(fixed)
    df = df.drop([c for c in (REASONS,) if c in df.columns])
    tmp = "__rowgate_replay_key"
    if store is not None and KEY not in df.columns:
        df = store.with_keys(df, contract)
    if KEY in df.columns:
        df = df.rename({KEY: tmp})
    result = contract.split(df)
    if tmp in df.columns:
        if store is not None:
            store.resolve(result.valid[tmp].to_list(), by="replay", at=at)
        result = SplitResult(result.valid.drop(tmp), result.quarantine.rename({tmp: KEY}),
                             result.input_rows, contract)
    return result


def _n(k: int, word: str) -> str:
    return f"{k:,} {word}" + ("" if k == 1 else "s")


def _key_expr(cols: Sequence[str]) -> pl.Expr:
    return pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("<null>") for c in cols], separator="|").alias(KEY)


def _content_hash(df: pl.DataFrame, cols: Sequence[str]) -> pl.Series:
    encoded = df.select(pl.struct(list(cols)).struct.json_encode()).to_series().to_list()
    return pl.Series([hashlib.sha1(s.encode()).hexdigest()[:16] for s in encoded], dtype=pl.Utf8)


def _as_dt(v: datetime | date | None) -> datetime:
    if v is None:
        return datetime.now(UTC).replace(microsecond=0)
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    return datetime(v.year, v.month, v.day, tzinfo=UTC)


def _atomic_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.write_parquet(tmp)
    tmp.replace(path)


__all__ = ["QuarantineStore", "RunRecord", "replay"]
