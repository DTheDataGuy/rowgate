"""Validate a 454-row sample of Seattle building permits with rowgate.

    python examples/seattle_permits.py
"""
from datetime import date
from pathlib import Path

import polars as pl

import rowgate as rg
from rowgate import checks as ck

HERE = Path(__file__).parent

# The open-data API returns every field as text; read it that way and type it below.
FIELDS = ["permitnum", "permitclassmapped", "permittypemapped", "statuscurrent", "applieddate", "issueddate",
          "completeddate", "estprojectcost", "housingunits", "originalzip", "latitude", "longitude"]
raw = pl.read_ndjson(HERE / "seattle_permits_sample.ndjson", schema=dict.fromkeys(FIELDS, pl.Utf8))
permits = raw.select(
    pl.col("permitnum").alias("permit_num"),
    pl.col("permitclassmapped").alias("permit_class"),
    pl.col("statuscurrent").alias("status"),
    *[pl.col(src).str.to_date("%Y-%m-%dT%H:%M:%S%.3f", strict=False).alias(dst)
      for src, dst in [("applieddate", "applied_date"), ("issueddate", "issued_date"),
                       ("completeddate", "completed_date")]],
    pl.col("estprojectcost").cast(pl.Float64).alias("est_cost"),
    pl.col("originalzip").alias("zip"),
    pl.col("latitude").cast(pl.Float64),
    pl.col("longitude").cast(pl.Float64),
)

contract = rg.Contract.from_toml(HERE / "permits_contract.toml")
result = contract.split(permits)

report = rg.Gate([
    ck.schema_matches(contract),
    ck.reconciles(result),
    ck.quarantine_rate(result, max_pct=5),
    ck.unique("permit_num"),
    ck.not_null(["permit_num", "permit_class"]),
    ck.null_rate("applied_date", max_pct=20, severity="warn"),
    ck.null_rate("zip", max_pct=10, severity="warn"),
    ck.freshness("applied_date", max_age_days=30, as_of=date(2026, 10, 20), severity="warn"),
]).run(result.valid, source="seattle_permits_sample.ndjson")

print(report)
print()
print(result.reason_counts())
print(result.quarantine.select("permit_num", "applied_date", "issued_date", "completed_date", rg.REASONS).head(5))
