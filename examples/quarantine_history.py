"""Three days of the Seattle permits pipeline, with quarantine history.

    python examples/quarantine_history.py

Day 1 quarantines 7 rows. On day 2 the $25 billion cost typo is corrected upstream and
replayed. On day 3 the team decides sub-permit numbers such as 6112685-EX-002 are valid
and widens the pattern. The 3 permits issued before they were applied for are still
failing after 3 days, which is exactly what someone needs to chase with the source.
"""
import io
import shutil
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path

import polars as pl

import rowgate as rg

HERE = Path(__file__).parent
with redirect_stdout(io.StringIO()):  # reuse the typed frame from the permits example
    from seattle_permits import permits  # noqa: E402

STORE = Path("quality/seattle_permits")
shutil.rmtree(STORE, ignore_errors=True)
store = rg.QuarantineStore(STORE)
contract = rg.Contract.from_toml(HERE / "permits_contract.toml")

print("Day 1")
out = rg.validate(permits, contract, max_quarantine_pct=5, history=store, at=date(2026, 11, 18))
print(" ", out.run)

print("Day 2: the cost typo is fixed upstream and replayed")
q = store.quarantined_rows()
fixed = q.filter(pl.col(rg.REASONS).list.contains("est_cost:max")).with_columns(pl.lit(25_000_000.0).alias("est_cost"))
replayed = rg.replay(contract, fixed, store, at=date(2026, 11, 19))
print(f"  replayed {replayed.valid.height} row(s) into the valid set")
out = rg.validate(permits.with_columns(pl.when(pl.col("est_cost") > 1e10).then(25_000_000.0)
                                       .otherwise(pl.col("est_cost")).alias("est_cost")),
                  contract, max_quarantine_pct=5, history=store, at=date(2026, 11, 19))
print(" ", out.run)

print("Day 3: sub-permit numbers are valid, so the pattern is widened")
wider = rg.Contract({**contract.columns, "permit_num": rg.Column(pl.Utf8, required=True, unique=True,
                                                                 pattern=r"^\d{6,7}-[A-Z]{2}(-\d{3})?$")},
                    contract.rules)
out = rg.validate(permits.with_columns(pl.when(pl.col("est_cost") > 1e10).then(25_000_000.0)
                                       .otherwise(pl.col("est_cost")).alias("est_cost")),
                  wider, max_quarantine_pct=5, history=store, at=date(2026, 11, 21))
print(" ", out.run)
print()
print(store.summary(as_of=date(2026, 11, 21)))

from rowgate.dashboard import render  # noqa: E402

page = STORE / "dashboard.html"
page.write_text(render(store, title="Seattle building permits: quarantine history", as_of=date(2026, 11, 21)))
print(f"\nwrote {page}")
