from datetime import date

import polars as pl

import rowgate as rg

C = rg.Contract(columns={"id": rg.Column(pl.Int64, required=True, unique=True), "amt": rg.Column(pl.Float64, min=0)})


def test_history_tracks_new_recurring_and_fixed(tmp_path):
    store = rg.QuarantineStore(tmp_path)
    r1 = store.record(C.split(pl.DataFrame({"id": [1, 2, 3, 3], "amt": [1.0, -1.0, -2.0, 5.0]})),
                      at=date(2026, 11, 18))
    assert (r1.new, r1.recurring, r1.resolved, r1.open_after) == (3, 0, 0, 3)
    r2 = store.record(C.split(pl.DataFrame({"id": [2, 3, 4], "amt": [-1.0, 2.0, -9.0]})), at=date(2026, 11, 19))
    # 2 still fails; 3 now passes (and its duplicate is gone); 4 is new
    assert (r2.new, r2.recurring, r2.resolved, r2.open_after) == (1, 1, 2, 2)
    open_ = store.open_issues(date(2026, 11, 21))
    assert open_["_rowgate_key"].to_list() == ["2", "4"]
    assert open_["days_open"].to_list() == [3, 2]
    assert open_.filter(pl.col("_rowgate_key") == "2")["times_seen"].item() == 2
    s = store.summary(date(2026, 11, 21))
    assert "2 rows are in quarantine; 1 has been failing for 3+ days" in s
    assert store.aging(date(2026, 11, 21))["rows"].to_list() == [0, 1, 1, 0, 0]
    assert store.runs().height == 2


def test_replay_resolves_fixed_rows(tmp_path):
    store = rg.QuarantineStore(tmp_path)
    store.record(C.split(pl.DataFrame({"id": [1, 2, 4], "amt": [1.0, -1.0, -9.0]})), at=date(2026, 11, 18))
    q = store.quarantined_rows()
    assert rg.KEY in q.columns and rg.REASONS in q.columns
    fixed = q.with_columns(pl.when(pl.col("id") == 2).then(1.0).otherwise(pl.col("amt")).alias("amt"))
    res = rg.replay(C, fixed, store)
    assert res.valid.columns == ["id", "amt"] and res.valid["id"].to_list() == [2]
    assert res.quarantine["id"].to_list() == [4]
    issues = store.issues()
    assert issues.filter(pl.col("status") == "resolved")["resolved_by"].to_list() == ["replay"]


def test_keyless_contract_matches_by_content(tmp_path):
    c = rg.Contract(columns={"amt": rg.Column(pl.Float64, min=0)})
    store = rg.QuarantineStore(tmp_path)
    df = pl.DataFrame({"amt": [-1.0, 2.0]})
    store.record(c.split(df), at=date(2026, 11, 1))
    r = store.record(c.split(df), at=date(2026, 11, 2))
    assert (r.new, r.recurring) == (0, 1)
    assert store.open_issues()["_rowgate_key"][0].startswith("row:")


def test_dashboard_renders(tmp_path):
    from rowgate.dashboard import render
    store = rg.QuarantineStore(tmp_path)
    store.record(C.split(pl.DataFrame({"id": [1, 2], "amt": [1.0, -1.0]})), at=date(2026, 11, 18))
    page = render(store, as_of=date(2026, 11, 21))
    assert page.startswith("<!doctype html>") and "amt:min" in page and "failing for 3+ days" in page
