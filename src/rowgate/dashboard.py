"""A single-file HTML page for a :class:`~rowgate.QuarantineStore`.

``rowgate history quality/orders --html quality.html`` writes a page with no external
assets: open rows, how long they've been failing, the quarantine trend per run, the
most common reasons and the oldest open rows. Attach it to a CI run or publish it on
GitHub Pages.
"""
from __future__ import annotations

import html
from datetime import date, datetime
from typing import Any

import polars as pl

from .history import QuarantineStore

_CSS = """
:root{--bg:#f7f7f4;--card:#fff;--ink:#1d2321;--muted:#5d6763;--line:#e3e4df;--ok:#2f7d5b;--bad:#b4442f;
--warn:#a86b12;--accent:#1f5f8b}
@media (prefers-color-scheme:dark){:root{--bg:#141716;--card:#1c201f;--ink:#e8ebe9;--muted:#9aa39f;--line:#2c3230;
--ok:#5fbf92;--bad:#e2775f;--warn:#d9a441;--accent:#6fb1df}}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);
color:var(--ink)}main{max-width:1100px;margin:0 auto;padding:32px 20px 48px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:0 0 12px;color:var(--muted);font-weight:600}
.sub{color:var(--muted);margin:0 0 24px}.grid{display:grid;gap:16px;grid-template-columns:repeat(4,1fr)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px}
.kpi b{display:block;font-size:28px;font-variant-numeric:tabular-nums}.kpi span{color:var(--muted);font-size:13px}
.two{display:grid;gap:16px;grid-template-columns:2fr 1fr;margin-top:16px}.full{margin-top:16px}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:6px 8px;
border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-weight:600}
td.n{text-align:right;font-variant-numeric:tabular-nums}code{font-size:12px;word-break:break-all}
td.k code,td.d{white-space:nowrap;word-break:normal}
.bar{height:10px;border-radius:3px;background:var(--bad)}.lbl{display:flex;justify-content:space-between;font-size:13px}
.summary{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--accent);border-radius:8px;
padding:12px 16px;margin-bottom:16px}footer{color:var(--muted);font-size:12px;margin-top:28px}
footer a{color:inherit}svg text{fill:var(--muted);font-size:11px}
@media (max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}.two{grid-template-columns:1fr}}
"""


def _e(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def _trend_svg(runs: pl.DataFrame, width: int = 640, height: int = 180) -> str:
    if runs.is_empty():
        return "<p>No runs yet.</p>"
    r = runs.tail(30)
    n = r.height
    rates = [100 * q / i if i else 0 for q, i in zip(r["quarantined_rows"], r["input_rows"], strict=True)]
    opens = r["open_after"].to_list()
    top = max(max(opens), 1)
    top = top + (-top % 2)  # even, so the midpoint tick is a whole number
    pad_l, pad_b, pad_t = 34, 22, 8
    w, h = width - pad_l - 8, height - pad_b - pad_t
    bw = w / n
    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" '
             f'aria-label="Open quarantined rows per run">']
    for frac in (0, 0.5, 1):
        y = pad_t + h - frac * h
        parts.append(f'<line x1="{pad_l}" x2="{width - 8}" y1="{y:.1f}" y2="{y:.1f}" stroke="var(--line)"/>'
                     f'<text x="{pad_l - 6}" y="{y + 4:.1f}" text-anchor="end">{round(top * frac):,}</text>')
    for i, (o, rate, at) in enumerate(zip(opens, rates, r["at"], strict=True)):
        bh = h * o / top
        x = pad_l + i * bw + bw * 0.15
        parts.append(f'<rect x="{x:.1f}" y="{pad_t + h - bh:.1f}" width="{bw * 0.7:.1f}" height="{bh:.1f}" rx="2" '
                     f'fill="var(--accent)"><title>{at:%Y-%m-%d %H:%M}: {o:,} open, {rate:.2f}% of batch '
                     f'quarantined</title></rect>')
    for i in (range(n) if n <= 10 else sorted({0, n // 2, n - 1})):
        x = pad_l + i * bw + bw / 2
        parts.append(f'<text x="{x:.1f}" y="{height - 6}" text-anchor="middle">{r["at"][i]:%b %d}</text>')
    parts.append("</svg>")
    return "".join(parts)


def render(store: QuarantineStore, *, title: str | None = None, as_of: datetime | date | None = None,
           max_rows: int = 50) -> str:
    runs, open_ = store.runs(), store.open_issues(as_of)
    aging = store.aging(as_of)
    stale = open_.filter(pl.col("days_open") >= 3).height
    last = runs.row(-1, named=True) if runs.height else None
    title = title or f"rowgate quality history: {store.path.name}"
    kpis = [(f"{open_.height:,}", "rows in quarantine"), (f"{stale:,}", "failing for 3+ days"),
            (f"{last['resolved']:,}" if last else "0", "fixed in the last run"),
            (f"{100 * last['quarantined_rows'] / last['input_rows']:.2f}%" if last and last["input_rows"] else "n/a",
             "of the last batch quarantined")]
    reasons = (open_.select(pl.col("reasons").explode().alias("reason")).group_by("reason").len()
               .sort(["len", "reason"], descending=[True, False]).head(8)) if open_.height else None
    out = [f"<!doctype html><html lang=en><head><meta charset=utf-8>"
           f"<meta name=viewport content='width=device-width,initial-scale=1'><title>{_e(title)}</title>"
           f"<style>{_CSS}</style></head><body><main>",
           f"<h1>{_e(title)}</h1><p class=sub>{runs.height:,} runs recorded"
           + (f", latest {last['at']:%Y-%m-%d %H:%M} UTC" if last else "") + "</p>",
           f"<div class=summary>{_e(store.summary(as_of).removeprefix('quarantine history: '))}</div><div class=grid>"]
    out += [f"<div class='card kpi'><b>{v}</b><span>{_e(k)}</span></div>" for v, k in kpis]
    out.append("</div><div class=two><div class=card><h2>Open rows after each run</h2>" + _trend_svg(runs)
               + "</div><div class=card><h2>How long rows have been failing</h2>")
    top_age = max(aging["rows"].max() or 1, 1)
    for age, n in aging.iter_rows():
        out.append(f"<div class=lbl><span>{_e(age)}</span><span>{n:,}</span></div>"
                   f"<div class=bar style='width:{100 * n / top_age:.0f}%;margin:2px 0 10px;"
                   f"opacity:{0.35 + 0.65 * n / top_age:.2f}'></div>")
    out.append("</div></div>")
    if reasons is not None and reasons.height:
        out.append("<div class='card full'><h2>Most common open reasons</h2><table><tr><th>Reason</th>"
                   "<th style=text-align:right>Rows</th></tr>")
        out += [f"<tr><td><code>{_e(r)}</code></td><td class=n>{k:,}</td></tr>" for r, k in reasons.iter_rows()]
        out.append("</table></div>")
    if open_.height:
        out.append(f"<div class='card full'><h2>Oldest open rows (first {min(max_rows, open_.height):,} of "
                   f"{open_.height:,})</h2><table><tr><th>Key</th><th>Reasons</th><th>First seen</th>"
                   "<th style=text-align:right>Days</th><th style=text-align:right>Runs</th><th>Row</th></tr>")
        for r in open_.head(max_rows).iter_rows(named=True):
            row = r["row"] if len(r["row"] or "") < 160 else r["row"][:157] + "..."
            out.append(f"<tr><td class=k><code>{_e(r['_rowgate_key'])}</code></td>"
                       f"<td>{_e(', '.join(r['reasons']))}</td><td class=d>{r['first_seen']:%Y-%m-%d}</td>"
                       f"<td class=n>{r['days_open']:,}</td>"
                       f"<td class=n>{r['times_seen']:,}</td><td><code>{_e(row)}</code></td></tr>")
        out.append("</table></div>")
    out.append("<footer>Generated by <a href='https://github.com/DTheDataGuy/rowgate'>rowgate</a> · "
               "The Data Guys™</footer></main></body></html>")
    return "".join(out)
