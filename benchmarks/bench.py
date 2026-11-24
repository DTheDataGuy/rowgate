"""Benchmark rowgate, Pandera and Pointblank on the same data and the same rules.

    pip install rowgate "pandera[polars]" pointblank numpy psutil
    python benchmarks/bench.py --rows 100000 1000000 5000000

Every library runs in a fresh subprocess. Time is the median of the repeats; memory is
the peak resident memory above the level before validation started, sampled every 2 ms.
Two scenarios: "dirty" has about 1.2% bad rows spread across every rule, "clean" has none. The task
is the one rowgate is built for: validate a batch and hand back the rows that failed.

* rowgate:    Contract.split(df) -> valid and quarantine frames, every reason per row
* Pandera:    DataFrameSchema.validate(df, lazy=True) -> SchemaErrors with failure cases
* Pointblank: Validate(...).interrogate() then get_sundered_data("fail")

Rules (identical across libraries; nulls pass range/set/regex checks, as in SQL):
order_id not null and unique, customer not null, amount between 0 and 10,000,
status in a 4-value set, sku matches ^[A-Z]{3}-\\d{4}$.
"""
from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
STATUSES = ["open", "shipped", "delivered", "returned"]
SKU = r"^[A-Z]{3}-\d{4}$"


def make_data(n: int, path: Path, dirty: bool = True) -> None:
    import numpy as np
    import polars as pl

    rng = np.random.default_rng(42)
    rate = 1.0 if dirty else 0.0
    ids = np.arange(n, dtype=np.int64)
    dup = rng.random(n) < 0.002 * rate
    ids[dup] = rng.integers(0, n, dup.sum())
    amount = np.minimum(rng.gamma(2.0, 120.0, n), 9_999.0)
    amount[rng.random(n) < 0.003 * rate] *= -1
    amount[rng.random(n) < 0.001 * rate] *= 1000
    status = np.array(STATUSES)[rng.integers(0, 4, n)].astype(object)
    status[rng.random(n) < 0.002 * rate] = "Shipped "
    letters = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    sku = np.char.add(np.char.add(np.char.add(letters[rng.integers(0, 26, n)], letters[rng.integers(0, 26, n)]),
                                  letters[rng.integers(0, 26, n)]),
                      np.char.add("-", np.char.zfill(rng.integers(0, 10_000, n).astype(str), 4))).astype(object)
    sku[rng.random(n) < 0.002 * rate] = "BAD"
    customer = np.char.add("c", rng.integers(0, n // 10 + 1, n).astype(str)).astype(object)
    customer[rng.random(n) < 0.002 * rate] = None
    pl.DataFrame({"order_id": ids, "customer": customer, "amount": amount, "status": status, "sku": sku},
                 schema={"order_id": pl.Int64, "customer": pl.Utf8, "amount": pl.Float64, "status": pl.Utf8,
                         "sku": pl.Utf8}).write_parquet(path)


# ------------------------------------------------------------------ runners (in subprocess)
def run_rowgate(df):
    import polars as pl

    import rowgate as rg
    c = rg.Contract(columns={
        "order_id": rg.Column(pl.Int64, required=True, unique=True),
        "customer": rg.Column(pl.Utf8, required=True),
        "amount": rg.Column(pl.Float64, min=0, max=10_000),
        "status": rg.Column(pl.Utf8, accepted=STATUSES),
        "sku": rg.Column(pl.Utf8, pattern=SKU),
    })
    res = c.split(df)
    return res.quarantine.height


def run_pandera(df):
    import pandera.polars as pa
    import polars as pl
    schema = pa.DataFrameSchema({
        "order_id": pa.Column(pl.Int64, nullable=False, unique=True),
        "customer": pa.Column(pl.Utf8, nullable=False),
        "amount": pa.Column(pl.Float64, pa.Check.in_range(0, 10_000), nullable=True),
        "status": pa.Column(pl.Utf8, pa.Check.isin(STATUSES), nullable=True),
        "sku": pa.Column(pl.Utf8, pa.Check.str_matches(SKU), nullable=True),
    })
    try:
        schema.validate(df, lazy=True)
        return 0
    except pa.errors.SchemaErrors as e:
        fc = e.failure_cases
        return fc.height  # failure cases (one per failing value), not rows


def run_pointblank(df):
    import pointblank as pb
    v = (pb.Validate(data=df)
         .col_vals_not_null(columns=["order_id", "customer"])
         .rows_distinct(columns_subset=["order_id"])
         .col_vals_between(columns="amount", left=0, right=10_000, na_pass=True)
         .col_vals_in_set(columns="status", set=STATUSES)
         .col_vals_regex(columns="sku", pattern=SKU, na_pass=True)
         .interrogate())
    return v.get_sundered_data(type="fail").height


RUNNERS = {"rowgate": run_rowgate, "pandera": run_pandera, "pointblank": run_pointblank}


def child(lib: str, path: str) -> None:
    import threading

    import polars as pl
    import psutil
    proc = psutil.Process()
    df = pl.read_parquet(path)
    RUNNERS[lib](df.head(1000))  # warm imports and caches outside the timed section
    base = proc.memory_info().rss
    peak = [base]
    done = threading.Event()

    def sample() -> None:  # resident memory every 2 ms while the library works
        while not done.is_set():
            peak[0] = max(peak[0], proc.memory_info().rss)
            done.wait(0.002)

    t = threading.Thread(target=sample, daemon=True)
    t.start()
    t0 = time.perf_counter()
    found = RUNNERS[lib](df)
    secs = time.perf_counter() - t0
    done.set()
    t.join()
    mb = 1024 * 1024
    print(json.dumps({"seconds": secs, "extra_mb": (peak[0] - base) / mb, "found": found}))


def versions() -> dict[str, str]:
    from importlib.metadata import version
    out = {"python": platform.python_version(), "machine": f"{platform.machine()} {platform.system()}"}
    for pkg in ("polars", "rowgate", "pandera", "pointblank"):
        try:
            out[pkg] = version(pkg)
        except Exception:  # noqa: BLE001
            out[pkg] = "not installed"
    return out


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child(sys.argv[2], sys.argv[3])
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, nargs="+", default=[100_000, 1_000_000, 5_000_000])
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--libs", nargs="+", default=list(RUNNERS))
    ap.add_argument("--out", type=Path, default=HERE / "results.json")
    a = ap.parse_args()
    results = {"versions": versions(), "cpus": __import__("os").cpu_count(), "runs": []}
    for scenario, n in [(s, n) for s in ("dirty", "clean") for n in a.rows]:
        path = HERE / f".data_{scenario}_{n}.parquet"
        if not path.exists():
            make_data(n, path, dirty=scenario == "dirty")
        for lib in a.libs:
            samples = []
            for _ in range(a.repeat):
                p = subprocess.run([sys.executable, __file__, "--child", lib, str(path)], capture_output=True,
                                   text=True, timeout=1800)
                if p.returncode:
                    print(f"{lib} failed at {n:,} rows:\n{p.stderr[-2000:]}", file=sys.stderr)
                    break
                samples.append(json.loads(p.stdout.strip().splitlines()[-1]))
            if not samples:
                continue
            row = {"scenario": scenario, "lib": lib, "rows": n,
                   "seconds": statistics.median(s["seconds"] for s in samples),
                   "extra_mb": statistics.median(s["extra_mb"] for s in samples), "found": samples[0]["found"]}
            results["runs"].append(row)
            print(f"{scenario:<6}{n:>10,}  {lib:<11} {row['seconds']:8.3f}s  +{row['extra_mb']:7.0f} MB  "
                  f"found {row['found']:,}")
    a.out.write_text(json.dumps(results, indent=2) + "\n")
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
