"""Draw benchmarks/results.json as docs/assets/benchmark.png."""
import json
from pathlib import Path

import matplotlib.pyplot as plt

HERE = Path(__file__).parent
data = json.loads((HERE / "results.json").read_text())
runs = [r for r in data["runs"] if r["rows"] == max(x["rows"] for x in data["runs"])]
libs = ["rowgate", "pandera", "pointblank"]
colors = {"rowgate": "#127a6b", "pandera": "#9aa3a0", "pointblank": "#c5ccc9"}
fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), dpi=160)
for ax, metric, label in [(axes[0], "seconds", "Seconds (lower is better)"),
                          (axes[1], "extra_mb", "Extra memory, MB (lower is better)")]:
    for i, scen in enumerate(["clean", "dirty"]):
        for j, lib in enumerate(libs):
            r = next(x for x in runs if x["lib"] == lib and x["scenario"] == scen)
            x = i * 4 + j
            ax.bar(x, r[metric], color=colors[lib], label=lib if i == 0 else None)
            ax.text(x, r[metric], f"{r[metric]:.2f}" if metric == "seconds" else f"{r[metric]:.0f}",
                    ha="center", va="bottom", fontsize=8)
    ax.set_xticks([1, 5], ["clean batch", "batch with ~1.2% bad rows"])
    ax.set_title(label, fontsize=10, loc="left")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=8)
axes[0].legend(frameon=False, fontsize=8)
n = runs[0]["rows"]
v = data["versions"]
fig.suptitle(f"Validate {n:,} rows and return the failures  ·  polars {v['polars']}, {data['cpus']} CPUs",
             fontsize=10, x=0.01, ha="left")
fig.tight_layout()
fig.savefig(HERE.parent / "docs" / "assets" / "benchmark.png")
