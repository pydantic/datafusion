#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "matplotlib>=3.10",
#   "numpy>=2.0",
# ]
# ///
"""
Generate the four chart PNGs for the Marp deck from the benchmarks/results/
JSONs.  Run as a self-contained uv script:

    uv run slides/make_plots.py

(or just `./slides/make_plots.py` once the file is executable).
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"
OUT = Path(__file__).parent / "img"
OUT.mkdir(exist_ok=True, parents=True)


def med(lst):
    vals = [x["elapsed"] if isinstance(x, dict) else x for x in lst]
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def load(p):
    d = json.load(open(p))
    return {q["query"]: med(q["iterations"]) for q in d["queries"]}


# Color palette: pushdown=off subdued, pushdown=on (regression) red-tinted, PR brand-blue
C_OFF = "#7a8b99"
C_ON = "#d4504e"
C_PR = "#2e86c1"

plt.rcParams.update({
    "font.size": 16,
    "axes.titlesize": 22,
    "axes.labelsize": 18,
    "xtick.labelsize": 16,
    "ytick.labelsize": 16,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "axes.axisbelow": True,
    "savefig.bbox": "tight",
    "savefig.facecolor": "white",
    "figure.facecolor": "white",
})


def trio_bar(ax, labels, off, on, pr, ylabel="time (ms)"):
    """Grouped bars: pushdown=off / pushdown=on / PR.

    No chart title (slide titles supply context).  Legend sits above the axes
    in a single horizontal row.  Y-axis has 15 % headroom so value labels
    never collide with the legend.
    """
    import numpy as np

    x = np.arange(len(labels))
    w = 0.27
    b1 = ax.bar(x - w, off, width=w, label="main pushdown=off", color=C_OFF)
    b2 = ax.bar(x,     on,  width=w, label="main pushdown=on",  color=C_ON)
    b3 = ax.bar(x + w, pr,  width=w, label="PR pushdown=on",    color=C_PR)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel(ylabel)
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=3,
        frameon=False,
        fontsize=14,
    )
    all_heights = list(off) + list(on) + list(pr)
    if all_heights:
        ymax = max(all_heights)
        for bars in (b1, b2, b3):
            for r in bars:
                h = r.get_height()
                ax.text(
                    r.get_x() + r.get_width() / 2,
                    h + ymax * 0.015,
                    f"{h:,.0f}",
                    ha="center", va="bottom", fontsize=12,
                )
        ax.set_ylim(0, ymax * 1.15)


def render_pair(off_path, on_path, pr_path, query_name, query_label,
                total_label, out_name):
    off_d = load(RESULTS / off_path)
    on_d  = load(RESULTS / on_path)
    pr_d  = load(RESULTS / pr_path)
    keys  = sorted(off_d, key=lambda s: int(s.split()[-1]))
    off_total = sum(off_d[k] for k in keys)
    on_total  = sum(on_d[k]  for k in keys)
    pr_total  = sum(pr_d[k]  for k in keys)
    fig, ax = plt.subplots(figsize=(10, 3.8))
    trio_bar(
        ax,
        labels=[query_label, total_label],
        off=[off_d[query_name], off_total],
        on=[on_d[query_name],   on_total],
        pr=[pr_d[query_name],   pr_total],
    )
    plt.savefig(OUT / out_name, dpi=140)
    plt.close()
    return off_total, on_total, pr_total


print("ClickBench SSD →", render_pair(
    "MAIN-nopushdown/clickbench_partitioned.json",
    "MAIN-pushdown/clickbench_partitioned.json",
    "PR-pushdown/clickbench_partitioned.json",
    query_name="Query 23",
    query_label="Q23 (URL LIKE '%google%')",
    total_label="Total (43 q, sum of medians)",
    out_name="clickbench_nolat.png",
))

print("TPC-DS SSD    →", render_pair(
    "MAIN-nopushdown/tpcds_sf1.json",
    "MAIN-pushdown/tpcds_sf1.json",
    "PR-pushdown/tpcds_sf1.json",
    query_name="Query 64",
    query_label="Q64",
    total_label="Total (99 q, sum of medians)",
    out_name="tpcds_nolat.png",
))

print("TPC-H SSD     →", render_pair(
    "MAIN-nopushdown/tpch_sf1.json",
    "MAIN-pushdown/tpch_sf1.json",
    "PR-pushdown/tpch_sf1.json",
    query_name="Query 9",
    query_label="Q9 (worst loss)",
    total_label="Total (22 q, sum of medians)",
    out_name="tpch_nolat.png",
))

print("TPC-H S3      →", render_pair(
    "MAIN-nopushdown-lat/tpch_sf1.json",
    "MAIN-pushdown-lat/tpch_sf1.json",
    "PR-pushdown-lat/tpch_sf1.json",
    query_name="Query 9",
    query_label="Q9 (was 3.1× loss on SSD)",
    total_label="Total (22 q, sum of medians)",
    out_name="tpch_lat.png",
))

print(f"\nWrote 4 PNGs to {OUT}")
