#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot point-query benchmark comparison using only:
  Overall geometric mean time (sec): <value>

Example:
  python3 scripts/ch2_point_geomean_plot.py \
    --input 128K=benchmark_plots/500G128_49GColumnPointRes \
    --input 256K=benchmark_plots/500G256_49GColumnPointRes \
    --input 512K=benchmark_plots/500G512_49GColumnPointRes \
    -o benchmark_plots/figures_500G_compare/point_average_time.png
"""

import argparse
import re
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np

LABEL_ORDER = ["128K", "256K", "512K"]
ROW_COUNT_DENOM = 15_000_000.0
GEOMEAN_RE = re.compile(
    r"Overall\s+geometric\s+mean\s+time\s+\(sec\)\s*:\s*([\d.]+)",
    re.IGNORECASE,
)
ROWCOUNT_RE = re.compile(
    r"avg_result_row_count\s*=\s*([\d.]+)",
    re.IGNORECASE,
)


def parse_metrics(path: Path) -> Dict[str, float]:
    text = path.read_text(encoding="utf-8", errors="replace")
    geomean_match = GEOMEAN_RE.search(text)
    rowcount_match = ROWCOUNT_RE.search(text)

    if not geomean_match:
        raise SystemExit(
            f"Could not find 'Overall geometric mean time (sec): ...' in {path}"
        )
    if not rowcount_match:
        raise SystemExit(f"Could not find 'avg_result_row_count=...' in {path}")

    return {
        "geomean_sec": float(geomean_match.group(1)),
        "avg_result_row_count": float(rowcount_match.group(1)),
    }


def load_inputs(inputs: List[str]) -> Dict[str, Dict[str, float]]:
    if len(inputs) != len(LABEL_ORDER):
        raise SystemExit(
            f"Expected {len(LABEL_ORDER)} --input entries, got {len(inputs)}"
        )

    data: Dict[str, Dict[str, float]] = {}
    for item in inputs:
        if "=" not in item:
            raise SystemExit(f"Invalid --input (use LABEL=PATH): {item!r}")
        label, path_str = item.split("=", 1)
        label = label.strip()
        path = Path(path_str.lstrip()).expanduser().resolve()

        if label not in LABEL_ORDER:
            raise SystemExit(f"Unknown label {label!r}; use one of {LABEL_ORDER}")
        if label in data:
            raise SystemExit(f"Duplicate label {label!r}")
        if not path.is_file():
            raise SystemExit(f"Input file not found: {path}")

        data[label] = parse_metrics(path)

    for label in LABEL_ORDER:
        if label not in data:
            raise SystemExit(f"Missing --input for {label}")
    return data


def plot_average_time(data: Dict[str, float], out_path: Path) -> None:
    labels = LABEL_ORDER
    values = [data[label]["geomean_sec"] for label in labels]
    labels_with_pct = []
    for label in labels:
        row_count = data[label]["avg_result_row_count"]
        pct = (row_count / ROW_COUNT_DENOM) * 100.0
        labels_with_pct.append(f"{label}\n({pct:.6f}%)")
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    bars = ax.bar(x, values, width=0.58, color="steelblue", edgecolor="black")

    for rect, v in zip(bars, values):
        ax.annotate(
            f"{v:.6f}",
            xy=(rect.get_x() + rect.get_width() / 2.0, rect.get_height()),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_xticks(x)
    ax.set_xticklabels(labels_with_pct)
    ax.set_title("Point Query Mean")
    ax.set_xlabel("Page size and frame size (row_count / 15,000,000 as %)")
    ax.set_ylabel("Point Query Mean (s)")
    ymax = max(values) * 1.2 if values else 1.0
    ax.set_ylim(0, ymax)

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a bar chart comparing overall geometric mean time from three files."
        )
    )
    parser.add_argument(
        "--input",
        action="append",
        metavar="LABEL=PATH",
        required=True,
        help="Repeat three times with labels 128K, 256K, 512K",
    )
    parser.add_argument(
        "-o",
        "--out",
        type=Path,
        default=Path("benchmark_plots/point_average_time_comparison.png"),
        help="Output PNG path",
    )
    args = parser.parse_args()

    data = load_inputs(args.input)
    plot_average_time(data, args.out.resolve())
    print(f"Wrote chart to: {args.out.resolve()}")
    for label in LABEL_ORDER:
        geomean = data[label]["geomean_sec"]
        row_count = data[label]["avg_result_row_count"]
        pct = (row_count / ROW_COUNT_DENOM) * 100.0
        print(f"  {label}: geomean={geomean:.6f} s  row_count={row_count:.6f} ({pct:.6f}%)")


if __name__ == "__main__":
    main()
