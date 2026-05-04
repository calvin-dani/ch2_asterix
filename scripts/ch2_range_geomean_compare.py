#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare range-query benchmark results across page sizes.

Each input file has lines like:
  1m : Overall geometric mean time (sec): 0.030794

Writes one PNG per window (1m, 5m, 15m, 30m, 60m), each chart comparing 128K vs 256K vs 512K.

Example:
  python3 scripts/ch2_range_geomean_compare.py \\
    --input 128K=benchmark_plots/500G128_49GColumnRangeRes \\
    --input 256K=benchmark_plots/500G256_49GColumnRangeRes \\
    --input 512K="benchmark_plots/500G512_49GColumnRangeRes " \\
    -o benchmark_plots/figures_500G_compare/range
"""

import argparse
import re
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np

LABEL_ORDER = ["128K", "256K", "512K"]
BUCKET_ORDER = ["1m", "5m", "15m", "30m", "60m"]

LINE_RE = re.compile(
    r"^\s*(\d+m)\s*:\s*Overall\s+geometric\s+mean\s+time\s+\(sec\)\s*:\s*([\d.]+)\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def parse_range_file(path: Path) -> Dict[str, float]:
    text = path.read_text(encoding="utf-8", errors="replace")
    found: Dict[str, float] = {}
    for m in LINE_RE.finditer(text):
        bucket = m.group(1).lower()
        if bucket not in [b.lower() for b in BUCKET_ORDER]:
            continue
        # normalize key to canonical form (e.g. 1M -> 1m)
        canon = next(b for b in BUCKET_ORDER if b.lower() == bucket)
        if canon in found:
            raise SystemExit(f"{path}: duplicate entry for {canon}")
        found[canon] = float(m.group(2))

    missing = [b for b in BUCKET_ORDER if b not in found]
    if missing:
        raise SystemExit(f"{path}: missing windows {missing}; found keys {sorted(found)}")
    return found


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
        # lstrip only: paths may intentionally end in spaces (e.g. mis-saved filenames).
        path = Path(path_str.lstrip()).expanduser().resolve()
        if label not in LABEL_ORDER:
            raise SystemExit(f"Unknown label {label!r}; use one of {LABEL_ORDER}")
        if label in data:
            raise SystemExit(f"Duplicate label {label!r}")
        if not path.is_file():
            raise SystemExit(f"Input file not found: {path}")
        data[label] = parse_range_file(path)

    for lab in LABEL_ORDER:
        if lab not in data:
            raise SystemExit(f"Missing --input for {lab}")
    return data


def plot_one_window(
    data: Dict[str, Dict[str, float]],
    window: str,
    out_path: Path,
) -> None:
    values = [data[lab][window] for lab in LABEL_ORDER]
    x = np.arange(len(LABEL_ORDER))

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
    ax.set_xticklabels(LABEL_ORDER)
    ax.set_title(f"Range Query Mean ({window})")
    ax.set_xlabel("Page size and frame size")
    ax.set_ylabel("Range query mean (s)")
    ymax = max(values) * 1.2 if values else 1.0
    ax.set_ylim(0, ymax)

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Plot five bar charts (1m, 5m, 15m, 30m, 60m) comparing range-query "
            "geometric mean across 128K / 256K / 512K result files."
        )
    )
    parser.add_argument(
        "--input",
        action="append",
        metavar="LABEL=PATH",
        required=True,
        help="Three times: 128K=..., 256K=..., 512K=...",
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path("benchmark_plots/range_compare"),
        help="Directory for PNG outputs (default: benchmark_plots/range_compare)",
    )
    args = parser.parse_args()

    data = load_inputs(args.input)
    out_dir = args.out_dir.resolve()

    for window in BUCKET_ORDER:
        safe = window.replace(" ", "_")
        out_path = out_dir / f"range_compare_{safe}.png"
        plot_one_window(data, window, out_path)
        print(f"Wrote {out_path}")

    print("\nValues used:")
    for window in BUCKET_ORDER:
        parts = [f"{lab}={data[lab][window]:.6f}" for lab in LABEL_ORDER]
        print(f"  {window}:  {'  '.join(parts)}")


if __name__ == "__main__":
    main()
