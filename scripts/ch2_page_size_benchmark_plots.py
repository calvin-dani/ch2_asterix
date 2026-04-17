#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Plot CH2 analytical query response times across buffer/page sizes (32K-256K).

Reads one text file per page size (Q01..Q22 lines + optional summary footer),
then writes:
  - one bar chart per query (Q01.png ... Q22.png) under out_dir/per_query/
  - summary_metrics_by_page_size.png (geometric mean, set time, Qph)

Dependencies: matplotlib (see requirements.txt).

Example (result files named 40G32Res, 40G64Res, 40G128Res under benchmark_plots/):
  There is no 40G256Res file yet. Point 256K at the same file as 128K so the
  fourth bar matches 128K until a 256K run exists (or use a symlink to 40G128Res).

  python3 scripts/ch2_page_size_benchmark_plots.py \\
    --input 32K=benchmark_plots/40G32Res \\
    --input 64K=benchmark_plots/40G64Res \\
    --input 128K=benchmark_plots/40G128Res \\
    --input 256K=benchmark_plots/40G128Res \\
    -o benchmark_plots/figures
"""

import argparse
import math
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np

PAGE_ORDER = ["32K", "64K", "128K", "256K"]
N_QUERIES = 22


def _get_viridis_cmap():
    """Matplotlib 3.7+ uses plt.colormaps; older versions use cm.get_cmap."""
    try:
        return plt.colormaps["viridis"]
    except (AttributeError, KeyError, TypeError):
        import matplotlib.cm as cm

        return cm.get_cmap("viridis")


def _annotate_bar_values(ax, bars, heights, fontsize: int = 8) -> None:
    """Label each bar with its value (one decimal place), centered above the bar."""
    for rect, h in zip(bars, heights):
        ax.annotate(
            f"{h:.1f}",
            xy=(rect.get_x() + rect.get_width() / 2.0, h),
            ha="center",
            va="bottom",
            fontsize=fontsize,
        )


# Lines like: Q01  10.64   or   Q1 10.64
QUERY_LINE_RE = re.compile(
    r"^\s*Q\s*(\d{1,2})\s+([\d.]+)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
FOOTER_GEOM_RE = re.compile(
    r"GEOMETRIC\s+MEAN\s*=\s*([\d.]+)",
    re.IGNORECASE,
)
FOOTER_SET_RE = re.compile(
    r"AVERAGE\s+TIME\s+PER\s+QUERY\s+SET\s*=\s*([\d.]+)",
    re.IGNORECASE,
)
FOOTER_QPH_RE = re.compile(
    r"QUERIES\s+PER\s+HOUR\s+\(Qph\)\s*=\s*([\d.]+)",
    re.IGNORECASE,
)


class ParseError(ValueError):
    pass


def parse_benchmark_text(text: str) -> Tuple[List[float], Optional[Dict[str, float]]]:
    """
    Parse benchmark log text. Returns (22 times in Q01..Q22 order, optional footer stats).
    Footer dict keys: reported_geom_mean, reported_set_sec, reported_qph (if present).
    """
    found: Dict[int, float] = {}
    for m in QUERY_LINE_RE.finditer(text):
        idx = int(m.group(1))
        if not 1 <= idx <= N_QUERIES:
            continue
        if idx in found:
            raise ParseError(f"Duplicate Q{idx:02d} in file")
        found[idx] = float(m.group(2))

    missing = [i for i in range(1, N_QUERIES + 1) if i not in found]
    if missing:
        raise ParseError(f"Missing queries: {missing}")

    times = [found[i] for i in range(1, N_QUERIES + 1)]

    footer: Dict[str, float] = {}
    gm = FOOTER_GEOM_RE.search(text)
    st = FOOTER_SET_RE.search(text)
    qp = FOOTER_QPH_RE.search(text)
    if gm:
        footer["reported_geom_mean"] = float(gm.group(1))
    if st:
        footer["reported_set_sec"] = float(st.group(1))
    if qp:
        footer["reported_qph"] = float(qp.group(1))

    footer_out = footer if footer else None
    return times, footer_out


def parse_benchmark_file(path: Path) -> Tuple[List[float], Optional[Dict[str, float]]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        return parse_benchmark_text(text)
    except ParseError as e:
        raise ParseError(f"{path}: {e}") from e


def geometric_mean(values: List[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / len(values))


def total_time_per_set(times: List[float]) -> float:
    return float(sum(times))


def queries_per_hour(total_sec: float, n_queries: int = N_QUERIES) -> float:
    return (n_queries * 3600.0) / total_sec


def compute_summaries(data: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for label, times in data.items():
        tset = total_time_per_set(times)
        out[label] = {
            "geom_mean": geometric_mean(times),
            "avg_time_per_set_sec": tset,
            "qph": queries_per_hour(tset),
        }
    return out


def plot_one_query(
    data: Dict[str, List[float]],
    query_index: int,
    out_path: Path,
    *,
    figsize: Tuple[float, float] = (5.5, 3.8),
) -> None:
    """Single query: one bar per page size."""
    q = query_index
    x = np.arange(len(PAGE_ORDER))
    width = 0.55
    cmap = _get_viridis_cmap()
    colors = [cmap(i / (len(PAGE_ORDER) - 1 or 1)) for i in range(len(PAGE_ORDER))]
    heights = [data[ps][q] for ps in PAGE_ORDER]

    fig, ax = plt.subplots(figsize=figsize)
    bars = ax.bar(x, heights, width, color=colors, edgecolor="black", linewidth=0.4)
    _annotate_bar_values(ax, bars, heights, fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(PAGE_ORDER)
    ax.set_ylabel("Response time (s)")
    ax.set_title(f"Q{q + 1:02d} - response time by page size")
    ymax = max(heights) * 1.22 if heights else 1.0
    ax.set_ylim(0, ymax)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_all_queries_separate(
    data: Dict[str, List[float]],
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for q in range(N_QUERIES):
        name = f"Q{q + 1:02d}.png"
        plot_one_query(data, q, out_dir / name)


def plot_per_query_grid(
    data: Dict[str, List[float]],
    out_path: Path,
    *,
    figsize: Tuple[float, float] = (16, 20),
) -> None:
    """Optional: all 22 queries in one figure (grid)."""
    n = N_QUERIES
    ncols = 4
    nrows = int(math.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, squeeze=False)
    x = np.arange(len(PAGE_ORDER))
    width = 0.65
    cmap = _get_viridis_cmap()
    colors = [cmap(i / (len(PAGE_ORDER) - 1 or 1)) for i in range(len(PAGE_ORDER))]

    for q in range(n):
        r, c = divmod(q, ncols)
        ax = axes[r][c]
        heights = [data[ps][q] for ps in PAGE_ORDER]
        bars = ax.bar(x, heights, width, color=colors, edgecolor="black", linewidth=0.3)
        _annotate_bar_values(ax, bars, heights, fontsize=6)
        ax.set_title(f"Q{q + 1:02d}", fontsize=9)
        ax.set_xticks(x)
        ax.set_xticklabels(PAGE_ORDER, rotation=0, fontsize=7)
        ax.set_ylabel("s", fontsize=7)
        ax.tick_params(axis="y", labelsize=7)
        ymax = max(heights) * 1.2 if heights else 1.0
        ax.set_ylim(0, ymax)

    for q in range(n, nrows * ncols):
        r, c = divmod(q, ncols)
        axes[r][c].set_visible(False)

    fig.suptitle("Query response time (s) by page size", fontsize=14, y=1.01)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_summary(
    summaries: Dict[str, Dict[str, float]],
    out_path: Path,
    *,
    figsize: Tuple[float, float] = (10, 8),
) -> None:
    labels = PAGE_ORDER
    gm = [summaries[k]["geom_mean"] for k in labels]
    tset = [summaries[k]["avg_time_per_set_sec"] for k in labels]
    qph = [summaries[k]["qph"] for k in labels]

    x = np.arange(len(labels))
    width = 0.55
    fig, axes = plt.subplots(3, 1, figsize=figsize, sharex=True)

    b0 = axes[0].bar(x, gm, width, color="steelblue", edgecolor="black", linewidth=0.4)
    _annotate_bar_values(axes[0], b0, gm, fontsize=9)
    axes[0].set_ylabel("Geometric mean (s)")
    axes[0].set_title("Overall geometric mean of per-query times")

    b1 = axes[1].bar(x, tset, width, color="seagreen", edgecolor="black", linewidth=0.4)
    _annotate_bar_values(axes[1], b1, tset, fontsize=9)
    axes[1].set_ylabel("Seconds per 22-query set")
    axes[1].set_title("Total time for one full query set (sum of Q01-Q22)")

    b2 = axes[2].bar(x, qph, width, color="coral", edgecolor="black", linewidth=0.4)
    _annotate_bar_values(axes[2], b2, qph, fontsize=9)
    axes[2].set_ylabel("Qph")
    axes[2].set_title("Queries per hour (22 x 3600 / total set time)")
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(labels)

    fig.suptitle("Summary metrics by page size", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def load_inputs(inputs: List[str]) -> Dict[str, List[float]]:
    """
    Each item is 'LABEL=PATH' (e.g. 32K=./runs/32k.txt).
    Labels must be exactly: 32K, 64K, 128K, 256K.
    """
    if len(inputs) != len(PAGE_ORDER):
        raise SystemExit(
            f"Expected {len(PAGE_ORDER)} --input entries, got {len(inputs)}"
        )
    data: Dict[str, List[float]] = {}
    footers: Dict[str, Dict[str, float]] = {}
    for item in inputs:
        if "=" not in item:
            raise SystemExit(f"Invalid --input (use LABEL=PATH): {item!r}")
        label, path_str = item.split("=", 1)
        label = label.strip()
        path = Path(path_str.strip()).expanduser().resolve()
        if label not in PAGE_ORDER:
            raise SystemExit(f"Unknown label {label!r}; use one of {PAGE_ORDER}")
        if label in data:
            raise SystemExit(f"Duplicate label {label!r}")
        times, footer = parse_benchmark_file(path)
        data[label] = times
        if footer:
            footers[label] = footer

    for lab in PAGE_ORDER:
        if lab not in data:
            raise SystemExit(f"Missing --input for {lab}")

    if footers:
        print("Reported values in file footers (informational):")
        for lab in PAGE_ORDER:
            if lab in footers:
                print(f"  {lab}: {footers[lab]}")
        print()

    return data


def load_dir(benchmark_dir: Path) -> Dict[str, List[float]]:
    """Load 32K.txt, 64K.txt, 128K.txt, 256K.txt from a directory."""
    data: Dict[str, List[float]] = {}
    for lab in PAGE_ORDER:
        candidates = [
            benchmark_dir / f"{lab}.txt",
            benchmark_dir / f"{lab.lower()}.txt",
        ]
        path = next((p for p in candidates if p.is_file()), None)
        if path is None:
            raise SystemExit(
                f"Could not find {lab}.txt (or {lab.lower()}.txt) in {benchmark_dir}"
            )
        times, footer = parse_benchmark_file(path)
        data[lab] = times
        if footer:
            print(f"{path.name} footer: {footer}")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot CH2 page-size benchmarks from text files (one per page size)."
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path("benchmark_plots"),
        help="Output directory (default: ./benchmark_plots)",
    )
    parser.add_argument(
        "--input",
        action="append",
        metavar="LABEL=PATH",
        help="Repeat four times, e.g. --input 32K=path/32k.txt "
        "(labels: 32K, 64K, 128K, 256K)",
    )
    parser.add_argument(
        "--benchmark-dir",
        type=Path,
        default=None,
        help="Directory containing 32K.txt, 64K.txt, 128K.txt, 256K.txt",
    )
    parser.add_argument(
        "--combined-grid",
        action="store_true",
        help="Also write query_times_by_page_size.png (22 subplots on one sheet)",
    )
    args = parser.parse_args()
    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.benchmark_dir is not None:
        if args.input:
            raise SystemExit("Use either --benchmark-dir or --input, not both")
        data = load_dir(args.benchmark_dir.resolve())
    elif args.input and len(args.input) == len(PAGE_ORDER):
        data = load_inputs(args.input)
    else:
        parser.error(
            f"Provide --benchmark-dir DIR or exactly {len(PAGE_ORDER)} "
            f"--input LABEL=PATH arguments (see --help)"
        )

    summaries = compute_summaries(data)
    print("Computed summaries (from parsed per-query times):")
    for k in PAGE_ORDER:
        s = summaries[k]
        print(
            f"  {k}: geom_mean={s['geom_mean']:.4f}  "
            f"set_time={s['avg_time_per_set_sec']:.2f}s  Qph={s['qph']:.2f}"
        )

    per_query_dir = out_dir / "per_query"
    plot_all_queries_separate(data, per_query_dir)
    summary_path = out_dir / "summary_metrics_by_page_size.png"
    plot_summary(summaries, summary_path)

    print(f"\nWrote {N_QUERIES} per-query charts under:\n  {per_query_dir}/")
    print(f"Wrote summary:\n  {summary_path}")

    if args.combined_grid:
        grid_path = out_dir / "query_times_by_page_size.png"
        plot_per_query_grid(data, grid_path)
        print(f"Wrote combined grid:\n  {grid_path}")


if __name__ == "__main__":
    main()
