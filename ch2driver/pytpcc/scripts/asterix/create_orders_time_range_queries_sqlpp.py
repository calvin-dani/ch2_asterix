#!/usr/bin/env python3
"""Write separate .sqlpp files: random time windows on ``orders.o_entry_d`` (open bounds).

Each query::

  USE <dataverse>;
  SELECT o.o_id AS id, o.o_entry_d AS entry
  FROM orders o
  WHERE o.o_entry_d > \"<low>\" AND o.o_entry_d < \"<high>\";

For each interval length in minutes, ``<high> = <low> + interval``. ``low`` is drawn uniformly
at random so the window lies strictly between ``--min-ts`` and ``--max-ts`` (endpoints
excluded via a 1 second margin).

**Counts** use ``--counts 1:500,5:200,15:100,60:50`` (minutes:statements). Each interval
gets its own file under ``--out-dir`` named ``<prefix>_<M>m.sqlpp``.

Then run ``run_point_queries_benchmark.py`` per file, or ``run_orders_time_range_batch.py``.

Example::

  python scripts/asterix/create_orders_time_range_queries_sqlpp.py -D mydv --out-dir /tmp/qr \\
    --counts 1:100,5:100,15:100,60:100 --seed 42
"""

from __future__ import annotations

import argparse
import random
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

_TS_FMT = "%Y-%m-%d %H:%M:%S"
_EPS = timedelta(seconds=1)
_DEFAULT_MIN = "2014-01-01 00:00:07"
_DEFAULT_MAX = "2020-08-01 23:59:57"


def _parse_ts(s: str) -> datetime:
    s = s.strip()
    m = re.match(
        r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})$",
        s,
    )
    if not m:
        raise ValueError(
            f"expected timestamp like '{_DEFAULT_MIN}' (space or T between date and time)"
        )
    return datetime.strptime(f"{m.group(1)} {m.group(2)}", _TS_FMT)


def _format_ts(d: datetime) -> str:
    return d.strftime(_TS_FMT)


def _random_low(
    min_ts: datetime,
    max_ts: datetime,
    interval: timedelta,
    rng: random.Random,
) -> datetime:
    """Uniform ``low`` with min_ts < low and low+interval < max_ts (using 1s margins)."""
    lo = min_ts + _EPS
    hi = max_ts - interval - _EPS
    if lo >= hi:
        raise ValueError(
            f"no valid window: need max-min > interval+2s; got interval {interval!r} "
            f"in [{min_ts!s}, {max_ts!s}]"
        )
    span_sec = (hi - lo).total_seconds()
    return lo + timedelta(seconds=rng.random() * span_sec)


def _parse_counts(s: str) -> dict[int, int]:
    out: dict[int, int] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^(\d+)\s*:\s*(\d+)$", part)
        if not m:
            raise ValueError(f"bad --counts segment {part!r} (use M:N)")
        m_min = int(m.group(1))
        n = int(m.group(2))
        if m_min < 1 or n < 1:
            raise ValueError("each interval (minutes) and count must be >= 1")
        if m_min in out:
            raise ValueError(f"duplicate interval {m_min} in --counts")
        out[m_min] = n
    if not out:
        raise ValueError("--counts is empty")
    return out


def main() -> int:
    p = argparse.ArgumentParser(
        description="Write one .sqlpp per time-window length (random ranges on o_entry_d)",
    )
    p.add_argument(
        "-D",
        "--dataverse",
        required=True,
        metavar="NAME",
        help="Dataverse in USE; each line is USE <dv>; SELECT ...",
    )
    p.add_argument(
        "--min-ts",
        default=_DEFAULT_MIN,
        help=f"Global lower bound (exclusive; default {_DEFAULT_MIN})",
    )
    p.add_argument(
        "--max-ts",
        default=_DEFAULT_MAX,
        help=f"Global upper bound (exclusive; default {_DEFAULT_MAX})",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory for <prefix>_<M>m.sqlpp files (created if missing)",
    )
    p.add_argument(
        "--name-prefix",
        default="orders_range",
        help="File names: {prefix}_1m.sqlpp, {prefix}_5m.sqlpp, ... (default: orders_range)",
    )
    p.add_argument(
        "--counts",
        required=True,
        metavar="M:N,…",
        help="Comma-separated minutes:statements, e.g. 1:500,5:200,15:100,60:50",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed (reproducible window draws)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned files and per-interval counts, do not write",
    )
    args = p.parse_args()

    try:
        min_ts = _parse_ts(args.min_ts)
        max_ts = _parse_ts(args.max_ts)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if min_ts >= max_ts:
        print("error: --min-ts must be < --max-ts", file=sys.stderr)
        return 1

    try:
        counts = _parse_counts(args.counts)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    rng = random.Random(args.seed)

    if args.dry_run:
        print(
            f"Would write under {args.out_dir} with prefix {args.name_prefix!r}: {counts}",
            file=sys.stderr,
        )
        return 0

    args.out_dir = args.out_dir.expanduser().resolve()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    dv = args.dataverse.strip()
    prefix = args.name_prefix.strip() or "orders_range"

    for im, c in sorted(counts.items()):
        path = args.out_dir / f"{prefix}_{im}m.sqlpp"
        itd = timedelta(minutes=im)
        lines: list[str] = [
            f"-- o_entry_d range queries ({im} min window) —"
            f" create_orders_time_range_queries_sqlpp.py",
        ]
        try:
            for _ in range(c):
                low = _random_low(min_ts, max_ts, itd, rng)
                high = low + itd
                a = _format_ts(low)
                b = _format_ts(high)
                lines.append(
                    f'USE {dv}; SELECT o.o_id AS id, o.o_entry_d AS entry FROM orders o '
                    f'WHERE o.o_entry_d > "{a}" AND o.o_entry_d < "{b}";'
                )
        except ValueError as e:
            print(f"error (interval {im} min): {e}", file=sys.stderr)
            return 1

        text = "\n".join(lines) + "\n"
        path.write_text(text, encoding="utf-8")
        print(
            f"Wrote {path} ({c} statements, {im} min window)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
