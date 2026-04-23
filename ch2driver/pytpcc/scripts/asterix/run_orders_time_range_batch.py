#!/usr/bin/env python3
"""Run ``run_point_queries_benchmark.py`` on each time-range .sqlpp from ``create_orders_time_range_queries_sqlpp.py``.

For each minute length ``M`` in ``--intervals``, looks for ``<out_dir>/<prefix>_<M>m.sqlpp`` and
writes ``<out_dir>/result_<M>m.json`` (unless a run fails, then non-zero exit).

Example::

  python scripts/asterix/run_orders_time_range_batch.py \\
    --url http://127.0.0.1:19002/query/service -D mydv \\
    --out-dir /tmp/qr --name-prefix orders_range
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent


def main() -> int:
    p = argparse.ArgumentParser(
        description="POST each orders_range_*.sqlpp to Asterix and write result_*.json",
    )
    p.add_argument(
        "--url",
        default="http://127.0.0.1:19002/query/service",
        help="Asterix /query/service URL",
    )
    p.add_argument(
        "-D",
        "--dataverse",
        default="",
        metavar="NAME",
        help="Override dataverse (same as run_point_queries_benchmark / load_ddl)",
    )
    p.add_argument(
        "--dataverse-from",
        default="bench",
        help="Placeholder rewritten when -D is set",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Directory with <prefix>_<M>m.sqlpp and output JSONs",
    )
    p.add_argument(
        "--name-prefix",
        default="orders_range",
        help="Match files {prefix}_1m.sqlpp, … (default: orders_range)",
    )
    p.add_argument(
        "--intervals",
        default="1,5,15,60",
        help="Comma-separated minutes M to look for (default: 1,5,15,60)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="Per-query HTTP timeout (0 = no limit; default: 600)",
    )
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Forward to run_point_queries_benchmark (keep going on bad query)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands only",
    )
    args = p.parse_args()

    out = args.out_dir.expanduser().resolve()
    if not out.is_dir():
        print(f"error: not a directory: {out}", file=sys.stderr)
        return 1

    prefix = args.name_prefix.strip() or "orders_range"
    bench = _SCRIPT_DIR / "run_point_queries_benchmark.py"
    if not bench.is_file():
        print(f"error: missing {bench}", file=sys.stderr)
        return 1

    minutes: list[int] = []
    for s in args.intervals.split(","):
        s = s.strip()
        if not s:
            continue
        try:
            minutes.append(int(s))
        except ValueError:
            print(f"error: bad interval {s!r}", file=sys.stderr)
            return 1

    if not minutes:
        print("error: no intervals", file=sys.stderr)
        return 1

    to = None if args.timeout == 0 else args.timeout
    for m in minutes:
        sqlpp = out / f"{prefix}_{m}m.sqlpp"
        if not sqlpp.is_file():
            print(f"error: file not found: {sqlpp}", file=sys.stderr)
            return 1
        jpath = out / f"result_{m}m.json"
        cmd: list[str] = [
            sys.executable,
            str(bench),
            "--url",
            args.url,
            "--file",
            str(sqlpp),
            "--json-summary",
            str(jpath),
        ]
        if args.dataverse:
            cmd.extend(["-D", args.dataverse.strip(), "--dataverse-from", args.dataverse_from])
        if to is not None:
            cmd.extend(["--timeout", str(to)])
        if args.continue_on_error:
            cmd.append("--continue-on-error")

        print(" ".join(cmd), file=sys.stderr)
        if args.dry_run:
            continue
        r = subprocess.run(cmd, check=False)
        if r.returncode != 0:
            print(f"error: benchmark failed for {m}m (exit {r.returncode})", file=sys.stderr)
            return r.returncode

    if args.dry_run:
        return 0
    print("run_orders_time_range_batch: all intervals finished OK.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
