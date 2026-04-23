#!/usr/bin/env python3
"""Run point-lookup SELECTs from a .sqlpp file and report timing (geom mean, total, Qph).

Expects statements like those from ``create_point_queries_sqlpp.py`` (``USE`` + ``SELECT * FROM
<dataset> WHERE ...``). Each statement is POSTed to Asterix ``/query/service`` serially.

Metrics (successful queries only for geometric mean unless all fail):

- **Wall clock**: real elapsed time for the whole run.
- **Sum of query times**: sum of per-HTTP round-trip durations (serial run, ~ wall).
- **Geometric mean** (overall): ``exp(mean(log(t_i)))`` for each successful query time ``t_i`` (sec).
- **Geometric mean per dataset**: inferred from ``FROM <name>`` on each ``SELECT``.
- **Queries per hour**: ``N * 3600 / T`` using wall time and using sum-of-times (both reported).

Example::

  python scripts/asterix/create_point_queries_sqlpp.py -D bench --min 1 --max 100 \\
    --datasets item,warehouse --out /tmp/pq.sqlpp

  python scripts/asterix/run_point_queries_benchmark.py \\
    --url http://127.0.0.1:19002/query/service -D bench --file /tmp/pq.sqlpp
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import load_ddl  # noqa: E402


def _strip_block_comments(raw: str) -> str:
    return re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)


def _strip_leading_line_comments(block: str) -> str:
    lines = block.split("\n")
    while lines and lines[0].strip().startswith("--"):
        lines.pop(0)
    return "\n".join(lines).strip()


def _infer_active_dataverse(raw: str, dataverse_cli: str) -> str:
    if dataverse_cli.strip():
        return dataverse_cli.strip()
    m = re.search(r"(?i)\bUSE\s+(\w+)\s*;", raw)
    if m:
        return m.group(1)
    return "bench"


def _dataset_from_select(stmt: str) -> str | None:
    m = re.search(r"(?i)\bSELECT\s+\*\s+FROM\s+(\w+)", stmt)
    if m:
        return m.group(1).lower()
    m = re.search(r"(?i)\bFROM\s+(\w+)", stmt)
    return m.group(1).lower() if m else None


def geometric_mean(values: list[float]) -> float | None:
    if not values:
        return None
    pos = [v for v in values if v > 0]
    if not pos:
        return None
    return math.exp(sum(math.log(v) for v in pos) / len(pos))


def main() -> int:
    p = argparse.ArgumentParser(
        description="Benchmark Asterix point-query SELECTs from a .sqlpp file",
    )
    p.add_argument(
        "--url",
        default="http://127.0.0.1:19002/query/service",
        help="Cluster Controller query service URL",
    )
    p.add_argument("--file", required=True, type=Path, help=".sqlpp file of point SELECTs")
    p.add_argument(
        "-D",
        "--dataverse",
        default="",
        metavar="NAME",
        help="Override dataverse name (rewrite from --dataverse-from, like load_ddl.py)",
    )
    p.add_argument(
        "--dataverse-from",
        default="bench",
        metavar="NAME",
        help="Placeholder replaced when --dataverse is set",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="Per-query HTTP timeout in seconds (0 = unlimited)",
    )
    p.add_argument(
        "--max-queries",
        type=int,
        default=0,
        metavar="N",
        help="Stop after N executed SELECTs (0 = no limit)",
    )
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going after a failed query (default: stop on first error)",
    )
    p.add_argument(
        "--json-summary",
        type=Path,
        default=None,
        help="Write machine-readable summary JSON to this path",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Less stderr output (still prints final summary)",
    )
    args = p.parse_args()

    path = args.file.expanduser().resolve()
    if not path.is_file():
        print(f"error: file not found: {path}", file=sys.stderr)
        return 1

    raw = path.read_text(encoding="utf-8")
    raw = _strip_block_comments(raw)
    if args.dataverse:
        raw = load_ddl._apply_dataverse(
            raw, args.dataverse.strip(), args.dataverse_from.strip()
        )

    parts = [
        _strip_leading_line_comments(s)
        for s in load_ddl._split_statements(raw)
        if _strip_leading_line_comments(s)
    ]

    active = _infer_active_dataverse(raw, args.dataverse)
    times_all: list[float] = []
    times_by_ds: dict[str, list[float]] = defaultdict(list)
    n_ok = 0
    n_fail = 0
    to_secs = None if args.timeout == 0 else args.timeout

    wall0 = time.perf_counter()
    for stmt in parts:
        st = stmt.strip()
        lean = " ".join(st.split())

        use_m = re.match(r"(?i)^USE\s+(\w+)\s*;?\s*$", st)
        if use_m:
            active = use_m.group(1)
            continue

        if re.match(r"(?i)^DROP\s+DATAVERSE", st) or re.match(
            r"(?i)^CREATE\s+DATAVERSE", st
        ):
            print(
                "warning: DDL statement in file; skipping (point benchmark expects SELECT)",
                file=sys.stderr,
            )
            continue

        to_post = f"USE {active}; {lean}"
        ds = _dataset_from_select(lean)

        t0 = time.perf_counter()
        try:
            body = load_ddl._post_statement(args.url, to_post, timeout=to_secs)
        except urllib.error.HTTPError as e:
            n_fail += 1
            try:
                err = e.read().decode("utf-8")
            except Exception:
                err = str(e)
            print(f"HTTP error: {e.code} {err[:400]}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
            continue
        except Exception as ex:
            n_fail += 1
            print(f"Request failed: {ex}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
            continue

        elapsed = time.perf_counter() - t0

        if body.get("status") != "success":
            n_fail += 1
            print(f"Query failed: {body}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
            continue

        n_ok += 1
        times_all.append(elapsed)
        if ds:
            times_by_ds[ds].append(elapsed)

        if not args.quiet and n_ok % 500 == 0:
            print(f"... {n_ok} queries OK", file=sys.stderr, flush=True)

        if args.max_queries and n_ok >= args.max_queries:
            break

    wall1 = time.perf_counter()
    wall_sec = wall1 - wall0
    sum_sec = sum(times_all)
    n_exec = len(times_all)
    gm_all = geometric_mean(times_all)
    qph_wall = (n_exec * 3600.0 / wall_sec) if wall_sec > 0 else 0.0
    qph_sum = (n_exec * 3600.0 / sum_sec) if sum_sec > 0 else 0.0

    print("", file=sys.stderr)
    print("=== SQL++ query benchmark ===", file=sys.stderr)
    print(f"File: {path}", file=sys.stderr)
    print(f"Successful queries: {n_exec}  Failed/aborted: {n_fail}", file=sys.stderr)
    print(f"Wall clock (sec): {wall_sec:.4f}", file=sys.stderr)
    print(f"Sum of query times (sec): {sum_sec:.4f}", file=sys.stderr)
    if gm_all is not None:
        print(f"Overall geometric mean time (sec): {gm_all:.6f}", file=sys.stderr)
    else:
        print("Overall geometric mean: n/a", file=sys.stderr)
    print(f"Queries per hour (wall): {qph_wall:.2f}", file=sys.stderr)
    print(f"Queries per hour (sum of times): {qph_sum:.2f}", file=sys.stderr)
    print("", file=sys.stderr)
    print("Per-dataset (successful queries):", file=sys.stderr)
    for name in sorted(times_by_ds.keys()):
        tt = times_by_ds[name]
        gm = geometric_mean(tt)
        s = sum(tt)
        if gm is not None:
            gm_s = f"{gm:.6f}"
        else:
            gm_s = "n/a"
        print(
            f"  {name}: count={len(tt)}  sum_sec={s:.4f}  geom_mean_sec={gm_s}",
            file=sys.stderr,
        )

    summary = {
        "file": str(path),
        "successful_queries": n_exec,
        "failed_queries": n_fail,
        "wall_sec": wall_sec,
        "sum_query_times_sec": sum_sec,
        "geometric_mean_sec": gm_all,
        "queries_per_hour_wall": qph_wall,
        "queries_per_hour_sum_times": qph_sum,
        "per_dataset": {
            name: {
                "count": len(tt),
                "sum_sec": sum(tt),
                "geometric_mean_sec": geometric_mean(tt),
            }
            for name, tt in times_by_ds.items()
        },
    }

    if args.json_summary:
        args.json_summary.write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
        print(f"Wrote JSON summary: {args.json_summary}", file=sys.stderr)

    # Echo key numbers on stdout for piping
    print(
        f"geom_mean_sec={gm_all} wall_sec={wall_sec} sum_sec={sum_sec} "
        f"qph_wall={qph_wall} n={n_exec}"
    )

    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
