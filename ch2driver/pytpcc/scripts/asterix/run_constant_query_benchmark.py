#!/usr/bin/env python3
"""Run the same SQL++ statement repeatedly over HTTP and report timing (as run_point_queries_benchmark).

Default is ``USE <dataverse>; SELECT VALUE 1+1;`` posted ``/query/service`` in a loop (serial).

Metrics: wall clock, sum of per-request times, geometric mean, queries/hour (wall and sum of times),
optional ``--json-summary``.

Example (from ``ch2driver/pytpcc``)::

  python scripts/asterix/run_constant_query_benchmark.py \\
    --url http://127.0.0.1:19002/query/service -D bench -n 10000
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.error
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import load_ddl  # noqa: E402


def geometric_mean(values: list[float]) -> float | None:
    if not values:
        return None
    pos = [v for v in values if v > 0]
    if not pos:
        return None
    return math.exp(sum(math.log(v) for v in pos) / len(pos))


def main() -> int:
    p = argparse.ArgumentParser(
        description="Benchmark a constant SQL++ query N times (default: SELECT VALUE 1+1;)",
    )
    p.add_argument(
        "--url",
        default="http://127.0.0.1:19002/query/service",
        help="Cluster Controller query service URL",
    )
    p.add_argument(
        "-D",
        "--dataverse",
        default="bench",
        metavar="NAME",
        help="Dataverse for USE (default: bench)",
    )
    p.add_argument(
        "-n",
        "--count",
        type=int,
        default=10_000,
        help="Number of times to run the query (default: 10000)",
    )
    p.add_argument(
        "--statement",
        default="SELECT VALUE 1+1;",
        help="SQL++ fragment after USE (default: SELECT VALUE 1+1;)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="Per-request HTTP timeout in seconds (0 = unlimited; default: 600)",
    )
    p.add_argument(
        "--json-summary",
        type=Path,
        default=None,
        help="Write machine-readable JSON summary to this path",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Less progress on stderr (still print final summary)",
    )
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going on HTTP/response failure (default: stop on first error)",
    )
    args = p.parse_args()

    if args.count < 1:
        print("error: -n / --count must be >= 1", file=sys.stderr)
        return 1

    dv = args.dataverse.strip() or "bench"
    st = args.statement.strip()
    if not st.endswith(";"):
        st = st + ";"
    to_post = f"USE {dv}; {st}"

    to_secs = None if args.timeout == 0 else args.timeout
    n_fail = 0
    times_all: list[float] = []

    wall0 = time.perf_counter()
    for i in range(args.count):
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

        times_all.append(elapsed)
        if not args.quiet and (i + 1) % 2000 == 0:
            print(f"... {i + 1} queries OK", file=sys.stderr, flush=True)

    wall1 = time.perf_counter()
    wall_sec = wall1 - wall0
    n_exec = len(times_all)
    sum_sec = sum(times_all)
    gm_all = geometric_mean(times_all)
    qph_wall = (n_exec * 3600.0 / wall_sec) if wall_sec > 0 else 0.0
    qph_sum = (n_exec * 3600.0 / sum_sec) if sum_sec > 0 else 0.0

    print("", file=sys.stderr)
    print("=== Constant query benchmark ===", file=sys.stderr)
    print(f"Dataverse: {dv}", file=sys.stderr)
    print(f"Statement: {st}", file=sys.stderr)
    print(f"Successful queries: {n_exec}  Failed: {n_fail}", file=sys.stderr)
    print(f"Wall clock (sec): {wall_sec:.4f}", file=sys.stderr)
    print(f"Sum of query times (sec): {sum_sec:.4f}", file=sys.stderr)
    if gm_all is not None:
        print(f"Geometric mean time (sec): {gm_all:.6f}", file=sys.stderr)
    else:
        print("Geometric mean: n/a", file=sys.stderr)
    print(f"Queries per hour (wall): {qph_wall:.2f}", file=sys.stderr)
    print(f"Queries per hour (sum of times): {qph_sum:.2f}", file=sys.stderr)

    summary = {
        "dataverse": dv,
        "statement": st,
        "posted_body": to_post,
        "successful_queries": n_exec,
        "failed_queries": n_fail,
        "wall_sec": wall_sec,
        "sum_query_times_sec": sum_sec,
        "geometric_mean_sec": gm_all,
        "queries_per_hour_wall": qph_wall,
        "queries_per_hour_sum_times": qph_sum,
    }
    if args.json_summary:
        args.json_summary.write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
        print(f"Wrote JSON summary: {args.json_summary}", file=sys.stderr)

    print(
        f"geom_mean_sec={gm_all} wall_sec={wall_sec} sum_sec={sum_sec} "
        f"qph_wall={qph_wall} n={n_exec}"
    )

    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
