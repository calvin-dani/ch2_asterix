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

With ``--json-summary``, the file also includes average **result row count** per successful query
(``len(results)``, with ``metrics.resultCount`` as fallback if the list is absent) and average
**result payload size** in bytes when Asterix returns ``metrics.resultSize`` (engine-reported; see
Asterix ``/query/service`` docs), overall and per dataset.

Optional: ``--print-query-response`` prints the first successful response JSON (keys + pretty body) to stderr for exploring the API payload.

``--profile-timings-one`` posts only the first ``SELECT`` with ``profile=timings`` and ``optimized-logical-plan=true``, prints the **full** JSON response on stdout, and exits (see ``load_ddl.profile_timings_query_form_params``).

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


def _result_row_count_and_size_bytes(body: dict) -> tuple[int, int | None]:
    """Row count from ``results`` (prefer length); ``metrics.resultSize`` when int-like (bytes)."""
    results = body.get("results")
    n_rows = len(results) if isinstance(results, list) else 0
    m_raw = body.get("metrics")
    m: dict = m_raw if isinstance(m_raw, dict) else {}
    if n_rows == 0:
        rc = m.get("resultCount")
        if isinstance(rc, int):
            n_rows = rc
        elif isinstance(rc, str) and rc.isdigit():
            n_rows = int(rc)
    size_b: int | None = None
    rs = m.get("resultSize")
    if isinstance(rs, int):
        size_b = rs
    elif isinstance(rs, str) and rs.isdigit():
        size_b = int(rs)
    return n_rows, size_b


def _mean_int_list(values: list[int]) -> float | None:
    return sum(values) / len(values) if values else None


def geometric_mean(values: list[float]) -> float | None:
    if not values:
        return None
    pos = [v for v in values if v > 0]
    if not pos:
        return None
    return math.exp(sum(math.log(v) for v in pos) / len(pos))


def _print_decoded_query_response(body: object, *, max_chars: int) -> None:
    """Pretty-print Asterix ``/query/service`` JSON once for inspection (stderr)."""
    print("=== /query/service response (decoded) ===", file=sys.stderr)
    if isinstance(body, dict):
        print(f"top-level keys: {list(body.keys())}", file=sys.stderr)
    else:
        print(f"body type: {type(body).__name__}", file=sys.stderr)
    try:
        text = json.dumps(body, indent=2, default=str, ensure_ascii=False)
    except TypeError:
        text = repr(body)
    if len(text) > max_chars:
        print(
            text[:max_chars]
            + f"\n... ({len(text) - max_chars} more chars omitted; "
            "raise --print-query-response-chars)",
            file=sys.stderr,
        )
    else:
        print(text, file=sys.stderr)
    print("=== end response decode ===", file=sys.stderr)


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
        help="Write summary JSON (timing + avg result rows / resultSize when present)",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Less stderr output (still prints final summary)",
    )
    p.add_argument(
        "--print-query-response",
        action="store_true",
        help="After the first successful SELECT, print decoded JSON (keys + pretty body) to stderr",
    )
    p.add_argument(
        "--print-query-response-chars",
        type=int,
        default=20_000,
        metavar="N",
        help="Max chars of pretty-printed JSON for --print-query-response (default: 20000)",
    )
    p.add_argument(
        "--profile-timings-one",
        action="store_true",
        help="Run only the first SELECT with profile=timings + optimized-logical-plan; print full JSON on stdout and exit",
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
    rows_all: list[int] = []
    sizes_all: list[int] = []
    rows_by_ds: dict[str, list[int]] = defaultdict(list)
    sizes_by_ds: dict[str, list[int]] = defaultdict(list)
    n_ok = 0
    n_fail = 0
    printed_response = False
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
            body = load_ddl._post_statement(
                args.url,
                to_post,
                timeout=to_secs,
                extra_form=(
                    load_ddl.profile_timings_query_form_params()
                    if args.profile_timings_one
                    else None
                ),
            )
        except urllib.error.HTTPError as e:
            if args.profile_timings_one:
                try:
                    err = e.read().decode("utf-8")
                    try:
                        obj = json.loads(err)
                        print(
                            json.dumps(obj, indent=2, default=str),
                            flush=True,
                        )
                    except json.JSONDecodeError:
                        print(err, flush=True)
                except Exception:
                    print(str(e), flush=True)
                return 1
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
            if args.profile_timings_one:
                print(
                    json.dumps(
                        {"status": "fatal", "errors": [str(ex)]},
                        indent=2,
                        default=str,
                    ),
                    flush=True,
                )
                return 1
            n_fail += 1
            print(f"Request failed: {ex}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
            continue

        if args.profile_timings_one:
            print(json.dumps(body, indent=2, default=str), flush=True)
            return (
                0
                if isinstance(body, dict) and body.get("status") == "success"
                else 1
            )

        elapsed = time.perf_counter() - t0

        if body.get("status") != "success":
            n_fail += 1
            print(f"Query failed: {body}", file=sys.stderr)
            if not args.continue_on_error:
                return 1
            continue

        n_ok += 1
        times_all.append(elapsed)
        n_rows, sz = _result_row_count_and_size_bytes(body if isinstance(body, dict) else {})
        rows_all.append(n_rows)
        if ds:
            rows_by_ds[ds].append(n_rows)
            times_by_ds[ds].append(elapsed)
        if sz is not None:
            sizes_all.append(sz)
            if ds:
                sizes_by_ds[ds].append(sz)

        if args.print_query_response and not printed_response:
            printed_response = True
            prev = to_post if len(to_post) <= 400 else to_post[:400] + "..."
            print(
                f"First successful statement (elapsed {elapsed:.4f}s): {prev}",
                file=sys.stderr,
            )
            _print_decoded_query_response(
                body, max_chars=max(500, args.print_query_response_chars)
            )

        if not args.quiet and n_ok % 500 == 0:
            print(f"... {n_ok} queries OK", file=sys.stderr, flush=True)

        if args.max_queries and n_ok >= args.max_queries:
            break

    if args.profile_timings_one:
        print(
            "error: --profile-timings-one: no executable SELECT in file (after USE/DDL skips)",
            file=sys.stderr,
        )
        return 1

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
    if n_exec and rows_all:
        avg_rows = sum(rows_all) / len(rows_all)
        if sizes_all:
            avg_sz = sum(sizes_all) / len(sizes_all)
            sz_line = (
                f"avg_result_size_bytes={avg_sz:.2f} "
                f"(from {len(sizes_all)}/{n_exec} responses with metrics.resultSize)"
            )
        else:
            sz_line = "avg_result_size_bytes=n/a (no metrics.resultSize in responses)"
        print("", file=sys.stderr)
        print("Result payload (successful queries, mean per query):", file=sys.stderr)
        print(f"  avg_result_row_count={avg_rows:.6f}", file=sys.stderr)
        print(f"  {sz_line}", file=sys.stderr)
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
        rr = rows_by_ds.get(name, [])
        avg_r_s = f"{sum(rr) / len(rr):.6f}" if rr else "n/a"
        ss = sizes_by_ds.get(name, [])
        if ss:
            avg_sz_ds = sum(ss) / len(ss)
            sz_ds_s = f"{avg_sz_ds:.2f} ({len(ss)}/{len(tt)} with resultSize)"
        else:
            sz_ds_s = "n/a"
        print(
            f"  {name}: count={len(tt)}  sum_sec={s:.4f}  geom_mean_sec={gm_s}  "
            f"avg_result_rows={avg_r_s}  avg_result_size_bytes={sz_ds_s}",
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
        "avg_result_row_count": (sum(rows_all) / n_exec) if n_exec else None,
        "avg_result_size_bytes": _mean_int_list(sizes_all),
        "queries_with_result_size": len(sizes_all),
        "per_dataset": {
            name: {
                "count": len(tt),
                "sum_sec": sum(tt),
                "geometric_mean_sec": geometric_mean(tt),
                "avg_result_row_count": (
                    (sum(rows_by_ds[name]) / len(rows_by_ds[name]))
                    if rows_by_ds.get(name)
                    else None
                ),
                "avg_result_size_bytes": _mean_int_list(sizes_by_ds.get(name, [])),
                "queries_with_result_size": len(sizes_by_ds.get(name, [])),
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
