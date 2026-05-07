#!/usr/bin/env python3
"""Run ``USE <dataverse>; SELECT VALUE 1;`` repeatedly and report **arithmetic mean** latency.

Defaults: ``http://127.0.0.1:19002/query/service``, **10000** iterations, dataverse ``bench``.
Each request is a serial HTTP POST (same pattern as ``run_constant_query_benchmark.py``).

Example (from ``ch2driver/pytpcc``)::

  python scripts/asterix/run_select_value_one_benchmark.py -D mydv
  python scripts/asterix/run_select_value_one_benchmark.py -D mydv -n 50000 --json-summary /tmp/mean.json

Profile (single request, ``profile=timings`` + ``optimized-logical-plan``; see ``load_ddl.profile_timings_query_form_params``)::

  python scripts/asterix/run_select_value_one_benchmark.py -D mydv --profile-out /tmp/select_value_one_profile.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import load_ddl  # noqa: E402

_DEFAULT_URL = "http://127.0.0.1:19002/query/service"
_DEFAULT_COUNT = 10_000
_STATEMENT = "SELECT VALUE 1;"


def _run_profile_timings_once(args: argparse.Namespace, *, to_post: str) -> int:
    """POST once with profiler form fields; write envelope JSON to ``args.profile_out``."""
    to_secs = None if args.timeout == 0 else args.timeout
    profile_form = load_ddl.profile_timings_query_form_params()
    path = args.profile_out.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    try:
        body = load_ddl._post_statement(
            args.url,
            to_post,
            timeout=to_secs,
            extra_form=profile_form,
        )
    except urllib.error.HTTPError as e:
        elapsed = time.perf_counter() - t0
        err_obj: object = None
        try:
            err_raw = e.read().decode("utf-8")
            try:
                err_obj = json.loads(err_raw)
            except json.JSONDecodeError:
                err_obj = err_raw
        except Exception:
            err_obj = str(e)
        envelope = {
            "mode": "profile_timings_once",
            "url": args.url,
            "posted_body": to_post,
            "profile_form_fields": dict(profile_form),
            "elapsed_sec": elapsed,
            "http_status": e.code,
            "response": err_obj,
        }
        path.write_text(json.dumps(envelope, indent=2, default=str) + "\n", encoding="utf-8")
        print(f"Wrote profile error envelope: {path}", file=sys.stderr)
        return 1
    except Exception as ex:
        elapsed = time.perf_counter() - t0
        envelope = {
            "mode": "profile_timings_once",
            "url": args.url,
            "posted_body": to_post,
            "profile_form_fields": dict(profile_form),
            "elapsed_sec": elapsed,
            "response": {"status": "fatal", "errors": [str(ex)]},
        }
        path.write_text(json.dumps(envelope, indent=2, default=str) + "\n", encoding="utf-8")
        print(f"Wrote profile exception envelope: {path}", file=sys.stderr)
        return 1

    elapsed = time.perf_counter() - t0
    envelope = {
        "mode": "profile_timings_once",
        "url": args.url,
        "posted_body": to_post,
        "profile_form_fields": dict(profile_form),
        "elapsed_sec": elapsed,
        "response": body,
    }
    path.write_text(json.dumps(envelope, indent=2, default=str) + "\n", encoding="utf-8")
    ok = isinstance(body, dict) and body.get("status") == "success"
    print(f"Wrote profile response: {path} (elapsed {elapsed:.6f}s)", file=sys.stderr)
    return 0 if ok else 1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Benchmark SELECT VALUE 1; N times (mean round-trip per request)",
    )
    p.add_argument(
        "--url",
        default=_DEFAULT_URL,
        help=f"Query service URL (default: {_DEFAULT_URL})",
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
        default=_DEFAULT_COUNT,
        help=f"Iterations (default: {_DEFAULT_COUNT})",
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
        help="Optional JSON path with mean_sec, wall_sec, counts",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="No progress lines on stderr",
    )
    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going after a failed request (default: exit on first failure)",
    )
    p.add_argument(
        "--profile-out",
        type=Path,
        default=None,
        metavar="FILE",
        help="Single request with profile=timings + optimized-logical-plan; write envelope JSON "
        "to FILE and exit (skips the -n loop)",
    )
    args = p.parse_args()

    dv = args.dataverse.strip() or "bench"
    to_post = f"USE {dv}; {_STATEMENT}"

    if args.profile_out:
        return _run_profile_timings_once(args, to_post=to_post)

    if args.count < 1:
        print("error: -n / --count must be >= 1", file=sys.stderr)
        return 1

    to_secs = None if args.timeout == 0 else args.timeout
    n_fail = 0
    times: list[float] = []

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

        times.append(elapsed)
        if not args.quiet and (i + 1) % 2000 == 0:
            print(f"... {i + 1} OK", file=sys.stderr, flush=True)

    wall1 = time.perf_counter()
    wall_sec = wall1 - wall0
    n_ok = len(times)
    sum_sec = sum(times)
    mean_sec = (sum_sec / n_ok) if n_ok else None

    print("", file=sys.stderr)
    print("=== SELECT VALUE 1; latency (arithmetic mean) ===", file=sys.stderr)
    print(f"URL: {args.url}", file=sys.stderr)
    print(f"Dataverse: {dv}", file=sys.stderr)
    print(f"Statement: {_STATEMENT}", file=sys.stderr)
    print(f"Successful: {n_ok}  Failed: {n_fail}", file=sys.stderr)
    print(f"Wall clock (sec): {wall_sec:.4f}", file=sys.stderr)
    if mean_sec is not None:
        print(f"Average query time (sec): {mean_sec:.6f}", file=sys.stderr)
        print(f"Average query time (ms): {mean_sec * 1000.0:.6f}", file=sys.stderr)
    else:
        print("Average query time: n/a (no successful requests)", file=sys.stderr)

    summary = {
        "url": args.url,
        "dataverse": dv,
        "statement": _STATEMENT,
        "posted_body": to_post,
        "requested_iterations": args.count,
        "successful_queries": n_ok,
        "failed_queries": n_fail,
        "wall_sec": wall_sec,
        "sum_query_times_sec": sum_sec,
        "mean_sec": mean_sec,
        "mean_ms": (mean_sec * 1000.0) if mean_sec is not None else None,
    }
    if args.json_summary:
        args.json_summary.write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
        print(f"Wrote JSON summary: {args.json_summary}", file=sys.stderr)

    # stdout: easy to grep / pipe
    if mean_sec is not None:
        print(
            f"mean_sec={mean_sec} mean_ms={mean_sec * 1000.0} "
            f"wall_sec={wall_sec} n_ok={n_ok} n_fail={n_fail}"
        )
    else:
        print(f"mean_sec=None wall_sec={wall_sec} n_ok=0 n_fail={n_fail}")

    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
