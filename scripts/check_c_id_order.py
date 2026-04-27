#!/usr/bin/env python3
"""Read the first N lines of a JSONL file and verify c_id is non-decreasing."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Optional


def main() -> int:
    p = argparse.ArgumentParser(
        description="Check that c_id is in sorted (non-decreasing) order in the first N lines.",
    )
    p.add_argument(
        "path",
        nargs="?",
        default="-",
        help="JSONL file path, or '-' for stdin (default: -)",
    )
    p.add_argument(
        "-n",
        "--max-lines",
        type=int,
        default=2_000_000,
        metavar="N",
        help="Maximum physical lines to read from the file (default: 2000000)",
    )
    p.add_argument(
        "--strict-increasing",
        action="store_true",
        help="Require strictly increasing c_id (reject duplicates)",
    )
    args = p.parse_args()

    opener = (
        open(args.path, encoding="utf-8")
        if args.path != "-"
        else sys.stdin
    )

    prev: Optional[int] = None
    prev_record_line = 0
    records = 0
    last_line_no = 0

    with opener as fh:
        for line_no, line in enumerate(fh, start=1):
            last_line_no = line_no
            if line_no > args.max_lines:
                break
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj: Any = json.loads(stripped)
            except json.JSONDecodeError as e:
                print(f"line {line_no}: invalid JSON: {e}", file=sys.stderr)
                return 1
            if not isinstance(obj, dict) or "c_id" not in obj:
                print(f"line {line_no}: missing object or c_id", file=sys.stderr)
                return 1
            cid = obj["c_id"]
            if not isinstance(cid, int):
                print(f"line {line_no}: c_id is not int: {cid!r}", file=sys.stderr)
                return 1

            if prev is not None:
                if args.strict_increasing:
                    if cid <= prev:
                        print(
                            f"NOT STRICTLY INCREASING at line {line_no}: c_id={cid} "
                            f"(previous c_id={prev} at line {prev_record_line})"
                        )
                        return 2
                elif cid < prev:
                    print(
                        f"OUT OF ORDER at line {line_no}: c_id={cid} < previous c_id={prev} "
                        f"(previous record at line {prev_record_line})"
                    )
                    return 2

            prev, prev_record_line = cid, line_no
            records += 1

    mode = "strictly increasing" if args.strict_increasing else "non-decreasing"
    print(
        f"OK: checked {records} records in first {min(last_line_no, args.max_lines)} "
        f"physical lines; c_id is {mode}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
