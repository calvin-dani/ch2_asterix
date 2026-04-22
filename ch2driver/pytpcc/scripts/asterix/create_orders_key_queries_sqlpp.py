#!/usr/bin/env python3
"""Write N unique point queries over ``orders`` (o_w_id, o_d_id, o_id).

Each line is a single SQL++ statement of the form::

  USE <dataverse>; SELECT o.o_orderline AS orderline, o.o_entry_d AS entry
  FROM orders o WHERE o.o_w_id = W AND o.o_d_id = D AND o.o_id = O;

Primary-key bounds are CLI-configurable. Unique triples are chosen without
replacement (uniform among all valid combinations) using ``random.sample``
over a linear index into the (w,d,o) product space. If *N* is larger than the
number of possible triples, the program exits with an error.

**Note on ``i_id``:** the ``orders`` point key in CH2++ is ``(o_w_id, o_d_id, o_id)``.
The third column is the **order** id (``o_id``), not ``item``’s ``i_id``. If you
need queries keyed by line-item id, you would UNNEST ``o_orderline`` and filter
on ``ol_i_id`` (different program).

Example::

  python scripts/asterix/create_orders_key_queries_sqlpp.py -D mydv -n 10000 \\
    --min-o-w-id 1 --max-o-w-id 500 --min-o-d-id 1 --max-o-d-id 10 \\
    --min-o-id 1 --max-o-id 3000 --seed 42 --out /tmp/orders_points.sqlpp
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path


def _decode_triple(
    flat: int,
    *,
    min_w: int,
    min_d: int,
    min_o: int,
    d_span: int,
    o_span: int,
) -> tuple[int, int, int]:
    o = min_o + (flat % o_span)
    t = flat // o_span
    d = min_d + (t % d_span)
    w = min_w + (t // d_span)
    return w, d, o


def main() -> int:
    p = argparse.ArgumentParser(
        description="Write N unique (o_w_id, o_d_id, o_id) point SELECTs on orders",
    )
    p.add_argument("-D", "--dataverse", required=True, metavar="NAME", help="USE target")
    p.add_argument(
        "-n",
        "--count",
        type=int,
        required=True,
        metavar="N",
        help="Number of unique queries to emit",
    )
    p.add_argument("--min-o-w-id", type=int, default=1)
    p.add_argument("--max-o-w-id", type=int, default=500)
    p.add_argument("--min-o-d-id", type=int, default=1)
    p.add_argument("--max-o-d-id", type=int, default=10)
    p.add_argument("--min-o-id", type=int, default=1)
    p.add_argument("--max-o-id", type=int, default=3000)
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible triples",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output .sqlpp (default: stdout)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print total combinations and N only, exit 0",
    )
    args = p.parse_args()

    w0, w1 = args.min_o_w_id, args.max_o_w_id
    d0, d1 = args.min_o_d_id, args.max_o_d_id
    o0, o1 = args.min_o_id, args.max_o_id

    if w0 > w1 or d0 > d1 or o0 > o1:
        print("error: each min* must be <= max*", file=sys.stderr)
        return 1

    w_span = w1 - w0 + 1
    d_span = d1 - d0 + 1
    o_span = o1 - o0 + 1
    total = w_span * d_span * o_span
    n = args.count

    if n < 1:
        print("error: -n must be at least 1", file=sys.stderr)
        return 1
    if n > total:
        print(
            f"error: need n <= {total} (unique (o_w_id,o_d_id,o_id) in range) but got n={n}",
            file=sys.stderr,
        )
        return 1

    if args.dry_run:
        print(
            f"Would write {n} unique queries; space size = {total} combinations",
            file=sys.stderr,
        )
        return 0

    if args.seed is not None:
        random.seed(args.seed)

    # Uniform sample of distinct indices in [0, total)
    indices = random.sample(range(total), n)

    lines = [
        "-- Point lookups on orders — created by create_orders_key_queries_sqlpp.py",
    ]
    dv = args.dataverse.strip()
    for flat in sorted(indices):
        w, d, o = _decode_triple(
            flat,
            min_w=w0,
            min_d=d0,
            min_o=o0,
            d_span=d_span,
            o_span=o_span,
        )
        lines.append(
            f"USE {dv}; SELECT o.o_orderline AS orderline, o.o_entry_d AS entry "
            f"FROM orders o WHERE o.o_w_id = {w} AND o.o_d_id = {d} AND o.o_id = {o};"
        )
    text = "\n".join(lines) + "\n"

    if args.out:
        args.out.write_text(text, encoding="utf-8")
        print(
            f"Wrote {args.out} ({n} statements, dataverse={dv})",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
