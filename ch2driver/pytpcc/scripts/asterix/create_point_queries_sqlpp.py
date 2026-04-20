#!/usr/bin/env python3
"""Create a .sqlpp file of PRIMARY KEY point SELECTs for CH2++ bench datasets.

Output is meant for ``run_point_queries_benchmark.py`` (execute + timing), not for ``LOAD``.

Primary keys match ``ddl/asterix/ch2pp_bench.sqlpp``. See module docstring in
``run_point_queries_benchmark.py`` for execution and metrics.

Example::

  python scripts/asterix/create_point_queries_sqlpp.py -D bench --min 1 --max 500 \\
    --datasets warehouse,item,customer --out /tmp/points.sqlpp
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _district_d_id(k: int) -> int:
    return ((k - 1) % 10) + 1


def generate_lines(
    dataverse: str,
    min_k: int,
    max_k: int,
    w_id: int,
    d_id: int,
    datasets: set[str],
) -> list[str]:
    lines: list[str] = [
        "-- Point lookups (PK) — created by create_point_queries_sqlpp.py",
        f"USE {dataverse};",
        "",
    ]

    for k in range(min_k, max_k + 1):
        if "warehouse" in datasets:
            lines.append(f"SELECT * FROM warehouse WHERE w_id = {k};")
        if "district" in datasets:
            dd = _district_d_id(k)
            lines.append(
                f"SELECT * FROM district WHERE d_w_id = {w_id} AND d_id = {dd};"
            )
        if "customer" in datasets:
            lines.append(
                "SELECT * FROM customer WHERE "
                f"c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {k};"
            )
        if "stock" in datasets:
            lines.append(
                f"SELECT * FROM stock WHERE s_w_id = {w_id} AND s_i_id = {k};"
            )
        if "orders" in datasets:
            lines.append(
                "SELECT * FROM orders WHERE "
                f"o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {k};"
            )
        if "neworder" in datasets:
            lines.append(
                "SELECT * FROM neworder WHERE "
                f"no_w_id = {w_id} AND no_d_id = {d_id} AND no_o_id = {k};"
            )
        if "item" in datasets:
            lines.append(f"SELECT * FROM item WHERE i_id = {k};")
        if "supplier" in datasets:
            lines.append(f"SELECT * FROM supplier WHERE su_suppkey = {k};")
        if "nation" in datasets:
            lines.append(f"SELECT * FROM nation WHERE n_nationkey = {k};")
        if "region" in datasets:
            lines.append(f"SELECT * FROM region WHERE r_regionkey = {k};")

    return lines


def main() -> int:
    all_ds = {
        "warehouse",
        "district",
        "customer",
        "stock",
        "orders",
        "neworder",
        "item",
        "supplier",
        "nation",
        "region",
    }
    p = argparse.ArgumentParser(
        description="Write CH2++ PK point-lookup SELECTs to a .sqlpp file",
    )
    p.add_argument(
        "-D",
        "--dataverse",
        required=True,
        metavar="NAME",
        help="Dataverse name for USE",
    )
    p.add_argument("--min", type=int, default=1, help="Start of swept id (inclusive)")
    p.add_argument("--max", type=int, default=100_000, help="End of swept id (inclusive)")
    p.add_argument("--w-id", type=int, default=1, metavar="W", help="Fixed warehouse id")
    p.add_argument(
        "--d-id",
        type=int,
        default=1,
        metavar="D",
        help="Fixed district id for 3-part keys (1..10 in TPC-C)",
    )
    p.add_argument(
        "--datasets",
        default=",".join(sorted(all_ds)),
        help=f"Comma-separated subset of: {','.join(sorted(all_ds))}",
    )
    p.add_argument("--out", type=Path, default=None, help="Output .sqlpp (default: stdout)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print how many statements would be written, exit 0",
    )
    args = p.parse_args()

    if args.min < 1 or args.max < args.min:
        print("error: need 1 <= --min <= --max", file=sys.stderr)
        return 1
    if not (1 <= args.d_id <= 10):
        print("warning: TPC-C districts are 1..10", file=sys.stderr)

    requested = {s.strip() for s in args.datasets.split(",") if s.strip()}
    unknown = requested - all_ds
    if unknown:
        print(f"error: unknown dataset(s): {unknown}", file=sys.stderr)
        return 1

    n_k = args.max - args.min + 1
    n_stmts = n_k * len(requested)
    if n_stmts > 500_000 and not args.dry_run:
        print(
            f"warning: about to emit {n_stmts} SELECTs. Use smaller --max or --datasets.",
            file=sys.stderr,
        )

    if args.dry_run:
        print(
            f"Would write {n_stmts} SELECT statements (k={args.min}..{args.max})",
            file=sys.stderr,
        )
        return 0

    text = "\n".join(
        generate_lines(
            args.dataverse,
            args.min,
            args.max,
            args.w_id,
            args.d_id,
            requested,
        )
    )
    if not text.endswith("\n"):
        text += "\n"

    if args.out:
        args.out.write_text(text, encoding="utf-8")
        print(
            f"Wrote {args.out} ({n_stmts} SELECTs, {len(text.splitlines())} lines)",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
