#!/usr/bin/env python3
"""Create a .sqlpp file of PRIMARY KEY point SELECTs for CH2++ bench datasets.

Output is meant for ``run_point_queries_benchmark.py`` (execute + timing), not for ``LOAD``.

Primary keys match ``ddl/asterix/ch2pp_bench.sqlpp``.

Modes:

- **sweep** (default): legacy ``--min``..``--max`` loop with fixed ``--w-id`` / ``--d-id``.
- **random**: ``--count`` queries **per dataset** over built-in bounds (see
  ``DEFAULT_RANDOM_BOUNDS``). Requires ``--seed``. **Orders** uses **independent**
  ``randint`` per PK column via ``random_orders_pk_tuple`` / ``orders_point_query_sql``;
  other tables use **distinct** Cartesian samples. Skips **nation** and **history**
  (see ``POINT_QUERY_DATASET_DENSITY.md``). Non-orders datasets are skipped if Cartesian
  size **&lt; count**.

Examples::

  python scripts/asterix/create_point_queries_sqlpp.py -D bench --min 1 --max 500 \\
    --datasets warehouse,item,customer --out /tmp/points.sqlpp

  python scripts/asterix/create_point_queries_sqlpp.py -D mydv --mode random \\
    --count 10000 --seed 42 --out /tmp/points_rand.sqlpp
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path


def _district_d_id(k: int) -> int:
    return ((k - 1) % 10) + 1


def random_orders_pk_tuple(
    rng: random.Random,
    o_w_id_lo: int,
    o_w_id_hi: int,
    o_d_id_lo: int,
    o_d_id_hi: int,
    o_id_lo: int,
    o_id_hi: int,
) -> tuple[int, int, int]:
    ow = rng.randint(o_w_id_lo, o_w_id_hi)
    od = rng.randint(o_d_id_lo, o_d_id_hi)
    oid = rng.randint(o_id_lo, o_id_hi)
    return ow, od, oid


def orders_point_query_sql(o_w_id: int, o_d_id: int, o_id: int) -> str:
    """Single ``SELECT`` for ``orders`` PK equality (no ``USE`` line)."""
    return (
        "SELECT * FROM orders WHERE "
        f"o_w_id = {o_w_id} AND o_d_id = {o_d_id} AND o_id = {o_id};"
    )


def _cartesian_size(ranges: list[tuple[int, int]]) -> int:
    p = 1
    for lo, hi in ranges:
        if lo > hi:
            return 0
        p *= hi - lo + 1
    return p


def _decode_index(idx: int, ranges: list[tuple[int, int]]) -> list[int]:
    """Map linear index to tuple (outer dimensions first in ``ranges``)."""
    vals: list[int] = []
    x = idx
    for lo, hi in reversed(ranges):
        span = hi - lo + 1
        vals.append(lo + (x % span))
        x //= span
    return list(reversed(vals))


def _random_select_lines(
    dataverse: str,
    datasets: set[str],
    count: int,
    rng: random.Random,
    bounds: dict[str, list[tuple[str, tuple[int, int]]]],
) -> tuple[list[str], list[str]]:
    """Return (sqlpp_lines, skip_messages)."""
    lines: list[str] = [
        "-- Point lookups (PK) random — create_point_queries_sqlpp.py --mode random",
        f"USE {dataverse};",
        "",
    ]
    notes: list[str] = []

    for ds in sorted(datasets):
        spec = bounds.get(ds)
        if spec is None:
            notes.append(f"skip {ds}: no bound configuration")
            continue
        cols = [c for c, _ in spec]
        ranges = [r for _, r in spec]
        space = _cartesian_size(ranges)
        if space < 1:
            notes.append(f"skip {ds}: empty or invalid PK range(s)")
            continue

        if ds == "orders":
            ow_lo, ow_hi = spec[0][1]
            od_lo, od_hi = spec[1][1]
            oid_lo, oid_hi = spec[2][1]
            for _ in range(count):
                ow, od, oid = random_orders_pk_tuple(
                    rng,
                    ow_lo,
                    ow_hi,
                    od_lo,
                    od_hi,
                    oid_lo,
                    oid_hi,
                )
                lines.append(orders_point_query_sql(ow, od, oid))
            continue

        if space < count:
            notes.append(
                f"skip {ds}: key space size {space} < --count {count}"
            )
            continue
        indices = rng.sample(range(space), count)
        for flat in sorted(indices):
            vals = _decode_index(flat, ranges)
            parts = [f"{cols[i]} = {vals[i]}" for i in range(len(cols))]
            wh = " AND ".join(parts)
            lines.append(f"SELECT * FROM {ds} WHERE {wh};")

    return lines, notes


# Bounds for CH2++-shaped loads (see POINT_QUERY_DATASET_DENSITY.md).
DEFAULT_RANDOM_BOUNDS: dict[str, list[tuple[str, tuple[int, int]]]] = {
    "warehouse": [("w_id", (1, 500))],
    "district": [("d_w_id", (1, 500)), ("d_id", (1, 10))],
    "customer": [
        ("c_w_id", (1, 500)),
        ("c_d_id", (1, 10)),
        ("c_id", (1, 3000)),
    ],
    "stock": [("s_w_id", (1, 500)), ("s_i_id", (1, 100_000))],
    "orders": [
        ("o_w_id", (1, 500)),
        ("o_d_id", (1, 10)),
        ("o_id", (1, 3000)),
    ],
    "neworder": [
        ("no_w_id", (1, 500)),
        ("no_d_id", (1, 10)),
        ("no_o_id", (2101, 3000)),
    ],
    "item": [("i_id", (1, 100_000))],
    "supplier": [("su_suppkey", (1, 10_000))],
    "region": [("r_regionkey", (0, 4))],
}


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
    non_dense_random = frozenset({"nation", "history"})

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
    p.add_argument(
        "--mode",
        choices=("sweep", "random"),
        default="sweep",
        help="sweep: legacy --min/--max; random: sample PKs per dataset (orders: per-column randint)",
    )
    p.add_argument(
        "--count",
        type=int,
        default=0,
        metavar="N",
        help="[random] number of SELECTs per dataset (required)",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="[random] required RNG seed",
    )
    p.add_argument("--min", type=int, default=1, help="[sweep] start of swept id")
    p.add_argument("--max", type=int, default=100_000, help="[sweep] end of swept id")
    p.add_argument("--w-id", type=int, default=1, metavar="W", help="[sweep] fixed w_id")
    p.add_argument(
        "--d-id",
        type=int,
        default=1,
        metavar="D",
        help="[sweep] fixed district id",
    )
    p.add_argument(
        "--datasets",
        default=",".join(sorted(all_ds)),
        help=f"comma-separated subset of: {','.join(sorted(all_ds))}",
    )
    p.add_argument("--out", type=Path, default=None, help="output .sqlpp (default: stdout)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="print planned counts only",
    )
    args = p.parse_args()

    requested = {s.strip() for s in args.datasets.split(",") if s.strip()}
    unknown = requested - all_ds
    if unknown:
        print(f"error: unknown dataset(s): {unknown}", file=sys.stderr)
        return 1

    if args.mode == "random":
        if args.count < 1:
            print("error: --mode random requires --count >= 1", file=sys.stderr)
            return 1
        if args.seed is None:
            print("error: --mode random requires --seed", file=sys.stderr)
            return 1

        for s in sorted(requested & non_dense_random):
            print(
                f"note: skipping non-dense dataset {s!r} "
                f"(see scripts/asterix/POINT_QUERY_DATASET_DENSITY.md)",
                file=sys.stderr,
            )
        work = requested - non_dense_random

        rng = random.Random(args.seed)
        lines, notes = _random_select_lines(
            args.dataverse,
            work,
            args.count,
            rng,
            DEFAULT_RANDOM_BOUNDS,
        )
        for msg in notes:
            print(f"note: {msg}", file=sys.stderr)

        n_stmts = sum(1 for l in lines if l.strip().startswith("SELECT"))
        if args.dry_run:
            print(
                f"Would write {n_stmts} SELECTs (--mode random, --count {args.count})",
                file=sys.stderr,
            )
            return 0

        text = "\n".join(lines) + "\n"
        if args.out:
            args.out.write_text(text, encoding="utf-8")
            print(
                f"Wrote {args.out} ({n_stmts} SELECTs, random mode)",
                file=sys.stderr,
            )
        else:
            sys.stdout.write(text)
        return 0

    if args.min < 1 or args.max < args.min:
        print("error: need 1 <= --min <= --max", file=sys.stderr)
        return 1
    if not (1 <= args.d_id <= 10):
        print("warning: TPC-C districts are 1..10", file=sys.stderr)

    if requested & non_dense_random:
        for s in sorted(requested & non_dense_random):
            print(
                f"warning: {s!r} is a poor fit for dense PK probes; "
                f"see POINT_QUERY_DATASET_DENSITY.md (generating sweep anyway)",
                file=sys.stderr,
            )

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
