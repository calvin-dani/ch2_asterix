#!/usr/bin/env python3
"""Run Asterix DDL then bulk-load JSON in one process (no separate generator + load_ddl).

Two modes:

**CH2++ docgen layout** (``--docgen-dir``): one ``-D`` dataverse; DDL creates all datasets and
types; the load phase runs the same multi-dataset script as ``generate_load_sqlpp.py`` (one
statement per dataset, all in one POST sequence — not one shell command per table).

**Single flat directory** (``--json-dir`` + ``--dataset``): same as
``generate_json_dir_load_sqlpp.py``.

Run from ``ch2driver/pytpcc`` — CH2 example::

  python scripts/asterix/asterix_ddl_and_json_load.py \\
    --url http://127.0.0.1:19002/query/service \\
    --ddl-file ddl/asterix/ch2pp_bench.sqlpp \\
    -D mydv \\
    --docgen-dir /tmp/ch2_data \\
    --nc-host 10.16.229.103

Single-dataset example::

  python scripts/asterix/asterix_ddl_and_json_load.py \\
    --ddl-file path/to/schema.sqlpp \\
    -D mydataverse \\
    --json-dir /path/to/flat/json \\
    --dataset tweetsdatasetE \\
    --nc-host 10.16.229.101 \\
    --syntax copy

Use ``--dry-run`` / ``--dry-run-all`` / ``--skip-ddl`` as in ``--help``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import load_ddl  # noqa: E402
from generate_json_dir_load_sqlpp import build_json_dir_load_sqlpp  # noqa: E402
from generate_load_sqlpp import build_ch2_docgen_load_sqlpp  # noqa: E402


def _err(msg: str) -> None:
    print(msg, file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser(
        description="POST DDL then bulk-load JSON (CH2 docgen tree or single flat directory)",
    )
    p.add_argument(
        "--url",
        default="http://127.0.0.1:19002/query/service",
        help="Cluster Controller /query/service URL",
    )
    p.add_argument(
        "--ddl-file",
        type=Path,
        default=None,
        help="SQL++ with DROP/CREATE DATAVERSE, types, CREATE DATASET, etc.",
    )
    p.add_argument(
        "-D",
        "--dataverse",
        default="",
        metavar="NAME",
        help="Target dataverse (-D mydv): rewrite from --dataverse-from; USE in generated load SQL",
    )
    p.add_argument(
        "--dataverse-from",
        default="bench",
        metavar="NAME",
        help="Placeholder name in DDL to replace with --dataverse (default: bench)",
    )
    p.add_argument(
        "--skip-ddl",
        action="store_true",
        help="Skip posting --ddl-file (only run bulk load; schema must already exist)",
    )
    p.add_argument(
        "--docgen-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="CH2++ docgen root (output_dir with warehouse/, customer/, ... subdirs). "
        "Loads every dataset that has *.json — same as generate_load_sqlpp.py.",
    )
    p.add_argument(
        "--json-dir",
        type=Path,
        default=None,
        help="Single-dataset mode: directory of *.json shards (omit when using --docgen-dir)",
    )
    p.add_argument(
        "--dataset",
        default=None,
        help="Single-dataset mode: dataset name (required with --json-dir)",
    )
    p.add_argument(
        "--nc-host",
        default=None,
        help="Node Controller host for localfs URIs (required for load)",
    )
    p.add_argument(
        "--syntax",
        choices=("load", "copy"),
        default="load",
        help="Bulk load SQL form (default: load)",
    )
    p.add_argument(
        "--remote-base",
        type=Path,
        default=None,
        help="Single-dataset mode only: see generate_json_dir_load_sqlpp.py",
    )
    p.add_argument(
        "--max-uris-per-load",
        type=int,
        default=0,
        metavar="N",
        help="Single-dataset mode only: split bulk load into chunks of N URIs if > 0",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated load SQL to stdout; do not post load (post DDL unless --skip-ddl)",
    )
    p.add_argument(
        "--dry-run-all",
        action="store_true",
        help="Print generated load SQL only; do not POST anything",
    )
    args = p.parse_args()

    if args.dry_run_all and args.dry_run:
        p.error("use either --dry-run or --dry-run-all, not both")

    if args.docgen_dir and (args.json_dir or args.dataset):
        p.error("use either --docgen-dir (CH2) or --json-dir + --dataset, not both")

    if args.docgen_dir and (
        args.remote_base is not None or args.max_uris_per_load != 0
    ):
        p.error("--remote-base and --max-uris-per-load apply only to single-dataset mode (--json-dir)")

    ch2_mode = args.docgen_dir is not None
    flat_mode = args.json_dir is not None or args.dataset is not None

    if ch2_mode and flat_mode:
        p.error("conflicting modes")

    if not ch2_mode and not flat_mode:
        p.error("need --docgen-dir (CH2 layout) or both --json-dir and --dataset (single dataset)")

    if flat_mode:
        if not args.json_dir or not args.dataset:
            p.error("single-dataset mode requires both --json-dir and --dataset")
    if not args.nc_host:
        p.error("--nc-host is required for bulk load")

    run_schema = not args.skip_ddl and not args.dry_run_all
    if run_schema and not args.ddl_file:
        p.error("--ddl-file is required unless --skip-ddl or --dry-run-all")

    if run_schema and args.ddl_file:
        ddl_path = args.ddl_file.expanduser().resolve()
        if not ddl_path.is_file():
            _err(f"error: DDL file not found: {ddl_path}")
            return 1
        ddl_raw = ddl_path.read_text(encoding="utf-8")
        _err("[1/3] Posting DDL (schema): " + str(ddl_path))
        rc = load_ddl.run_sqlpp_text(
            args.url,
            ddl_raw,
            dataverse=args.dataverse,
            dataverse_from=args.dataverse_from,
        )
        if rc != 0:
            return rc
        _err("[1/3] DDL step finished.")
    elif args.skip_ddl:
        _err("[1/3] Skipped DDL (--skip-ddl).")
    elif args.dry_run_all:
        _err("[1/3] Skipped DDL (dry-run-all).")

    dv = args.dataverse.strip() if args.dataverse else ""
    if not dv:
        _err("error: --dataverse (-D) is required for bulk load")
        return 1

    _err("[2/3] Building bulk-load SQL in memory (no per-file I/O here).")
    try:
        if ch2_mode:
            load_raw = build_ch2_docgen_load_sqlpp(
                args.docgen_dir,
                args.nc_host.strip(),
                dv,
                syntax=args.syntax,
            )
        else:
            rb = args.remote_base.expanduser().resolve() if args.remote_base else None
            load_raw = build_json_dir_load_sqlpp(
                args.json_dir,
                args.dataset,
                dv,
                args.nc_host,
                syntax=args.syntax,
                remote_base=rb,
                max_uris_per_load=args.max_uris_per_load,
            )
    except (FileNotFoundError, ValueError) as e:
        _err(f"error: {e}")
        return 1

    _err("[2/3] Load script ready.")

    if args.dry_run or args.dry_run_all:
        _err("[3/3] Skipped posting load SQL (--dry-run or --dry-run-all).")
        _err("--- generated load SQL (stdout below) ---")
        print(load_raw, end="")
        return 0

    _err("[3/3] Posting bulk-load SQL to " + args.url)
    rc = load_ddl.run_sqlpp_text(
        args.url,
        load_raw,
        dataverse=args.dataverse,
        dataverse_from=args.dataverse_from,
    )
    if rc == 0:
        _err("[3/3] Bulk-load step finished.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
