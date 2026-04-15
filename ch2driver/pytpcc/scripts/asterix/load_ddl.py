#!/usr/bin/env python3
"""POST a SQL++ script (multiple statements) to AsterixDB /query/service."""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

stmt_split = re.compile(r";\s*")


def _post_statement(url: str, statement: str, timeout: float = 600.0):
    data = urllib.parse.urlencode({"statement": statement}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    p = argparse.ArgumentParser(description="Load DDL/SQL++ file into AsterixDB")
    p.add_argument(
        "--url",
        default="http://127.0.0.1:19002/query/service",
        help="Cluster Controller query service URL",
    )
    p.add_argument("--file", required=True, help="Path to .sqlpp or .sql file")
    p.add_argument(
        "--dataverse",
        default="",
        help="If set, prefix each statement with USE <name>;",
    )
    args = p.parse_args()

    with open(args.file, encoding="utf-8") as f:
        raw = f.read()
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)

    def _strip_leading_line_comments(block: str) -> str:
        lines = block.split("\n")
        while lines and lines[0].strip().startswith("--"):
            lines.pop(0)
        return "\n".join(lines).strip()

    parts = [
        _strip_leading_line_comments(s)
        for s in stmt_split.split(raw)
        if _strip_leading_line_comments(s)
    ]

    for i, stmt in enumerate(parts):
        if args.dataverse:
            stmt = f"USE {args.dataverse};\n" + stmt
        lean = " ".join(stmt.split())
        try:
            body = _post_statement(args.url, lean)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
            except Exception:
                err_body = str(e)
            print("HTTP error:", e.code, err_body[:800], file=sys.stderr)
            return 1
        except Exception as ex:
            print("Request failed:", ex, file=sys.stderr)
            return 1
        if body.get("status") != "success":
            print("Statement failed:", stmt[:200], "...", file=sys.stderr)
            print(body, file=sys.stderr)
            return 1
        print("OK", i + 1, "/", len(parts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
