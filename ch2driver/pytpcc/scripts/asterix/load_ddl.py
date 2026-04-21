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


def _split_statements(text: str) -> list[str]:
    """Split on ';' outside `--` line comments and outside '...' / \"...\" strings."""
    stmts: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(text)
    state = "norm"

    def flush() -> str:
        s = "".join(buf).strip()
        buf.clear()
        return s

    while i < n:
        c = text[i]
        if state == "lcom":
            if c == "\n":
                state = "norm"
            i += 1
            continue
        if state == "squote":
            buf.append(c)
            if c == "'":
                if i + 1 < n and text[i + 1] == "'":
                    buf.append(text[i + 1])
                    i += 2
                    continue
                state = "norm"
            i += 1
            continue
        if state == "dquote":
            buf.append(c)
            if c == '"':
                state = "norm"
            i += 1
            continue
        if c == "'":
            state = "squote"
            buf.append(c)
            i += 1
            continue
        if c == '"':
            state = "dquote"
            buf.append(c)
            i += 1
            continue
        if c == "-" and i + 1 < n and text[i + 1] == "-":
            state = "lcom"
            i += 2
            continue
        if c == ";":
            st = flush()
            if st:
                stmts.append(st)
            i += 1
            continue
        buf.append(c)
        i += 1

    tail = flush()
    if tail:
        stmts.append(tail)
    return stmts


def _infer_active_dataverse(raw: str, args) -> str:
    """Default dataverse for statements (file USE line, --dataverse, or bench)."""
    if getattr(args, "dataverse", None) and args.dataverse.strip():
        return args.dataverse.strip()
    m = re.search(r"(?i)\bUSE\s+(\w+)\s*;", raw)
    if m:
        return m.group(1)
    return "bench"


def _apply_dataverse(raw: str, target: str, source: str) -> str:
    """Rewrite USE/DROP/CREATE DATAVERSE from source name to target (identifiers in DDL)."""
    if not target.strip() or target == source:
        return raw
    esc = re.escape(source)
    raw = re.sub(
        rf"(?i)\bDROP\s+DATAVERSE\s+{esc}\s+IF\s+EXISTS\s*;",
        f"DROP DATAVERSE {target} IF EXISTS;",
        raw,
    )
    raw = re.sub(
        rf"(?i)\bCREATE\s+DATAVERSE\s+{esc}\s*;",
        f"CREATE DATAVERSE {target};",
        raw,
    )
    raw = re.sub(rf"(?i)\bUSE\s+{esc}\s*;", f"USE {target};", raw)
    return raw


def _post_statement(url: str, statement: str, timeout: float | None = 600.0):
    data = urllib.parse.urlencode({"statement": statement}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    # timeout None = wait indefinitely (for very large LOAD/COPY jobs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_sqlpp_text(
    url: str,
    raw: str,
    *,
    dataverse: str = "",
    dataverse_from: str = "bench",
    timeout: float | None = 600.0,
    preview_chars: int = 240,
    verbose: bool = False,
) -> int:
    """
    Post each statement from a SQL++ script to ``url`` (same rules as the ``--file`` path in main:
    strip ``/* */``, optional rewrite via ``--dataverse`` / ``dataverse_from``, split on ``;``,
    prepend ``USE`` per statement when required). Returns 0 on success, 1 on failure.

    With ``verbose=True``, print each statement preview before POST and ``OK n`` after success.
    With ``verbose=False`` (default), run quietly and print a one-line summary on success.
    Errors are always printed.
    """
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)
    if dataverse:
        raw = _apply_dataverse(raw, dataverse.strip(), dataverse_from.strip())

    class _Ns:
        pass

    ns = _Ns()
    ns.dataverse = dataverse

    def _strip_leading_line_comments(block: str) -> str:
        lines = block.split("\n")
        while lines and lines[0].strip().startswith("--"):
            lines.pop(0)
        return "\n".join(lines).strip()

    parts = [
        _strip_leading_line_comments(s)
        for s in _split_statements(raw)
        if _strip_leading_line_comments(s)
    ]

    active = _infer_active_dataverse(raw, ns)
    n_post = 0
    for stmt in parts:
        st = stmt.strip()
        lean = " ".join(st.split())

        use_m = re.match(r"(?i)^USE\s+(\w+)\s*;?\s*$", st)
        if use_m:
            active = use_m.group(1)
            continue

        if re.match(r"(?i)^DROP\s+DATAVERSE", st):
            to_post = lean
        elif re.match(r"(?i)^CREATE\s+DATAVERSE", st):
            to_post = lean
        else:
            to_post = f"USE {active}; {lean}"

        if verbose:
            preview = (
                to_post
                if len(to_post) <= preview_chars
                else to_post[:preview_chars] + "..."
            )
            print(
                f"[posting {n_post + 1}] {preview}",
                file=sys.stderr,
                flush=True,
            )
        try:
            body = _post_statement(url, to_post, timeout=timeout)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
            except Exception:
                err_body = str(e)
            print("HTTP error:", e.code, err_body[:800], file=sys.stderr)
            print("Statement that failed:\n", to_post, file=sys.stderr)
            return 1
        except Exception as ex:
            print("Request failed:", ex, file=sys.stderr)
            print("Statement that was loading:\n", to_post, file=sys.stderr)
            return 1
        if body.get("status") != "success":
            print("Statement failed:", to_post[:200], "...", file=sys.stderr)
            print(body, file=sys.stderr)
            return 1
        n_post += 1
        if verbose:
            print("OK", n_post)
    if not verbose:
        print(
            f"load_ddl: posted {n_post} statement(s) successfully.",
            file=sys.stderr,
        )
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="Load DDL/SQL++ file into AsterixDB",
        epilog=(
            "Examples (from ch2driver/pytpcc): schema, generate LOAD, then load (optional dataverse name): "
            "python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service "
            "--dataverse mydv --file ddl/asterix/ch2pp_bench.sqlpp && "
            "python scripts/asterix/generate_load_sqlpp.py --output-dir /tmp/ch2_data --dataverse mydv "
            "--out /tmp/ch2_load.sqlpp && "
            "python scripts/asterix/load_ddl.py --url http://127.0.0.1:19002/query/service "
            "--dataverse mydv --file /tmp/ch2_load.sqlpp"
        ),
    )
    p.add_argument(
        "--url",
        default="http://127.0.0.1:19002/query/service",
        help="Cluster Controller query service URL",
    )
    p.add_argument("--file", required=True, help="Path to .sqlpp or .sql file")
    p.add_argument(
        "-D",
        "--dataverse",
        default="",
        metavar="NAME",
        help="Target dataverse: rewrite USE/DROP DATAVERSE/CREATE DATAVERSE from --dataverse-from",
    )
    p.add_argument(
        "--dataverse-from",
        default="bench",
        metavar="NAME",
        help="Name to replace in the file when --dataverse is set (default: bench)",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        metavar="SEC",
        help="Per-statement HTTP timeout in seconds (0 = no limit; default: 600)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Log each statement before POST and 'OK' after (default: quiet, one-line summary only)",
    )
    args = p.parse_args()

    with open(args.file, encoding="utf-8") as f:
        raw = f.read()
    to = None if args.timeout == 0 else args.timeout
    return run_sqlpp_text(
        args.url,
        raw,
        dataverse=args.dataverse,
        dataverse_from=args.dataverse_from,
        timeout=to,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    sys.exit(main())
