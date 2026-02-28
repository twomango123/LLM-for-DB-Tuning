#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Measure DML (INSERT/UPDATE) execution time once per SQL file and persist to a reusable JSON cache.

For UPDATE with WHERE, also run an EXPLAIN ANALYZE of a SELECT COUNT(*) on the same table and WHERE
clause to estimate the filter (scan) time, and record both raw exec_time_s and where_select_time_s,
as well as effective_time_s = max(0, exec_time_s - where_select_time_s).

Cache format (JSON):
{
  "generated_at": "ISO8601",
  "entries": [
    {
      "filename": "upd_xxx.sql",      # basename of file
      "type": "UPDATE"|"INSERT"|"REPLACE",
      "table": "t",
      "columns": ["c1","c2"],        # only for UPDATE; columns in SET
      "exec_time_s": 0.0123,           # program-measured time for DML statement
      "where_select_time_s": 0.0045,   # EXPLAIN ANALYZE SELECT COUNT(*) ... WHERE ... (if applicable)
      "effective_time_s": 0.0078       # exec_time_s - where_select_time_s (>=0)
    },
    ...
  ]
}

Notes:
- This script executes statements against the target DB and COMMITs; run on a test DB or
  wrap in your own transaction/snapshot if needed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Ensure project root is on sys.path for `DataBase.*` imports when invoked from subdir
import sys
_THIS_DIR = os.path.dirname(__file__)
_ROOT_DIR = os.path.dirname(_THIS_DIR)
if _ROOT_DIR and _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)


def _read_sql_files(sql_dir: str) -> List[Tuple[str, str]]:
    base = Path(sql_dir)
    out: List[Tuple[str, str]] = []
    for p in sorted(base.rglob("*.sql")):
        if not p.is_file():
            continue
        try:
            out.append((p.name, p.read_text(encoding="utf-8").strip()))
        except Exception:
            continue
    return out


def _unquote_ident(name: str) -> str:
    name = name.strip()
    if name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name


def _norm_table(name: str) -> str:
    name = _unquote_ident(name)
    if "." in name:
        name = name.split(".")[-1]
    return name


def _extract_update_sets(sql: str) -> Tuple[Optional[str], List[str]]:
    # Try simple UPDATE t SET ...
    m = re.search(r"\bUPDATE\s+(`?[A-Za-z_][A-Za-z0-9_]*`?(?:\s*\.\s*`?[A-Za-z_][A-Za-z0-9_]*`?)?)\s+(?:AS\s+`?[A-Za-z_][A-Za-z0-9_]*`?\s+)?SET\s+(.+?)(?=\bWHERE\b|$)", sql, re.IGNORECASE | re.DOTALL)
    if m:
        tbl = _norm_table(m.group(1))
        sets = m.group(2)
        cols: List[str] = []
        for s in sets.split(","):
            s = s.strip()
            m2 = re.match(r"(`?[A-Za-z_][A-Za-z0-9_]*`?(?:\s*\.\s*`?[A-Za-z_][A-Za-z0-9_]*`?)?)\s*=", s)
            if m2:
                lhs = _unquote_ident(m2.group(1)).replace(" ", "")
                col = lhs.split(".")[-1]
                cols.append(col)
        return tbl, cols
    # Fallback: unknown/multi-table
    return None, []


def _extract_where_clause(sql: str) -> Optional[str]:
    try:
        m = re.search(r"\bUPDATE\b.+?\bSET\b.+?(?:\bWHERE\b(?P<w>.+?))?(?:\bORDER\b|\bLIMIT\b|;|$)", sql, re.IGNORECASE | re.DOTALL)
        if m and m.group("w"):
            return m.group("w").strip()
    except Exception:
        return None
    return None


def _run_explain_analyze(db, sql: str) -> float:
    from query_latency.explain_analyze import analyze_sql
    res = analyze_sql(db, sql)
    nodes = res.get("nodes") or []
    total = 0.0
    for n in nodes:
        v = n.get("exclusive_time")
        if v is None:
            v = n.get("avg_time")
        try:
            total += float(v or 0.0)
        except Exception:
            pass
    return max(0.0, total)


def main() -> None:
    ap = argparse.ArgumentParser(description="Measure DML timing once and persist to JSON cache for reuse by PART2.")
    ap.add_argument("--sql-dir", required=True, help="Directory of .sql files to measure")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="")
    ap.add_argument("--out", default=str(Path("debug/part2/dml_time_cache.json")), help="Output cache path")
    args = ap.parse_args()

    try:
        from DataBase.MySQLDriver import MySQLDriver
    except ModuleNotFoundError as e:
        msg = (
            "MySQLDriver 模块不可用：" + str(e) + "\n"
            "排查建议：\n"
            "- 请在仓库根目录运行，或设置 PYTHONPATH 指向仓库根目录\n"
            "- pip install -r requirements.txt（需要 mysql-connector-python）\n"
        )
        raise SystemExit(msg)
    except Exception as e:
        raise SystemExit(f"MySQLDriver 模块不可用: {e}")

    cfg = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise SystemExit("无法连接 MySQL")

    entries: List[Dict[str, Any]] = []
    try:
        for fname, sql in _read_sql_files(args.sql_dir):
            if not re.match(r"^\s*(INSERT|REPLACE|UPDATE)\b", sql, re.IGNORECASE):
                continue
            typ = re.match(r"^\s*(INSERT|REPLACE|UPDATE)\b", sql, re.IGNORECASE).group(1).upper()

            # Program timing of the DML
            t0 = time.perf_counter()
            ok = drv.execute_statement(sql)
            t1 = time.perf_counter()
            if not ok:
                continue
            exec_time_s = max(0.0, t1 - t0)

            rec: Dict[str, Any] = {
                "filename": fname,
                "type": typ,
                "exec_time_s": exec_time_s,
            }
            if typ in ("INSERT", "REPLACE"):
                # Try to detect table name
                m = re.search(r"\b(INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+(`?[A-Za-z_][A-Za-z0-9_]*`?(?:\s*\.\s*`?[A-Za-z_][A-Za-z0-9_]*`?)?)", sql, re.IGNORECASE)
                if m:
                    rec["table"] = _norm_table(m.group(2))
                entries.append(rec)
                continue

            # UPDATE case
            tbl, cols = _extract_update_sets(sql)
            if tbl:
                rec["table"] = tbl
                if cols:
                    rec["columns"] = cols
            # WHERE-based filter costing
            where_time_s = 0.0
            wc = _extract_where_clause(sql)
            if tbl and wc:
                select_sql = f"SELECT COUNT(*) FROM `{tbl}` WHERE {wc}"
                try:
                    where_time_s = _run_explain_analyze(drv, select_sql)
                except Exception:
                    where_time_s = 0.0
            rec["where_select_time_s"] = where_time_s
            rec["effective_time_s"] = max(0.0, exec_time_s - where_time_s)
            entries.append(rec)
    finally:
        try:
            drv.disconnect()
        except Exception:
            pass

    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "entries": entries,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(entries)} entries to {out_path}")


if __name__ == "__main__":
    main()
