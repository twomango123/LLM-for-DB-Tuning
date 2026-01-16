from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict
from pathlib import Path
import sys

# Ensure repository root on sys.path so sibling imports (DataBase/*) work
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Prefer in-repo drivers if available (MySQL only)
try:
    from DataBase.MySQLDriver import MySQLDriver  # type: ignore
except Exception:  # pragma: no cover - optional
    MySQLDriver = None  # type: ignore

from .mysql_explain import MySQLCardinalityEstimator
from .sql_builder import build_filter_sql, build_join_sql, build_select_sql


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cardinality estimation via EXPLAIN (MySQL only)")
    p.add_argument("--dialect", choices=["mysql"], default="mysql")
    p.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "0") or 0))
    p.add_argument("--user", default=os.getenv("DB_USER"))
    p.add_argument("--password", default=os.getenv("DB_PASSWORD"))
    p.add_argument("--database", default=os.getenv("DB_NAME"))

    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("filter", help="Estimate filter cardinality")
    f.add_argument("--table", required=True)
    f.add_argument("--column", required=True)
    f.add_argument("--predicate", required=True, help="SQL predicate or operator expression")

    j = sub.add_parser("join", help="Estimate join cardinality")
    j.add_argument("--left", required=True)
    j.add_argument("--right", required=True)
    j.add_argument("--left-col", required=True)
    j.add_argument("--right-col", required=True)
    j.add_argument("--join-predicate", help="Custom ON expression, defaults to equality of the given columns")
    j.add_argument("--join-type", default="INNER")

    s = sub.add_parser("select", help="Estimate SELECT/GROUP BY/ORDER BY cardinality")
    s.add_argument("--table", required=True)
    s.add_argument("--columns", nargs="*", help="Columns to select; empty means *")
    s.add_argument("--where", help="WHERE predicate expression")
    s.add_argument("--group-by", nargs="*", help="GROUP BY columns")
    s.add_argument("--order-by", nargs="*", help="ORDER BY columns")

    return p


def connect_driver(args) -> Any:
    if MySQLDriver is None:
        raise RuntimeError("MySQL driver not available in environment")
    cfg: Dict[str, Any] = {
        "host": args.host,
        "port": args.port or 3306,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise RuntimeError("Failed to connect to MySQL")
    return drv


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    drv = connect_driver(args)
    try:
        estimator = MySQLCardinalityEstimator(drv)

        if args.cmd == "filter":
            sql = build_filter_sql(args.table, args.column, args.predicate)
        elif args.cmd == "join":
            sql = build_join_sql(
                left=args.left,
                right=args.right,
                left_col=args.left_col,
                right_col=args.right_col,
                join_predicate=args.join_predicate,
                join_type=args.join_type,
            )
        else:  # select
            sql = build_select_sql(
                table=args.table,
                columns=args.columns,
                where=args.where,
                group_by=args.group_by,
                order_by=args.order_by,
            )

        result = estimator.estimate(sql)
        result["query"] = sql
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        try:
            drv.disconnect()
        except Exception:
            pass


if __name__ == "__main__":  # pragma: no cover
    main()
