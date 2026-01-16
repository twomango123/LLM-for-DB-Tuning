from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from DataBase.MySQLDriver import MySQLDriver  # type: ignore
except Exception:
    MySQLDriver = None  # type: ignore

try:
    from DataBase.PostgreSqlDriver import PostgreSQLDriver  # type: ignore
except Exception:
    PostgreSQLDriver = None  # type: ignore

from .estimator import ColumnStorageEstimator


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="列级存储开销估算（MySQL）")
    p.add_argument("--dialect", choices=["mysql"], default="mysql")
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--schema", help="MySQL 忽略该参数", default=None)
    p.add_argument("--tables", nargs="*", help="可选：限定表名集合")
    p.add_argument("--sample-ratio", type=float, default=0.01)
    p.add_argument("--min-sample-rows", type=int, default=100)
    p.add_argument("--output", type=Path, required=True)
    return p


def connect_driver(args) -> Any:
    if MySQLDriver is None:
        raise RuntimeError("MySQL 驱动不可用")
    cfg: Dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise RuntimeError("连接 MySQL 失败")
    return drv


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    drv = connect_driver(args)
    try:
        est = ColumnStorageEstimator(drv, dialect="mysql")
        est.estimate_tables(
            schema=None,
            tables=args.tables,
            sample_ratio=args.sample_ratio,
            min_sample_rows=args.min_sample_rows,
            out_path=args.output,
        )
        print(json.dumps({"ok": True, "output": str(args.output)}, ensure_ascii=False))
    finally:
        try:
            drv.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
