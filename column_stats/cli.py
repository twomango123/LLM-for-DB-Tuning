#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from typing import Any, Dict

from column_stats.estimator import ColumnLengthEstimator


def main() -> None:
    ap = argparse.ArgumentParser(description="MySQL 列长度估算（抽样 + 定长映射）")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--sample-ratio", type=float, default=0.01)
    ap.add_argument("--limit", type=int, default=10000)
    args = ap.parse_args()

    # 延用项目已有的 MySQLDriver
    try:
        from DataBase.MySQLDriver import MySQLDriver
    except Exception:
        raise SystemExit("MySQLDriver 模块不可用")

    cfg: Dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise SystemExit("无法连接 MySQL")

    try:
        est = ColumnLengthEstimator(drv)
        res = est.estimate_table(args.table, sample_ratio=args.sample_ratio, sample_limit=args.limit)
        print(json.dumps(res, ensure_ascii=False, indent=2))
    finally:
        try:
            drv.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
