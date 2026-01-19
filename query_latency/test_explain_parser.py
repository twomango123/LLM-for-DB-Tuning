#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试 EXPLAIN ANALYZE 解析的小工具

两种用法：
1) 从文本文件读取 EXPLAIN ANALYZE 原文并解析（无需连接数据库）：
   python3 LLM-for-DB-Tuning/query_latency/test_explain_parser.py \
       --from-file LLM-for-DB-Tuning/debug/part2/explain/q12.txt

2) 直接连接 MySQL 执行 EXPLAIN ANALYZE 并解析：
   python3 LLM-for-DB-Tuning/query_latency/test_explain_parser.py \
       --sql "SELECT COUNT(*) FROM tpcch.item" \
       --config LLM-for-DB-Tuning/query_latency/db_config.ini

输出：
- 解析到的节点数组（包含 avg_time 与 exclusive_time，单位：秒）
- 每类算子的出现次数与平均耗时（秒）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _THIS_DIR.parent  # LLM-for-DB-Tuning/
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))

from query_latency.explain_analyze import parse_explain_analyze, summarize_nodes  # type: ignore


def _load_mysql_cfg(ini_path: str) -> Dict[str, Any]:
    import configparser

    p = Path(ini_path)
    if not p.exists():
        raise SystemExit(f"配置文件不存在：{ini_path}")
    cfg = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=())
    cfg.read(p)
    if "mysql" not in cfg:
        raise SystemExit("配置文件缺少 [mysql] 段")
    sec = cfg["mysql"]
    return {
        "host": sec.get("host", fallback="127.0.0.1"),
        "port": sec.getint("port", fallback=3306),
        "user": sec.get("user", fallback="root"),
        "password": sec.get("password", fallback=""),
        "database": sec.get("database", fallback=""),
    }


def _run_explain(db, sql: str) -> str:
    rows = db.execute_query(f"EXPLAIN ANALYZE {sql}")
    parts = []
    for r in rows:
        if isinstance(r, dict):
            val = r.get("EXPLAIN")
        else:
            val = r[0] if r else None
        if val is not None:
            parts.append(str(val))
    return "\n".join(parts)


def main() -> None:
    ap = argparse.ArgumentParser(description="测试 MySQL EXPLAIN ANALYZE 文本解析")
    ap.add_argument("--from-file", help="从文本文件读取 EXPLAIN ANALYZE 原文解析")
    ap.add_argument("--sql", help="直接提供要 EXPLAIN ANALYZE 的 SQL")
    ap.add_argument("--sql-file", help="从文件读取 SQL 文本")
    ap.add_argument("--config", default=str(_ROOT_DIR / "query_latency" / "db_config.ini"), help="数据库 INI 配置文件路径")
    ap.add_argument("--print-raw", action="store_true", help="打印 EXPLAIN 原文")
    args = ap.parse_args()

    text = None
    if args.from_file:
        text = Path(args.from_file).read_text(encoding="utf-8")
    else:
        sql = None
        if args.sql:
            sql = args.sql
        elif args.sql_file:
            sql = Path(args.sql_file).read_text(encoding="utf-8").strip()
        if not sql:
            ap.error("必须提供 --from-file 或 --sql/--sql-file 其一")
        try:
            from DataBase.MySQLDriver import MySQLDriver  # type: ignore
        except Exception:
            raise SystemExit("无法导入 MySQLDriver，不能直连数据库执行 EXPLAIN")
        cfg = _load_mysql_cfg(args.config)
        if not cfg.get("database"):
            raise SystemExit("配置缺少 database，无法连接")
        drv = MySQLDriver(cfg)
        if not drv.connect():
            raise SystemExit("无法连接 MySQL，检查配置与权限")
        try:
            text = _run_explain(drv, sql)
        finally:
            try:
                drv.disconnect()
            except Exception:
                pass

    if args.print_raw and text:
        print("==== RAW EXPLAIN ANALYZE ====")
        print(text)
        print("==============================")

    dbg: list[str] = []
    nodes = parse_explain_analyze(text or "", debug=dbg)
    summary = summarize_nodes(nodes)

    print("==== NODES (parsed) ====")
    print(json.dumps(nodes, ensure_ascii=False, indent=2))
    if dbg:
        print("==== DEBUG TRACE ====")
        for line in dbg:
            print(line)

    # 额外给出总 avg 与总 exclusive 的加和，便于快速对比
    total_avg = sum(float(n.get("avg_time") or 0.0) for n in nodes)
    total_excl = sum(float(n.get("exclusive_time") or (n.get("avg_time") or 0.0)) for n in nodes)
    print("==== SUMMARY (by op) ====")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Totals: avg_sum={total_avg:.6f}s, exclusive_sum={total_excl:.6f}s, nodes={len(nodes)}")


if __name__ == "__main__":
    main()
