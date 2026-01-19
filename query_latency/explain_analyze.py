#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL EXPLAIN ANALYZE 解析器

- 连接 MySQL，执行 EXPLAIN ANALYZE <SQL>
- 解析文本树中的每个算子，抽取实际耗时(actual time)、rows、loops
- 汇总每条 SQL 的各算子平均耗时（按 actual_time_high/loops 近似）与出现次数

注意：MySQL 8.0 的 EXPLAIN ANALYZE 输出为文本树，不提供 JSON。
本模块采用正则表达式做尽量稳健的解析，但不同版本格式可能略有差异。
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _run_explain_analyze(db, sql: str) -> str:
    rows = db.execute_query(f"EXPLAIN ANALYZE {sql}")
    # mysql-connector 返回单列 'EXPLAIN'
    parts: List[str] = []
    for r in rows:
        if isinstance(r, dict):
            val = r.get("EXPLAIN")
        else:
            val = r[0] if r else None
        if val is None:
            continue
        parts.append(str(val))
    return "\n".join(parts)


_NODE_PAT = re.compile(
    r"^(?P<indent>[ \t\|\u2502\u251c\u2514\u2500]*)->\s*(?P<op>[^\(\n]+?)\s*"  # 允许空格/制表/竖线/Unicode 树形字符
    r"(?:\([^\)]*\)\s*)*"              # 可能存在 0-N 个括号段 (cost=..., rows=...)
    r"\((?:actual\s+)?time\s*=\s*(?P<tlow>[\d.eE+\-]+)\.\.(?P<thi>[\d.eE+\-]+)\s*,?\s*rows\s*=\s*(?P<rows>[\d.eE+\-]+)\s*,?\s*loops\s*=\s*(?P<loops>[\d.eE+\-]+)\)"  # 实际信息；允许无逗号分隔与科学计数
    , re.IGNORECASE)


def parse_explain_analyze(text: str, debug: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    # 先按缩进构建父子关系
    raw_nodes: List[Dict[str, Any]] = []
    stack: List[Tuple[int, int]] = []  # (indent_len, index_in_raw_nodes)

    lines = text.splitlines()
    for idx_line, line in enumerate(lines):
        m = _NODE_PAT.search(line)
        if not m:
            # 近似匹配：当正则未命中，但包含关键字和 (time=..rows=..loops=..)
            lw = line.lower()
            if ("join" in lw or "sort" in lw or "group" in lw or "filter" in lw or "scan" in lw or "count" in lw or "lookup" in lw or "hash" in lw or "aggregate" in lw):
                # 提取操作名：去掉前导树形符与 '->'
                op = re.sub(r"^[ \t\|\u2502\u251c\u2514\u2500]*->\s*", "", line).strip()
                # 搜索 time/rows/loops 片段
                mm = re.search(r"\((?:actual\s+)?time\s*=\s*([\d.eE+\-]+)\.\.([\d.eE+\-]+)\s*,?\s*rows\s*=\s*([\d.eE+\-]+)\s*,?\s*loops\s*=\s*([\d.eE+\-]+)\)", line, re.IGNORECASE)
                if not mm:
                    # 尝试在后续1-2行寻找 time 片段（处理换行拆分）
                    lookahead = " ".join(lines[idx_line: idx_line+3])
                    mm = re.search(r"\((?:actual\s+)?time\s*=\s*([\d.eE+\-]+)\.\.([\d.eE+\-]+)\s*,?\s*rows\s*=\s*([\d.eE+\-]+)\s*,?\s*loops\s*=\s*([\d.eE+\-]+)\)", lookahead, re.IGNORECASE)
                if mm:
                    try:
                        thi = float(mm.group(2))
                        loops = float(mm.group(4))
                        rows = float(mm.group(3))
                        avg_time = thi / loops if loops > 0 else None
                        # 去掉末尾 actual time 段，保留其余文本作为 text（含谓词/lookup 等）
                        text_no_time = re.sub(r"\((?:actual\s+)?time\s*=.*?loops\s*=.*?\)\s*$", "", line).rstrip()
                        if debug is not None:
                            debug.append(f"FALLBACK HIT l{idx_line+1}: op='{op}', thi={thi}, loops={loops}, rows={rows}, avg={avg_time}")
                        raw_nodes.append({
                            "op": op,
                            "actual_time": thi,
                            "avg_time": avg_time,
                            "loops": loops,
                            "rows": rows,
                            "text": text_no_time,
                            "indent": 0,
                            "parent": None,
                            "children": [],
                        })
                    except Exception:
                        if debug is not None:
                            debug.append(f"FALLBACK PARSE ERROR l{idx_line+1}: {line}")
                        pass
            continue
        indent_len = len(m.group("indent") or "")
        op = m.group("op").strip()
        try:
            t_hi = float(m.group("thi"))
            loops = float(m.group("loops"))
            rows = float(m.group("rows"))
        except Exception:
            t_hi, loops, rows = None, None, None
        avg_time = None
        if t_hi is not None and loops and loops > 0:
            avg_time = t_hi / float(loops)
        # 去掉末尾 actual time 段，保留其余文本作为 text（含谓词/lookup 等）
        text_no_time = re.sub(r"\((?:actual\s+)?time\s*=.*?loops\s*=.*?\)\s*$", "", line).rstrip()

        # 维护父子栈
        while stack and indent_len <= stack[-1][0]:
            stack.pop()
        parent_idx: Optional[int] = stack[-1][1] if stack else None
        idx = len(raw_nodes)
        raw_nodes.append({
            "op": op,
            "actual_time": t_hi,
            "avg_time": avg_time,
            "loops": loops,
            "rows": rows,
            "text": text_no_time,
            "indent": indent_len,
            "parent": parent_idx,
            "children": [],
        })
        if parent_idx is not None:
            raw_nodes[parent_idx]["children"].append(idx)
        stack.append((indent_len, idx))
        if debug is not None:
            debug.append(f"MATCH l{idx_line+1}: indent={indent_len}, op='{op}', thi={t_hi}, loops={loops}, rows={rows}, avg={avg_time}, parent={parent_idx}")

    # 计算独占时间（每 loop）：exclusive = avg_time - sum(child.avg_time * (child.loops/parent.loops))
    for idx in reversed(range(len(raw_nodes))):
        node = raw_nodes[idx]
        avg = float(node.get("avg_time") or 0.0)
        loops = float(node.get("loops") or 0) or 0.0
        contrib = 0.0
        if node["children"]:
            for cidx in node["children"]:
                child = raw_nodes[cidx]
                c_avg = float(child.get("avg_time") or 0.0)
                c_loops = float(child.get("loops") or 0.0)
                if loops > 0 and c_loops > 0:
                    contrib += c_avg * (c_loops / loops)
                else:
                    contrib += c_avg
        exclusive = avg - contrib
        if exclusive < 0:
            exclusive = 0.0
        node["exclusive_time"] = exclusive
        if debug is not None:
            debug.append(f"EXCLUSIVE idx={idx}: op='{node.get('op')}', avg={avg}, contrib={contrib}, exclusive={exclusive}")

    # 输出扁平节点列表（保留 exclusive_time）
    nodes: List[Dict[str, Any]] = []
    for n in raw_nodes:
        nodes.append({
            "op": n.get("op"),
            "actual_time": n.get("actual_time"),
            "avg_time": n.get("avg_time"),
            "exclusive_time": n.get("exclusive_time"),
            "loops": n.get("loops"),
            "rows": n.get("rows"),
        })
    return nodes


def summarize_nodes(nodes: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    agg: Dict[str, Dict[str, Any]] = {}
    for n in nodes:
        op = n.get("op") or "(unknown)"
        d = agg.setdefault(op, {"count": 0, "sum_avg_time": 0.0})
        d["count"] += 1
        if n.get("avg_time") is not None:
            d["sum_avg_time"] += float(n["avg_time"])
    # finalize avg_time
    for op, d in agg.items():
        cnt = d["count"]
        d["avg_time"] = (d["sum_avg_time"] / cnt) if cnt > 0 else None
        del d["sum_avg_time"]
    return agg


def analyze_sql(db, sql: str) -> Dict[str, Any]:
    txt = _run_explain_analyze(db, sql)
    nodes = parse_explain_analyze(txt)
    summary = summarize_nodes(nodes)
    return {
        "raw": txt,
        "nodes": nodes,
        "summary": summary,
    }


def analyze_dir(db, sql_dir: str) -> Dict[str, Any]:
    base = Path(sql_dir)
    out: Dict[str, Any] = {}
    for p in base.iterdir():
        m = re.match(r"query_?(\d+)\.sql$", p.name)
        if not m or not p.is_file():
            continue
        qid = int(m.group(1))
        sql = p.read_text(encoding="utf-8").strip()
        try:
            out[qid] = analyze_sql(db, sql)
        except Exception as e:
            out[qid] = {"error": str(e)}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="对目录内 queryN.sql 执行 EXPLAIN ANALYZE，并统计各算子平均耗时与出现次数")
    ap.add_argument("--sql-dir", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--out", help="输出 JSON 文件路径；省略则打印到标准输出")
    args = ap.parse_args()

    try:
        from DataBase.MySQLDriver import MySQLDriver
    except Exception:
        raise SystemExit("MySQLDriver 模块不可用")

    cfg = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise SystemExit("无法连接 MySQL（用于 EXPLAIN ANALYZE）")
    try:
        result = analyze_dir(drv, args.sql_dir)
    finally:
        try:
            drv.disconnect()
        except Exception:
            pass

    s = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(s, encoding="utf-8")
    else:
        print(s)


if __name__ == "__main__":
    main()
