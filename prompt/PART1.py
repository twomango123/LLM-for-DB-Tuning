#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from typing import Dict, List


# 支持 schema.table 以及可选 IF NOT EXISTS，捕获最后的表名部分
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
    re.IGNORECASE,
)


def _extract_columns(block: str) -> Dict[str, str]:
    cols: Dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(',')
        if not line:
            continue
        up = line.upper()
        if up.startswith((
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'KEY', 'CONSTRAINT'
        )):
            continue
        m = re.match(r"`?([A-Za-z0-9_]+)`?\s+([A-Za-z]+)(?:\s*\([^)]*\))?", line)
        if not m:
            continue
        col, typ = m.group(1), m.group(2).upper()
        cols[col] = typ
    return cols


def parse_schema(schema_sql_path: str) -> Dict[str, Dict[str, str]]:
    with open(schema_sql_path, 'r', encoding='utf-8') as f:
        sql = f.read()
    tables: Dict[str, Dict[str, str]] = {}
    pos = 0
    while True:
        m = _CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        tname = m.group(1)
        start = m.end()
        end = sql.find(');', start)
        if end == -1:
            end = sql.find(')\n', start)
            if end == -1:
                break
        block = sql[start:end]
        tables[tname] = _extract_columns(block)
        pos = end + 2
    return tables


def render_schema(tables: Dict[str, Dict[str, str]]) -> str:
    pieces: List[str] = []
    for tname, cols in tables.items():
        lines: List[str] = [f'"{tname}": {{']
        items = list(cols.items())
        for i, (cname, ctype) in enumerate(items):
            comma = ',' if i < len(items) - 1 else ''
            lines.append(f'\t"{cname}": "{ctype}"{comma}')
        lines.append('}')
        pieces.append("\n".join(lines))
    return ",\n".join(pieces)


def build_part1(schema_sql_path: str) -> str:
    tables = parse_schema(schema_sql_path)
    if not tables:
        raise SystemExit(
            f"解析失败：未从 schema.sql 解析到任何表（路径：{schema_sql_path}）。请检查文件内容与 SQL 定义格式。"
        )
    schema_block = render_schema(tables)
    background = (
        "背景：\n\n"
        "你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。\n\n"
    )
    info = (
        "信息：\n\n"
        "数据库当前的模式为：\n\n"
        f"{schema_block}\n\n"
    )
    return background + info


def main() -> None:
    ap = argparse.ArgumentParser(description="PART1: 背景 + schema.sql 转换为模板片段")
    ap.add_argument("schema_sql", help="Path to schema.sql")
    ap.add_argument("--out", help="Output file; if omitted, prints to stdout")
    args = ap.parse_args()

    content = build_part1(args.schema_sql)
    if args.out:
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(content)
    else:
        print(content)


if __name__ == "__main__":
    main()
