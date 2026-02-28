#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PART3：统计列集合频次

功能：
- 遍历指定目录下的 SQL 文件（递归），对每个查询，提取其中按表引用到的所有列集合；
- 按 <表, 列集合> 进行计数聚合，输出为可读文本。

说明：
- 主要通过识别形如 alias.column 或 table.column 的标识来归集列；
- 适度处理 INSERT/UPDATE 的列清单与赋值（若列未限定别名，则按目标表归集）；
- 未依赖外部延迟/历史负载文件。
"""

from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path
import re

# 复用 PART2 中的收集与解析工具，避免重复实现
try:
    from PART2 import (
        collect_queries2,  # 收集 *.sql
        _collect_aliases,  # alias -> base_table
        _unquote_ident,
        _Q_IDENT,
        _Q_NAME,
        _norm_table,
        parse_schema,
        _extract_order_group,
        _extract_where_predicates,
    )
except Exception:
    # 兜底：若导入失败，定义最小化的等价实现（功能较弱，但能工作）
    collect_queries2 = None  # type: ignore
    _Q_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"  # type: ignore
    _Q_NAME = r"(?:`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)"  # type: ignore

    def _unquote_ident(name: str) -> str:  # type: ignore
        if name.startswith("`") and name.endswith("`"):
            return name[1:-1].replace("``", "`")
        if name.startswith('"') and name.endswith('"'):
            return name[1:-1].replace('""', '"')
        return name

    def _norm_table(name: str) -> str:  # type: ignore
        name = _unquote_ident(name)
        if "." in name:
            name = name.split(".")[-1]
        return name

    def _collect_aliases(sql: str) -> Dict[str, str]:  # type: ignore
        aliases: Dict[str, str] = {}
        kw = r"WHERE|GROUP|ORDER|LIMIT|JOIN|LEFT|RIGHT|INNER|OUTER|ON|HAVING|UNION|EXCEPT|INTERSECT"
        # FROM ... [AS] alias
        for m in re.finditer(r"\bFROM\s+(" + _Q_NAME + r")\s+(?:AS\s+)?(?!" + kw + r"\b)(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
            table = _norm_table(m.group(1))
            alias = _unquote_ident(m.group(2))
            aliases[alias] = table
            aliases[table] = table
        # JOIN ... [AS] alias
        for m in re.finditer(r"\bJOIN\s+(" + _Q_NAME + r")\s+(?:AS\s+)?(?!" + kw + r"\b)(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
            table = _norm_table(m.group(1))
            alias = _unquote_ident(m.group(2))
            aliases[alias] = table
            aliases[table] = table
        # bare FROM/JOINS without alias
        for m in re.finditer(r"\bFROM\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
            table = _norm_table(m.group(1))
            aliases[table] = table
        for m in re.finditer(r"\bJOIN\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
            table = _norm_table(m.group(1))
            aliases[table] = table
        # UPDATE table [AS] alias
        for m in re.finditer(r"\bUPDATE\s+(" + _Q_NAME + r")\s+(?:AS\s+)?(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
            table = _norm_table(m.group(1))
            alias = _unquote_ident(m.group(2))
            aliases[alias] = table
            aliases[table] = table
        for m in re.finditer(r"\bUPDATE\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
            table = _norm_table(m.group(1))
            aliases[table] = table
        return aliases

    # 最小化 schema 解析（兼容 CREATE TABLE 块）
    _CREATE_TABLE_RE = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
        re.IGNORECASE,
    )

    def _extract_columns_from_block(block: str) -> Dict[str, str]:
        cols: Dict[str, str] = {}
        for raw_line in block.splitlines():
            line = raw_line.strip().rstrip(',')
            if not line:
                continue
            up = line.upper()
            if up.startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "KEY", "CONSTRAINT")):
                continue
            m = re.match(r"`?([A-Za-z0-9_]+)`?\s+([A-Za-z]+)(?:\s*\([^)]*\))?", line)
            if not m:
                continue
            col, typ = m.group(1), m.group(2).upper()
            cols[col] = typ
        return cols

    def parse_schema(path: str) -> Dict[str, Dict[str, str]]:  # type: ignore
        sql = Path(path).read_text(encoding='utf-8')
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
            tables[tname] = _extract_columns_from_block(block)
            pos = end + 2
        return tables


def _strip_comments(sql: str) -> str:
    # 去除 /* */ 注释、-- 行注释、# 行注释
    s = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    s = re.sub(r"--.*?$", " ", s, flags=re.MULTILINE)
    s = re.sub(r"#.*?$", " ", s, flags=re.MULTILINE)
    return s


def _extract_table_column_sets(sql: str, schema: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, Set[str]]:
    """从单条 SQL 中提取：每个基础表 -> 该查询引用到的列集合。
    仅依据限定名 alias.col / table.col；
    同时解析：
      - INSERT ... INTO t(col, ...) ...
      - UPDATE t [AS a] SET col=..., a.col2=...
    """
    per_table: Dict[str, Set[str]] = {}
    sql = _strip_comments(sql)
    aliases = _collect_aliases(sql)
    # 作用域中的基础表集合（FROM/JOIN/UPDATE 捕获到的 base tables）
    in_scope_tables: Set[str] = set([t for t in aliases.values()])

    def add_col(tbl: Optional[str], col: str) -> None:
        if not tbl:
            return
        base = aliases.get(tbl, tbl)
        base = _norm_table(base)
        col2 = _unquote_ident(col)
        per_table.setdefault(base, set()).add(col2)

    # 1) 通用：捕获 a.b 或 t.c（仅当 a/t 在别名映射中，避免 schema.table 被误当作 别名.列）
    for m in re.finditer(r"(" + _Q_IDENT + r")\s*\.\s*(" + _Q_IDENT + r")", sql):
        left = _unquote_ident(m.group(1))
        right = _unquote_ident(m.group(2))
        if left not in aliases:
            continue
        base = aliases.get(left, left)
        add_col(base, right)

    # 2) INSERT ... INTO t(col, ...) ...
    for m in re.finditer(r"\b(?:INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+(" + _Q_NAME + r")\s*\(([^)]*)\)", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(1))
        cols = [c.strip() for c in m.group(2).split(',') if c.strip()]
        for c in cols:
            add_col(tbl, _unquote_ident(c))

    # 3) UPDATE t [AS a] SET col=..., a.col2=...
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_NAME + r")(?:\s+(?:AS\s+)?(" + _Q_IDENT + r"))?\s+SET\s+(.+?)(?=\bWHERE\b|$)", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2)) if m.group(2) else None
        sets = m.group(3)
        for s in sets.split(','):
            s = s.strip()
            m2 = re.match(r"(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*=", s, flags=re.IGNORECASE)
            if not m2:
                continue
            lhs = _unquote_ident(m2.group(1)).replace(" ", "")
            if "." in lhs:
                a, c = lhs.split(".", 1)
                add_col(aliases.get(a) or _norm_table(a), c)
            else:
                # 未限定：归到 UPDATE 目标表/别名
                add_col(alias or tbl, lhs)

    # 4) 未限定列（只出现列名 col 而非 a.col）：
    #    解析 WHERE/ORDER BY/GROUP BY 以及 SELECT 列清单中的未限定列，
    #    将其归属于“作用域内包含该列名的所有基础表”。
    if schema:
        # WHERE 子句中出现的未限定列
        for lhs, _pred in _extract_where_predicates(sql):
            tok = lhs.replace(" ", "")
            if "." in tok:
                continue
            col = _unquote_ident(tok)
            for base in in_scope_tables:
                if col in schema.get(base, {}):
                    add_col(base, col)

        # ORDER BY / GROUP BY 未限定列
        for clause in ("ORDER", "GROUP"):
            for idexpr in _extract_order_group(sql, clause):
                tok = idexpr.replace(" ", "")
                if "." in tok:
                    continue
                col = _unquote_ident(tok)
                for base in in_scope_tables:
                    if col in schema.get(base, {}):
                        add_col(base, col)

        # SELECT 列清单（简单提取，忽略函数/星号展开），仅未限定列
        m_sel = re.search(r"\bSELECT\b(.+?)\bFROM\b", sql, flags=re.IGNORECASE | re.DOTALL)
        if m_sel:
            frag = m_sel.group(1)
            # 以逗号分隔，提取第一个标识符
            parts = [p.strip() for p in frag.split(',') if p.strip()]
            for p in parts:
                m1 = re.match(r"(" + _Q_IDENT + r")(?!\s*\()", p)
                if not m1:
                    continue
                tok = _unquote_ident(m1.group(1))
                if "." in tok:
                    continue
                for base in in_scope_tables:
                    if tok in schema.get(base, {}):
                        add_col(base, tok)

    return per_table


def _aggregate_sets(sql_dir: str, schema_sql: Optional[str] = None) -> Dict[str, Dict[Tuple[str, ...], int]]:
    """遍历目录下 SQL，按 <table, sorted(columns)> 进行计数。"""
    counts: Dict[str, Dict[Tuple[str, ...], int]] = {}
    schema: Optional[Dict[str, Dict[str, str]]] = None
    if schema_sql:
        try:
            schema = parse_schema(schema_sql)
        except Exception:
            schema = None

    # 收集 SQL
    items: List[Tuple[int, str, str]]
    if collect_queries2 is not None:
        items, _skipped = collect_queries2(sql_dir)  # type: ignore
    else:
        base = Path(sql_dir)
        files = sorted([p for p in base.rglob("*.sql") if p.is_file()], key=lambda p: str(p))
        items = []
        for i, p in enumerate(files, 1):
            try:
                body = p.read_text(encoding="utf-8").rstrip()
            except Exception:
                try:
                    body = p.read_text(encoding="utf-8-sig").rstrip()
                except Exception:
                    body = p.read_text(encoding="latin-1").rstrip()
            items.append((i, body, str(p)))

    for _i, sql, _path in items:
        tbl2cols = _extract_table_column_sets(sql, schema=schema)
        for tbl, cols in tbl2cols.items():
            key = tuple(sorted(cols))
            counts.setdefault(tbl, {})
            counts[tbl][key] = counts[tbl].get(key, 0) + 1
    return counts


def _render_counts(counts: Dict[str, Dict[Tuple[str, ...], int]]) -> str:
    lines: List[str] = []
    lines.append("单查询中每个表中同时出现的列集合 频次统计：\n")
    for tbl in sorted(counts.keys()):
        lines.append(f"{tbl}:")
        # 排序：先按集合大小，再按字典序
        for cols, cnt in sorted(counts[tbl].items(), key=lambda kv: (len(kv[0]), kv[0])):
            inner = ", ".join(cols)
            lines.append(f"<{inner}> count :{cnt}")
        lines.append("")
    return "\n".join(lines).rstrip()


def build_part3(sql_dir: Optional[str] = None, latency_path: Optional[str] = None, schema_sql: Optional[str] = None) -> str:
    if not sql_dir:
        return ""
    try:
        counts = _aggregate_sets(sql_dir, schema_sql=schema_sql)
    except Exception:
        return ""
    if not counts:
        return ""
    return _render_counts(counts)


def main() -> None:
    # 简单 CLI：传入 --sql-dir 时输出统计，否则输出空
    import argparse
    ap = argparse.ArgumentParser(description="PART3: 统计每表列集合频次")
    ap.add_argument("--sql-dir", help="包含 .sql 的目录（递归）")
    ap.add_argument("--schema-sql", help="schema.sql 路径，用于未限定列归属", default=None)
    args = ap.parse_args()
    if not args.sql_dir:
        print("")
        return
    print(build_part3(sql_dir=args.sql_dir, schema_sql=args.schema_sql))


if __name__ == "__main__":
    main()
