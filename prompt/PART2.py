#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import csv
import json
import re
import os
import sys
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

# 将项目根目录加入 sys.path，便于从 sibling 包导入模块
_THIS_DIR = os.path.dirname(__file__)
_ROOT_DIR = os.path.dirname(_THIS_DIR)
if _ROOT_DIR and _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

# 字段长度估算与 EXPLAIN ANALYZE（仅 MySQL）
try:
    from column_stats.estimator import ColumnLengthEstimator  # type: ignore
    from DataBase.MySQLDriver import MySQLDriver  # type: ignore
    from query_latency.explain_analyze import analyze_sql as qa_analyze_sql  # type: ignore
except Exception:
    ColumnLengthEstimator = None  # type: ignore
    MySQLDriver = None  # type: ignore
    qa_analyze_sql = None  # type: ignore

# ----- schema parsing -----

# 兼容 IF NOT EXISTS 与 schema.table，仅捕获末尾表名
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
    re.IGNORECASE,
)


def _extract_columns(block: str) -> Dict[str, str]:
    cols: Dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(",")
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


def parse_schema(schema_sql_path: str) -> Dict[str, Dict[str, str]]:
    sql = Path(schema_sql_path).read_text(encoding="utf-8")
    tables: Dict[str, Dict[str, str]] = {}
    pos = 0
    while True:
        m = _CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        tname = m.group(1)
        start = m.end()
        end = sql.find(");", start)
        if end == -1:
            end = sql.find(")\n", start)
            if end == -1:
                break
        block = sql[start:end]
        tables[tname] = _extract_columns(block)
        pos = end + 2
    return tables


# ----- workload collection (queries + latencies) -----

def _parse_query_id(val: str) -> Optional[int]:
    if not val:
        return None
    m = re.search(r"(\d+)$", str(val).strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def collect_queries2(dir_path: str) -> Tuple[List[Tuple[int, str, str]], List[Dict[str, str]]]:
    """收集目录下所有 .sql（递归），不再局限于 queryN.sql 命名。
    返回：
      - items: 按路径排序的 (序号, SQL 文本, 文件路径)
      - skipped: [{index, path, error}] 无法读取的文件列表
    """
    base = Path(dir_path)
    files = sorted([p for p in base.rglob("*.sql") if p.is_file()], key=lambda p: str(p))
    # 允许文件名中自然包含序号：优先按提取到的数字排序，其次按路径排序
    def _num_key(path: Path) -> int:
        m = re.search(r"(\d+)", path.name)
        return int(m.group(1)) if m else 10**9
    files.sort(key=lambda p: (_num_key(p), str(p)))
    items: List[Tuple[int, str, str]] = []
    skipped: List[Dict[str, str]] = []
    for i, p in enumerate(files, start=1):
        body: Optional[str] = None
        err: Optional[str] = None
        try:
            body = p.read_text(encoding="utf-8").rstrip()
        except UnicodeDecodeError as e:
            # 逐步降级尝试多种编码
            try:
                body = p.read_text(encoding="utf-8-sig").rstrip()
            except Exception:
                try:
                    body = p.read_text(encoding="gbk").rstrip()
                except Exception:
                    try:
                        body = p.read_text(encoding="latin-1").rstrip()
                    except Exception as e2:
                        err = f"decode_failed: {e2}"
        except Exception as e:
            err = str(e)
        if body is None:
            skipped.append({"index": str(i), "path": str(p), "error": err or "unknown"})
            continue
        items.append((i, body, str(p)))
    return items, skipped


# 已移除对外部延迟文件的依赖（avg_time 通过 EXPLAIN ANALYZE 估计）


# ----- SQL operation extraction -----

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_Q_IDENT = r"`[^`]+`|\"[^\"]+\"|" + _IDENT
# 带可选 schema 的限定名：schema.table 或 table
_Q_NAME = (
    r"(?:`[^`]+`|\"[^\"]+\"|" + _IDENT + r")"
    r"(?:\s*\.\s*(?:`[^`]+`|\"[^\"]+\"|" + _IDENT + r"))?"
)


def _unquote_ident(name: str) -> str:
    if name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name


def _norm_table(name: str) -> str:
    name = _unquote_ident(name)
    # strip schema.
    if "." in name:
        name = name.split(".")[-1]
    return name


def _collect_aliases(sql: str) -> Dict[str, str]:
    # Map alias -> base_table and also table -> table
    aliases: Dict[str, str] = {}
    # FROM table [AS] alias  (avoid keyword like WHERE/GROUP/ORDER captured as alias)
    kw = r"WHERE|GROUP|ORDER|LIMIT|JOIN|LEFT|RIGHT|INNER|OUTER|ON|HAVING|UNION|EXCEPT|INTERSECT"
    for m in re.finditer(r"\bFROM\s+(" + _Q_NAME + r")\s+(?:AS\s+)?(?!" + kw + r"\b)(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2))
        aliases[alias] = table
        aliases[table] = table
    # JOIN table [AS] alias
    for m in re.finditer(r"\bJOIN\s+(" + _Q_NAME + r")\s+(?:AS\s+)?(?!" + kw + r"\b)(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2))
        aliases[alias] = table
        aliases[table] = table
    # UPDATE table [AS] alias
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_NAME + r")\s+(?:AS\s+)?(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2))
        aliases[alias] = table
        aliases[table] = table
    # bare UPDATE table
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        aliases[table] = table
    # Also capture bare FROM/JOINS without alias: FROM table
    for m in re.finditer(r"\bFROM\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        aliases[table] = table
    for m in re.finditer(r"\bJOIN\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        aliases[table] = table
    return aliases


def _resolve_table_for_column(token: str, aliases: Dict[str, str], schema: Dict[str, Dict[str, str]]) -> Optional[str]:
    # token may be alias.col or col
    if "." in token:
        a, c = token.split(".", 1)
        base = aliases.get(_unquote_ident(a))
        return base
    # unqualified: infer by uniqueness across schema
    c = _unquote_ident(token)
    candidates = [t for t, cols in schema.items() if c in cols]
    if len(candidates) == 1:
        return candidates[0]
    return None


@dataclass
class OpEvent:
    table: str
    column: str
    operation: str  # e.g., 'select', 'order by', 'group by', 'join(<other_col>)', 'insert', 'update'
    predicate: Optional[str] = None  # for select-like operations
    join_partner: Optional[Tuple[str, str]] = None  # (other_table, other_col)


def _extract_join_pairs(sql: str, aliases: Dict[str, str]) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    # 预先收集 FROM/JOIN 出场顺序（用于 USING 的相邻配对）
    seq: List[str] = []
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+(" + _Q_NAME + r")\s*(?:AS\s+)?(" + _Q_IDENT + r")?", sql, re.IGNORECASE):
        base = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2)) if m.group(2) else base
        seq.append(alias)
    # ON a.b = c.d (only equality joins handled)
    for on in re.finditer(r"\bON\b(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\)|$)", sql, re.IGNORECASE | re.DOTALL):
        frag = on.group(1)
        for m in re.finditer(r"((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))\s*=\s*((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))", frag):
            left = _unquote_ident(m.group(1).replace(" ", ""))
            right = _unquote_ident(m.group(2).replace(" ", ""))
            pairs.append((left, right))
    # WHERE-based joins (legacy style): a.b = c.d
    for where in re.finditer(r"\bWHERE\b(.+?)(?=\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\)|$)", sql, re.IGNORECASE | re.DOTALL):
        frag = where.group(1)
        for m in re.finditer(r"((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))\s*=\s*((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))", frag):
            left = _unquote_ident(m.group(1).replace(" ", ""))
            right = _unquote_ident(m.group(2).replace(" ", ""))
            pairs.append((left, right))
    # JOIN ... USING (col1, col2, ...) 近似处理：为 USING 中的每个列，在参与查询的表之间配对
    # 优先使用别名（alias）；若无别名则退回基表名。
    using_pat = re.compile(r"\bJOIN\s+(" + _Q_NAME + r")\s*(?:AS\s+)?(" + _Q_IDENT + r")?\s*USING\s*\(([^)]*)\)", re.IGNORECASE | re.DOTALL)
    for m in using_pat.finditer(sql):
        j_alias = _unquote_ident(m.group(2)) if m.group(2) else _norm_table(m.group(1))
        cols_s = m.group(3) or ""
        cols = [_unquote_ident(x.strip()) for x in cols_s.split(',') if x.strip()]
        # 仅与“相邻前一个”表进行配对，减少过度分摊
        try:
            idx = [i for i, tok in enumerate(seq) if tok == j_alias]
        except Exception:
            idx = []
        for pos in idx or []:
            if pos <= 0:
                continue
            prev_alias = seq[pos - 1]
            for c in cols:
                left = f"{prev_alias}.{c}"
                right = f"{j_alias}.{c}"
                pairs.append((left, right))
    return pairs


def _extract_order_group(sql: str, clause: str) -> List[str]:
    cols: List[str] = []
    m = re.search(r"\b" + clause + r"\s+BY\b(.+?)(?=\bLIMIT\b|\bUNION\b|\)|$)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return cols
    frag = m.group(1)
    # split by commas that are not within parentheses (naive)
    parts = [p.strip() for p in frag.split(",") if p.strip()]
    for p in parts:
        # get first identifier (possibly alias.col)
        m2 = re.search(r"(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)", p)
        if m2:
            cols.append(_unquote_ident(m2.group(1).replace(" ", "")))
    return cols


def _extract_where_predicates(sql: str) -> List[Tuple[str, str]]:
    # returns list of (lhs, pred_str) where lhs is identifier (alias.col or col), pred_str starts with operator, e.g., "> 10" or "= 'A'"
    preds: List[Tuple[str, str]] = []
    m = re.search(r"\bWHERE\b(.+?)(?=\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\)|$)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return preds
    frag = m.group(1)
    # split by AND/OR (naive)
    parts = re.split(r"\bAND\b|\bOR\b", frag, flags=re.IGNORECASE)
    for p in parts:
        p = p.strip()
        # a.b >= 10  |  col like 'X%'
        # 修复别名.列 的正则：显式分组 (alias.col)|(col)
        m2 = re.match(r"((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r")|(?:" + _Q_IDENT + r"))\s*(=|<>|!=|>=|<=|>|<|LIKE|NOT\s+LIKE|IN|NOT\s+IN|BETWEEN)\s*(.+)$", p, re.IGNORECASE)
        if not m2:
            continue
        lhs = _unquote_ident(m2.group(1).replace(" ", ""))
        op = m2.group(2).upper()
        rhs = m2.group(3).strip()
        # If RHS looks like another column (contains dot), skip; will be considered as join in other extractor
        if re.search(r"\b(?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r")\b", rhs):
            continue
        preds.append((lhs, f"{op} {rhs}"))
    return preds


def _extract_insert_columns(sql: str, schema: Dict[str, Dict[str, str]]) -> List[Tuple[str, List[str]]]:
    """返回 list[(table, [cols])]
    支持：
      - INSERT [IGNORE] INTO t(col,...) VALUES(...)
      - INSERT [IGNORE] INTO t VALUES(...)
      - INSERT [IGNORE] INTO t(col,...) SELECT ...
      - REPLACE INTO t(col,...) VALUES/SELECT ... 视作 insert
      - ON DUPLICATE KEY UPDATE col=...（仅用于后续 _extract_update_sets_on_dup 处理）
    """
    results: List[Tuple[str, List[str]]] = []
    # 带列清单
    for m in re.finditer(r"\b(INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+(" + _Q_NAME + r")\s*\(([^)]*)\)\s*(VALUES|SELECT)\b", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(2))
        cols = [_unquote_ident(x.strip()) for x in m.group(3).split(",") if x.strip()]
        if cols:
            results.append((tbl, cols))
    # 无列清单：退化为全列表
    for m in re.finditer(r"\b(INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+(" + _Q_NAME + r")\s*(VALUES|SELECT)\b", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(2))
        cols = list(schema.get(tbl, {}).keys())
        if cols:
            results.append((tbl, cols))
    return results


def _extract_update_sets(sql: str, schema: Dict[str, Dict[str, str]]) -> List[Tuple[str, List[str]]]:
    """返回 list[(table, [cols])] 对每个 UPDATE。
    支持：
      - 简单 UPDATE t SET col=...
      - UPDATE t1 JOIN t2 ... SET t1.c=..., t2.c2=...
      - 多表 UPDATE：UPDATE t1, t2 SET t1.c=..., t2.c2=...
      - INSERT ... ON DUPLICATE KEY UPDATE col=...（由本函数提取更新列）
    """
    results: List[Tuple[str, List[str]]] = []
    # 1) 简单 UPDATE t SET ...
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)(?:\s+(?:AS\s+)?" + _Q_IDENT + r")?\s+SET\s+(.+?)(?=\bWHERE\b|$)", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(1))
        sets = m.group(2)
        cols: List[str] = []
        for s in sets.split(","):
            s = s.strip()
            m2 = re.match(r"(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*=", s)
            if m2:
                lhs = _unquote_ident(m2.group(1)).replace(" ", "")
                col = lhs.split(".")[-1]
                cols.append(col)
        if cols:
            results.append((tbl, cols))

    # 2) UPDATE ... JOIN ... SET ... 或 UPDATE t1, t2 SET ...
    for m in re.finditer(r"\bUPDATE\s+(.+?)\s+SET\s+(.+?)(?=\bWHERE\b|$)", sql, re.IGNORECASE | re.DOTALL):
        head = m.group(1)
        sets = m.group(2)
        # 粗略跳过已被简单 UPDATE 匹配的情况：如果 head 是单表且前面没有 JOIN/逗号，则已处理过
        if re.fullmatch(r"\s*(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)(?:\s+(?:AS\s+)?" + _Q_IDENT + r")?\s*", head, re.IGNORECASE):
            continue
        # 用全 SQL 构造别名映射
        aliases = _collect_aliases(sql)
        tb2cols: Dict[str, Set[str]] = {}
        for s in sets.split(","):
            s = s.strip()
            m2 = re.match(r"(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*=", s)
            if not m2:
                continue
            lhs = _unquote_ident(m2.group(1)).replace(" ", "")
            base = _resolve_table_for_column(lhs, aliases, schema)
            col = lhs.split(".")[-1]
            if base and col in schema.get(base, {}):
                tb2cols.setdefault(base, set()).add(col)
        for tname, cols in tb2cols.items():
            results.append((tname, sorted(cols)))

    # 3) INSERT ... ON DUPLICATE KEY UPDATE col=...
    for m in re.finditer(r"\b(INSERT|REPLACE)\b.+?\bINTO\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?).+?\bON\s+DUPLICATE\s+KEY\s+UPDATE\s+(.+)$", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(2))
        upd = m.group(3)
        cols: Set[str] = set()
        for s in upd.split(","):
            s = s.strip()
            m2 = re.match(r"(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*=", s)
            if m2:
                lhs = _unquote_ident(m2.group(1)).replace(" ", "")
                col = lhs.split(".")[-1]
                if col in schema.get(tbl, {}):
                    cols.add(col)
        if cols:
            results.append((tbl, sorted(cols)))

    return results


def _extract_operations_block(sql: str, schema: Dict[str, Dict[str, str]]) -> List[OpEvent]:
    ops: List[OpEvent] = []
    aliases = _collect_aliases(sql)

    # JOINs
    for left, right in _extract_join_pairs(sql, aliases):
        if "." not in left or "." not in right:
            continue
        l_alias, l_col = left.split(".", 1)
        r_alias, r_col = right.split(".", 1)
        lt = aliases.get(_unquote_ident(l_alias))
        rt = aliases.get(_unquote_ident(r_alias))
        if not lt or not rt:
            continue
        # record for both sides
        ops.append(OpEvent(table=lt, column=_unquote_ident(l_col), operation=f"join({_unquote_ident(r_col)})", join_partner=(rt, _unquote_ident(r_col))))
        ops.append(OpEvent(table=rt, column=_unquote_ident(r_col), operation=f"join({_unquote_ident(l_col)})", join_partner=(lt, _unquote_ident(l_col))))

    # WHERE predicates (filters) → now recorded as 'scan' operations
    for lhs, pred in _extract_where_predicates(sql):
        base_table = _resolve_table_for_column(lhs, aliases, schema)
        col = lhs.split(".")[-1]
        if base_table and col in schema.get(base_table, {}):
            # 将原先的 filter 操作修改为 scan（仍保留 predicate 用于基数估计）
            ops.append(OpEvent(table=base_table, column=col, operation="scan", predicate=pred))

    # ORDER BY
    for tok in _extract_order_group(sql, "ORDER"):
        base_table = _resolve_table_for_column(tok, aliases, schema)
        col = tok.split(".")[-1]
        if base_table and col in schema.get(base_table, {}):
            ops.append(OpEvent(table=base_table, column=col, operation="order by"))

    # GROUP BY
    for tok in _extract_order_group(sql, "GROUP"):
        base_table = _resolve_table_for_column(tok, aliases, schema)
        col = tok.split(".")[-1]
        if base_table and col in schema.get(base_table, {}):
            ops.append(OpEvent(table=base_table, column=col, operation="group by"))

    # INSERT columns（无论是否在 schema 中，都记录该列，便于统计 count）
    for tbl, cols in _extract_insert_columns(sql, schema):
        for c in cols:
            ops.append(OpEvent(table=tbl, column=_unquote_ident(c), operation="insert"))

    # UPDATE sets and predicates（无论是否在 schema 中，都记录该列，便于统计 count）
    for tbl, cols in _extract_update_sets(sql, schema):
        for c in cols:
            ops.append(OpEvent(table=tbl, column=_unquote_ident(c), operation="update"))
    return ops


def _find_subquery_spans(sql: str) -> List[Tuple[int, int, Optional[str]]]:
    spans: List[Tuple[int, int, Optional[str]]] = []
    stack: List[int] = []
    n = len(sql)
    i = 0
    while i < n:
        ch = sql[i]
        if ch == '(':
            stack.append(i)
        elif ch == ')':
            if stack:
                start = stack.pop()
                inner = sql[start + 1:i]
                if re.match(r"\s*SELECT\b", inner, re.IGNORECASE):
                    # capture alias after ')'
                    j = i + 1
                    # skip whitespace
                    while j < n and sql[j].isspace():
                        j += 1
                    # optional AS
                    if re.match(r"AS\b", sql[j:j+2], re.IGNORECASE):
                        j += 2
                        while j < n and sql[j].isspace():
                            j += 1
                    # alias token
                    m = re.match(r"(`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)", sql[j:])
                    alias = None
                    if m:
                        alias = _unquote_ident(m.group(1))
                    spans.append((start, i, alias))
        i += 1
    return spans


def _mask_spans(sql: str, spans: List[Tuple[int, int]]) -> str:
    if not spans:
        return sql
    chars = list(sql)
    for s, e in spans:
        for k in range(s, e + 1):
            chars[k] = ' '
    return ''.join(chars)


def extract_operations_from_sql(sql: str, schema: Dict[str, Dict[str, str]]) -> List[OpEvent]:
    ops: List[OpEvent] = []
    sub_spans = _find_subquery_spans(sql)
    # recurse into subqueries
    for s, e, _alias in sub_spans:
        sub_sql = sql[s + 1:e]
        ops.extend(extract_operations_from_sql(sub_sql, schema))
    # mask subqueries to avoid double counting in this block
    masked = _mask_spans(sql, [(s, e) for s, e, _ in sub_spans])
    ops.extend(_extract_operations_block(masked, schema))
    return ops


# ----- cardinality estimation (optional, MySQL) -----

def _maybe_build_estimator(dialect: str, host: str, port: int, user: str, password: str, database: str):
    if dialect.lower() != "mysql":
        return None
    try:
        from cardinality.mysql_explain import MySQLCardinalityEstimator
        from DataBase.MySQLDriver import MySQLDriver
    except Exception:
        return None
    cfg: Dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        return None
    return MySQLCardinalityEstimator(drv)


def _estimate_rows_for_event(estimator, evt: OpEvent) -> Optional[float]:
    if estimator is None:
        return None
    try:
        from cardinality.sql_builder import build_filter_sql, build_join_sql, build_select_sql
    except Exception:
        return None
    try:
        if evt.operation.startswith("join(") and evt.join_partner:
            other_table, other_col = evt.join_partner
            sql = build_join_sql(left=evt.table, right=other_table, left_col=evt.column, right_col=other_col)
            res = estimator.estimate(sql)
            return res.get("estimated_rows")
        if evt.operation == "select" and evt.predicate:
            sql = build_filter_sql(evt.table, evt.column, evt.predicate)
            res = estimator.estimate(sql)
            return res.get("estimated_rows")
        if evt.operation in ("order by", "group by"):
            sql = build_select_sql(table=evt.table, columns=[evt.column], where=None, group_by=[evt.column] if evt.operation == "group by" else None, order_by=[evt.column] if evt.operation == "order by" else None)
            res = estimator.estimate(sql)
            return res.get("estimated_rows")
        # insert/update: cardinality not meaningful via EXPLAIN here; return None
        return None
    except Exception:
        return None


def _estimate_rows_and_filtered(estimator, evt: OpEvent) -> Tuple[Optional[float], Optional[float]]:
    if estimator is None:
        return None, None
    try:
        from cardinality.sql_builder import build_filter_sql, build_join_sql, build_select_sql
    except Exception:
        return None, None
    try:
        sql = None
        if evt.operation.startswith("join(") and evt.join_partner:
            other_table, other_col = evt.join_partner
            sql = build_join_sql(left=evt.table, right=other_table, left_col=evt.column, right_col=other_col)
        elif evt.predicate:
            sql = build_filter_sql(evt.table, evt.column, evt.predicate)
        elif evt.operation in ("order by", "group by"):
            sql = build_select_sql(table=evt.table, columns=[evt.column], where=None, group_by=[evt.column] if evt.operation == "group by" else None, order_by=[evt.column] if evt.operation == "order by" else None)
        else:
            return None, None
        res = estimator.estimate(sql)
        est_rows = res.get("estimated_rows")
        fbt = res.get("filtered_by_table") or {}
        filtered = None
        # 优先取当前事件表的 filtered
        if evt.table in fbt:
            try:
                filtered = float(fbt[evt.table])
            except Exception:
                filtered = None
        return est_rows, filtered
    except Exception:
        return None, None


# ----- aggregation and rendering -----

def _extract_ident_pairs_from_text(text: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    # match a.b = c.d patterns in text
    pat = re.compile(r"((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))\s*=\s*((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))")
    for m in pat.finditer(text):
        left = _unquote_ident(m.group(1).replace(" ", ""))
        right = _unquote_ident(m.group(2).replace(" ", ""))
        pairs.append((left, right))
    return pairs


def _extract_column_tokens(text: str) -> List[str]:
    toks: List[str] = []
    pat = re.compile(r"(?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r")")
    for m in pat.finditer(text):
        toks.append(_unquote_ident(m.group(0).replace(" ", "")))
    return toks


def _attribute_costs_for_sql(sql: str, nodes: List[Dict[str, Any]], schema: Dict[str, Dict[str, str]], evt_list: List[OpEvent], debug: bool = False, debug_prefix: Optional[str] = None) -> Dict[Tuple[str, str, str], float]:
    # Build quick lookup: ops present in this SQL
    ops_present: Set[Tuple[str, str, str]] = set((e.table, e.column, e.operation) for e in evt_list)
    # Pre-calc order/group columns in this SQL
    aliases = _collect_aliases(sql)
    order_cols = []
    for tok in _extract_order_group(sql, "ORDER"):
        base_table = _resolve_table_for_column(tok, aliases, schema)
        col = tok.split(".")[-1]
        if base_table:
            order_cols.append((base_table, col, "order by"))
    group_cols = []
    for tok in _extract_order_group(sql, "GROUP"):
        base_table = _resolve_table_for_column(tok, aliases, schema)
        col = tok.split(".")[-1]
        if base_table:
            group_cols.append((base_table, col, "group by"))

    # Aggregate time per op key for this SQL
    per_key_time: Dict[Tuple[str, str, str], float] = {}

    total_avg = 0.0
    total_excl = 0.0

    # 置信度加权（可通过环境变量调整）
    def _f(name: str, default: float) -> float:
        try:
            v = os.environ.get(name)
            return float(v) if v is not None and str(v).strip() != '' else default
        except Exception:
            return default
    W_JOIN_EQ = _f('PART2_W_JOIN_EQ', 1.0)
    W_JOIN_GENERIC = _f('PART2_W_JOIN_GENERIC', 0.6)
    W_FILTER_COLUMN = _f('PART2_W_FILTER_COLUMN', 0.6)
    W_FILTER_GLOBAL = _f('PART2_W_FILTER_GLOBAL', 0.3)
    W_SCAN_GENERIC = _f('PART2_W_SCAN_GENERIC', 1.0)

    # 从算子文本中提取可能出现的别名/表名，限制回退分摊范围
    def _tables_in_text(text: str) -> Set[str]:
        found: Set[str] = set()
        s = text or ''
        # 优先匹配别名
        for alias, base in aliases.items():
            try:
                if re.search(r"\b" + re.escape(alias) + r"\b", s):
                    found.add(base)
            except re.error:
                continue
        # 再匹配基表名
        for base in set(aliases.values()):
            try:
                if re.search(r"\b" + re.escape(base) + r"\b", s):
                    found.add(base)
            except re.error:
                continue
        return found
    matched = 0
    for n in nodes:
        optext = (n.get("text") or n.get("op") or "").strip()
        # 使用独占时间（若可用）；否则回退到 avg_time（父子未去重）
        avg_time = n.get("exclusive_time", None)
        if avg_time is None:
            avg_time = n.get("avg_time")
        if avg_time is None:
            continue
        matched += 1
        try:
            total_avg += float(n.get("avg_time") or 0.0)
            total_excl += float(n.get("exclusive_time") or float(avg_time))
        except Exception:
            pass
        low = optext.lower()

        # 1) Filters (含连接条件)：多级回退，尽量归因
        #    级别1（最准确）：等值条件 -> 两侧 join(col)
        #    级别2：非连接谓词中出现的列 -> 对应列的 scan
        #    级别3：仍无法定位 -> 将时间均分到本 SQL 的所有 scan 操作
        if "filter" in low or " where " in low or low.startswith("filter"):
            # 1a) 先尝试识别 a.b = c.d 作为连接条件，分摊到两侧 join 操作
            eq_pairs = _extract_ident_pairs_from_text(optext)
            if eq_pairs:
                for left, right in eq_pairs:
                    for tok, other in ((left, right), (right, left)):
                        base_table = _resolve_table_for_column(tok, aliases, schema)
                        col = tok.split(".")[-1]
                        other_col = other.split(".")[-1]
                        if not base_table:
                            continue
                        # 精确 join(other_col) 匹配；否则回退到任意 join(*)
                        key = (base_table, col, f"join({other_col})")
                        if key not in ops_present:
                            any_join = next((e.operation for e in evt_list if e.table == base_table and e.column == col and e.operation.startswith("join(")), None)
                            if any_join:
                                key = (base_table, col, any_join)
                        if key in ops_present:
                            per_key_time[key] = per_key_time.get(key, 0.0) + (float(avg_time) * W_JOIN_EQ) / 2.0
                continue

            # 1b) 非连接谓词：将时间按出现的列分摊到对应表列的 scan
            toks = _extract_column_tokens(optext)  # alias.col 近似提取
            scan_targets: List[Tuple[str, str, str]] = []
            for tok in toks:
                base_table = _resolve_table_for_column(tok, aliases, schema)
                col = tok.split(".")[-1]
                key = (base_table, col, "scan") if base_table else None
                if key and key in ops_present:
                    scan_targets.append(key)
            if scan_targets:
                share = (float(avg_time) * W_FILTER_COLUMN) / float(len(scan_targets))
                for key in scan_targets:
                    per_key_time[key] = per_key_time.get(key, 0.0) + share
                continue

            # 1c) 兜底：仅在文本中无法识别表时，才对“相关表”的 scan 做均分；仍无法定位则缩小为全局 scan 且较低权重
            rel_tables = _tables_in_text(optext)
            any_scans = [
                (e.table, e.column, e.operation)
                for e in evt_list
                if e.operation == "scan" and (not rel_tables or e.table in rel_tables)
            ]
            if any_scans:
                share = (float(avg_time) * W_FILTER_GLOBAL) / float(len(any_scans))
                for key in any_scans:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue

        # 2) Sort: distribute to ORDER BY columns present in this SQL
        if "sort" in low or " order " in low or low.startswith("sort"):
            if order_cols:
                share = float(avg_time) / float(len(order_cols))
                for key in order_cols:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue

        # 3) Group/Aggregate: distribute to GROUP BY columns
        if " group " in low or "aggregate" in low or low.startswith("group"):
            if group_cols:
                share = float(avg_time) / float(len(group_cols))
                for key in group_cols:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue

        # 4) Join/Lookup/Hash: 分层归因
        #    级别1：使用等值列对 a.b = c.d 归因到具体 join(col)
        #    级别2：若未识别到列对，将该节点时间均分到本 SQL 的所有 join(...) 操作
        if "join" in low or "nested loop" in low or "lookup" in low or "index lookup" in low or "hash join" in low:
            eqs = _extract_ident_pairs_from_text(optext)
            if eqs:
                for left, right in eqs:
                    for tok in (left, right):
                        base_table = _resolve_table_for_column(tok, aliases, schema)
                        col = tok.split(".")[-1]
                        if not base_table:
                            continue
                        key = (base_table, col, "join(" + (right.split(".")[-1] if tok == left else left.split(".")[-1]) + ")")
                        if key not in ops_present:
                            any_join_key = (base_table, col, next((e.operation for e in evt_list if e.table == base_table and e.column == col and e.operation.startswith("join(")), None))
                            if any_join_key[2]:
                                key = any_join_key  # type: ignore
                        if key in ops_present:
                            per_key_time[key] = per_key_time.get(key, 0.0) + (float(avg_time) * W_JOIN_EQ) / 2.0
                continue
            # 级别2：没有等值列对时，均分到“相关表”的 join(col)
            rel_tables = _tables_in_text(optext)
            join_targets = [
                (e.table, e.column, e.operation)
                for e in evt_list
                if e.operation.startswith("join(") and (not rel_tables or e.table in rel_tables)
            ]
            if join_targets:
                share = (float(avg_time) * W_JOIN_GENERIC) / float(len(join_targets))
                for key in join_targets:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
                continue

        # 5) Table scan / lookup on <table>: attribute to that table's scan columns
        # 覆盖更多 MySQL 文案：index lookup/range scan/full table scan
        m_scan = re.search(r"\b(?:index\s+lookup|index\s+range\s+scan|range\s+scan|full\s+table\s+scan|table\s+scan|scan|lookup)\s+on\s+(`?[A-Za-z0-9_]+`?)", optext, re.IGNORECASE)
        if m_scan:
            tbl = _norm_table(m_scan.group(1))
            # 仅将 scan 耗时分配给该表的 'scan' 列操作（来自 WHERE 谓词）
            related = [
                (e.table, e.column, e.operation)
                for e in evt_list
                if e.table == tbl and e.operation == "scan"
            ]
            if related:
                share = (float(avg_time) * W_SCAN_GENERIC) / float(len(related))
                for key in related:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
            # 若没有相关 scan 目标，则不做表级兜底分摊，避免误分摊
            continue

        # 5) Insert/Update: attribute to involved columns evenly
        lw = low
        if lw.startswith("insert"):
            targets = [(e.table, e.column, e.operation) for e in evt_list if e.operation == "insert"]
            if targets:
                share = float(avg_time) / float(len(targets))
                for key in targets:
                    per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue
        if lw.startswith("update"):
            targets = [(e.table, e.column, e.operation) for e in evt_list if e.operation == "update"]
            if targets:
                share = float(avg_time) / float(len(targets))
                for key in targets:
                    per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue

    return per_key_time


def _aggregate_operations(
    schema: Dict[str, Dict[str, str]],
    queries: List[Tuple[int, str, str]],
    latencies_ms: Optional[Dict[int, float]],
    estimator,
    analyze_driver,
    debug: bool = False,
    debug_dir: Optional[str] = None,
    exec_counts: Optional[Dict[str, int]] = None,
    dml_cache_by_basename: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    # stats: (table, column, operation) -> {count, sum_time, rows, filtered}
    stats: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    index_map: List[Dict[str, Any]] = []
    # 表级 DML 计数（insert/update），按 SQL 执行频率加权
    table_dml_counts: Dict[str, Dict[str, int]] = {}
    # 表级 DML 计时累积（秒）：{table: {"insert": sum_time_s, "update": sum_time_s}}
    table_dml_times: Dict[str, Dict[str, float]] = {}
    # 表级 JOIN 计数：table -> other_table -> {count: int, pairs: set[(col, other_col)]}
    table_join_counts: Dict[str, Dict[str, Dict[str, Any]]] = {}
    # 调试：记录每条 SQL 的 DML 命中（表级）
    dml_hits: List[Dict[str, Any]] = []
    for qid, sql, path in queries:
        index_map.append({"id": qid, "path": path})
        ops = extract_operations_from_sql(sql, schema)
        lat = (latencies_ms or {}).get(qid)
        seen_keys: Set[Tuple[str, str, str]] = set()
        for evt in ops:
            key = (evt.table, evt.column, evt.operation)
            if key not in stats:
                stats[key] = {"count": 0, "sum_time": 0.0, "rows": None, "filtered": None}
                # compute rows/filtered once (best-effort)
                rows, filtered = _estimate_rows_and_filtered(estimator, evt)
                stats[key]["rows"] = rows
                stats[key]["filtered"] = filtered
        # Attribute EXPLAIN ANALYZE time to per-column ops for this SQL
        # 跳过对 DML 的 EXPLAIN 分析；UPDATE/INSERT 只做计数统计
        is_dml = bool(re.match(r"^\s*(INSERT|UPDATE|REPLACE|DELETE)\b", sql, re.IGNORECASE))
        if (qa_analyze_sql is not None) and (analyze_driver is not None) and (not is_dml):
            try:
                res = qa_analyze_sql(analyze_driver, sql)
                nodes = res.get("nodes") or []
                raw = res.get("raw") or ""
                # 调试输出：写入原始 EXPLAIN、节点 JSON、统计概览
                if debug:
                    base = Path(debug_dir or (Path(_ROOT_DIR) / "debug" / "part2"))
                    (base / "explain").mkdir(parents=True, exist_ok=True)
                    (base / "nodes").mkdir(parents=True, exist_ok=True)
                    (base / "per_key").mkdir(parents=True, exist_ok=True)
                    (base / "sql").mkdir(parents=True, exist_ok=True)
                    # 保存 SQL 与 EXPLAIN 文本
                    (base / "sql" / f"q{qid}.sql").write_text(sql, encoding="utf-8")
                    (base / "explain" / f"q{qid}.txt").write_text(str(raw), encoding="utf-8")
                    # 保存索引映射（id->path）
                    try:
                        (base / "index_map.json").write_text(json.dumps(index_map, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                    # 保存节点
                    try:
                        (base / "nodes" / f"q{qid}.json").write_text(json.dumps(nodes, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                # 归因
                per_key_time = _attribute_costs_for_sql(sql, nodes, schema, ops, debug=debug, debug_prefix=f"q{qid}")
                if debug:
                    # 写每条 SQL 的 per_key_time
                    try:
                        base = Path(debug_dir or (Path(_ROOT_DIR) / "debug" / "part2"))
                        (base / "per_key").mkdir(parents=True, exist_ok=True)
                        dump = {f"{k[0]}.{k[1]}::{k[2]}": v for k, v in per_key_time.items()}
                        (base / "per_key" / f"q{qid}.json").write_text(json.dumps(dump, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception:
                        pass
            except Exception:
                per_key_time = {}
        else:
            per_key_time = {}

        # ---------------------- DML 计时（来自缓存文件），仅对 INSERT/UPDATE ----------------------
        if is_dml and dml_cache_by_basename is not None:
            base = os.path.basename(path)
            mult = int((exec_counts or {}).get(base, 1))
            cache_entries = dml_cache_by_basename.get(base) or []
            for ent in cache_entries:
                typ = str(ent.get("type") or "").upper()
                tname = str(ent.get("table") or "")
                eff_s = None
                try:
                    if ent.get("effective_time_s") is not None:
                        eff_s = float(ent.get("effective_time_s"))
                    else:
                        # 回退：exec_time_s - where_select_time_s
                        et = float(ent.get("exec_time_s") or 0.0)
                        wt = float(ent.get("where_select_time_s") or 0.0)
                        eff_s = max(0.0, et - wt)
                except Exception:
                    eff_s = None
                if not tname or eff_s is None:
                    continue
                if typ == "UPDATE":
                    cols = ent.get("columns") or []
                    cols = [ _unquote_ident(str(c)) for c in cols ]
                    if cols:
                        per_col = eff_s / max(1, len(cols))
                        for c in cols:
                            key = (tname, c, "update")
                            if key not in stats:
                                stats[key] = {"count": 0, "sum_time": 0.0, "rows": None, "filtered": None}
                            stats[key]["sum_time"] = float(stats[key].get("sum_time") or 0.0) + per_col * mult
                    # 表级时间累计
                    tm2 = table_dml_times.setdefault(tname, {})
                    tm2["update"] = float(tm2.get("update") or 0.0) + eff_s * mult
                elif typ in ("INSERT", "REPLACE"):
                    tm = table_dml_times.setdefault(tname, {})
                    tm["insert"] = float(tm.get("insert") or 0.0) + float(eff_s) * mult
        # Update counts and sum_time per SQL (count only once per key per SQL)
        # 对本条 SQL 命中的每个列操作，仅计一次；其出现次数按“该 SQL 的执行频率”加权
        base = os.path.basename(path)
        mult = int((exec_counts or {}).get(base, 1))
        for e in ops:
            key = (e.table, e.column, e.operation)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            stats[key]["count"] += mult
            if key in per_key_time:
                stats[key]["sum_time"] += float(per_key_time[key]) * mult

        # ---- 表级 JOIN 聚合：对同一条 SQL 的同一“表对”仅计一次；记录列对集合 ----
        join_pairs_per_sql: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
        for e in ops:
            if not (e.operation.startswith("join(") and e.join_partner):
                continue
            t1, c1 = e.table, _unquote_ident(e.column)
            t2, c2 = e.join_partner
            # 规范化表对顺序，避免双向重复
            if t1 <= t2:
                key_tb = (t1, t2)
                pair = (c1, _unquote_ident(c2))
            else:
                key_tb = (t2, t1)
                pair = (_unquote_ident(c2), c1)
            join_pairs_per_sql.setdefault(key_tb, set()).add(pair)
        # 将本 SQL 的表对聚合到全局（按执行次数加权）
        for (a, b), pairs in join_pairs_per_sql.items():
            a_map = table_join_counts.setdefault(a, {})
            b_map = table_join_counts.setdefault(b, {})
            a_entry = a_map.setdefault(b, {"count": 0, "pairs": set()})
            b_entry = b_map.setdefault(a, {"count": 0, "pairs": set()})
            a_entry["count"] = int(a_entry.get("count", 0)) + mult
            b_entry["count"] = int(b_entry.get("count", 0)) + mult
            a_entry["pairs"].update(pairs)
            # 对称加入（交换列顺序）
            b_entry["pairs"].update({(y, x) for (x, y) in pairs})

        # 表级 DML：基于 SQL 中识别到的 INSERT/UPDATE 表集合进行累计
        try:
            ins_tbls = {t for (t, _cols) in _extract_insert_columns(sql, schema)}
            upd_tbls = {t for (t, _cols) in _extract_update_sets(sql, schema)}
        except Exception:
            ins_tbls, upd_tbls = set(), set()
        for t in ins_tbls:
            d = table_dml_counts.setdefault(t, {})
            d["insert"] = int(d.get("insert", 0)) + mult
        for t in upd_tbls:
            d = table_dml_counts.setdefault(t, {})
            d["update"] = int(d.get("update", 0)) + mult
        if debug:
            dml_hits.append({
                "id": qid,
                "path": path,
                "basename": base,
                "mult": mult,
                "insert_tables": sorted(list(ins_tbls)),
                "update_tables": sorted(list(upd_tbls)),
            })

    # initialize result lazily: 仅包含出现过列级操作的列
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    # fill in operations
    for (table, column, op), info in stats.items():
        rows_val = info.get("rows")
        if rows_val is None:
            rows_val = 1
        # 跳过 INSERT 的列级输出；UPDATE 现在改为列级展示
        if op == "insert":
            continue
        cnt = int(info.get("count") or 0)
        # 其它操作保持 rows/avg_time/count 输出，并增加 sum_time_ms（来自 EXPLAIN 归因）
        item = {
            "operation": op,
            "rows": rows_val if isinstance(rows_val, (int, float)) else 1,
        }
        # 不再输出 filtered 字段
        # avg_time 和 count：avg_time = 总时间 / 次数
        if cnt > 0:
            avg_time_s = (float(info.get("sum_time") or 0.0)) / float(cnt)
            avg_time_ms = avg_time_s * 1000.0
            item["avg_time"] = avg_time_ms
            item["count"] = cnt
            # 直接输出由 EXPLAIN 归因得到的总时间（毫秒），避免使用 avg*count 近似
            try:
                item["sum_time_ms"] = float(info.get("sum_time") or 0.0) * 1000.0
                # cost 定义为 avg_time*count，与 sum_time_ms 等价
                item["cost"] = item["sum_time_ms"]
            except Exception:
                pass
        result.setdefault(table, {}).setdefault(column, []).append(item)
    # 在表级别挂接 DML 计数与（若可用）平均耗时/成本
    for t in schema.keys():
        dml = table_dml_counts.get(t) or {}
        t_times = table_dml_times.get(t) or {}
        if dml.get("insert"):
            entry = {"count": int(dml["insert"]) }
            if "insert" in t_times and dml["insert"] > 0:
                avg_ms = (float(t_times["insert"]) / float(dml["insert"])) * 1000.0
                entry["avg_time"] = avg_ms
                entry["cost"] = avg_ms * int(dml["insert"])  # 成本 = 平均耗时 * 次数
            result.setdefault(t, {})["insert"] = entry
        if dml.get("update"):
            entry = {"count": int(dml["update"]) }
            if "update" in t_times and dml["update"] > 0:
                avg_ms = (float(t_times["update"]) / float(dml["update"])) * 1000.0
                entry["avg_time"] = avg_ms
                entry["cost"] = avg_ms * int(dml["update"])  # 成本 = 平均耗时 * 次数
            result.setdefault(t, {})["update"] = entry

    # 在表级别挂接 JOIN 计数：
    # result[table]["join"] = [{"table": other, "count": N, "pairs": [[col, other_col], ...]}, ...]
    if table_join_counts:
        for t, neighbors in table_join_counts.items():
            items: List[Dict[str, Any]] = []
            for other, entry in neighbors.items():
                pairs = sorted(list(entry.get("pairs") or []))
                items.append({
                    "table": other,
                    "count": int(entry.get("count", 0) or 0),
                    "pairs": [[a, b] for (a, b) in pairs],
                })
            # 稳定排序：count desc, table asc
            items.sort(key=lambda x: (-int(x.get("count", 0) or 0), str(x.get("table", ""))))
            if items:
                result.setdefault(t, {})["join"] = items

    # 调试：输出表级 DML 计数与逐 SQL 命中
    if debug:
        try:
            base = Path(debug_dir or (Path(_ROOT_DIR) / "debug" / "part2"))
            base.mkdir(parents=True, exist_ok=True)
            (base / "dml_counts.json").write_text(json.dumps(table_dml_counts, ensure_ascii=False, indent=2), encoding="utf-8")
            (base / "dml_hits.json").write_text(json.dumps(dml_hits, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    # sort operations per column deterministically；同时对表级 join 列表排序
    for t, cols in result.items():
        for c, arr in list(cols.items()):
            if c == "join" and isinstance(arr, list):
                arr.sort(key=lambda x: (-int(x.get("count", 0) or 0), str(x.get("table", ""))))
            elif isinstance(arr, list):
                arr.sort(key=lambda x: (x.get("operation", ""), str(x.get("rows", 0))))
    return result


def _connect_mysql_driver(dialect: str, host: str, port: int, user: str, password: str, database: str):
    if dialect.lower() != "mysql":
        raise SystemExit("当前实现仅支持 MySQL")
    if MySQLDriver is None:
        raise SystemExit("缺少 MySQLDriver，无法连接数据库")
    if not database:
        raise SystemExit("未提供 --database，用于字段长度估算")
    cfg: Dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise SystemExit("无法连接 MySQL，用于字段长度估算")
    return drv


def _load_exec_counts(path_or_none: Optional[str]) -> Dict[str, float]:
    """加载执行频率/次数映射：filename -> float 值。
    - 如果给定路径是文件，直接读取；
    - 如果是目录，优先寻找 sample_execution_counts_chbench.csv；
    - 若不存在或读取失败，返回空映射。

    注意：此处返回 float，后续会根据是否提供 total_runs 进行次数归一（四舍五入）。
    """
    mapping: Dict[str, float] = {}
    try:
        if not path_or_none:
            # 默认位置：项目内示例统计
            cand = Path(_ROOT_DIR) / "Data" / "cleaned_sql" / "query_and_update" / "sample_execution_counts_chbench.csv"
        else:
            cand = Path(path_or_none)
        if cand.is_dir():
            f = cand / "sample_execution_counts_chbench.csv"
        else:
            f = cand
        if not f.exists():
            return mapping

        # 先尝试按带表头的 CSV 读取
        text = f.read_text(encoding="utf-8").strip()
        if not text:
            return mapping
        first_line = text.splitlines()[0]
        used_header = False
        if ("filename" in first_line.lower()) and ("count" in first_line.lower()):
            # 有表头
            used_header = True
            with f.open("r", encoding="utf-8") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    fn = (row.get("filename") or "").strip()
                    try:
                        cnt = float(row.get("count") or 0)
                    except Exception:
                        continue
                    if fn:
                        mapping[fn] = cnt
        if not used_header:
            # 无表头：每行形如 filename,count
            with f.open("r", encoding="utf-8") as fh:
                rdr = csv.reader(fh)
                for parts in rdr:
                    if not parts:
                        continue
                    if len(parts) < 2:
                        continue
                    fn = str(parts[0]).strip()
                    try:
                        cnt = float(parts[1])
                    except Exception:
                        continue
                    if fn:
                        mapping[fn] = cnt
    except Exception:
        return {}
    return mapping

def _normalize_exec_counts(raw: Optional[Dict[str, float]]) -> Dict[str, int]:
    """将可能为频率的小数，按总运行次数（来自环境变量）放大为整数次数。

    规则：
    - 若值为整数（或近似整数）且 >=1，直接四舍五入为 int。
    - 否则将其视为“每轮频率”，与 total_runs 相乘后四舍五入为 int。
    - 当 total_runs 未提供时，回退如下：>0 的频率按 1 处理，<=0 视为 0。

    total_runs 从环境变量读取，优先级：EXEC_TOTAL_RUNS > TOTAL_RUNS > PART2_TOTAL_RUNS。
    """
    raw = raw or {}
    eff: Dict[str, int] = {}
    env = os.environ
    tr_val = env.get("EXEC_TOTAL_RUNS") or env.get("TOTAL_RUNS") or env.get("PART2_TOTAL_RUNS")
    total_runs: Optional[int] = None
    try:
        if tr_val is not None and str(tr_val).strip():
            total_runs = int(float(str(tr_val).strip()))
            if total_runs <= 0:
                total_runs = None
    except Exception:
        total_runs = None

    for fn, v in raw.items():
        try:
            val = float(v)
        except Exception:
            continue
        # 近似整数判断
        is_int_like = abs(val - round(val)) < 1e-9
        if val >= 1.0 and is_int_like:
            eff[fn] = int(round(val))
            continue
        # 否则按频率放大
        if total_runs is not None:
            eff[fn] = int(val * total_runs + 0.5)
        else:
            # 没有 total_runs 时，防止被归零：>0 取 1，否则 0
            eff[fn] = 1 if val > 0 else 0
    return eff

def _fetch_table_rows(driver, database: str, tables: List[str]) -> Dict[str, int]:
    """读取 information_schema.tables 的 TABLE_ROWS 估计值。
    返回 {table_name: rows}；失败时返回空映射。
    """
    if not driver or not database or not tables:
        return {}
    try:
        # 仅允许安全表名（字母数字下划线）；不安全的跳过
        safe = [t for t in tables if re.fullmatch(r"[A-Za-z0-9_]+", t)]
        if not safe:
            return {}
        in_list = ",".join("'" + t.replace("'", "''") + "'" for t in safe)
        db_esc = database.replace("'", "''")
        sql = (
            "SELECT table_name, table_rows FROM information_schema.tables "
            f"WHERE table_schema = '{db_esc}' AND table_name IN ({in_list})"
        )
        rows = driver.execute_query(sql)
        out: Dict[str, int] = {}
        for r in rows:
            tname = str(r.get("table_name") or r.get("TABLE_NAME") or "")
            try:
                val = int(r.get("table_rows") or r.get("TABLE_ROWS") or 0)
            except Exception:
                val = 0
            if tname:
                out[tname] = val
        return out
    except Exception:
        return {}


def _load_mysql_config(config_path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not config_path:
        return None
    path = Path(config_path)
    if not path.exists():
        return None
    cfg = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=())
    cfg.read(path)
    if "mysql" not in cfg:
        return None
    sec = cfg["mysql"]
    def _opt_str(key, default=None):
        val = sec.get(key, fallback=default)
        if val is None:
            return None
        s = str(val).strip()
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1]
        return s
    return {
        "host": _opt_str("host", "127.0.0.1"),
        "port": sec.getint("port", fallback=3306),
        "user": _opt_str("user", "root"),
        "password": _opt_str("password", ""),
        "database": _opt_str("database", ""),
    }


def build_part2(schema_sql_path: str, sql_dir: str,
                dialect: str = "mysql", host: str = "127.0.0.1", port: int = 3306,
                user: str = "root", password: str = "", database: str = "",
                config_path: Optional[str] = None,
                debug: bool = False,
                debug_dir: Optional[str] = None,
                exec_counts_path: Optional[str] = None,
                dml_time_cache: Optional[str] = None) -> str:
    # Load schema
    tables = parse_schema(schema_sql_path)
    if not tables:
        raise SystemExit(
            f"解析失败：未在 {schema_sql_path} 中解析到任何表。请确认 schema.sql 内容与 SQL 定义格式。"
        )

    # Workload
    queries, skipped = collect_queries2(sql_dir)
    lat_map: Dict[int, float] = {}

    # 读取持久化配置（如提供则优先），否则使用传入参数
    cfg = _load_mysql_config(config_path)
    if cfg:
        host = cfg.get("host", host)
        port = int(cfg.get("port", port))
        user = cfg.get("user", user)
        password = cfg.get("password", password)
        database = cfg.get("database", database)

    # Optional estimator for rows (best-effort); 若无法连接，返回 None
    estimator = _maybe_build_estimator(dialect, host, port, user, password, database)

    # 用于 EXPLAIN ANALYZE 的连接：若已构造 estimator，则复用其内部驱动；否则置为 None（跳过 EXPLAIN）
    analyze_driver = getattr(estimator, 'db', None)
    # 加载 DML 执行次数映射，用于 INSERT/UPDATE 的 count
    # 执行频率/次数：先加载 float，再按总轮次规范化为整数次数
    exec_counts_float = _load_exec_counts(exec_counts_path)
    exec_counts = _normalize_exec_counts(exec_counts_float)

    # 读取可复用的 DML 计时缓存（可选）
    dml_cache_by_basename: Optional[Dict[str, List[Dict[str, Any]]]] = None
    try:
        cache_path = dml_time_cache
        if cache_path:
            p = Path(cache_path)
            if p.exists():
                raw = json.loads(p.read_text(encoding="utf-8"))
                # 期望结构：{
                #   "entries": [
                #       {"filename": "upd_xxx.sql", "type": "UPDATE", "table": "t", "columns": [..], "exec_time_s": 0.01, "where_select_time_s": 0.004, "effective_time_s": 0.006},
                #       ...
                #   ]
                # }
                entries = raw.get("entries") if isinstance(raw, dict) else None
                if isinstance(entries, list):
                    dml_cache_by_basename = {}
                    for e in entries:
                        try:
                            fn = os.path.basename(str(e.get("filename") or "").strip())
                            if not fn:
                                continue
                            dml_cache_by_basename.setdefault(fn, []).append(e)
                        except Exception:
                            pass
    except Exception:
        dml_cache_by_basename = None

    mapping = _aggregate_operations(
        tables,
        queries,
        lat_map,
        estimator,
        analyze_driver,
        debug=debug,
        debug_dir=debug_dir,
        exec_counts=exec_counts,
        dml_cache_by_basename=dml_cache_by_basename,
    )

    # 在每个表级别增加 表行数 字段（使用 INFORMATION_SCHEMA 估算）
    try:
        if analyze_driver is not None and database:
            table_rows_map = _fetch_table_rows(analyze_driver, database, list(tables.keys()))
            for t in list(mapping.keys()):
                mapping.setdefault(t, {})
                mapping[t]["表行数"] = int(table_rows_map.get(t, 0) or 0)
        else:
            raise RuntimeError("no-db")
    except Exception:
        for t in list(mapping.keys()):
            mapping.setdefault(t, {})
            mapping[t]["表行数"] = 0

    # 关闭连接
    try:
        if estimator is not None and hasattr(estimator, "db") and hasattr(estimator.db, "disconnect"):
            estimator.db.disconnect()
    except Exception:
        pass

    # 调试：记录无法读取的 SQL 文件 + 执行次数与倍率
    if debug:
        try:
            base = Path(debug_dir or (Path(_ROOT_DIR) / "debug" / "part2"))
            base.mkdir(parents=True, exist_ok=True)
            if skipped:
                (base / "skipped.json").write_text(json.dumps(skipped, ensure_ascii=False, indent=2), encoding="utf-8")
            # 同时输出原始频率与规范化后的次数
            (base / "exec_counts_raw.json").write_text(json.dumps(exec_counts_float or {}, ensure_ascii=False, indent=2), encoding="utf-8")
            (base / "exec_counts.json").write_text(json.dumps(exec_counts or {}, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    # JSON 输出
    return json.dumps(mapping, ensure_ascii=False, indent=2)


def main() -> None:
    ap = argparse.ArgumentParser(description="PART2: 基于 schema.sql 与历史查询，输出列级操作到基数（rows）的 JSON 映射；可选连接数据库估计基数")
    ap.add_argument("--schema-sql", required=True, help="schema.sql 路径")
    ap.add_argument("--sql-dir", required=True, help="包含 queryN.sql/ query_XX.sql 的目录")
    # 移除外部延迟依赖，avg_time 将来自 EXPLAIN ANALYZE
    ap.add_argument("--out", help="输出文件；省略则打印到标准输出")
    # optional DB for cardinality via EXPLAIN
    ap.add_argument("--dialect", default="mysql", help="方言（仅支持 mysql）")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="")
    ap.add_argument("--config", default=str(Path(_ROOT_DIR) / "query_latency" / "db_config.ini"), help="INI 配置路径（含 [mysql] 段）")
    ap.add_argument("--debug", action="store_true", help="开启调试输出（保存 EXPLAIN 文本、节点与归因）")
    ap.add_argument("--debug-dir", default=str(Path(_ROOT_DIR) / "debug" / "part2"), help="调试输出目录")
    ap.add_argument("--exec-counts", default=str(Path(_ROOT_DIR) / "Data" / "cleaned_sql" / "query_and_update" / "sample_execution_counts_chbench.csv"), help="执行次数 CSV（filename,count）")
    ap.add_argument("--dml-time-cache", default=str(Path(_ROOT_DIR) / "debug" / "part2" / "dml_time_cache.json"), help="DML 计时缓存 JSON 路径（由 dml_timer.py 生成），可复用 INSERT/UPDATE 的 avg_time 成本。")
    args = ap.parse_args()

    content = build_part2(
        schema_sql_path=args.schema_sql,
        sql_dir=args.sql_dir,
        dialect=args.dialect,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        config_path=args.config,
        debug=args.debug,
        debug_dir=args.debug_dir,
        exec_counts_path=args.exec_counts,
        dml_time_cache=args.dml_time_cache,
    )
    if args.out:
        Path(args.out).write_text(content, encoding="utf-8")
    else:
        print(content)


if __name__ == "__main__":
    main()
