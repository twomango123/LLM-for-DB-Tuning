#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import json
import re
import os
import sys
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
    for m in re.finditer(r"\bFROM\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s+(?:AS\s+)?(?!" + kw + r"\b)(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2))
        aliases[alias] = table
        aliases[table] = table
    # JOIN table [AS] alias
    for m in re.finditer(r"\bJOIN\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s+(?:AS\s+)?(?!" + kw + r"\b)(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2))
        aliases[alias] = table
        aliases[table] = table
    # UPDATE table [AS] alias
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s+(?:AS\s+)?(" + _Q_IDENT + r")\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        alias = _unquote_ident(m.group(2))
        aliases[alias] = table
        aliases[table] = table
    # bare UPDATE table
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        aliases[table] = table
    # Also capture bare FROM/JOINS without alias: FROM table
    for m in re.finditer(r"\bFROM\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\b", sql, re.IGNORECASE):
        table = _norm_table(m.group(1))
        aliases[table] = table
    for m in re.finditer(r"\bJOIN\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\b", sql, re.IGNORECASE):
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
        m2 = re.match(r"(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*(=|<>|!=|>=|<=|>|<|LIKE|NOT\s+LIKE|IN|NOT\s+IN|BETWEEN)\s*(.+)$", p, re.IGNORECASE)
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
    # returns list of (table, [cols]) for each INSERT INTO
    results: List[Tuple[str, List[str]]] = []
    # INSERT INTO t(col, ...) VALUES (...)
    for m in re.finditer(r"\bINSERT\s+INTO\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*\(([^)]*)\)\s*VALUES\b", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(1))
        cols = [
            _unquote_ident(x.strip()) for x in m.group(2).split(",") if x.strip()
        ]
        results.append((tbl, cols))
    # INSERT INTO t VALUES (...): 退化为全列（按 schema 中列）
    for m in re.finditer(r"\bINSERT\s+INTO\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)\s*VALUES\s*\(", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(1))
        cols = list(schema.get(tbl, {}).keys())
        if cols:
            results.append((tbl, cols))
    return results


def _extract_update_sets(sql: str) -> List[Tuple[str, List[str]]]:
    # returns list of (table, [cols]) for each UPDATE ... SET ...
    results: List[Tuple[str, List[str]]] = []
    # 支持可选别名与无 WHERE 的 UPDATE
    for m in re.finditer(r"\bUPDATE\s+(" + _Q_IDENT + r"(?:\s*\.\s*" + _Q_IDENT + r")?)(?:\s+(?:AS\s+)?" + _Q_IDENT + r")?\s+SET\s+(.+?)(?=\bWHERE\b|$)", sql, re.IGNORECASE | re.DOTALL):
        tbl = _norm_table(m.group(1))
        sets = m.group(2)
        cols: List[str] = []
        for s in sets.split(","):
            s = s.strip()
            m2 = re.match(r"(" + _Q_IDENT + r")\s*=", s)
            if m2:
                cols.append(_unquote_ident(m2.group(1)))
        results.append((tbl, cols))
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

    # WHERE predicates (filters)
    for lhs, pred in _extract_where_predicates(sql):
        base_table = _resolve_table_for_column(lhs, aliases, schema)
        col = lhs.split(".")[-1]
        if base_table and col in schema.get(base_table, {}):
            # operation 名含谓词，形如 filter(col = 'X')
            op_text = f"filter({col} {pred})"
            ops.append(OpEvent(table=base_table, column=col, operation=op_text, predicate=pred))

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

    # INSERT columns
    for tbl, cols in _extract_insert_columns(sql, schema):
        for c in cols:
            if c in schema.get(tbl, {}):
                ops.append(OpEvent(table=tbl, column=c, operation="insert"))

    # UPDATE sets and predicates
    for tbl, cols in _extract_update_sets(sql):
        for c in cols:
            if c in schema.get(tbl, {}):
                ops.append(OpEvent(table=tbl, column=c, operation="update"))
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
    matched = 0
    for n in nodes:
        optext = (n.get("op") or "").strip()
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

        # 1) Filters (含连接条件)：优先将等值条件归因到 join(col)，否则归因到 filter()/select
        if "filter" in low or "where" in low:
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
                            per_key_time[key] = per_key_time.get(key, 0.0) + float(avg_time) / 2.0
                continue

            # 1b) 非连接谓词：按列归因到 filter(…)，否则回退到 select
            cols = _extract_column_tokens(optext)
            # Fallback: 用原始 SQL 的 WHERE 列集合
            if not cols:
                for lhs, _pred in _extract_where_predicates(sql):
                    cols.append(lhs)
            for tok in cols:
                base_table = _resolve_table_for_column(tok, aliases, schema)
                col = tok.split(".")[-1]
                if not base_table:
                    continue
                filter_key = next((k for k in ops_present if k[0] == base_table and k[1] == col and isinstance(k[2], str) and k[2].startswith("filter(")), None)
                key = filter_key if filter_key else (base_table, col, "select")
                if key in ops_present:
                    per_key_time[key] = per_key_time.get(key, 0.0) + float(avg_time)
            continue

        # 2) Sort: distribute to ORDER BY columns present in this SQL
        if "sort" in low or "order" in low:
            if order_cols:
                share = float(avg_time) / float(len(order_cols))
                for key in order_cols:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue

        # 3) Group/Aggregate: distribute to GROUP BY columns
        if "group" in low or "aggregate" in low:
            if group_cols:
                share = float(avg_time) / float(len(group_cols))
                for key in group_cols:
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + share
            continue

        # 4) Join: try to attribute using equality pairs a.b = c.d
        if "join" in low or "nested loop" in low:
            for left, right in _extract_ident_pairs_from_text(optext):
                for tok in (left, right):
                    base_table = _resolve_table_for_column(tok, aliases, schema)
                    col = tok.split(".")[-1]
                    if not base_table:
                        continue
                    key = (base_table, col, "join(" + (right.split(".")[-1] if tok == left else left.split(".")[-1]) + ")")
                    # 放宽：如果具体右列名不匹配，至少按 join(*) 归集
                    if key not in ops_present:
                        # Fallback to any join(*) op in this SQL
                        any_join_key = (base_table, col, next((e.operation for e in evt_list if e.table == base_table and e.column == col and e.operation.startswith("join(")), None))
                        if any_join_key[2]:
                            key = any_join_key  # type: ignore
                    if key in ops_present:
                        per_key_time[key] = per_key_time.get(key, 0.0) + float(avg_time) / 2.0  # split between two sides
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
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    # stats: (table, column, operation) -> {count, sum_time, rows, filtered}
    stats: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    index_map: List[Dict[str, Any]] = []
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
        if qa_analyze_sql is not None and analyze_driver is not None:
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
        # Update counts and sum_time per SQL (count only once per key per SQL)
        for key in set((e.table, e.column, e.operation) for e in ops):
            stats[key]["count"] += 1
            if key in per_key_time:
                stats[key]["sum_time"] += float(per_key_time[key])

    # initialize result with all tables/columns to ensure 字段长度 可插入
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        t: {c: [] for c in cols.keys()} for t, cols in schema.items()
    }
    # fill in operations
    for (table, column, op), info in stats.items():
        rows_val = info.get("rows")
        if rows_val is None:
            rows_val = 1
        item = {
            "operation": op,
            "rows": rows_val if isinstance(rows_val, (int, float)) else 1,
        }
        if info.get("filtered") is not None:
            item["filtered"] = info.get("filtered")
        # avg_time 和 count：avg_time = 总时间 / 次数
        cnt = int(info.get("count") or 0)
        if cnt > 0:
            avg_time_s = (float(info.get("sum_time") or 0.0)) / float(cnt)
            avg_time_ms = avg_time_s * 1000.0
            item["avg_time"] = avg_time_ms
            item["count"] = cnt
        result.setdefault(table, {}).setdefault(column, []).append(item)
    # sort operations per column deterministically
    for t, cols in result.items():
        for c, arr in cols.items():
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
                debug_dir: Optional[str] = None) -> str:
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

    # Optional estimator for rows (best-effort)
    estimator = _maybe_build_estimator(dialect, host, port, user, password, database)

    # 字段长度估算（必须连接 MySQL，无法连接则报错）
    col_driver = _connect_mysql_driver(dialect, host, port, user, password, database)
    if ColumnLengthEstimator is None:
        try:
            col_driver.disconnect()
        except Exception:
            pass
        raise SystemExit("缺少字段长度估算器模块 column_stats.estimator")
    length_est = ColumnLengthEstimator(col_driver)
    lengths_by_table: Dict[str, Dict[str, int]] = {}
    for t in tables.keys():
        try:
            stats = length_est.estimate_table(t)
            lengths_by_table[t] = {col: int(round(info.get("length", 0))) for col, info in stats.items()}
        except Exception as e:
            try:
                col_driver.disconnect()
            except Exception:
                pass
            raise SystemExit(f"字段长度估算失败（{t}）：{e}")

    # 聚合操作
    # 构建用于 EXPLAIN ANALYZE 的连接（与字段长度共用一个驱动即可）
    analyze_driver = col_driver
    mapping = _aggregate_operations(tables, queries, lat_map, estimator, analyze_driver, debug=debug, debug_dir=debug_dir)

    # 将 字段长度 插入到每个列的列表首位；没有操作的列也要输出
    for t, cols in tables.items():
        for c in cols.keys():
            arr = mapping.setdefault(t, {}).setdefault(c, [])
            length_val = lengths_by_table.get(t, {}).get(c, 0)
            # 仅在未插入过时添加（避免重复）
            if not arr or (arr and "字段长度" not in arr[0]):
                arr.insert(0, {"字段长度": length_val})

    # 关闭连接
    try:
        if estimator is not None and hasattr(estimator, "db") and hasattr(estimator.db, "disconnect"):
            estimator.db.disconnect()
    except Exception:
        pass
    try:
        col_driver.disconnect()
    except Exception:
        pass

    # 调试：记录无法读取的 SQL 文件
    if debug and skipped:
        try:
            base = Path(debug_dir or (Path(_ROOT_DIR) / "debug" / "part2"))
            base.mkdir(parents=True, exist_ok=True)
            (base / "skipped.json").write_text(json.dumps(skipped, ensure_ascii=False, indent=2), encoding="utf-8")
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
    )
    if args.out:
        Path(args.out).write_text(content, encoding="utf-8")
    else:
        print(content)


if __name__ == "__main__":
    main()
