#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, List, Tuple, Optional
import re

try:
    # Optional: richer constraint helper if available
    from rewrite.base import MySQLConstraintHelper
except Exception:  # pragma: no cover
    MySQLConstraintHelper = None  # type: ignore

from utils.schema_introspect import (
    table_exists,
    get_table_columns,
    get_primary_key_columns,
    find_fk_between,
    get_outbound_fks_info,
)


def _ensure_tables_exist(db, tables: List[str]) -> List[str]:
    issues: List[str] = []
    for t in tables:
        if not table_exists(db, t):
            issues.append(f"表不存在: {t}")
    return issues


def check_table_join(db, t1: str, t2: str, join_pairs: List[Tuple[str, str]]) -> List[str]:
    issues = _ensure_tables_exist(db, [t1, t2])
    if issues:
        return issues
    cols1 = set(get_table_columns(db, t1))
    cols2 = set(get_table_columns(db, t2))
    for a, b in join_pairs:
        if a not in cols1:
            issues.append(f"连接键不存在: {t1}.{a}")
        if b not in cols2:
            issues.append(f"连接键不存在: {t2}.{b}")
    return issues


def check_vertical_split(db, src_table: str, new_tables: List[str], column_lists: List[List[str]]) -> List[str]:
    issues = _ensure_tables_exist(db, [src_table])
    if issues:
        return issues
    src_cols = set(get_table_columns(db, src_table))
    # columns validity
    for i, cols in enumerate(column_lists):
        for c in cols:
            if c not in src_cols:
                issues.append(f"子表列不存在于源表: {new_tables[i]}.{c}")
    # primary key subset rule
    pks = get_primary_key_columns(db, src_table)
    if pks:
        for i, cols in enumerate(column_lists):
            if not set(pks).issubset(set(cols)):
                issues.append(f"子表未包含全部主键列: {new_tables[i]} 缺少 {set(pks)-set(cols)}")
    # same FK columns must not split across different child tables
    fk_infos = get_outbound_fks_info(db, src_table)
    child_sets = [set(cols) for cols in column_lists]
    for fk in fk_infos:
        cols = set(fk['columns'])
        fits = [cols.issubset(s) for s in child_sets]
        if not any(fits):
            issues.append(f"外键列被拆散，无法完整落在某个子表: {fk['columns']}")
    return issues


def _extract_columns_from_predicate(pred: str) -> List[str]:
    """Heuristically extract column-like identifiers from a predicate.

    - Strips single/double-quoted string literals first to avoid picking tokens
      from constants like 'recent'.
    - Filters common SQL keywords.
    - Keeps bare identifiers (no table alias resolution here).
    """
    # Remove quoted strings (supports simple escape by backslash)
    s = re.sub(r"('([^'\\]|\\.)*'|\"([^\"\\]|\\.)*\")", " ", pred)
    toks = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", s)
    keywords = {
        'and','or','not','null','true','false','in','like','between','is','select','from','where','exists','case','when','then','else','end','as'
    }
    return [t for t in toks if t.lower() not in keywords]


def check_horizontal_split(db, table: str, predicates: List[Tuple[str, str]]) -> List[str]:
    issues = _ensure_tables_exist(db, [table])
    if issues:
        return issues
    cols = set(get_table_columns(db, table))
    for name, pred in predicates:
        for c in _extract_columns_from_predicate(pred):
            if c not in cols:
                # allow constants/functions; only flag if looks like column
                issues.append(f"谓词引用未知列: {name}.{c}")
    # 互为补集校验：要求两个子表的并集覆盖原表，且互不重叠。
    # 优先用 DB 进行强校验；无 DB 时回退到“语法级互补识别”（仅覆盖常见形式）。
    if len(predicates) == 2 and db:
        (n1, p1), (n2, p2) = predicates[0], predicates[1]
        # 覆盖性：是否存在不属于任一子表的行
        sql_uncovered = (
            f"SELECT 1 FROM `{table}` WHERE NOT (({p1}) OR ({p2})) LIMIT 1"
        )
        # 互斥性：是否存在同时落入两个子表的行
        sql_overlap = (
            f"SELECT 1 FROM `{table}` WHERE ({p1}) AND ({p2}) LIMIT 1"
        )
        try:
            uncovered = db.execute_query(sql_uncovered)
        except Exception:
            uncovered = []
        try:
            overlap = db.execute_query(sql_overlap)
        except Exception:
            overlap = []
        if uncovered:
            issues.append(
                "水平拆分谓词未覆盖全部行：存在不满足任一子表条件的记录。"
                f" 建议将其中一个谓词写为另一个谓词的补集（如 {n1}: {p1} 与 {n2}: NOT({p1})），"
                "或改用 '< vs >=', '<= vs >', '= vs <>' 等互补形式。"
            )
        if overlap:
            issues.append(
                "水平拆分谓词存在重叠：存在同时满足两个子表条件的记录。"
                f" 请调整为互斥的补集划分（例如 {n1}: {p1} 与 {n2}: NOT({p1})）。"
            )
    elif len(predicates) == 2 and not db:
        # 语法级互补识别（保守）：
        # 支持以下互补对：
        #   - col = v  vs  col <> v / col != v / NOT(col = v)
        #   - col < k  vs  col >= k（阈值 k 相同）
        #   - col <= k vs  col > k
        #   - col IN (v1,...) vs NOT IN (v1,...)
        # 其余情况无法在无 DB 场景证明互补与覆盖，提示用户修正。
        (n1, p1), (n2, p2) = predicates[0], predicates[1]

        def _norm_ws(s: str) -> str:
            return re.sub(r"\s+", " ", s.strip())

        def _parse_simple(pred: str) -> Optional[Tuple[str, str, str]]:
            s = pred.strip()
            # 去掉外围 NOT(...) 便于识别
            m_not = re.match(r"^NOT\s*\(\s*(.+)\s*\)$", s, re.IGNORECASE)
            if m_not:
                inner = m_not.group(1)
                # 标记为 取反
                t = _parse_simple(inner)
                if t is None:
                    return None
                col, op, lit = t
                return (col, f"NOT {op}", lit)
            # 1) = / <> / !=
            m = re.match(r"^`?([A-Za-z_][A-Za-z0-9_]*)`?\s*(=|<>|!=)\s*(.+)$", s)
            if m:
                col, op, lit = m.group(1), m.group(2), m.group(3).strip()
                return (col.lower(), op.upper(), _norm_ws(lit.strip("'\"")))
            # 2) <, <=, >, >=
            m = re.match(r"^`?([A-Za-z_][A-Za-z0-9_]*)`?\s*(<=|>=|<|>)\s*(.+)$", s)
            if m:
                col, op, lit = m.group(1), m.group(2), m.group(3).strip()
                return (col.lower(), op, _norm_ws(lit.strip("'\"")))
            return None

        def _parse_in(pred: str) -> Optional[Tuple[str, bool, List[str]]]:
            # col IN (...), col NOT IN (...)
            m = re.match(r"^`?([A-Za-z_][A-Za-z0-9_]*)`?\s*(NOT\s+)?IN\s*\((.*)\)\s*$", pred, re.IGNORECASE|re.DOTALL)
            if not m:
                return None
            col = m.group(1).lower()
            neg = bool(m.group(2))
            body = m.group(3)
            # 粗略分割列表（忽略逗号内空格）
            vals = [v.strip().strip("'\"") for v in body.split(',') if v.strip()]
            return (col, neg, [v.lower() for v in vals])

        def _is_complementary(p1: str, p2: str) -> bool:
            # IN vs NOT IN 同域补集
            i1, i2 = _parse_in(p1), _parse_in(p2)
            if i1 and i2 and i1[0] == i2[0]:
                # 同一列；一个为 NOT IN，一个为 IN；列表元素相同
                if i1[1] != i2[1] and set(i1[2]) == set(i2[2]) and len(i1[2]) >= 1:
                    return True
            a = _parse_simple(p1)
            b = _parse_simple(p2)
            if not a or not b:
                return False
            col1, op1, lit1 = a
            col2, op2, lit2 = b
            if col1 != col2:
                return False
            # =  vs  NOT = / <> / !=
            if op1 == '=' and (op2 in ('<>', '!=') or op2.upper().startswith('NOT ')) and lit1.lower() == lit2.lower():
                return True
            if op2 == '=' and (op1 in ('<>', '!=') or op1.upper().startswith('NOT ')) and lit1.lower() == lit2.lower():
                return True
            # <  vs  >=
            if op1 == '<' and op2 == '>=' and lit1 == lit2:
                return True
            if op2 == '<' and op1 == '>=' and lit1 == lit2:
                return True
            # <= vs  >
            if op1 == '<=' and op2 == '>' and lit1 == lit2:
                return True
            if op2 == '<=' and op1 == '>' and lit1 == lit2:
                return True
            return False

        if not _is_complementary(p1, p2):
            issues.append(
                "水平拆分谓词在无数据库校验时未能证明是互补的。"
                f" 当前: {n1}: {p1} 与 {n2}: {p2}。建议改写为一对语法互补谓词（例如 '=' vs '<>'、'< ' vs '>= '、'IN' vs 'NOT IN'），"
                "或在连接数据库后重跑以进行强校验。"
            )
    return issues


def check_horizontal_merge(db, sources: List[str]) -> List[str]:
    issues = _ensure_tables_exist(db, sources)
    if issues:
        return issues
    # ensure identical structures (simple check: same column list in order)
    cols = [get_table_columns(db, t) for t in sources]
    base = cols[0]
    for i, c in enumerate(cols[1:], start=1):
        if c != base:
            issues.append(f"源表结构不一致: {sources[0]} vs {sources[i]}")
    return issues


def check_redundant_add(db, source_table: str, source_column: str, target_table: str) -> List[str]:
    issues = _ensure_tables_exist(db, [source_table, target_table])
    if issues:
        return issues
    scols = set(get_table_columns(db, source_table))
    tcols = set(get_table_columns(db, target_table))
    if source_column not in scols:
        issues.append(f"源列不存在: {source_table}.{source_column}")
    # FK required between target (child) and source (parent)
    fk = find_fk_between(db, target_table, source_table)
    if not fk:
        issues.append(f"未检测到外键关系: {target_table} -> {source_table}")
    return issues


def check_redundant_drop(db, table: str, column: str) -> List[str]:
    issues = _ensure_tables_exist(db, [table])
    if issues:
        return issues
    cols = set(get_table_columns(db, table))
    if column not in cols:
        issues.append(f"待删除列不存在: {table}.{column}")
    # 数据丢失校验无法在不分析业务的情况下自动保证，这里提示风险
    return issues
