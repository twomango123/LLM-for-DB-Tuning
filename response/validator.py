#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, List, Tuple
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
    # naive: words that look like identifiers; skip SQL keywords and numbers
    toks = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", pred)
    keywords = {
        'and','or','not','null','true','false','in','like','between','is','select','from','where','exists','case','when','then','else','end'
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

