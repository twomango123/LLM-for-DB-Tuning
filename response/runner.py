#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse operations from response/response.txt and instantiate rewrite operations.

Workflow:
- Stage 1 (check): parse each textual op, validate syntax and required args.
- Stage 2 (fill): query MySQL (via DataBase.MySQLDriver) to fetch missing info
  like column lists and foreign-key join pairs where possible.
- Stage 3 (execute): call rewrite operators' apply_to_schema. If no DB is
  connected, emit SQL scripts instead.

Supported operations (mapped to rewrite classes):
- ColumnSplit -> rewrite.ColumnSplit.ColumnSplit
- VerticalSplit -> rewrite.TableSplit.TableSplit
- HorizontalSplit -> rewrite.HorizontalSplit.HorizontalSplit
- HorizontalMerge -> rewrite.HorizontalMerge.HorizontalMerge
- RedundantColumnAdd -> rewrite.RedundantColumnAdd.RedundantColumnAdd
- RedundantColumnDrop -> rewrite.RedundantColumnDrop.RedundantColumnDrop
- TableJoin -> mapped to rewrite.TableMerge.TableMerge for physical merge

How to run:
  Dry-run (no DB):
    python3 LLM-for-DB-Tuning/response/runner.py
  With DB (auto-fill columns/FKs):
    python3 LLM-for-DB-Tuning/response/runner.py --use-db \
      --host localhost --port 3306 --user root --password 'xxx' --database tpcch
"""

from __future__ import annotations

import os
import re
import sys
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple, Dict

import argparse
import shutil

# Import rewrite ops
_ROOT = os.path.dirname(os.path.dirname(__file__))  # LLM-for-DB-Tuning
if _ROOT not in sys.path:
    sys.path.append(_ROOT)

try:
    from rewrite.ColumnSplit import ColumnSplit
    from rewrite.TableSplit import TableSplit as VerticalSplit
    from rewrite.HorizontalSplit import HorizontalSplit
    from rewrite.HorizontalMerge import HorizontalMerge
    from rewrite.RedundantColumnAdd import RedundantColumnAdd
    from rewrite.RedundantColumnDrop import RedundantColumnDrop
    from rewrite.TableJoin import TableJoin as TableJoin
    from utils.schema_introspect import (
        get_table_columns,
        get_tables_columns,
        get_primary_key_columns,
        find_fk_between,
    )
    from response.validator import (
        check_table_join,
        check_vertical_split,
        check_horizontal_split,
        check_horizontal_merge,
        check_redundant_add,
        check_redundant_drop,
    )
except Exception as e:  # pragma: no cover
    print(f"[runner] 导入 rewrite 模块失败: {e}")
    raise


"""Global snapshot from sequential validation.
_SEQCAT_TABLE_COLS maps table name (lowercased) to a sorted list of columns.
plan_statements(TableJoin) consults this to avoid depending on DB state.
"""
_SEQCAT_TABLE_COLS: Dict[str, List[str]] = {}


@dataclass
class StatsCatalog:
    table_columns: Dict[str, List[str]]
    foreign_keys: List[Dict[str, Any]]
    join_keys: List[Dict[str, Any]]
    meta: Optional[Dict[str, Any]] = None
    meta_path: Optional[str] = None


_STATS_CATALOG: Optional[StatsCatalog] = None


def _load_stats_bundle(stats_dir: Optional[str]) -> Optional[StatsCatalog]:
    if not stats_dir:
        return None
    base = Path(stats_dir)
    if not base.exists():
        return None

    manifest = base / 'manifest.json'
    manifest_data: Dict[str, Any] = {}
    if manifest.exists():
        try:
            manifest_data = json.loads(manifest.read_text(encoding='utf-8'))
        except Exception:
            manifest_data = {}

    def _path_from_manifest(key: str, fallback: str) -> Path:
        rel = None
        try:
            rel = (manifest_data.get('artifacts') or {}).get(key)
        except Exception:
            rel = None
        return base / (rel or fallback)

    # table columns
    table_columns: Dict[str, List[str]] = {}
    tc_path = _path_from_manifest('table_columns', 'table_columns.json')
    schema_path = _path_from_manifest('schema', 'schema.json')
    if tc_path.exists():
        try:
            data = json.loads(tc_path.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                table_columns = {str(t): list(cols) for t, cols in data.items() if isinstance(cols, list)}
        except Exception:
            table_columns = {}
    elif schema_path.exists():
        try:
            data = json.loads(schema_path.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                table_columns = {str(t): list(cols.keys()) for t, cols in data.items() if isinstance(cols, dict)}
        except Exception:
            table_columns = {}

    fk_path = _path_from_manifest('foreign_keys', 'foreign_keys.json')
    join_path = _path_from_manifest('join_keys', 'join_keys.json')
    meta_path = _path_from_manifest('meta', 'meta.json')

    foreign_keys: List[Dict[str, Any]] = []
    if fk_path.exists():
        try:
            foreign_keys = json.loads(fk_path.read_text(encoding='utf-8'))
        except Exception:
            foreign_keys = []

    join_keys: List[Dict[str, Any]] = []
    if join_path.exists():
        try:
            join_keys = json.loads(join_path.read_text(encoding='utf-8'))
        except Exception:
            join_keys = []

    meta = None
    meta_p = None
    if meta_path.exists():
        meta_p = str(meta_path)
        try:
            meta = json.loads(meta_path.read_text(encoding='utf-8'))
        except Exception:
            meta = None

    return StatsCatalog(table_columns=table_columns, foreign_keys=foreign_keys, join_keys=join_keys, meta=meta, meta_path=meta_p)


def _schema_seed_from_stats(stats: Optional[StatsCatalog]) -> Optional[Dict[str, List[str]]]:
    if not stats or not stats.table_columns:
        return None
    return {t: list(cols) for t, cols in stats.table_columns.items()}


def _find_fk_between_stats(stats: StatsCatalog, child_table: str, parent_table: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    ct = str(child_table).lower()
    pt = str(parent_table).lower()
    for fk in (stats.foreign_keys or []):
        try:
            ft = str(fk.get('from_table') or '').lower()
            tt = str(fk.get('to_table') or '').lower()
            if ft != ct or tt != pt:
                continue
            from_cols = fk.get('from_columns') or []
            to_cols = fk.get('to_columns') or []
            if len(from_cols) == len(to_cols) and from_cols:
                out = list(zip(from_cols, to_cols))
                return out
        except Exception:
            continue
    return out


def _fk_matches_join_keys(stats: StatsCatalog, source_table: str, target_table: str, join_keys: List[Tuple[str, str]]) -> bool:
    if not join_keys:
        return False
    jk = {(str(a).lower(), str(b).lower()) for (a, b) in join_keys}
    # target -> source
    for fk in (stats.foreign_keys or []):
        try:
            ft = str(fk.get('from_table') or '').lower()
            tt = str(fk.get('to_table') or '').lower()
            if ft == str(target_table).lower() and tt == str(source_table).lower():
                pairs = list(zip(fk.get('to_columns') or [], fk.get('from_columns') or []))
                if {(str(a).lower(), str(b).lower()) for (a, b) in pairs} == jk:
                    return True
            if ft == str(source_table).lower() and tt == str(target_table).lower():
                pairs = list(zip(fk.get('from_columns') or [], fk.get('to_columns') or []))
                if {(str(a).lower(), str(b).lower()) for (a, b) in pairs} == jk:
                    return True
        except Exception:
            continue
    return False

@dataclass
class ParsedOp:
    kind: str
    raw: str
    # normalized parameters
    params: dict = field(default_factory=dict)
    # human-readable problems that block execution
    missing: List[str] = field(default_factory=list)


def _split_operations(line: str) -> List[str]:
    """Split a line into operations. Supports either one-op-per-line or '-' chained.
    Use a parenthesis-aware scan to avoid splitting inside arguments.
    """
    s = line.strip()
    if not s:
        return []
    ops = []
    i, n = 0, len(s)
    start = 0
    depth = 0
    while i < n:
        ch = s[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth - 1)
        elif ch == '-' and depth == 0:
            # end current op before '-'
            piece = s[start:i].strip()
            if piece:
                ops.append(piece)
            start = i + 1
        i += 1
    last = s[start:].strip()
    if last:
        ops.append(last)
    return ops


def _find_top_level_hyphens(s: str) -> List[int]:
    """Return indices of '-' that are at top level (not inside parentheses)."""
    idxs: List[int] = []
    depth = 0
    for i, ch in enumerate(s):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth - 1)
        elif ch == '-' and depth == 0:
            idxs.append(i)
    return idxs


_OP_PREFIXES = (
    'TableJoin(', 'VerticalSplit(', 'HorizontalSplit(', 'HorizontalMerge(', 'RedundantColumnAdd(', 'RedundantColumnDrop(', 'ColumnSplit('
)


def _looks_like_op(s: str) -> bool:
    t = s.strip().lstrip('#').strip()
    return any(t.startswith(p) for p in _OP_PREFIXES)


def _maybe_split_correction(line: str) -> Optional[Tuple[str, str]]:
    """Detect a correction line of the form "原操作-新操作".

    Ambiguity note: runner also supports chaining multiple ops with '-'.
    We treat a line as a correction if and only if there is exactly one
    top-level '-' and both sides look like known operations. In that case,
    we interpret it as a correction pair and ignore the left side when
    generating ops, using the right side as the replacement.
    """
    s = line.strip()
    if not s:
        return None
    hyps = _find_top_level_hyphens(s)
    if len(hyps) != 1:
        return None
    i = hyps[0]
    left, right = s[:i].strip(), s[i + 1 :].strip()
    if _looks_like_op(left) and _looks_like_op(right):
        # 降低歧义：仅当左右为相同类型的操作时，视为“修正”而非“-”链式两个操作
        def _kind(x: str) -> Optional[str]:
            m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\(", x)
            return m.group(1) if m else None
        if _kind(left) and _kind(left) == _kind(right):
            return left.rstrip(';'), right.rstrip(';')
    return None


def _print_correction_request(issues: Optional[List[str]] = None) -> None:
    """Prints a concise instruction on how to reply with corrections.

    It asks for: 原操作-新操作, then shows 1-2 short examples, and
    optionally reprints the issues list so users can map to ops.
    """
    print("\n—— 修正格式要求 ——")
    print("请给出需要修改的操作和修改后的操作，格式为：原操作-新操作。")
    print("示例1：VerticalSplit(t, False):t_a(col1,col2),t_b(col3) - VerticalSplit(t, True):t_a(col1,col2),t_b(col3)")
    print("示例2：TableJoin(A,B, a_id, b_id, False):AB - TableJoin(A,B, a_id, b_id, True):AB")
    if issues:
        print("\n错误信息提示：")
        for e in issues:
            print("- ", e)
        # Heuristic, common fix directions
        tips: List[str] = []
        has_missing_table = any('表不存在' in x for x in issues)
        has_col_missing = any('列不存在' in x for x in issues)
        if has_missing_table:
            tips.append("若由上游 is_retained=False 删除导致，请将上游操作的 is_retained 改为 True，或在下游改为引用新表名。")
        if has_col_missing:
            tips.append("确认列属于源表；若是垂直拆分，请将列放入相应子表；或在冗余列新增中选用存在的列名。")
        if tips:
            print("\n常见修复方向：")
            for t in tips:
                print("- ", t)


def _strip_outer(s: str) -> str:
    s2 = s.strip()
    if s2.startswith('(') and s2.endswith(')'):
        return s2[1:-1].strip()
    return s2


def _tokenize_top_level(csv_like: str) -> List[str]:
    """Split by commas at top level (ignore commas inside parentheses)."""
    s = csv_like.strip()
    out, cur, depth = [], [], 0
    for ch in s:
        if ch == '(':
            depth += 1
            cur.append(ch)
        elif ch == ')':
            depth = max(0, depth - 1)
            cur.append(ch)
        elif ch == ',' and depth == 0:
            token = ''.join(cur).strip()
            if token:
                out.append(token)
            cur = []
        else:
            cur.append(ch)
    tail = ''.join(cur).strip()
    if tail:
        out.append(tail)
    return out


def parse_table_join(raw: str) -> ParsedOp:
    # TableJoin(Table1,Table2, k1, k2, is_retained): NewTable
    m = re.match(r"^TableJoin\((.*)\)\s*:\s*([\w.]+)\s*$", raw.strip())
    if not m:
        return ParsedOp(kind='TableJoin', raw=raw, missing=["无法解析 TableJoin 语法"])
    args_s = m.group(1)
    new_table = m.group(2).split('.')[-1]
    args = _tokenize_top_level(args_s)
    if len(args) != 5:
        return ParsedOp(kind='TableJoin', raw=raw, missing=[f"参数数量应为5，实际为{len(args)}"])
    def _norm_ident(x: str) -> str:
        s = (x or '').strip()
        # strip surrounding quotes/backticks if present
        if (s.startswith('`') and s.endswith('`')) or (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
            s = s[1:-1]
        # allow dotted form, take last segment
        s = s.split('.')[-1]
        # final cleanup for any stray quotes/backticks
        return s.strip("`'\"")

    t1 = _norm_ident(args[0])
    t2 = _norm_ident(args[1])
    k1_raw = _strip_outer(args[2])
    k2_raw = _strip_outer(args[3])
    retained_s = args[4].strip()
    is_retained = retained_s.lower() in ('true', '1')

    # keys: support single or list in parentheses
    def _norm_col_name(s: str) -> str:
        s2 = (s or '').strip().strip("`'\"")
        # 兼容 table.column 或纯列名，统一取末段
        s2 = s2.split('.')[-1]
        # 去除可能残留的包裹符
        return s2.strip("`'\"")
    k1_list = [c.strip() for c in _tokenize_top_level(k1_raw)] if ',' in k1_raw else ([k1_raw] if k1_raw else [])
    k2_list = [c.strip() for c in _tokenize_top_level(k2_raw)] if ',' in k2_raw else ([k2_raw] if k2_raw else [])
    # 归一化：去掉表前缀
    k1 = [_norm_col_name(x) for x in k1_list]
    k2 = [_norm_col_name(x) for x in k2_list]

    missing: List[str] = []
    join_pairs: List[Tuple[str, str]] = []
    if not k1 or not k2:
        missing.append("缺少连接键（table1_join_key, table2_join_key）")
    elif len(k1) != len(k2):
        missing.append(f"连接键数量不匹配：{len(k1)} vs {len(k2)}")
    else:
        join_pairs = list(zip(k1, k2))

    # Columns (old_columns_list) 将在后续阶段自动从数据库获取（_auto_fill_with_db），不标记为缺失。

    return ParsedOp(
        kind='TableJoin',
        raw=raw,
        params={
            'table1': t1,
            'table2': t2,
            'join_pairs': join_pairs,
            'is_retained': is_retained,
            'new_table': new_table,
        },
        missing=missing,
    )


def parse_vertical_split(raw: str) -> ParsedOp:
    # VerticalSplit(SourceTable, is_retained):t1(col,...), t2(col,...)
    m = re.match(r"^VerticalSplit\((.*)\)\s*:\s*(.*)$", raw.strip())
    if not m:
        return ParsedOp(kind='VerticalSplit', raw=raw, missing=["无法解析 VerticalSplit 语法"])
    args_s = m.group(1)
    children_s = m.group(2)
    args = _tokenize_top_level(args_s)
    if len(args) != 2:
        return ParsedOp(kind='VerticalSplit', raw=raw, missing=[f"参数数量应为2（源表, is_retained），实际为{len(args)}"])
    src_table = args[0].strip().split('.')[-1]
    is_retained = args[1].strip().lower() in ('true', '1')

    # children like: name(cols...), name2(cols...)
    child_specs = _tokenize_top_level(children_s)
    new_tables: List[str] = []
    column_lists: List[List[str]] = []
    for spec in child_specs:
        m2 = re.match(r"^([\w.]+)\((.*)\)$", spec.strip())
        if not m2:
            return ParsedOp(kind='VerticalSplit', raw=raw, missing=[f"子表语法错误: {spec}"])
        name = m2.group(1).split('.')[-1]
        cols = [c.strip() for c in _tokenize_top_level(m2.group(2)) if c.strip()]
        new_tables.append(name)
        column_lists.append(cols)

    # infer PK as intersection of all child column lists (per spec, each child contains all PK columns)
    pk_guess: List[str] = []
    if column_lists:
        inter = set(column_lists[0])
        for lst in column_lists[1:]:
            inter &= set(lst)
        pk_guess = list(inter)
    pk_dict = {t: pk_guess for t in new_tables}

    return ParsedOp(
        kind='VerticalSplit',
        raw=raw,
        params={
            'src_table': src_table,
            'is_retained': is_retained,
            'new_tables': new_tables,
            'column_lists': column_lists,
            'primary_keys': pk_dict,
        },
    )


def parse_horizontal_split(raw: str) -> ParsedOp:
    # HorizontalSplit(SourceTable[, is_retained]): t1(pred), t2(pred)
    m = re.match(r"^HorizontalSplit\((.*)\)\s*:\s*(.*)$", raw.strip())
    if not m:
        return ParsedOp(kind='HorizontalSplit', raw=raw, missing=["无法解析 HorizontalSplit 语法"])
    args_s = m.group(1)
    args = _tokenize_top_level(args_s)
    if len(args) == 0:
        return ParsedOp(kind='HorizontalSplit', raw=raw, missing=["缺少源表参数"])
    src_table = args[0].strip().split('.')[-1]
    is_retained = False
    if len(args) >= 2:
        is_retained = args[1].strip().lower() in ('true', '1', 'yes')
    parts = _tokenize_top_level(m.group(2))
    preds: List[Tuple[str, str]] = []
    for p in parts:
        m2 = re.match(r"^([\w.]+)\((.*)\)$", p.strip())
        if not m2:
            return ParsedOp(kind='HorizontalSplit', raw=raw, missing=[f"子表分片语法错误: {p}"])
        name = m2.group(1).split('.')[-1]
        pred = m2.group(2).strip()
        preds.append((name, pred))
    return ParsedOp(
        kind='HorizontalSplit',
        raw=raw,
        params={'table': src_table, 'predicates': preds, 'is_retained': is_retained},
    )


def parse_horizontal_merge(raw: str) -> ParsedOp:
    # HorizontalMerge(t1, t2, is_retained): NewTable
    m = re.match(r"^HorizontalMerge\((.*)\)\s*:\s*([\w.]+)\s*$", raw.strip())
    if not m:
        return ParsedOp(kind='HorizontalMerge', raw=raw, missing=["无法解析 HorizontalMerge 语法"])
    args = _tokenize_top_level(m.group(1))
    if len(args) != 3:
        return ParsedOp(kind='HorizontalMerge', raw=raw, missing=[f"参数数量应为3（t1,t2,is_retained），实际为{len(args)}"])
    t1 = args[0].strip().split('.')[-1]
    t2 = args[1].strip().split('.')[-1]
    new_table = m.group(2).strip().split('.')[-1]
    return ParsedOp(
        kind='HorizontalMerge',
        raw=raw,
        params={'sources': [t1, t2], 'new_table': new_table, 'is_retained': args[2].strip().lower() in ('true', '1', 'yes')},
    )


def parse_redundant_add(raw: str) -> ParsedOp:
    # RedundantColumnAdd(SourceTable.Col, TargetTable.NewCol[, JoinKeys])
    # JoinKeys（如提供）必须为等式字符串列表形式：
    #   ['Source.k1=Target.k1', 'Source.k2=Target.k2']
    m = re.match(r"^RedundantColumnAdd\((.*)\)\s*$", raw.strip())
    if not m:
        return ParsedOp(kind='RedundantColumnAdd', raw=raw, missing=["无法解析 RedundantColumnAdd 语法"])
    args = _tokenize_top_level(m.group(1))
    if len(args) not in (2, 3):
        return ParsedOp(kind='RedundantColumnAdd', raw=raw, missing=[f"参数数量应为2或3（源列, 目标表[.新列][, 连接键]），实际为{len(args)}"])
    src = args[0].strip()
    tgt = args[1].strip()
    if '.' not in src:
        return ParsedOp(kind='RedundantColumnAdd', raw=raw, missing=["参数格式应为 SourceTable.SourceColumn"])
    st, sc = src.split('.', 1)
    if '.' in tgt:
        tt, nc = tgt.split('.', 1)
    else:
        tt, nc = tgt, sc  # 默认新列名 = 源列名

    join_keys: Optional[List[Tuple[str, str]]] = None
    if len(args) == 3:
        jk_raw = args[2].strip()
        # 必须是 [ ... ]
        if not (jk_raw.startswith('[') and jk_raw.endswith(']')):
            return ParsedOp(
                kind='RedundantColumnAdd',
                raw=raw,
                params={'source_table': st, 'source_column': sc, 'target_table': tt, 'new_column': nc},
                missing=["join_keys 必须使用 ['Source.k=Target.k', ...] 形式（方括号+等式字符串列表）"],
            )
        inner = jk_raw[1:-1]
        tokens = [p.strip() for p in _tokenize_top_level(inner) if p.strip()]
        eq_pat = re.compile(r"^['\"]?([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)['\"]?$")
        pairs: List[Tuple[str, str]] = []
        bad_parts: List[str] = []
        for tok in tokens:
            m2 = eq_pat.match(tok)
            if not m2:
                bad_parts.append(tok)
                continue
            lt, lc, rt, rc = m2.group(1), m2.group(2), m2.group(3), m2.group(4)
            if lt == st and rt == tt:
                pairs.append((lc, rc))
            elif rt == st and lt == tt:
                pairs.append((rc, lc))
            else:
                bad_parts.append(tok)
        if bad_parts or not pairs:
            reason = f"非法项: {', '.join(bad_parts)}" if bad_parts else "未能解析到有效的键对"
            return ParsedOp(
                kind='RedundantColumnAdd',
                raw=raw,
                params={'source_table': st, 'source_column': sc, 'target_table': tt, 'new_column': nc},
                missing=[f"join_keys 必须使用 ['{st}.k={tt}.k', ...] 形式；{reason}"],
            )
        join_keys = pairs

    return ParsedOp(
        kind='RedundantColumnAdd',
        raw=raw,
        params={'source_table': st, 'source_column': sc, 'target_table': tt, 'new_column': nc, **({'join_keys': join_keys} if join_keys else {})},
        missing=([] if join_keys else ["join_keys 可省略（将尝试根据外键自动推断）；如提供，必须为 ['Source.k=Target.k', ...] 形式"]),
    )


def parse_redundant_drop(raw: str) -> ParsedOp:
    # RedundantColumnDrop(Table.Col)
    m = re.match(r"^RedundantColumnDrop\((.*)\)\s*$", raw.strip())
    if not m:
        return ParsedOp(kind='RedundantColumnDrop', raw=raw, missing=["无法解析 RedundantColumnDrop 语法"])
    arg = m.group(1).strip()
    if '.' not in arg:
        return ParsedOp(kind='RedundantColumnDrop', raw=raw, missing=["参数应为 Table.Column"])
    t, c = arg.split('.', 1)
    return ParsedOp(kind='RedundantColumnDrop', raw=raw, params={'table': t, 'column': c})


def parse_line_to_ops(line: str) -> List[ParsedOp]:
    # If the line is a single correction pair "orig - new", only parse the new.
    corr = _maybe_split_correction(line)
    if corr is not None:
        _, new_s = corr
        ops_raw = _split_operations(new_s)
    else:
        ops_raw = _split_operations(line)
    parsed: List[ParsedOp] = []
    for op in ops_raw:
        op = op.strip().rstrip(';')
        if not op:
            continue
        if op.startswith('TableJoin('):
            parsed.append(parse_table_join(op))
        elif op.startswith('VerticalSplit('):
            parsed.append(parse_vertical_split(op))
        elif op.startswith('HorizontalSplit('):
            parsed.append(parse_horizontal_split(op))
        elif op.startswith('HorizontalMerge('):
            parsed.append(parse_horizontal_merge(op))
        elif op.startswith('RedundantColumnAdd('):
            parsed.append(parse_redundant_add(op))
        elif op.startswith('RedundantColumnDrop('):
            parsed.append(parse_redundant_drop(op))
        elif op.startswith('ColumnSplit('):
            # Optional, not used in current response.txt; left unimplemented here
            parsed.append(ParsedOp(kind='ColumnSplit', raw=op, missing=["当前 runner 未实现 ColumnSplit 文本规则解析（需拆分规则）"]))
        else:
            parsed.append(ParsedOp(kind='Unknown', raw=op, missing=["未知操作类型"]))
    return parsed


# A minimal dry-run vertical split plan generator that outputs SQL script text
class VerticalSplitDryOp:
    def __init__(self, old_table: str, new_tables: List[str], column_lists: List[List[str]], primary_keys: dict[str, List[str]] | None = None):
        self.old_table = old_table
        self.new_tables = new_tables
        self.column_lists = column_lists
        self.primary_keys = primary_keys or {}

    def apply_to_schema(self, db=None) -> str:
        stmts: List[str] = []
        oldq = f"`{self.old_table}`"
        # 合并重复的新表项（LLM 输出中可能附带同名“主键提示”项）
        order: List[str] = []
        col_union: dict[str, List[str]] = {}
        seen = set()
        for i, newt in enumerate(self.new_tables):
            if newt not in seen:
                seen.add(newt)
                order.append(newt)
                col_union[newt] = []
            cols = self.column_lists[i] if i < len(self.column_lists) else []
            # 以出现顺序去重合并列
            for c in cols:
                if c not in col_union[newt]:
                    col_union[newt].append(c)
        # 生成去重后的建表/加主键语句；先删除再重建，保证幂等覆盖
        for newt in order:
            cols = col_union.get(newt) or []
            cols_sql = ", ".join(cols) if cols else "*"
            stmts.append(f"DROP TABLE IF EXISTS `{newt}`;")
            stmts.append(f"CREATE TABLE `{newt}` AS SELECT DISTINCT {cols_sql} FROM {oldq};")
            pks = self.primary_keys.get(newt) or []
            if pks:
                pk_sql = ", ".join(pks)
                stmts.append(f"ALTER TABLE `{newt}` ADD PRIMARY KEY ({pk_sql});")
        return "\n".join(stmts)


def _auto_fill_with_db(db, op: ParsedOp) -> None:
    """Use DB metadata to fill missing fields where feasible."""
    if not db:
        return
    k = op.kind
    p = op.params
    if k == 'TableJoin':
        t1, t2 = p['table1'], p['table2']
        # columns
        try:
            cols = get_tables_columns(db, [t1, t2])
            p['old_columns_list'] = [cols.get(t1, []), cols.get(t2, [])]
            # remove missing marker if present
            op.missing = [m for m in op.missing if 'old_columns_list' not in m]
        except Exception:
            pass
        # join key: prefer 1-col key for TableMerge; if multi-col, we still can pass join_key=None
        jpairs: List[Tuple[str, str]] = p.get('join_pairs') or []
        if len(jpairs) == 1 and jpairs[0][0] == jpairs[0][1]:
            p['join_key'] = jpairs[0][0]
        elif len(jpairs) == 1:
            # join_key exists but different names — TableMerge can still use join_key name as the common one
            p['join_key'] = jpairs[0][0]
        else:
            # multiple keys: TableMerge will fallback to full-outer-join simulation with ON 1=1; we warn by keeping params
            p['join_key'] = None
    elif k == 'VerticalSplit':
        # Prefer the original table's primary key for all child tables to avoid
        # accidentally constructing PKs that include nullable business columns.
        try:
            from utils.schema_introspect import get_primary_key_columns  # local import
        except Exception:
            get_primary_key_columns = None  # type: ignore
        if get_primary_key_columns is not None:
            try:
                orig_pk = get_primary_key_columns(db, p['src_table'])
            except Exception:
                orig_pk = []
            if orig_pk:
                p['primary_keys'] = {t: list(orig_pk) for t in p.get('new_tables', [])}
    elif k == 'RedundantColumnAdd':
        st, tt = p['source_table'], p['target_table']
        try:
            # target typically references source: find FK from target to source
            fks = find_fk_between(db, tt, st)
            if fks:
                # Map to (source_key, target_key)
                p['join_keys'] = [(src, tgt) for (tgt, src) in fks]
                op.missing = [m for m in op.missing if 'join_keys' not in m]
            else:
                # Heuristic fallback: if target has columns matching source PK names,
                # use (source_pk, target_same_name) pairs as join_keys.
                try:
                    from utils.schema_introspect import get_primary_key_columns, get_table_columns  # local import
                except Exception:
                    get_primary_key_columns = None  # type: ignore
                    get_table_columns = None  # type: ignore
                if get_primary_key_columns and get_table_columns:
                    try:
                        spk = get_primary_key_columns(db, st) or []
                        tcols = set(get_table_columns(db, tt) or [])
                    except Exception:
                        spk, tcols = [], set()
                    pairs = [(c, c) for c in spk if c in tcols]
                    if pairs:
                        p['join_keys'] = pairs
                        op.missing = [m for m in op.missing if 'join_keys' not in m]
        except Exception:
            pass


def _auto_fill_with_stats(stats: Optional[StatsCatalog], op: ParsedOp) -> None:
    if not stats:
        return
    k = op.kind
    p = op.params
    if k == 'TableJoin':
        t1, t2 = p['table1'], p['table2']
        try:
            cols1 = stats.table_columns.get(t1) or stats.table_columns.get(str(t1).lower()) or []
            cols2 = stats.table_columns.get(t2) or stats.table_columns.get(str(t2).lower()) or []
            if cols1 or cols2:
                p['old_columns_list'] = [cols1, cols2]
                op.missing = [m for m in op.missing if 'old_columns_list' not in m]
        except Exception:
            pass
        jpairs: List[Tuple[str, str]] = p.get('join_pairs') or []
        if len(jpairs) == 1:
            p['join_key'] = jpairs[0][0]
        elif len(jpairs) == 0:
            p['join_key'] = None
    elif k == 'VerticalSplit':
        # Use primary key from meta if available
        try:
            if stats.meta:
                pk = (stats.meta.get('tables') or {}).get(p['src_table'], {}).get('primary_key') or []
                if pk:
                    p['primary_keys'] = {t: list(pk) for t in p.get('new_tables', [])}
        except Exception:
            pass
    elif k == 'RedundantColumnAdd':
        st, tt = p['source_table'], p['target_table']
        try:
            fks = _find_fk_between_stats(stats, tt, st)
            if fks:
                # Map to (source_key, target_key)
                p['join_keys'] = [(src, tgt) for (tgt, src) in fks]
                op.missing = [m for m in op.missing if 'join_keys' not in m]
        except Exception:
            pass


def _validate_with_db(db, op: ParsedOp) -> List[str]:
    if not db:
        return []
    k = op.kind
    p = op.params
    if k == 'TableJoin':
        return check_table_join(db, p['table1'], p['table2'], p.get('join_pairs') or [])
    if k == 'VerticalSplit':
        return check_vertical_split(db, p['src_table'], p['new_tables'], p['column_lists'])
    if k == 'HorizontalSplit':
        return check_horizontal_split(db, p['table'], p['predicates'])
    if k == 'HorizontalMerge':
        return check_horizontal_merge(db, p['sources'])
    if k == 'RedundantColumnAdd':
        return check_redundant_add(db, p['source_table'], p['source_column'], p['target_table'])
    if k == 'RedundantColumnDrop':
        return check_redundant_drop(db, p['table'], p['column'])
    return []


def _validate_with_stats(stats: Optional[StatsCatalog], op: ParsedOp) -> List[str]:
    if not stats:
        return []
    issues: List[str] = []
    k = op.kind
    p = op.params

    def _has_table(t: str) -> bool:
        return t in stats.table_columns or str(t).lower() in {x.lower() for x in stats.table_columns.keys()}

    def _cols_of(t: str) -> List[str]:
        if t in stats.table_columns:
            return stats.table_columns.get(t) or []
        # lower-case match
        tl = str(t).lower()
        for k2, v2 in stats.table_columns.items():
            if str(k2).lower() == tl:
                return v2 or []
        return []

    if k == 'TableJoin':
        t1, t2 = p['table1'], p['table2']
        if not _has_table(t1):
            issues.append(f"表不存在: {t1}")
        if not _has_table(t2):
            issues.append(f"表不存在: {t2}")
        pairs = p.get('join_pairs') or []
        if pairs:
            c1 = set(_cols_of(t1))
            c2 = set(_cols_of(t2))
            for a, b in pairs:
                if a not in c1:
                    issues.append(f"连接键不存在: {t1}.{a}")
                if b not in c2:
                    issues.append(f"连接键不存在: {t2}.{b}")
        return issues

    if k == 'VerticalSplit':
        t = p['src_table']
        if not _has_table(t):
            issues.append(f"表不存在: {t}")
            return issues
        src_cols = set(_cols_of(t))
        for i, cols in enumerate(p.get('column_lists') or []):
            for c in cols:
                if c not in src_cols:
                    nt = (p.get('new_tables') or [])[i] if i < len(p.get('new_tables') or []) else '?'
                    issues.append(f"子表列不存在于源表: {nt}.{c}")
        return issues

    if k == 'HorizontalSplit':
        t = p['table']
        if not _has_table(t):
            issues.append(f"表不存在: {t}")
        return issues

    if k == 'HorizontalMerge':
        for t in p.get('sources') or []:
            if not _has_table(t):
                issues.append(f"表不存在: {t}")
        return issues

    if k == 'RedundantColumnAdd':
        st, sc = p['source_table'], p['source_column']
        tt = p['target_table']
        if not _has_table(st):
            issues.append(f"表不存在: {st}")
        if not _has_table(tt):
            issues.append(f"表不存在: {tt}")
        if sc and sc not in set(_cols_of(st)):
            issues.append(f"列不存在: {st}.{sc}")
        join_keys = p.get('join_keys') or []
        if join_keys:
            tcols = set(_cols_of(tt))
            for (skey, tkey) in join_keys:
                if skey not in set(_cols_of(st)):
                    issues.append(f"连接键不存在: {st}.{skey}")
                if tkey not in tcols:
                    issues.append(f"连接键不存在: {tt}.{tkey}")
            if stats.foreign_keys:
                if not _fk_matches_join_keys(stats, st, tt, join_keys):
                    issues.append("冗余列连接键未匹配任何外键关系")
        return issues

    if k == 'RedundantColumnDrop':
        t, c = p['table'], p['column']
        if not _has_table(t):
            issues.append(f"表不存在: {t}")
        elif c not in set(_cols_of(t)):
            issues.append(f"列不存在: {t}.{c}")
        return issues

    return issues


# --- Sequential validation (dependency-aware) ---
class _VirtualCatalog:
    """Minimal in-memory catalog to simulate schema changes across ops, with provenance.

    - `tables`: current live tables → set(columns)
    - `meta`: per-table provenance, e.g. who created/dropped it
    """
    def __init__(self, seed: Optional[dict] = None) -> None:
        self.tables: dict[str, set[str]] = {}
        # meta structure:
        # { table: { 'created_by': (op_idx, kind), 'dropped_by': Optional[(op_idx, kind, reason)], 'last_op': (op_idx, kind) } }
        self.meta: dict[str, dict] = {}
        if seed:
            for t, cols in seed.items():
                key = str(t).lower()
                self.tables[key] = set(c.lower() for c in (cols or []))
                self.meta.setdefault(key, {})

    # ---------- queries ----------
    def has_table(self, t: str) -> bool:
        return str(t).lower() in self.tables

    def get_cols(self, t: str) -> set[str]:
        return set(self.tables.get(str(t).lower(), set()))

    def get_meta(self, t: str) -> dict:
        return self.meta.get(str(t).lower(), {})

    # ---------- mutations ----------
    def create_like(self, new_t: str, src_t: str, *, by_idx: int, by_kind: str) -> None:
        new_key = str(new_t).lower()
        self.tables[new_key] = set(self.get_cols(src_t))
        m = self.meta.setdefault(new_key, {})
        m['created_by'] = (by_idx, by_kind)
        m['last_op'] = (by_idx, by_kind)
        m.pop('dropped_by', None)

    def create_with(self, new_t: str, cols: list[str], *, by_idx: int, by_kind: str) -> None:
        new_key = str(new_t).lower()
        self.tables[new_key] = set(c.lower() for c in cols)
        m = self.meta.setdefault(new_key, {})
        m['created_by'] = (by_idx, by_kind)
        m['last_op'] = (by_idx, by_kind)
        m.pop('dropped_by', None)

    def add_column(self, t: str, col: str, *, by_idx: int, by_kind: str) -> None:
        key = str(t).lower()
        self.tables.setdefault(key, set()).add(str(col).lower())
        m = self.meta.setdefault(key, {})
        m['last_op'] = (by_idx, by_kind)

    def drop_table(self, t: str, *, by_idx: int, by_kind: str, reason: str = '') -> None:
        key = str(t).lower()
        if key in self.tables:
            self.tables.pop(key, None)
        m = self.meta.setdefault(key, {})
        m['dropped_by'] = (by_idx, by_kind, reason)
        m['last_op'] = (by_idx, by_kind)


def _sequential_validate(db, ops: List[ParsedOp], schema_seed: Optional[Dict[str, List[str]]] = None) -> List[str]:
    """Validate ops in order using a virtual catalog that mutates as we go.
    Falls back to DB introspection for initial seed only.
    """
    issues: List[str] = []
    try:
        from utils.schema_introspect import get_tables_columns  # type: ignore
    except Exception:
        get_tables_columns = None  # type: ignore

    # 以本轮操作引用到的表为种子，从 DB 拉取列清单
    seed = {}
    if db is not None and get_tables_columns is not None:
        try:
            table_set: set[str] = set()
            for op in ops:
                k, p = op.kind, op.params
                if not p:
                    continue
                if k == 'VerticalSplit':
                    table_set.add(str(p.get('src_table') or ''))
                elif k == 'HorizontalSplit':
                    table_set.add(str(p.get('table') or ''))
                elif k == 'HorizontalMerge':
                    for t in (p.get('sources') or []):
                        table_set.add(str(t))
                elif k == 'RedundantColumnAdd':
                    table_set.add(str(p.get('source_table') or ''))
                    table_set.add(str(p.get('target_table') or ''))
                elif k == 'RedundantColumnDrop':
                    table_set.add(str(p.get('table') or ''))
                elif k == 'TableJoin':
                    table_set.add(str(p.get('table1') or ''))
                    table_set.add(str(p.get('table2') or ''))
            table_list = sorted(t for t in table_set if t)
            seed = get_tables_columns(db, table_list) if table_list else {}
        except Exception:
            seed = {}
    # 合并文件 schema 种子
    if schema_seed:
        for t, cols in schema_seed.items():
            key = str(t).lower()
            cur = set(c.lower() for c in (seed.get(t) or []))
            cur.update(c.lower() for c in (cols or []))
            seed[key] = sorted(list(cur))
    cat = _VirtualCatalog(seed)

    def _hint_dropped_by(t: str) -> str:
        meta = cat.get_meta(t)
        if 'dropped_by' in meta:
            idx, kind, reason = meta['dropped_by']
            rs = f"（上游 OP #{idx} {kind}{' '+reason if reason else ''} 删除了该表）"
            return rs
        return ''

    def ensure_table(t: str, ctx: str) -> bool:
        if not cat.has_table(t):
            import difflib
            hint = _hint_dropped_by(t)
            # suggest similar existing tables
            existing = sorted(list(cat.tables.keys()))
            cand = difflib.get_close_matches(str(t).lower(), existing, n=3, cutoff=0.6)
            sug = f"；可能想用: {', '.join(cand)}" if cand else ''
            extra = ((' ' + hint) if hint else '') + (f"（现有表：{', '.join(existing[:10])}{' …' if len(existing)>10 else ''}{sug}）" if existing else '')
            issues.append(f"{ctx}: 表不存在: {t}{extra}")
            return False
        return True

    def ensure_cols(t: str, cols: list[str], ctx: str) -> bool:
        """Ensure columns exist; on failure, emit actionable hints.

        Hints include:
        - table provenance (which OP created/dropped it)
        - top-3 similar column name suggestions
        - quick reminder when the table was produced by a split/join/merge
        """
        import difflib
        ok = True
        have = cat.get_cols(t)
        meta = cat.get_meta(t)
        have_sorted = sorted(list(have))
        prov = []
        if 'created_by' in meta:
            prov.append(f"由 OP #{meta['created_by'][0]} {meta['created_by'][1]} 生成")
        if 'dropped_by' in meta:
            di = meta['dropped_by']
            prov.append(f"曾被 OP #{di[0]} {di[1]} 删除{(' '+di[2]) if len(di) >= 3 and di[2] else ''}")
        prov_s = ("；".join(prov)) if prov else ''
        for c in cols:
            # 宽容：支持 'table.column' 形式，取末段进行比对
            if isinstance(c, str) and '.' in c:
                c = c.split('.')[-1]
            if c.lower() not in have:
                # similar names
                cand = difflib.get_close_matches(c.lower(), have_sorted, n=3, cutoff=0.6)
                sug = (f"；可能想用: {', '.join(cand)}" if cand else '')
                hint = (f"（{prov_s}；现有列：{', '.join(have_sorted[:10])}{' …' if len(have_sorted)>10 else ''}{sug}）" if prov_s or have_sorted else '')
                issues.append(f"{ctx}: 列不存在: {t}.{c}{hint}")
                ok = False
        return ok

    # ----- Helpers: VerticalSplit PK suggestion/validation -----
    def _get_pk_from_db(table: str) -> List[str]:
        try:
            from utils.schema_introspect import get_primary_key_columns  # type: ignore
        except Exception:
            return []
        try:
            return list(get_primary_key_columns(db, table) or []) if db is not None else []
        except Exception:
            return []

    def _is_unique_pk_on_projection(src_table: str, proj_cols: List[str], pk_cols: List[str]) -> Optional[bool]:
        if db is None or not proj_cols or not pk_cols:
            return None
        pk_expr = "CONCAT_WS('#', " + ", ".join([f"`{c}`" for c in pk_cols]) + ")"
        proj_list = ", ".join([f"`{c}`" for c in proj_cols])
        sql = (
            "SELECT COUNT(*) AS total, COUNT(DISTINCT " + pk_expr + ") AS uniq FROM ("
            f"SELECT DISTINCT {proj_list} FROM `{src_table}`"
            ") t"
        )
        try:
            rows = db.execute_query(sql)  # type: ignore[attr-defined]
            if rows:
                r = rows[0]
                # rows may be dict or tuple depending on driver
                if isinstance(r, dict):
                    total = int(r.get('total') or list(r.values())[0])
                    uniq = int(r.get('uniq') or list(r.values())[1])
                else:
                    total = int(r[0]); uniq = int(r[1])
                return total == uniq
        except Exception:
            return None
        return None

    def _auto_pick_composite(child_cols: List[str]) -> List[str]:
        lc = [c.lower() for c in child_cols]
        if 'regular_order_id' in lc and 'product_id' in lc:
            return ['regular_order_id', 'product_id']
        ids = [c for c in child_cols if c.lower().endswith('_id')]
        return ids[:2] if len(ids) >= 2 else []

    def _validate_vs_pk(src: str, child: str, child_cols: List[str], pk: List[str]) -> Tuple[List[str], str]:
        note = ""
        # prefer source PK when available
        src_pk = _get_pk_from_db(src)
        if (not pk) and src_pk and set(src_pk).issubset(set(child_cols)):
            pk = list(src_pk)
            note = f"采用源表主键{pk}。"
        # pk must be subset
        if pk and not set(pk).issubset(set(child_cols)):
            note = f"提供主键{pk}不在子表列中，已移除。"
            pk = []
        # check uniqueness if possible
        uniq = _is_unique_pk_on_projection(src, child_cols, pk) if pk else None
        if uniq is False:
            cand = _auto_pick_composite(child_cols)
            if cand and cand != pk and _is_unique_pk_on_projection(src, child_cols, cand) is True:
                note = f"主键{pk}在投影上非唯一，自动更正为复合主键{cand}。"
                return cand, note
            note = f"主键{pk}在投影上非唯一，移除主键；建议改为复合主键{_auto_pick_composite(child_cols) or ['<确认>']}。"
            return [], note
        if db is None and pk and len(pk) == 1 and pk != src_pk:
            # offline heuristic warning
            cand = _auto_pick_composite(child_cols)
            if cand:
                note = f"离线校验：单列主键{pk}可能不唯一，建议使用{cand}。"
        return pk, note

    for idx, op in enumerate(ops, 1):
        k, p = op.kind, op.params
        ctx = f"OP #{idx} {k}"
        if k == 'VerticalSplit':
            src = p['src_table']
            if not ensure_table(src, ctx):
                break
            # 去重并合并同名子表的列（LLM 常按规范给出：t1(cols), t2(cols), t1(pk...), t2(pk...)
            # 之前的实现会按顺序覆盖，导致只保留 pk 列，进而影响后续 RCA 校验。
            raw_names: List[str] = p.get('new_tables', [])
            raw_cols: List[List[str]] = p.get('column_lists', [])
            grouped: Dict[str, set] = {}
            for i, t in enumerate(raw_names):
                cols = raw_cols[i] if i < len(raw_cols) else []
                key = str(t).lower()
                if key not in grouped:
                    grouped[key] = set()
                # 合并列集合；若本条为空，则后续会用 src 的列集兜底
                grouped[key].update(c.lower() for c in cols)

            # 校验：所有声明列都应来自源表
            for tkey, cols in grouped.items():
                if cols:
                    if not ensure_cols(src, sorted(list(cols)), ctx):
                        break

            # 记录新表：对每个去重后的子表，使用合并后的列集；若为空则用源表列集兜底
            pk_dict: Dict[str, List[str]] = p.get('primary_keys', {}) or {}
            for t in sorted(grouped.keys()):
                cols = sorted(list(grouped[t]))
                eff_cols = cols if cols else list(cat.get_cols(src))
                cur_pk = list(pk_dict.get(t) or [])
                new_pk, note = _validate_vs_pk(src, t, eff_cols, cur_pk)
                if note:
                    print(f"[VS-PK] {t}: {note}")
                if new_pk != cur_pk:
                    pk_dict[t] = new_pk
                    p['primary_keys'] = pk_dict
                cat.create_with(t, eff_cols, by_idx=idx, by_kind=k)

            # 若不保留原表，删除源表
            if not bool(p.get('is_retained', False)):
                cat.drop_table(src, by_idx=idx, by_kind=k, reason='(is_retained=False)')

        elif k == 'HorizontalSplit':
            src = p['table']
            if not ensure_table(src, ctx):
                break
            # 新表结构默认与源表一致
            for name, _pred in p.get('predicates', []):
                cat.create_like(name, src, by_idx=idx, by_kind=k)
            if not bool(p.get('is_retained', False)):
                cat.drop_table(src, by_idx=idx, by_kind=k, reason='(is_retained=False)')

        elif k == 'HorizontalMerge':
            srcs: List[str] = p.get('sources', [])
            if not all(ensure_table(t, ctx) for t in srcs):
                break
            newt = p.get('new_table')
            if newt:
                # 取第一个源表结构（快速近似）
                cat.create_like(newt, srcs[0], by_idx=idx, by_kind=k)
            if not bool(p.get('is_retained', False)):
                for t in srcs:
                    cat.drop_table(t, by_idx=idx, by_kind=k, reason='(is_retained=False)')

        elif k == 'RedundantColumnAdd':
            st, sc = p['source_table'], p['source_column']
            tt, nc = p['target_table'], p['new_column']
            if ensure_table(st, ctx) and ensure_table(tt, ctx) and ensure_cols(st, [sc], ctx):
                if nc:
                    cat.add_column(tt, nc, by_idx=idx, by_kind=k)
            else:
                break

        elif k == 'RedundantColumnDrop':
            t, c = p['table'], p['column']
            if ensure_table(t, ctx) and ensure_cols(t, [c], ctx):
                # 删除列对后续影响较小，这里不真的移除，避免误报
                pass
            else:
                break

        elif k == 'TableJoin':
            t1, t2 = p['table1'], p['table2']
            if not (ensure_table(t1, ctx) and ensure_table(t2, ctx)):
                break
            pairs: List[Tuple[str, str]] = p.get('join_pairs') or []
            # 校验键列存在
            if pairs and (not ensure_cols(t1, [a for a, _ in pairs], ctx) or not ensure_cols(t2, [b for _, b in pairs], ctx)):
                break
            newt = p.get('new_table')
            if newt:
                # 新表列 = t1∪t2（近似）
                cols1 = cat.get_cols(t1)
                cols2 = cat.get_cols(t2)
                cols = list(cols1 | cols2)
                cat.create_with(newt, cols, by_idx=idx, by_kind=k)
                # 为后续 plan 阶段提供兜底列清单（old_columns_list），避免 _SEQCAT_TABLE_COLS 缺失时无法 CTAS
                try:
                    op.params.setdefault('old_columns_list', [sorted(list(cols1)), sorted(list(cols2))])  # type: ignore[attr-defined]
                except Exception:
                    pass
            # 若不保留源表，删除 t1/t2
            if not bool(p.get('is_retained', False)):
                cat.drop_table(t1, by_idx=idx, by_kind=k, reason='(is_retained=False)')
                cat.drop_table(t2, by_idx=idx, by_kind=k, reason='(is_retained=False)')

        else:
            # 未知操作：跳过顺序校验
            pass

    # 将当前虚拟目录中的表列快照导出，供后续规划阶段优先使用
    try:
        global _SEQCAT_TABLE_COLS
        _SEQCAT_TABLE_COLS = {str(t).lower(): sorted(list(cols)) for t, cols in cat.tables.items()}
    except Exception:
        # 快照失败不应阻断流程
        pass
    return issues


def _split_script_to_stmts(script: str) -> List[str]:
    """Split a SQL script by top-level semicolons, preserving multi-line CTAS.
    - Treat newlines as whitespace to keep tokens separated (avoids "ASSELECT").
    - Skip comment-only lines starting with "--".
    - Guarantee each returned statement ends with ';'.
    """
    stmts: List[str] = []
    if not script:
        return stmts
    # Remove pure comment lines but keep line breaks elsewhere
    lines = []
    for ln in script.splitlines(True):  # keepends=True
        s = ln.strip()
        if not s or s.startswith('--'):
            continue
        # Normalize: ensure SET statements end with ';' to avoid merging with next stmt
        if s.upper().startswith('SET ') and not s.endswith(';'):
            ln = ln.rstrip('\r\n') + ';\n'
        lines.append(ln)
    text = ''.join(lines)
    buf: List[str] = []
    in_s = in_d = in_bq = False
    esc = False
    def flush():
        s = ''.join(buf).strip()
        if s:
            if not s.endswith(';'):
                s += ';'
            stmts.append(s)
    for ch in text:
        # normalize newlines to single space to separate tokens
        if ch == '\n' or ch == '\r':
            ch = ' '
        buf.append(ch)
        if esc:
            esc = False
            continue
        if ch == '\\':
            esc = True
            continue
        if ch == "'" and not in_d and not in_bq:
            in_s = not in_s
            continue
        if ch == '"' and not in_s and not in_bq:
            in_d = not in_d
            continue
        if ch == '`' and not in_s and not in_d:
            in_bq = not in_bq
            continue
        if ch == ';' and not in_s and not in_d and not in_bq:
            flush()
            buf = []
    # leftover
    rest = ''.join(buf).strip()
    if rest:
        if not rest.endswith(';'):
            rest += ';'
        stmts.append(rest)
    return stmts


def plan_statements(db, op: ParsedOp) -> List[str]:
    """Plan SQL statements for this op without executing them."""
    k = op.kind
    p = op.params
    # try to fill with stats bundle first, then DB
    _auto_fill_with_stats(_STATS_CATALOG, op)
    _auto_fill_with_db(db, op)
    if op.missing:
        return []

    if k == 'VerticalSplit':
        tbl = p['src_table']
        new_tables: List[str] = p['new_tables']
        col_lists: List[List[str]] = p['column_lists']
        pk_dict: dict = p['primary_keys']
        # Use dry variant to get script regardless of DB
        vs = VerticalSplitDryOp(tbl, new_tables, col_lists, pk_dict)
        script = vs.apply_to_schema(db=None)
        return _split_script_to_stmts(script)

    if k == 'HorizontalSplit':
        hs = HorizontalSplit(p['table'], p['predicates'], is_retained=bool(p.get('is_retained', False)))
        script = hs.apply_to_schema(db=None)
        return _split_script_to_stmts(script)

    if k == 'HorizontalMerge':
        hm = HorizontalMerge(p['sources'], p['new_table'], is_retained=bool(p.get('is_retained', False)))
        script = hm.apply_to_schema(db=None)
        return _split_script_to_stmts(script)

    if k == 'TableJoin':
        # 仅用于规划阶段（dry-run 或事务预演）：生成一个可执行的 CTAS 脚本。
        # - 当 is_retained=True：创建物化视图表 new_table = t1 JOIN t2 ON join_pairs；
        # - 当 is_retained=False：为避免在无 DB 元信息下模拟全外连接的复杂性，这里同样使用 JOIN 的简化形式。
        t1 = p['table1']; t2 = p['table2']; newt = p['new_table']
        pairs = p.get('join_pairs') or []
        # 列清单：优先使用顺序校验阶段注入的 old_columns_list；否则从 _SEQCAT_TABLE_COLS 获取兜底；再退化为 "t1.* , t2.*"
        old_lists = p.get('old_columns_list') or []
        def cols_of(tbl: str):
            return (list(_SEQCAT_TABLE_COLS.get(str(tbl).lower()) or []))
        t1_cols = (old_lists[0] if len(old_lists) >= 1 else cols_of(t1)) or []
        t2_cols = (old_lists[1] if len(old_lists) >= 2 else cols_of(t2)) or []
        # SELECT 列：处理重复名简单加后缀 _2
        seen = set()
        sel = []
        for c in t1_cols or []:
            sel.append(f"t1.`{c}` AS `{c}`"); seen.add(c)
        for c in t2_cols or []:
            nc = c if c not in seen else f"{c}_2"
            sel.append(f"t2.`{c}` AS `{nc}`")
        if not sel:
            sel = ["t1.*", "t2.*"]
        # 连接条件：若无 join_pairs，则退化为 1=1（笛卡尔积）
        if pairs:
            on = " AND ".join([f"t1.`{a}` = t2.`{b}`" for (a,b) in pairs])
        else:
            on = "1=1"
        stmts = [
            f"DROP TABLE IF EXISTS `{newt}`;",
            (
                f"CREATE TABLE `{newt}` AS\n"
                f"SELECT {', '.join(sel)}\n"
                f"FROM `{t1}` t1 JOIN `{t2}` t2 ON {on};"
            )
        ]
        return _split_script_to_stmts("\n".join(stmts))

    if k == 'RedundantColumnAdd':
        join_keys: List[Tuple[str, str]] = p.get('join_keys') or []
        if not join_keys:
            return []
        r = RedundantColumnAdd(
            source_table=p['source_table'],
            source_column=p['source_column'],
            target_table=p['target_table'],
            new_column=p['new_column'],
            join_keys=join_keys,
        )
        # 如果无法连接 DB，则使用 stats 校验结果作为放行依据
        try:
            if _STATS_CATALOG and _fk_matches_join_keys(_STATS_CATALOG, p['source_table'], p['target_table'], join_keys):
                setattr(r, "_fk_ok", True)
        except Exception:
            pass
        script = r.apply_to_schema(db=None)
        return _split_script_to_stmts(script)

    if k == 'RedundantColumnDrop':
        # Constructor signature: RedundantColumnDrop(target_table, redundant_column, source_table=None, source_column=None, join_keys=None)
        # But we only have table+column; instantiate with those.
        try:
            r = RedundantColumnDrop(target_table=p['table'], redundant_column=p['column'])  # type: ignore[arg-type]
            script = r.apply_to_schema(db=None)
            return _split_script_to_stmts(script)
        except TypeError:
            return []

    if k == 'TableJoin':
        # Build CTAS JOIN; 优先使用顺序校验生成的列快照，缺失时再退回到 DB/参数
        raw_pairs: List[Tuple[str, str]] = p.get('join_pairs') or []
        # 兜底：生成 SQL 前再做一遍列名归一化，避免生成 t1.`table.col`
        def _base(c: str) -> str:
            cs = (c or '').strip('`')
            return cs.split('.')[-1]
        pairs: List[Tuple[str, str]] = [(_base(a), _base(b)) for (a, b) in raw_pairs]
        if not pairs:
            return []
        t1, t2 = p['table1'], p['table2']
        newt = p['new_table']

        def _cols_from_seqcat(t: str) -> Optional[List[str]]:
            try:
                return list(_SEQCAT_TABLE_COLS.get(str(t).lower()) or [])
            except Exception:
                return None

        c1 = _cols_from_seqcat(t1)
        c2 = _cols_from_seqcat(t2)

        if not c1 or not c2:
            # 参数中是否已有（例如 _auto_fill_with_db 提供的）
            oc = p.get('old_columns_list')
            if oc and len(oc) == 2:
                c1 = c1 or oc[0]
                c2 = c2 or oc[1]

        if (not c1 or not c2) and db is not None:
            try:
                cols_fallback = get_tables_columns(db, [t1, t2])
                c1 = c1 or cols_fallback.get(t1, [])
                c2 = c2 or cols_fallback.get(t2, [])
            except Exception:
                pass

        if not c1 or not c2:
            return []

        on_clause = ' AND '.join([f"t1.`{a}` = t2.`{b}`" for a, b in pairs])
        out_cols = []
        used_names = set()
        for c in c1:
            out_cols.append(f"t1.`{c}` AS `{c}`")
            used_names.add(c)
        for c in c2:
            name = c if c not in used_names else f"{c}_2"
            out_cols.append(f"t2.`{c}` AS `{name}`")
            used_names.add(name)
        select_list = ', '.join(out_cols)
        stmts = [
            'SET FOREIGN_KEY_CHECKS=0',
            # 目标表已存在则先删除（便于重试）
            f"DROP TABLE IF EXISTS `{newt}`;",
            f"CREATE TABLE `{newt}` AS SELECT {select_list} FROM `{t1}` t1 JOIN `{t2}` t2 ON {on_clause};",
            'SET FOREIGN_KEY_CHECKS=1',
        ]
        return stmts

    return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--use-db', action='store_true', help='Connect to MySQL and auto-fill missing params')
    ap.add_argument('--host', default='localhost')
    ap.add_argument('--port', type=int, default=3306)
    ap.add_argument('--user', default='root')
    ap.add_argument('--password', default='')
    ap.add_argument('--database', default='')
    ap.add_argument('--sql-dir', help='Directory of input SQL files to rewrite (optional)')
    ap.add_argument('--schema-sql', help='Path to initial schema.sql for validation seed (optional)')
    ap.add_argument('--stats-dir', help='prepare.py 输出目录（用于统一统计输入/避免 DB 查询）')
    ap.add_argument('--out-sql-dir', help='Directory to write rewritten SQL files (optional)')
    # 性能估算：对一批 EXPLAIN 计划按步骤累计评估，反馈给 LLM 调整操作序列
    ap.add_argument('--eval-perf', action='store_true', default=False,
                    help='After parsing ops, run per-step performance estimation on plan dir')
    ap.add_argument('--eval-explain-dir', default='part2_debug/explain',
                    help='Directory containing EXPLAIN texts (qN.txt)')
    ap.add_argument('--eval-meta', default='output_dir/meta.json',
                    help='Path to meta.json (row counts / avg column bytes)')
    ap.add_argument('--eval-samples', default='response/samples/samples.json',
                    help='Path to samples.json (JOIN/UNION samples)')
    ap.add_argument('--eval-out-json', default='response/auto_eval_runner.json')
    ap.add_argument('--eval-out-md', default='response/auto_eval_runner.md')
    # Storage estimation options (optional)
    ap.add_argument('--storage-meta', help='Path to storage meta JSON for budget checks (optional)')
    ap.add_argument('--storage-budget', help='Max allowed total storage, e.g. 10GB/500MB/100000000')
    ap.add_argument('--apply-schema', action='store_true', help='Execute schema changes on DB; if omitted, only plan (dry-run)')
    ap.add_argument('--validate-sequential', dest='validate_seq', action='store_true', default=True,
                    help='Validate ops in order using a virtual catalog (default: on)')
    ap.add_argument('--no-validate-sequential', dest='validate_seq', action='store_false',
                    help='Disable dependency-aware validation and use per-op DB checks only')
    args = ap.parse_args()
    # Backward-compatible default: if --use-db is given but --apply-schema not specified,
    # perform schema apply (previous runner behavior). Users can omit --use-db to force dry-run.
    if args.use_db and '--apply-schema' not in sys.argv:
        setattr(args, 'apply_schema', True)

    db = None
    if args.use_db:
        try:
            from DataBase.MySQLDriver import MySQLDriver  # lazy import to avoid mysql dep in dry-run
            cfg = {
                'host': args.host,
                'port': args.port,
                'user': args.user,
                'password': args.password,
                'database': args.database or None,
            }
            db = MySQLDriver(cfg)
            ok = db.connect()
            if not ok:
                print('[runner] 数据库连接失败，回退为 dry-run 模式。')
                db = None
        except Exception as e:
            print(f"[runner] 连接数据库出错，回退 dry-run: {e}")
            db = None
    here = os.path.dirname(__file__)
    resp_path = os.path.join(here, 'response.txt')
    if not os.path.exists(resp_path):
        print(f"未找到响应文件: {resp_path}")
        sys.exit(1)
    text = open(resp_path, 'r', encoding='utf-8').read()
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.strip().startswith('#')]

    all_parsed: List[ParsedOp] = []
    corrections: List[Tuple[str, str]] = []
    for ln in lines:
        c = _maybe_split_correction(ln)
        if c is not None:
            corrections.append(c)
        all_parsed.extend(parse_line_to_ops(ln))

    print("=== 解析结果（按顺序） ===")
    if corrections:
        print("(已识别修正：原操作 → 新操作)")
        for orig, new in corrections:
            print(f"- {orig}  →  {new}")
    for idx, po in enumerate(all_parsed, 1):
        print(f"[{idx}] {po.kind}: {po.raw}")
        if po.params:
            print(f"    参数: {po.params}")
        if po.missing:
            print(f"    缺失参数: {po.missing}")
    # 阶段1：基础格式校验（严格遵循 prompt.md 接口）
    has_format_error = False
    for idx, po in enumerate(all_parsed, 1):
        if po.kind in ('Unknown', 'ColumnSplit'):
            print(f"格式错误：不支持的操作类型（OP #{idx}）：{po.raw}")
            has_format_error = True
        if po.missing:
            print(f"格式缺失：OP #{idx} {po.kind} -> {', '.join(po.missing)}")
            has_format_error = True
    if has_format_error:
        print("\n检测到格式/参数错误，请按接口修正后重试。")
        _print_correction_request()
        return

    # 可选：阶段1.5 基于 DB/虚拟目录 的约束校验
    # stats 产物优先（避免重复 DB 查询）
    global _STATS_CATALOG
    _STATS_CATALOG = _load_stats_bundle(args.stats_dir) if getattr(args, 'stats_dir', None) else None
    # schema.sql 种子（可无 DB 时使用）
    schema_seed = _schema_seed_from_stats(_STATS_CATALOG)
    if args.schema_sql:
        try:
            import importlib
            P = importlib.import_module('prompt.PART2')
            tables = P.parse_schema(args.schema_sql)
            schema_seed = {t: list(cols.keys()) for t, cols in tables.items()}
        except Exception:
            schema_seed = None

    # 若未显式提供 storage_meta，且 stats bundle 有 meta.json，则默认使用
    try:
        if not args.storage_meta and _STATS_CATALOG and _STATS_CATALOG.meta_path:
            args.storage_meta = _STATS_CATALOG.meta_path
    except Exception:
        pass
    # 若 eval_* 使用默认值且 stats bundle 有对应产物，则优先切换
    try:
        if _STATS_CATALOG and _STATS_CATALOG.meta_path and args.eval_meta == 'output_dir/meta.json':
            args.eval_meta = _STATS_CATALOG.meta_path
    except Exception:
        pass
    try:
        if _STATS_CATALOG and args.eval_samples == 'response/samples/samples.json':
            cand = Path(getattr(args, 'stats_dir', '') or '') / 'samples' / 'op_cost_samples.json'
            if cand.exists():
                args.eval_samples = str(cand)
    except Exception:
        pass
    try:
        if _STATS_CATALOG and args.eval_explain_dir == 'part2_debug/explain':
            cand = Path(getattr(args, 'stats_dir', '') or '') / 'part2_debug' / 'explain'
            if cand.exists():
                args.eval_explain_dir = str(cand)
    except Exception:
        pass

    if args.validate_seq and (db is not None or schema_seed is not None):
        seq_issues = _sequential_validate(db, all_parsed, schema_seed=schema_seed)
        if seq_issues:
            print("\n约束校验失败（顺序校验）：")
            for e in seq_issues:
                print("- ", e)
            _print_correction_request(seq_issues)
            return
    elif db is not None:
        if args.validate_seq:
            seq_issues = _sequential_validate(db, all_parsed)
            if seq_issues:
                print("\n约束校验失败（顺序校验）：")
                for e in seq_issues:
                    print("- ", e)
                _print_correction_request(seq_issues)
                return
        else:
            db_issues: List[str] = []
            for idx, po in enumerate(all_parsed, 1):
                errs = _validate_with_db(db, po) or []
                if errs:
                    db_issues.append(f"OP #{idx} {po.kind}: " + "; ".join(errs))
            if db_issues:
                print("\n约束校验失败：")
                for e in db_issues:
                    print("- ", e)
                _print_correction_request(db_issues)
                return
    # stats 校验（无 DB 时使用）
    if db is None and _STATS_CATALOG is not None:
        stats_issues: List[str] = []
        for idx, po in enumerate(all_parsed, 1):
            errs = _validate_with_stats(_STATS_CATALOG, po) or []
            if errs:
                stats_issues.append(f"OP #{idx} {po.kind}: " + "; ".join(errs))
        if stats_issues:
            print("\n约束校验失败（stats）：")
            for e in stats_issues:
                print("- ", e)
            _print_correction_request(stats_issues)
            return

    # 保险起见：即使未启用顺序校验，也尽力构建一次“虚拟目录快照”，
    # 以便后续 plan 阶段（链式 TableJoin/派生表）能从 _SEQCAT_TABLE_COLS 获取列清单。
    try:
        if schema_seed is not None:
            _sequential_validate(None, all_parsed, schema_seed=schema_seed)
    except Exception:
        pass

    # 阶段2：存储开销预算检查（对齐接口，未提供元数据时跳过）
    def _parse_bytes(s: str) -> Optional[int]:
        if not s:
            return None
        sv = s.strip().lower()
        try:
            return int(float(sv))
        except Exception:
            pass
        units = {'kb': 1024, 'mb': 1024**2, 'gb': 1024**3, 'tb': 1024**4}
        for u, mul in units.items():
            if sv.endswith(u):
                try:
                    return int(float(sv[:-len(u)].strip()) * mul)
                except Exception:
                    return None
        return None

    meta_path = args.storage_meta
    budget_raw = args.storage_budget
    budget_bytes = _parse_bytes(budget_raw) if budget_raw else None

    # Prepare repo root for importing scripts.storage_transformer
    REPO_ROOT = os.path.dirname(_ROOT)
    if REPO_ROOT not in sys.path:
        sys.path.append(REPO_ROOT)

    def _to_storage_op(po: ParsedOp) -> Optional[str]:
        k, p = po.kind, po.params
        if k == 'VerticalSplit':
            body = []
            for i, t in enumerate(p['new_tables']):
                cols = p['column_lists'][i] if i < len(p['column_lists']) else []
                body.append(f"{t}({', '.join(cols)})")
            # optional pk hints
            pk_map = p.get('primary_keys') or {}
            for t, pkcols in pk_map.items():
                if pkcols:
                    body.append(f"{t}({', '.join(pkcols)})")
            return f"VerticalSplit({p['src_table']}, {str(bool(p.get('is_retained', False))).lower()}):" + ','.join(body)
        if k == 'HorizontalSplit':
            parts = [f"{name}({pred})" for name, pred in p.get('predicates', [])]
            return f"HorizontalSplit({p['table']}, {str(bool(p.get('is_retained', False))).lower()}):" + ','.join(parts)
        if k == 'HorizontalMerge':
            srcs = p['sources']
            is_r = str(bool(p.get('is_retained', False))).lower()
            return f"HorizontalMerge({srcs[0]}, {srcs[1]}, {is_r}):{p['new_table']}"
        if k == 'RedundantColumnAdd':
            tgt = f"{p['target_table']}.{p['new_column']}" if p.get('new_column') else p['target_table']
            return f"RedundantColumnAdd({p['source_table']}.{p['source_column']}, {tgt})"
        if k == 'RedundantColumnDrop':
            return f"RedundantColumnDrop({p['table']}.{p['column']})"
        if k == 'TableJoin':
            pairs = p.get('join_pairs') or []
            if len(pairs) == 1:
                a, b = pairs[0]
            else:
                # 多键时仅取首个以近似预算
                a, b = (pairs[0] if pairs else ('id', 'id'))
            return f"TableJoin({p['table1']}, {p['table2']}, {a}, {b}, {str(bool(p.get('is_retained', False))).lower()}): {p['new_table']}"
        return None

    # 存储估算与预算校验/报告
    if meta_path:
        def _fmt_bytes(v: int) -> str:
            units = ['B', 'KB', 'MB', 'GB', 'TB']
            val = float(v)
            i = 0
            while val >= 1024 and i < len(units) - 1:
                val /= 1024.0
                i += 1
            return f"{val:.2f}{units[i]}"

        try:
            import json as _json
            from scripts.storage_transformer import StorageModel  # type: ignore
            with open(meta_path, 'r', encoding='utf-8') as f:
                meta = _json.load(f)
            model = StorageModel(meta)
            model.update_total_storage()
            init_total = int(model.meta.get('total_storage_bytes') or model.compute_total_storage())
            cur_total = init_total
            # 打印初始总存储
            print(f"初始总存储大小：{_fmt_bytes(init_total)}（{init_total} 字节）")

            # 逐操作估算与可选预算限制
            for idx, po in enumerate(all_parsed, 1):
                sop = _to_storage_op(po)
                if not sop:
                    continue
                try:
                    model.apply(sop)
                except Exception as e:
                    print(f"存储估算失败（OP #{idx} {po.kind}）：{e}")
                    return
                model.update_total_storage()
                cur_total = int(model.meta.get('total_storage_bytes') or model.compute_total_storage())
                print(f"OP #{idx} {po.kind} 后总存储大小：{_fmt_bytes(cur_total)}（{cur_total} 字节）")
                if budget_bytes is not None and cur_total > int(budget_bytes):
                    print(f"超过存储限制：OP #{idx} {po.kind} 后预计总存储 {cur_total} 字节，超过阈值 {budget_bytes} 字节。")
                    return
            if budget_bytes is not None:
                print(f"存储预算检查通过：预计总存储 {cur_total} 字节（阈值 {budget_bytes} 字节）。")
        except Exception as e:
            print(f"存储估算/预算检查出错（忽略并继续）：{e}")

    # 阶段3：规划全部 Schema 语句（先不执行，确保全通过再应用）
    print("\n=== 事务预演（仅规划 Schema 语句） ===")
    per_op_statements: List[Tuple[ParsedOp, List[str]]] = []
    all_statements: List[str] = []
    for idx, po in enumerate(all_parsed, 1):
        stmts = plan_statements(db, po)
        if not stmts:
            why = "; ".join(po.missing) if getattr(po, 'missing', None) else "参数不全或该操作暂不支持规划"
            print(f"- 规划失败或参数不全：OP #{idx} {po.kind} -> {why}")
            return
        per_op_statements.append((po, stmts))
        all_statements.extend(stmts)

    if db is None or not args.apply_schema:
        print("[dry-run] 将执行以下语句（未实际执行）：")
        for s in all_statements:
            print(s)
        # 继续进行 SQL 改写输出
    else:
        print("\n=== 执行 Schema 改写（全部校验通过后一次性执行） ===")
        for idx, (po, stmts) in enumerate(per_op_statements, 1):
            for i, s in enumerate(stmts, 1):
                try:
                    ok = db.execute_statement(s)
                except Exception as e:
                    print(f"执行异常（OP #{idx} {po.kind} 第{i}条）: {e}\nSQL: {s}")
                    print("已停止后续执行；部分语句可能已生效，请人工回滚或修复后重试。")
                    return
                if not ok:
                    print(f"执行失败（OP #{idx} {po.kind} 第{i}条）: {s}")
                    print("已停止后续执行；部分语句可能已生效，请人工回滚或修复后重试。")
                    return

    # 阶段4：SQL 改写（仅当提供了 SQL 目录）
    in_sql_dir = args.sql_dir
    cur_sql_dir = None
    if in_sql_dir and os.path.isdir(in_sql_dir):
        cur_sql_dir = os.path.abspath(in_sql_dir)
        print(f"\n[runner] SQL 重写输入目录: {cur_sql_dir}")
    out_base = args.out_sql_dir or os.path.join(here, 'rewritten_sql')
    if cur_sql_dir:
        os.makedirs(out_base, exist_ok=True)

    if cur_sql_dir:
        print("\n=== SQL 改写阶段 ===")
        staging_dir = out_base + '.staging'
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        os.makedirs(staging_dir, exist_ok=True)
        next_sql_in = cur_sql_dir
        for idx, (po, _) in enumerate(per_op_statements, 1):
            step_out = os.path.join(staging_dir, f"step_{idx:02d}_{po.kind}")
            os.makedirs(step_out, exist_ok=True)
            try:
                op_instance = instantiate_for_sql_rewrite(db, po)
                if op_instance is not None:
                    apply_op_to_sql_dir(op_instance, next_sql_in, step_out)
                    next_sql_in = step_out
            except Exception as e:
                shutil.rmtree(staging_dir, ignore_errors=True)
                print(f"- SQL 改写失败：OP #{idx} {po.kind} -> {e}")
                return
        # 搬运 staging → out
        if os.path.exists(out_base):
            shutil.rmtree(out_base)
        shutil.move(staging_dir, out_base)

    # 阶段5：性能估算与反馈（可选）
    if args.eval_perf:
        try:
            from performance_eval.eval_bridge import run_eval_sequence
        except Exception as e:
            print(f"[eval] 无法导入性能评估桥接器：{e}")
            return

        # 组装 rewrite 实例（使用 SQL 改写同样的实例构造）
        rewrite_ops = []
        for po in all_parsed:
            try:
                inst = instantiate_for_sql_rewrite(db, po)
                if inst is not None:
                    rewrite_ops.append(inst)
            except Exception:
                continue

        plan_dir = os.path.abspath(args.eval_explain_dir)
        if not os.path.isdir(plan_dir):
            print(f"[eval] EXPLAIN 目录不存在：{plan_dir}")
        else:
            # 注入 UNION 样本（若存在）
            extras = {}
            try:
                import json as _json  # ensure local in this block
                smp = _json.loads(Path(args.eval_samples).read_text(encoding='utf-8'))
                u = smp.get('union') or {}
                if u:
                    extras['union'] = {
                        'sample_cost': u.get('append_cost_med', 0.0),
                        'sample_rows': u.get('total_rows_med', 1.0),
                    }
            except Exception:
                pass

            out_json: Dict[str, Any] = {}
            lines = []
            from pathlib import Path
            # Try to locate sibling sql/ folder for original SQL text
            sql_dir_guess = Path(plan_dir).parent / 'sql'
            has_sql_dir = sql_dir_guess.exists()
            # 先跑一遍，收集每个查询的结果以便聚合
            per_query_results: Dict[str, Dict[str, Any]] = {}
            for fn in sorted(os.listdir(plan_dir)):
                if not fn.startswith('q') or not fn.endswith('.txt'):
                    continue
                p = os.path.join(plan_dir, fn)
                plan_text = Path(p).read_text(encoding='utf-8')
                # Build per-plan extras (inherit global + attach sql_text when available)
                e_local = dict(extras)
                if has_sql_dir:
                    try:
                        qid = int(''.join(ch for ch in fn if ch.isdigit()))
                        sp = sql_dir_guess / f"q{qid}.sql"
                        if sp.exists():
                            e_local['sql_text'] = sp.read_text(encoding='utf-8', errors='ignore')
                    except Exception:
                        pass
                res = run_eval_sequence(plan_text, rewrite_ops, meta_path=args.eval_meta, extras=e_local)
                out_json[fn] = res
                per_query_results[fn] = res

            # ---------- 聚合：每步成本变化、影响率与 DML 写入行数 ----------
            def _class_name(x: Any) -> str:
                try:
                    return x.__class__.__name__
                except Exception:
                    return str(type(x))

            op_names = [_class_name(op) for op in rewrite_ops]
            # 初始化聚合容器
            agg = {
                'total_queries': 0,
                'impacted_queries_cost': 0,
                'impacted_queries_sql': 0,
                'per_op': [
                    {
                        'op': op_names[i] if i < len(op_names) else f'OP{i+1}',
                        'delta_list': [],
                        'sql_changed_list': [],
                    }
                    for i in range(len(rewrite_ops))
                ]
            }

            # 逐查询汇总
            all_before_sum = 0.0
            all_after_sum = 0.0
            impacted_samples_cost: List[str] = []
            impacted_samples_sql: List[str] = []
            for qname, res in per_query_results.items():
                before = res.get('before_total_cost')
                after = res.get('after_total_cost')
                if isinstance(before, (int, float)):
                    all_before_sum += float(before)
                if isinstance(after, (int, float)):
                    all_after_sum += float(after)
                steps = res.get('steps') or []
                agg['total_queries'] += 1
                # 是否该查询在任一步出现成本变化或 SQL 变化
                any_cost_change = False
                any_sql_change = False
                for i, st in enumerate(steps):
                    # 记录每步成本变化
                    d = st.get('delta_step')
                    if i < len(agg['per_op']):
                        if isinstance(d, (int, float)):
                            agg['per_op'][i]['delta_list'].append(float(d))
                            if abs(float(d)) > 1e-9:
                                any_cost_change = True
                        else:
                            # 统计为空也要占位，以便计算影响率分母
                            agg['per_op'][i]['delta_list'].append(None)  # type: ignore
                    # 记录 SQL 是否发生结构性变化（当可判断时）
                    sc = st.get('sql_changed')
                    if i < len(agg['per_op']):
                        agg['per_op'][i]['sql_changed_list'].append(sc)
                    if sc is True:
                        any_sql_change = True
                if any_cost_change:
                    agg['impacted_queries_cost'] += 1
                    impacted_samples_cost.append(qname)
                if any_sql_change:
                    agg['impacted_queries_sql'] += 1
                    impacted_samples_sql.append(qname)

            # 计算每步统计量
            import statistics as _stats
            per_op_summary: List[Dict[str, Any]] = []
            for i, slot in enumerate(agg['per_op']):
                deltas = [x for x in slot['delta_list'] if isinstance(x, (int, float))]
                # 影响率（按成本变化、按 SQL 改写）
                n_q = max(1, agg['total_queries'])
                cost_impact = sum(1 for x in deltas if abs(x) > 1e-9) / n_q
                sql_flags = [x for x in slot['sql_changed_list'] if x in (True, False)]
                sql_impact = (sum(1 for x in sql_flags if x is True) / n_q) if sql_flags else 0.0
                per_op_summary.append({
                    'op': slot['op'],
                    'delta_avg': (_stats.fmean(deltas) if deltas else 0.0),
                    'delta_sum': (float(sum(deltas)) if deltas else 0.0),
                    'delta_med': (_stats.median(deltas) if deltas else 0.0),
                    'impact_rate_cost': round(cost_impact, 4),
                    'impact_rate_sql': round(sql_impact, 4),
                })

            # DML 写入行数估计（基于 meta.json 和存储模型）
            def _build_storage_op_str(op_inst: Any) -> Optional[str]:
                try:
                    cname = op_inst.__class__.__name__
                    if cname == 'TableJoin':
                        t1, t2 = op_inst.old_tables[0], op_inst.old_tables[1]
                        keep_old = 'True' if getattr(op_inst, 'sign', 1) != 1 else 'False'
                        jk = getattr(op_inst, 'join_key', None) or []
                        if isinstance(jk, (list, tuple)) and len(jk) >= 1:
                            k1, k2 = jk[0][0], jk[0][1]
                        else:
                            return None
                        newt = op_inst.new_table
                        return f"TableJoin({t1}, {t2}, {t1}.{k1}, {t2}.{k2}, {keep_old}):{newt}"
                    if cname in ('TableSplit', 'VerticalSplit'):
                        src = getattr(op_inst, 'old_table', None) or getattr(op_inst, 'table', None) or getattr(op_inst, 'src_table', None)
                        is_retained = 'True' if getattr(op_inst, 'is_retained', False) else 'False'
                        new_tables = list(getattr(op_inst, 'new_tables', []) or [])
                        colmap = getattr(op_inst, 'columnList', {}) or {}
                        if not src or not new_tables or not colmap:
                            return None
                        body = []
                        for nt in new_tables:
                            cols = colmap.get(nt, []) or []
                            body.append(f"{nt}({', '.join(cols)})")
                        return f"VerticalSplit({src}, {is_retained}):" + ",".join(body)
                    if cname == 'HorizontalSplit':
                        src = op_inst.table
                        is_retained = 'True' if getattr(op_inst, 'is_retained', False) else 'False'
                        parts = []
                        for (nt, pred) in getattr(op_inst, 'predicates', []) or []:
                            parts.append(f"{nt}({pred})")
                        return f"HorizontalSplit({src}, {is_retained}):" + ",".join(parts)
                    if cname == 'HorizontalMerge':
                        t1, t2 = op_inst.sources[0], op_inst.sources[1]
                        is_retained = 'True' if getattr(op_inst, 'is_retained', False) else 'False'
                        newt = getattr(op_inst, 'new_table', None)
                        if not newt:
                            return None
                        return f"HorizontalMerge({t1}, {t2}, {is_retained}):{newt}"
                    if cname == 'RedundantColumnAdd':
                        st = op_inst.source_table; sc = op_inst.source_column
                        tt = op_inst.target_table; tc = op_inst.new_column
                        return f"RedundantColumnAdd({st}.{sc}, {tt}.{tc})"
                    if cname == 'RedundantColumnDrop':
                        tt = getattr(op_inst, 'target_table', None) or getattr(op_inst, 'table', None)
                        rc = getattr(op_inst, 'redundant_column', None) or getattr(op_inst, 'column', None)
                        if not tt or not rc:
                            return None
                        return f"RedundantColumnDrop({tt}.{rc})"
                except Exception:
                    return None
                return None

            def _collect_rows_snapshot(meta_obj: Dict[str, Any], tables: List[str]) -> Dict[str, int]:
                out: Dict[str, int] = {}
                try:
                    tdict = (meta_obj.get('tables') or {})
                    for t in tables:
                        if t in tdict and isinstance(tdict[t], dict):
                            out[t] = int(tdict[t].get('row_count', 0) or 0)
                except Exception:
                    pass
                return out

            def _op_related_tables_for_rows(op_inst: Any) -> List[str]:
                try:
                    cname = op_inst.__class__.__name__
                    if cname == 'TableJoin':
                        return [op_inst.new_table]
                    if cname in ('TableSplit', 'VerticalSplit'):
                        return list(getattr(op_inst, 'new_tables', []) or [])
                    if cname == 'HorizontalSplit':
                        return [nt for (nt, _p) in getattr(op_inst, 'predicates', []) or []]
                    if cname == 'HorizontalMerge':
                        return [getattr(op_inst, 'new_table', '')]
                    if cname == 'RedundantColumnAdd':
                        return [getattr(op_inst, 'target_table', '')]
                except Exception:
                    return []
                return []

            dml_per_op: List[Dict[str, Any]] = []
            # 加载初始 meta
            _meta_obj: Optional[Dict[str, Any]] = None
            try:
                import json as _json
                from pathlib import Path as _Path
                _meta_obj = _json.loads(_Path(args.eval_meta).read_text(encoding='utf-8'))
            except Exception:
                _meta_obj = None
            if _meta_obj is not None:
                try:
                    from scripts.storage_transformer import StorageModel  # type: ignore
                    model = StorageModel(_meta_obj)
                except Exception:
                    model = None  # type: ignore
            else:
                model = None  # type: ignore

            for op_inst in rewrite_ops:
                op_s = _build_storage_op_str(op_inst)
                ins_rows = 0
                upd_rows = 0
                new_tbl_cnt = 0
                if model is not None and op_s:
                    # 变更前快照（仅相关表）
                    rel = [t for t in _op_related_tables_for_rows(op_inst) if t]
                    before_rows = _collect_rows_snapshot(model.meta, rel)
                    # 应用到模型
                    try:
                        model.apply(op_s)
                    except Exception:
                        pass
                    after_rows = _collect_rows_snapshot(model.meta, rel)
                    for t in rel:
                        br = before_rows.get(t, 0)
                        ar = after_rows.get(t, 0)
                        if t not in before_rows:  # 新增表
                            new_tbl_cnt += 1
                        # 新建/填充表：计为 INSERT
                        if ar > br:
                            ins_rows += (ar - br) if br > 0 else ar
                    # 冗余列新增：估计为一次全表 UPDATE
                    if _class_name(op_inst) == 'RedundantColumnAdd':
                        tgt = getattr(op_inst, 'target_table', None)
                        if tgt and tgt in after_rows:
                            upd_rows += int(after_rows[tgt])
                dml_per_op.append({
                    'op': _class_name(op_inst),
                    'insert_rows': int(ins_rows),
                    'update_rows': int(upd_rows),
                    'new_tables': int(new_tbl_cnt),
                })

            # ---------- 组装 markdown 与输出 ----------
            # 受影响计划覆盖率（按成本变化）
            n_all = max(1, agg['total_queries'])
            cover_cost = agg['impacted_queries_cost'] / n_all
            # 代表性查询（按收益排序）
            top_effect: List[Tuple[str, float]] = []
            for qname, res in per_query_results.items():
                b = res.get('before_total_cost') or 0.0
                a = res.get('after_total_cost') or 0.0
                top_effect.append((qname, float(b) - float(a)))
            top_effect.sort(key=lambda x: x[1], reverse=True)
            # 头部信息
            head = []
            head.append(f"受影响计划覆盖率：{agg['impacted_queries_cost']}/{agg['total_queries']}（{cover_cost*100:.1f}%）；样本：" + (", ".join([n for n,_ in top_effect[:5]]) + (" …" if len(top_effect)>5 else "")))
            head.append("性能评估对比（cost）：")
            head.append(f"- 初始基线：{int(all_before_sum)} cost")
            head.append(f"- 预测结果：{int(all_after_sum)} cost（改善 {int(all_before_sum - all_after_sum)}）")
            head.append("代表性查询净效应：")
            for qn, gain in top_effect[:3]:
                head.append(f"- {qn}: 总成本下降 {gain:.3g}")
            head.append("写入开销估计：")
            for idx, dml in enumerate(dml_per_op, 1):
                ins = dml.get('insert_rows', 0)
                upd = dml.get('update_rows', 0)
                nm = dml.get('op')
                parts = []
                if ins:
                    parts.append(f"INSERT≈{ins}")
                if upd:
                    parts.append(f"UPDATE≈{upd}")
                if not parts:
                    parts.append("无显著写入")
                head.append(f"- OP{idx} {nm}: " + " ".join(parts))

            # Per-step 明细表
            lines.extend(head)
            lines.extend(["", "# Runner Auto Eval (per-step)", "", "| Plan | Final Total Cost | Note |", "|---|---:|---|"])
            for fn in sorted(per_query_results.keys()):
                res = per_query_results[fn]
                note_parts = []
                for st in res.get('steps') or []:
                    s = (st.get('result') or {}).get('note')
                    if s and '跳过' not in s:
                        note_parts.append(f"[{st.get('op')}] {s}")
                lines.append(f"| {fn} | {res.get('after_total_cost') if res.get('after_total_cost') is not None else ''} | {'；'.join(note_parts[:3])} |")

            # 将聚合附加到 JSON 结果
            out_json['__summary__'] = {
                'total_before_cost': all_before_sum,
                'total_after_cost': all_after_sum,
                'total_delta': all_after_sum - all_before_sum,
                'coverage_cost': cover_cost,
                'per_op': per_op_summary,
                'dml_rows': dml_per_op,
            }

    # 结束
    if db is None or not args.apply_schema:
        print("\n完成（dry-run）。")
    else:
        print("\n完成。所有步骤均已按序执行，Schema 改写与 SQL 改写已提交。")


## 入口位置已下移到文件末尾，确保辅助函数已定义

# --- Helpers for SQL rewrite pipeline ---
import shutil
from sqlglot import parse_one as _parse_one
from sqlglot import expressions as _exp

def instantiate_for_sql_rewrite(db, op: ParsedOp):
    k, p = op.kind, op.params
    if k == 'HorizontalSplit':
        return HorizontalSplit(p['table'], p['predicates'], is_retained=bool(p.get('is_retained', False)))
    if k == 'HorizontalMerge':
        return HorizontalMerge(p['sources'], p['new_table'], is_retained=bool(p.get('is_retained', False)))
    if k == 'VerticalSplit':
        # 对应 TableSplit 的接口：old_table, new_tables, columnList(dict), primary_keys_dict, new_view, is_retained
        column_map = {t: (p['column_lists'][i] if i < len(p['column_lists']) else []) for i, t in enumerate(p['new_tables'])}
        new_view = f"view_{p['src_table']}"
        return VerticalSplit(p['src_table'], p['new_tables'], column_map, p.get('primary_keys', {}), new_view, bool(p.get('is_retained', False)))
    if k == 'RedundantColumnAdd':
        if 'join_keys' not in p:
            return None
        return RedundantColumnAdd(p['source_table'], p['source_column'], p['target_table'], p['new_column'], p['join_keys'])
    if k == 'RedundantColumnDrop':
        try:
            return RedundantColumnDrop(target_table=p['table'], redundant_column=p['column'])
        except TypeError:
            return None
    if k == 'TableJoin':
        # 使用完整的 TableJoin 改写器，支持显式 JOIN、别名清理与 WHERE 等值剔除。
        # 将 is_retained 映射到 TableJoin.sign：True→2（保留源表），False→1（不保留）。
        try:
            join_pairs = p.get('join_pairs') or []
            # TableJoin.__init__(old_tables, new_table, old_columns_list, sign, join_key=None)
            # old_columns_list 可从顺序校验阶段注入的 _SEQCAT_TABLE_COLS 获取兜底列清单
            def _cols_of(t: str) -> list[str]:
                return list(_SEQCAT_TABLE_COLS.get(str(t).lower()) or [])
            old_cols = [_cols_of(p['table1']), _cols_of(p['table2'])]
            sign = 2 if bool(p.get('is_retained', False)) else 1
            tj = TableJoin([p['table1'], p['table2']], p['new_table'], old_cols, sign, join_key=join_pairs)
            return tj
        except Exception:
            # 回退到最小替换器
            class _MiniJoinRewriter:
                def __init__(self, old1, old2, newt):
                    self.old1, self.old2, self.newt = old1, old2, newt
                def apply_to_sql(self, sql: str) -> str:
                    s = sql
                    import re as _re
                    pattern = _re.compile(r"(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)", _re.IGNORECASE|_re.DOTALL)
                    def repl(m):
                        prefix, tables = m.group(1), m.group(2)
                        parts = [t.strip() for t in tables.split(',') if t.strip()]
                        bases = [p.split()[0] for p in parts]
                        if self.old1 in bases and self.old2 in bases:
                            new_parts, used_new = [], False
                            for t in parts:
                                base = t.split()[0]
                                if base in (self.old1, self.old2):
                                    if not used_new:
                                        new_parts.append(self.newt); used_new = True
                                else:
                                    new_parts.append(t)
                            return prefix + ', '.join(new_parts)
                        return m.group(0)
                    return pattern.sub(repl, s)
            return _MiniJoinRewriter(p['table1'], p['table2'], p['new_table'])
    return None

def apply_op_to_sql_dir(op, in_dir: str, out_dir: str) -> None:
    # Copy all non-sql files
    for root, _, files in os.walk(in_dir):
        rel = os.path.relpath(root, in_dir)
        target_root = os.path.join(out_dir, rel) if rel != '.' else out_dir
        os.makedirs(target_root, exist_ok=True)
        for fn in files:
            src = os.path.join(root, fn)
            dst = os.path.join(target_root, fn)
            if not fn.lower().endswith('.sql'):
                shutil.copy2(src, dst)
            else:
                with open(src, 'r', encoding='utf-8') as f:
                    sql = f.read()
                try:
                    rewritten = op.apply_to_sql(sql)
                except Exception:
                    rewritten = sql
                with open(dst, 'w', encoding='utf-8') as f:
                    f.write(rewritten)

# 入口放在文件末尾，确保上面辅助函数已完成定义
if __name__ == '__main__':
    main()
