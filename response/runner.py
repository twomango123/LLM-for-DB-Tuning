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
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

import argparse
import shutil

# Import rewrite ops
_ROOT = os.path.dirname(os.path.dirname(__file__))  # LLM-for-DB-Tuning
if _ROOT not in sys.path:
    sys.path.append(_ROOT)

try:
    from rewrite.ColumnSplit import ColumnSplit
    from rewrite.TableSplit import TableSplit as VerticalSplitOp
    from rewrite.HorizontalSplit import HorizontalSplit
    from rewrite.HorizontalMerge import HorizontalMerge
    from rewrite.RedundantColumnAdd import RedundantColumnAdd
    from rewrite.RedundantColumnDrop import RedundantColumnDrop
    from rewrite.TableJoin import TableJoin as TableJoinOp
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
    t1 = args[0].strip().split('.')[-1]
    t2 = args[1].strip().split('.')[-1]
    k1_raw = _strip_outer(args[2])
    k2_raw = _strip_outer(args[3])
    retained_s = args[4].strip()
    is_retained = retained_s.lower() in ('true', '1')

    # keys: support single or list in parentheses
    k1 = [c.strip() for c in _tokenize_top_level(k1_raw)] if ',' in k1_raw else ([k1_raw] if k1_raw else [])
    k2 = [c.strip() for c in _tokenize_top_level(k2_raw)] if ',' in k2_raw else ([k2_raw] if k2_raw else [])

    missing: List[str] = []
    join_pairs: List[Tuple[str, str]] = []
    if not k1 or not k2:
        missing.append("缺少连接键（table1_join_key, table2_join_key）")
    elif len(k1) != len(k2):
        missing.append(f"连接键数量不匹配：{len(k1)} vs {len(k2)}")
    else:
        join_pairs = list(zip(k1, k2))

    # Columns will be fetched from DB if enabled; mark missing until filled.
    missing.append("old_columns_list 将从数据库自动获取或需要手动提供")

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
        params={'table': src_table, 'predicates': preds},
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
        params={'sources': [t1, t2], 'new_table': new_table},
    )


def parse_redundant_add(raw: str) -> ParsedOp:
    # RedundantColumnAdd(SourceTable.Col, TargetTable.NewCol)
    m = re.match(r"^RedundantColumnAdd\((.*)\)\s*$", raw.strip())
    if not m:
        return ParsedOp(kind='RedundantColumnAdd', raw=raw, missing=["无法解析 RedundantColumnAdd 语法"])
    args = _tokenize_top_level(m.group(1))
    if len(args) != 2:
        return ParsedOp(kind='RedundantColumnAdd', raw=raw, missing=[f"参数数量应为2（源列, 目标表[.新列]），实际为{len(args)}"])
    src = args[0].strip()
    tgt = args[1].strip()
    if '.' not in src:
        return ParsedOp(kind='RedundantColumnAdd', raw=raw, missing=["参数格式应为 SourceTable.SourceColumn"])
    st, sc = src.split('.', 1)
    if '.' in tgt:
        tt, nc = tgt.split('.', 1)
    else:
        tt, nc = tgt, sc  # 默认新列名 = 源列名
    # join_keys may be auto-discovered from FK (target -> source) if DB is used
    return ParsedOp(
        kind='RedundantColumnAdd',
        raw=raw,
        params={'source_table': st, 'source_column': sc, 'target_table': tt, 'new_column': nc},
        missing=["join_keys 将从数据库外键自动获取或需要手动提供"],
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
    elif k == 'RedundantColumnAdd':
        st, tt = p['source_table'], p['target_table']
        try:
            # target typically references source: find FK from target to source
            fks = find_fk_between(db, tt, st)
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


def _split_script_to_stmts(script: str) -> List[str]:
    stmts: List[str] = []
    if not script:
        return stmts
    for line in script.splitlines():
        s = line.strip()
        if not s or s.startswith('--'):
            continue
        if not s.endswith(';') and not s.upper().startswith('SET '):
            s += ';'
        stmts.append(s)
    return stmts


def plan_statements(db, op: ParsedOp) -> List[str]:
    """Plan SQL statements for this op without executing them."""
    k = op.kind
    p = op.params
    # try to fill with DB
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
        hs = HorizontalSplit(p['table'], p['predicates'])
        script = hs.apply_to_schema(db=None)
        return _split_script_to_stmts(script)

    if k == 'HorizontalMerge':
        hm = HorizontalMerge(p['sources'], p['new_table'])
        script = hm.apply_to_schema(db=None)
        return _split_script_to_stmts(script)

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
        # Build CTAS join using provided join_pairs and discovered column lists
        cols = p.get('old_columns_list')
        pairs: List[Tuple[str, str]] = p.get('join_pairs') or []
        if not cols or len(cols) != 2 or not pairs:
            return []
        t1, t2 = p['table1'], p['table2']
        newt = p['new_table']
        on_clause = ' AND '.join([f"t1.`{a}` = t2.`{b}`" for a, b in pairs])
        c1, c2 = cols[0], cols[1]
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
    ap.add_argument('--out-sql-dir', help='Directory to write rewritten SQL files (optional)')
    args = ap.parse_args()

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
    for ln in lines:
        all_parsed.extend(parse_line_to_ops(ln))

    print("=== 解析结果（按顺序） ===")
    for idx, po in enumerate(all_parsed, 1):
        print(f"[{idx}] {po.kind}: {po.raw}")
        if po.params:
            print(f"    参数: {po.params}")
        if po.missing:
            print(f"    缺失参数: {po.missing}")

    # Prepare SQL rewrite workspace
    in_sql_dir = args.sql_dir
    cur_sql_dir = None
    if in_sql_dir and os.path.isdir(in_sql_dir):
        cur_sql_dir = os.path.abspath(in_sql_dir)
        print(f"\n[runner] SQL 重写输入目录: {cur_sql_dir}")
    out_base = args.out_sql_dir or os.path.join(here, 'rewritten_sql')
    if cur_sql_dir:
        os.makedirs(out_base, exist_ok=True)

    print("\n=== 事务预演（规划所有 Schema 语句并尝试 SQL 改写） ===")
    all_statements: List[str] = []
    staging_dir = None
    if cur_sql_dir:
        staging_dir = out_base + '.staging'
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        os.makedirs(staging_dir, exist_ok=True)
    next_sql_in = cur_sql_dir
    success = True
    for idx, po in enumerate(all_parsed, 1):
        stmts = plan_statements(db, po)
        if not stmts:
            print(f"- 规划失败或参数不全：OP #{idx} {po.kind}")
            success = False
            break
        all_statements.extend(stmts)
        if next_sql_in and staging_dir:
            step_out = os.path.join(staging_dir, f"step_{idx:02d}_{po.kind}")
            os.makedirs(step_out, exist_ok=True)
            try:
                op_instance = instantiate_for_sql_rewrite(db, po)
                if op_instance is not None:
                    apply_op_to_sql_dir(op_instance, next_sql_in, step_out)
                    next_sql_in = step_out
            except Exception as e:
                print(f"- SQL 改写失败：OP #{idx} {po.kind} -> {e}")
                success = False
                break

    if not success:
        if staging_dir and os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)
        print("\n事务终止：未对数据库进行任何更改。")
        return

    print("\n=== 提交阶段 ===")
    if db is None:
        print("[dry-run] 将执行以下语句（未实际执行）：")
        for s in all_statements:
            print(s)
        if staging_dir:
            if os.path.exists(out_base):
                shutil.rmtree(out_base)
            shutil.move(staging_dir, out_base)
        print("\n完成（dry-run）。")
        return

    # Execute all statements; if any fails, abort and keep staging for inspection
    for i, s in enumerate(all_statements, 1):
        try:
            ok = db.execute_statement(s)
        except Exception as e:
            print(f"执行异常 #{i}: {e}\nSQL: {s}")
            print("提交失败，已中止。")
            return
        if not ok:
            print(f"执行失败 #{i}: {s}")
            print("提交失败，已中止。")
            return

    if staging_dir:
        if os.path.exists(out_base):
            shutil.rmtree(out_base)
        shutil.move(staging_dir, out_base)
    print("\n完成。所有 Schema 改写与 SQL 改写已提交。")


if __name__ == '__main__':
    main()

# --- Helpers for SQL rewrite pipeline ---
import shutil
from sqlglot import parse_one as _parse_one
from sqlglot import expressions as _exp

def instantiate_for_sql_rewrite(db, op: ParsedOp):
    k, p = op.kind, op.params
    if k == 'HorizontalSplit':
        return HorizontalSplit(p['table'], p['predicates'])
    if k == 'HorizontalMerge':
        return HorizontalMerge(p['sources'], p['new_table'])
    if k == 'VerticalSplit':
        # For SQL rewrite we don’t need DB-only variant
        return VerticalSplitOp(p['src_table'], p['new_tables'], p['column_lists'], p['primary_keys'])
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
        # Use a minimal SQL replacer: replace natural join of two tables with the new one.
        # For complex SQL patterns, advanced rewriter can be added.
        class _MiniJoinRewriter:
            def __init__(self, old1, old2, newt):
                self.old1, self.old2, self.newt = old1, old2, newt
            def apply_to_sql(self, sql: str) -> str:
                s = sql
                # Basic: replace FROM a, b with FROM newt when both present
                import re as _re
                pattern = _re.compile(r"(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)", _re.IGNORECASE|_re.DOTALL)
                def repl(m):
                    prefix, tables = m.group(1), m.group(2)
                    parts = [t.strip() for t in tables.split(',') if t.strip()]
                    bases = [p.split()[0] for p in parts]
                    if self.old1 in bases and self.old2 in bases:
                        # keep non-join tables; swap first occurrence with newt and drop the other join table
                        new_parts = []
                        used_new = False
                        for t in parts:
                            base = t.split()[0]
                            if base in (self.old1, self.old2):
                                if not used_new:
                                    new_parts.append(self.newt)
                                    used_new = True
                                # else drop
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
