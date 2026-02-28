#!/usr/bin/env python3
"""
Generate executable test scripts (testN.py) from operation sequence lines.

Behavior:
- Reads an input text file where each non-empty, non-comment line is a single
  operation in the supported textual format, e.g.:
    TableJoin(a, b, a_id, b_id, False):ab
    HorizontalSplit(t):t_2023(year=2023), t_2024(year=2024)
    VerticalSplit(t, True):t1(c1,c2), t2(c1,c3)
- For each line, copies a template Python file (default: LLM-for-DB-Tuning/test.py)
  and replaces only the trailing "op = ..." assignment with a minimal constructor
  call for that operation. The rest of the template (imports, CLI args, execution
  pattern) remains intact.
- Writes files to an output directory as test1.py, test2.py, ...

Notes:
- For TableJoin, column lists are passed as empty lists [[], []] to keep the
  template change minimal; SQL rewrite does not require full columns. Schema
  application may require filling them later.
- For VerticalSplit, primary keys dict is set empty ({}). SQL rewrite only needs
  new_view / old_table; schema stage may need more details.
 - ColumnRename is supported as ColumnRename(Table.Col, NewCol) -> ColumnRename('Table','Col','NewCol').
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import List, Tuple


def split_top_level(s: str, sep: str = ',') -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == '(':
            depth += 1
            buf.append(ch)
        elif ch == ')':
            depth = max(0, depth - 1)
            buf.append(ch)
        elif ch == sep and depth == 0:
            token = ''.join(buf).strip()
            if token:
                parts.append(token)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = ''.join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_bool(token: str) -> bool:
    t = token.strip().lower()
    if t in ('true', '1', 'yes'):
        return True
    if t in ('false', '0', 'no'):
        return False
    # default false for safety
    return False


def q(s: str) -> str:
    return f"'{s}'"


def gen_op_line(line: str) -> str:
    s = line.strip().rstrip(';')
    # Split LHS(args): RHS
    # Find first ':' not inside parentheses
    depth = 0
    cut = -1
    for i, ch in enumerate(s):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth - 1)
        elif ch == ':' and depth == 0:
            cut = i
            break
    lhs = s if cut < 0 else s[:cut].strip()
    rhs = '' if cut < 0 else s[cut + 1:].strip()

    m = re.match(r"^(\w+)\s*\((.*)\)\s*$", lhs)
    if not m:
        raise ValueError(f"无法解析操作：{line}")
    op, argstr = m.group(1), m.group(2)

    # Dispatch per op
    if op == 'TableJoin':
        # TableJoin(t1, t2, k1, k2, is_retained): NewTable
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) < 2:
            raise ValueError(f"TableJoin 参数不足：{line}")
        t1, t2 = args[0], args[1]
        jk_expr = 'None'
        retained = False
        if len(args) >= 5:
            k1, k2 = args[2], args[3]
            retained = parse_bool(args[4])
            jk_expr = (q(k1) if k1 == k2 else f"[({q(k1)}, {q(k2)})]") if (k1 and k2) else 'None'
        elif len(args) == 3:
            k = args[2]
            jk_expr = q(k)
            retained = False
        newt = rhs
        if not newt:
            raise ValueError(f"TableJoin 缺少新表名：{line}")
        sign = 2 if retained else 1
        return (
            f"op = TableJoin([{q(t1)}, {q(t2)}], {q(newt)}, [[], []], sign={sign}, join_key={jk_expr})"
        )

    if op == 'HorizontalSplit':
        # HorizontalSplit(SourceTable, is_retained):t1(pred), t2(pred), ...
        args = [a.strip() for a in split_top_level(argstr)]
        if not args:
            raise ValueError(f"HorizontalSplit 参数不足：{line}")
        src = args[0]
        retained = False
        if len(args) >= 2:
            retained = parse_bool(args[1])
        items = [x for x in split_top_level(rhs) if x]
        pairs: List[Tuple[str, str]] = []
        for it in items:
            m2 = re.match(r"^(\w+)\s*\((.*)\)$", it.strip())
            if not m2:
                raise ValueError(f"HorizontalSplit 子项语法错误：{it}")
            name, pred = m2.group(1), m2.group(2)
            pred_esc = pred.replace('"', '\\"')
            pairs.append((name, pred_esc))
        return f"op = HorizontalSplit({q(src)}, {[ (n, p) for (n, p) in pairs ]}, is_retained={'True' if retained else 'False'})"

    if op == 'HorizontalMerge':
        # HorizontalMerge(t1, t2, is_retained):NewTable
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) < 3:
            raise ValueError(f"HorizontalMerge 参数不足：{line}")
        t1, t2, retained_tok = args[0], args[1], args[2]
        retained = parse_bool(retained_tok)
        newt = rhs
        if not newt:
            raise ValueError(f"HorizontalMerge 缺少新表名：{line}")
        return f"op = HorizontalMerge([{q(t1)}, {q(t2)}], {q(newt)}, is_retained={'True' if retained else 'False'})"

    if op == 'VerticalSplit':
        # VerticalSplit(SourceTable, is_retained): t1(cols...), t2(cols...)
        # Optional trailing pk lists are ignored here (kept minimal for SQL rewrite use cases)
        args = [a.strip() for a in split_top_level(argstr)]
        if not args:
            raise ValueError(f"VerticalSplit 参数不足：{line}")
        src = args[0]
        retained = False
        if len(args) >= 2:
            retained = parse_bool(args[1])
        defs = [x for x in split_top_level(rhs) if x]
        new_tables: List[str] = []
        colmap: dict[str, List[str]] = {}
        for d in defs:
            m2 = re.match(r"^(\w+)\s*\((.*)\)$", d.strip())
            if not m2:
                raise ValueError(f"VerticalSplit 子项语法错误：{d}")
            name = m2.group(1)
            cols = [c.strip() for c in split_top_level(m2.group(2)) if c.strip()]
            new_tables.append(name)
            colmap[name] = cols
        view_name = f"view_{src}"
        return (
            f"op = TableSplit({q(src)}, {new_tables}, {colmap}, {{}}, new_view={q(view_name)}, is_retained={'True' if retained else 'False'})"
        )

    if op == 'RedundantColumnAdd':
        # RedundantColumnAdd(SrcTable.Col, TargetTable[.NewCol])
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) != 2:
            raise ValueError(f"RedundantColumnAdd 参数错误：{line}")
        m1 = re.match(r"^(\w+)\.(\w+)$", args[0])
        m2 = re.match(r"^(\w+)(?:\.(\w+))?$", args[1])
        if not (m1 and m2):
            raise ValueError(f"RedundantColumnAdd 参数格式错误：{line}")
        st, sc = m1.group(1), m1.group(2)
        tt, nc = m2.group(1), (m2.group(2) or sc)
        # join_keys 需人工提供，这里放置占位
        return (
            f"op = RedundantColumnAdd({q(st)}, {q(sc)}, {q(tt)}, {q(nc)}, join_keys=[('SOURCE_KEY','TARGET_KEY')])"
        )

    if op == 'RedundantColumnDrop':
        # RedundantColumnDrop(Table.Col)
        arg = argstr.strip()
        m1 = re.match(r"^(\w+)\.(\w+)$", arg)
        if not m1:
            raise ValueError(f"RedundantColumnDrop 参数格式错误：{line}")
        t, c = m1.group(1), m1.group(2)
        return f"op = RedundantColumnDrop({q(t)}, {q(c)})"

    if op == 'ColumnRename':
        # ColumnRename(SourceTable.OldColumnName, NewColumnName)
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) != 2:
            raise ValueError(f"ColumnRename 参数不足：{line}")
        m1 = re.match(r"^(\w+)\.(\w+)$", args[0])
        if not m1:
            raise ValueError(f"ColumnRename 源列格式错误：{args[0]}")
        tbl, old_col = m1.group(1), m1.group(2)
        new_col = args[1]
        return f"op = ColumnRename({q(tbl)}, {q(old_col)}, {q(new_col)})"

    if op == 'ColumnSplit':
        # ColumnSplit(Table.Col, is_retained):New1(expr), New2(expr)
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) < 1:
            raise ValueError(f"ColumnSplit 参数不足：{line}")
        m1 = re.match(r"^(\w+)\.(\w+)$", args[0])
        if not m1:
            raise ValueError(f"ColumnSplit 目标格式错误：{args[0]}")
        tbl, col = m1.group(1), m1.group(2)
        items = [x for x in split_top_level(rhs) if x]
        new_cols: List[str] = []
        for it in items:
            m2 = re.match(r"^(\w+)\s*\((.*)\)$", it.strip())
            if not m2:
                raise ValueError(f"ColumnSplit 子项错误：{it}")
            new_cols.append(m2.group(1))
        # 使用默认分隔符占位；如需严格控制，请手工修改
        return f"op = ColumnSplit({q(tbl)}, {q(col)}, {new_cols}, split_delimiter=',')"

    raise ValueError(f"未知操作类型：{op}")


def replace_op_line(template_text: str, new_line: str) -> str:
    lines = template_text.splitlines()
    idx = -1
    for i, s in enumerate(lines):
        if re.match(r"^\s*op\s*=\s*", s):
            idx = i
            break
    if idx >= 0:
        indent = re.match(r"^(\s*)", lines[idx]).group(1)  # type: ignore[index]
        lines[idx] = indent + new_line
        return "\n".join(lines) + "\n"
    # fallback: insert above first apply_op_to_sql_dir call
    for i, s in enumerate(lines):
        if 'apply_op_to_sql_dir(' in s:
            indent = re.match(r"^(\s*)", s).group(1)
            lines.insert(i, indent + new_line)
            return "\n".join(lines) + "\n"
    # else append
    return "\n".join(lines + [new_line]) + "\n"


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description='Generate testN.py files from operation sequence lines.')
    ap.add_argument('-i', '--input', required=True, help='Path to input text file with one operation per line')
    ap.add_argument('-t', '--template', default='LLM-for-DB-Tuning/test.py', help='Path to template test.py')
    ap.add_argument('-o', '--out-dir', default='LLM-for-DB-Tuning/generated_tests', help='Output directory for testN.py files')
    ap.add_argument('--start-index', type=int, default=1, help='Starting index N for testN.py')
    args = ap.parse_args(argv)

    template_path = Path(args.template)
    if not template_path.exists():
        raise FileNotFoundError(str(template_path))
    template_text = template_path.read_text(encoding='utf-8')

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    src = Path(args.input)
    if not src.exists():
        raise FileNotFoundError(str(src))
    lines = [ln.strip() for ln in src.read_text(encoding='utf-8').splitlines()]
    # filter non-empty, non-comment
    ops = [ln for ln in lines if ln and not ln.startswith('#')]

    idx = args.start_index
    for op_text in ops:
        try:
            op_line = gen_op_line(op_text)
        except Exception as e:
            print(f"[SKIP] 无法解析：{op_text} -> {e}")
            continue
        content = replace_op_line(template_text, op_line)
        dest = out_dir / f"test{idx}.py"
        dest.write_text(content, encoding='utf-8')
        print(f"[OK] {dest} <- {op_text}")
        idx += 1

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
