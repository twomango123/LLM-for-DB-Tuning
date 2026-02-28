#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compute table/join usage diffs for a workload before vs. after applying a schema-op sequence,
without actually rewriting SQL files on disk.

- Input: --sql-dir (original workload), --ops-file (response_mX_rY.txt or any ops text)
- Output: JSON with per-SQL tables_before/after, joins_before/after, and sets added/removed/changed.

Notes:
- Table mapping heuristics:
  - TableJoin(A,B,...): New -> replace {A,B} with New
  - VerticalSplit(Src): {T1(...), T2(...)} -> replace Src with {T1,T2}
  - HorizontalSplit(Src): {P1(...), P2(...)} -> replace Src with {P1,P2}
  - HorizontalMerge(T1,T2): New -> replace {T1,T2} with New
  - RedundantColumnAdd/Drop: no table-level change
- Names are normalized to bare table (strip schema/backticks).
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Optional: prefer AST parsing when sqlglot is available to resolve aliases
try:
    import sqlglot
    from sqlglot import expressions as _exp
except Exception:  # pragma: no cover
    sqlglot = None
    _exp = None

_Q_IDENT = r"`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*"
_Q_NAME = (
    r"(?:`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[^`]+`|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*))?"
)


def _unquote_ident(name: str) -> str:
    s = name.strip()
    if s.startswith("`") and s.endswith("`"):
        s = s[1:-1]
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s


def _base_table(name: str) -> str:
    s = _unquote_ident(name)
    if "." in s:
        s = s.split(".")[-1]
    return s


def build_alias_map(sql: str) -> Dict[str, str]:
    """Return alias->base_table mapping using sqlglot when available.

    Fallback to regex when sqlglot is not available or parsing fails.
    """
    alias: Dict[str, str] = {}
    if sqlglot is not None:
        try:
            tree = sqlglot.parse_one(sql, read='mysql')
            for tbl in list(tree.find_all(_exp.Table)):
                base = _base_table(tbl.name or '')
                if not base:
                    continue
                a = None
                al = tbl.args.get('alias')
                if al is not None and hasattr(al, 'name'):
                    a = al.name
                if a:
                    alias[a] = base
        except Exception:
            alias = {}
    if not alias:
        # Regex fallback: FROM/JOIN <name> [AS alias]
        for m in re.finditer(r"\b(?:FROM|JOIN)\s+(" + _Q_NAME + r")\s*(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)?", sql, re.IGNORECASE):
            base = _base_table(m.group(1))
            al = m.group(2)
            if al:
                alias[al] = base
    return alias


def _extract_tables(sql: str) -> Set[str]:
    # Prefer AST when available
    if sqlglot is not None:
        try:
            tree = sqlglot.parse_one(sql, read='mysql')
            ts: Set[str] = set()
            for tbl in list(tree.find_all(_exp.Table)):
                base = _base_table(tbl.name or '')
                if base:
                    ts.add(base)
            return ts
        except Exception:
            pass
    # Fallback: regex
    t: Set[str] = set()
    for m in re.finditer(r"\bFROM\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
        t.add(_base_table(m.group(1)))
    for m in re.finditer(r"\bJOIN\s+(" + _Q_NAME + r")\b", sql, re.IGNORECASE):
        t.add(_base_table(m.group(1)))
    return t


def _extract_join_pairs(sql: str) -> Set[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    # Try AST first to resolve aliases accurately
    if sqlglot is not None:
        try:
            tree = sqlglot.parse_one(sql, read='mysql')
            alias = build_alias_map(sql)
            # Capture a.b = c.d in ON/WHERE expressions
            for cmp in list(tree.find_all(_exp.EQ)):
                # Extract tables on both sides when present
                def _lhs_rhs(n):
                    # Find deepest column reference
                    col = None
                    for c in n.find_all(_exp.Column):
                        col = c
                        break
                    if col is None:
                        return None, None
                    tbl = None
                    if col.table:
                        tname = col.table
                        # Map alias->base when needed
                        tbl = alias.get(tname, _base_table(tname))
                    return tbl, col.name
                lt, _ = _lhs_rhs(cmp.left)
                rt, _ = _lhs_rhs(cmp.right)
                if lt and rt:
                    a, b = sorted([lt, rt])
                    pairs.add((a, b))
            return pairs
        except Exception:
            pairs = set()
    # Regex fallback
    alias = build_alias_map(sql)
    for m in re.finditer(r"((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))\s*=\s*((?:" + _Q_IDENT + r")\s*\.\s*(?:" + _Q_IDENT + r"))", sql, re.IGNORECASE):
        left_qual = m.group(1)
        right_qual = m.group(2)
        ltbl = _base_table(left_qual.split(".")[0])
        rtbl = _base_table(right_qual.split(".")[0])
        # resolve alias to base if present
        ltbl = alias.get(ltbl, ltbl)
        rtbl = alias.get(rtbl, rtbl)
        a, b = sorted([ltbl, rtbl])
        pairs.add((a, b))
    return pairs


def remap_join_pairs(pairs: Set[Tuple[str, str]], merge_map: Dict[str, str], split_map: Dict[str, Set[str]]) -> Set[Tuple[str, str]]:
    """Apply merge/split mapping to join pairs, returning pairs after ops.

    - If both sides map to the same table (e.g., TableJoin(A,B)->New), the pair disappears.
    - For splits, keep original pair conservatively.
    """
    out: Set[Tuple[str, str]] = set()
    for a, b in pairs:
        aa = merge_map.get(a, a)
        bb = merge_map.get(b, b)
        if a in split_map or b in split_map:
            aa, bb = a, b
        if aa == bb:
            # eliminated by merge
            continue
        x, y = sorted([aa, bb])
        out.add((x, y))
    return out


def _split_top_level(s: str) -> List[str]:
    out, buf, depth = [], [], 0
    for ch in s:
        if ch == '(':
            depth += 1
            buf.append(ch)
        elif ch == ')':
            depth = max(0, depth - 1)
            buf.append(ch)
        elif ch == ',' and depth == 0:
            token = ''.join(buf).strip()
            if token:
                out.append(token)
            buf = []
        else:
            buf.append(ch)
    token = ''.join(buf).strip()
    if token:
        out.append(token)
    return out


def _parse_ops(ops_text: str) -> List[str]:
    ops: List[str] = []
    for line in [ln.strip() for ln in (ops_text or '').splitlines() if ln.strip()]:
        # split by '-' at top level
        buf, depth, start = [], 0, 0
        s = line
        for i, ch in enumerate(s):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif ch == '-' and depth == 0:
                token = s[start:i].strip().rstrip(';')
                if token:
                    ops.append(token)
                start = i + 1
        tail = s[start:].strip().rstrip(';')
        if tail:
            ops.append(tail)
    return ops


def _build_table_mapping(ops_text: str) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    """Return (merge_map, split_map).
    - merge_map: {old_table -> new_table}
    - split_map: {old_table -> {child_tables}}
    """
    merge_map: Dict[str, str] = {}
    split_map: Dict[str, Set[str]] = {}
    for op in _parse_ops(ops_text):
        if op.startswith('TableJoin('):
            m = re.match(r"^TableJoin\(([^)]*)\)\s*:\s*([A-Za-z0-9_\.]+)$", op)
            if not m:
                continue
            args = _split_top_level(m.group(1))
            if len(args) < 2:
                continue
            t1 = _base_table(args[0])
            t2 = _base_table(args[1])
            newt = _base_table(m.group(2))
            merge_map[t1] = newt
            merge_map[t2] = newt
        elif op.startswith('VerticalSplit('):
            m = re.match(r"^VerticalSplit\(([^)]*)\)\s*:\s*(.*)$", op)
            if not m:
                continue
            args = _split_top_level(m.group(1))
            if not args:
                continue
            src = _base_table(args[0])
            body = _split_top_level(m.group(2))
            children: Set[str] = set()
            for b in body:
                mm = re.match(r"^([A-Za-z0-9_\.]+)\(.*\)$", b)
                if mm:
                    children.add(_base_table(mm.group(1)))
            if children:
                split_map[src] = children
        elif op.startswith('HorizontalSplit('):
            m = re.match(r"^HorizontalSplit\(([^)]*)\)\s*:\s*(.*)$", op)
            if not m:
                continue
            args = _split_top_level(m.group(1))
            if not args:
                continue
            src = _base_table(args[0])
            body = _split_top_level(m.group(2))
            parts: Set[str] = set()
            for b in body:
                mm = re.match(r"^([A-Za-z0-9_\.]+)\(.*\)$", b)
                if mm:
                    parts.add(_base_table(mm.group(1)))
            if parts:
                split_map[src] = parts
        elif op.startswith('HorizontalMerge('):
            m = re.match(r"^HorizontalMerge\(([^)]*)\)\s*:\s*([A-Za-z0-9_\.]+)$", op)
            if not m:
                continue
            args = _split_top_level(m.group(1))
            if len(args) < 2:
                continue
            t1 = _base_table(args[0])
            t2 = _base_table(args[1])
            newt = _base_table(m.group(2))
            merge_map[t1] = newt
            merge_map[t2] = newt
    return merge_map, split_map


def _apply_mapping(tables: Set[str], merge_map: Dict[str, str], split_map: Dict[str, Set[str]]) -> Set[str]:
    out: Set[str] = set()
    for t in tables:
        if t in merge_map:
            out.add(merge_map[t])
        elif t in split_map:
            out.update(split_map[t])
        else:
            out.add(t)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare SQL table/join usage before vs after ops without rewriting.")
    ap.add_argument("--sql-dir", required=True, help="Directory of original SQL files")
    ap.add_argument("--ops-file", required=True, help="Operations text file (e.g., response/response_m*_r*.txt)")
    ap.add_argument("--out", help="Output JSON path; omit to print")
    args = ap.parse_args()

    ops_text = Path(args.ops_file).read_text(encoding="utf-8")
    merge_map, split_map = _build_table_mapping(ops_text)

    base = Path(args.sql_dir)
    results: Dict[str, Dict] = {}
    for p in sorted(base.rglob("*.sql")):
        try:
            sql = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        before = _extract_tables(sql)
        after = _apply_mapping(before, merge_map, split_map)
        joins_b = _extract_join_pairs(sql)
        # Approximate join pairs after mapping by remapping table names in pairs
        joins_a: Set[Tuple[str, str]] = set()
        for a, b in joins_b:
            aa = merge_map.get(a, a)
            bb = merge_map.get(b, b)
            if a in split_map or b in split_map:
                # Split: pairs become ambiguous; conservatively keep as-is
                aa = a
                bb = b
            x, y = sorted([aa, bb])
            joins_a.add((x, y))
        results[str(p)] = {
            "tables_before": sorted(before),
            "tables_after": sorted(after),
            "tables_added": sorted(list(after - before)),
            "tables_removed": sorted(list(before - after)),
            "joins_before": sorted([list(x) for x in joins_b]),
            "joins_after": sorted([list(x) for x in joins_a]),
        }

    s = json.dumps(results, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(s, encoding="utf-8")
    else:
        print(s)


if __name__ == "__main__":
    main()
