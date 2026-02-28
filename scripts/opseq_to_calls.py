#!/usr/bin/env python3
"""
Convert high-level operation sequence text into standardized rewrite calls.

Usage:
  python scripts/opseq_to_calls.py -i input.txt [-o output.py]

Input line examples (one per line):
  TableJoin(actual_orders, order_deliveries, actual_order_id, actual_order_id, False):orders_delivery_merged
  TableJoin(orders_delivery_merged, actual_order_products, actual_order_id, actual_order_id, False):order_details
  TableJoin(order_details, products, product_id, product_id, False):order_product_info
  HorizontalSplit(order_product_info):order_success(order_status_code = 'Success'), order_other(order_status_code != 'Success')
  HorizontalSplit(order_success):payment_visa(payment_method = 'Visa'), payment_other(payment_method != 'Visa')
  TableJoin(customer_addresses, customers, customer_id, customer_id, False):customer_full
  VerticalSplit(customer_full, True):customer_core(customer_id, customer_name, customer_phone, customer_email, date_became_customer, payment_method), customer_address(customer_id, address_id, date_from, address_type, date_to)
  TableJoin(customer_core, addresses, address_id, address_id, False):customer_with_address

Produces Python code that follows LLM-for-DB-Tuning/rewrite/README.md patterns, e.g.:
  from rewrite.TableJoin import TableJoin
  order_details_cols = [...]  # TODO
  products_cols = [...]       # TODO
  op1 = TableJoin(['order_details','products'], 'order_product_info', [order_details_cols, products_cols], sign=1, join_key=[('product_id','product_id')])

Notes:
- ColumnSplit: input signature ColumnSplit(Table.Col, is_retained):New1(split('d',1)),New2(split('d',2))
  is_retained is ignored (rewrite.ColumnSplit will drop the old column). The delimiter is inferred from split('x',pos).
- VerticalSplit maps to rewrite.TableSplit; we generate a default view name f"view_{old_table}". Primary key lists are optional in input.
- TableJoin requires full column lists per input table; placeholders are emitted for you to fill, unless you post-provide a DB metadata source.
- RedundantColumnAdd requires join_keys; if omitted, a TODO placeholder is emitted.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from typing import List, Tuple, Optional


def split_top_level(s: str, sep: str = ',') -> List[str]:
    """Split by sep but ignore separators inside parentheses."""
    parts = []
    buf = []
    level = 0
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == '(':
            level += 1
            buf.append(ch)
        elif ch == ')':
            level = max(0, level - 1)
            buf.append(ch)
        elif ch == sep and level == 0:
            parts.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        parts.append(''.join(buf).strip())
    return parts


def parse_bool(token: str) -> Optional[bool]:
    t = token.strip()
    if t.lower() == 'true':
        return True
    if t.lower() == 'false':
        return False
    return None


def quote(s: str) -> str:
    return f"'{s}'"


def py_bool(b: bool) -> str:
    return 'True' if b else 'False'


@dataclass
class GenResult:
    imports: set
    lines: List[str]


class OpSeqConverter:
    def __init__(self):
        self.imports: set[str] = set()
        self.lines: List[str] = []
        self.op_index = 0

    def new_op(self) -> str:
        self.op_index += 1
        return f"op{self.op_index}"

    def add(self, line: str):
        self.lines.append(line)

    def convert(self, text: str) -> GenResult:
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            # Skip JSON-like docs or blocks
            if line.startswith('{') or line.startswith('}') or line.startswith('"'):
                continue
            # Remove trailing comma-only lines
            if line in {',', '},', '],'}:
                continue
            # Skip pure comment lines
            if line.startswith('#') or line.startswith('//') or line.startswith('--'):
                continue

            # Expect pattern: Name(args):rhs
            lhs, rhs = self._split_lhs_rhs(line)
            if not lhs:
                continue
            m = re.match(r"^(\w+)\s*\((.*)\)\s*$", lhs)
            if not m:
                continue
            opname, argstr = m.group(1), m.group(2)
            dispatch = getattr(self, f"_handle_{opname}", None)
            if not callable(dispatch):
                # Unknown op: emit as comment
                self.add(f"# Unrecognized operation: {line}")
                continue
            dispatch(argstr, rhs, original=line)

        return GenResult(self.imports, self.lines)

    @staticmethod
    def _split_lhs_rhs(line: str) -> Tuple[Optional[str], Optional[str]]:
        # Split on first ':' that is not inside parentheses
        level = 0
        for i, ch in enumerate(line):
            if ch == '(':
                level += 1
            elif ch == ')':
                level = max(0, level - 1)
            elif ch == ':' and level == 0:
                return line[:i].strip(), line[i + 1:].strip()
        return line.strip(), ''

    # ---------- handlers ----------
    def _handle_ColumnSplit(self, argstr: str, rhs: str, original: str):
        # ColumnSplit(Table.Col, is_retained):New1(expr),New2(expr)[,...]
        # Parse args
        args = split_top_level(argstr)
        if len(args) < 2:
            self.add(f"# Malformed ColumnSplit: {original}")
            return
        tcol = args[0].strip()
        m = re.match(r"^(\w+)\.(\w+)$", tcol)
        if not m:
            self.add(f"# Malformed ColumnSplit target: {tcol}")
            return
        table, old_col = m.group(1), m.group(2)
        # is_retained present but rewrite.ColumnSplit does not take it; ignore
        # Parse rhs: NewCol(expr), NewCol(expr), ...
        items = [x for x in split_top_level(rhs) if x]
        new_cols: List[str] = []
        delimiter: Optional[str] = None
        position: Optional[int] = None
        split_call_re = re.compile(r"split\(\s*([\'\"])(.*?)\1\s*,\s*(\d+)\s*\)")
        for item in items:
            item = item.strip()
            m2 = re.match(r"^(\w+)\s*\((.*)\)$", item)
            if not m2:
                self.add(f"# Malformed ColumnSplit result item: {item}")
                continue
            new_name, expr = m2.group(1), m2.group(2)
            new_cols.append(new_name)
            m3 = split_call_re.search(expr)
            if m3:
                d = m3.group(2)
                pos = int(m3.group(3))
                # Prefer delimiter mode; ensure consistent delimiter
                if delimiter is None:
                    delimiter = d
                elif delimiter != d:
                    self.add(f"# Warning: inconsistent delimiter in ColumnSplit: {delimiter} vs {d}")
                position = pos  # last seen
        self.imports.add('from rewrite.ColumnSplit import ColumnSplit')
        var = self.new_op()
        if delimiter:
            self.add(f"# {original}")
            self.add(
                f"{var} = ColumnSplit({quote(table)}, {quote(old_col)}, {new_cols}, split_delimiter={quote(delimiter)})"
            )
        elif position is not None and len(new_cols) == 2:
            self.add(f"# {original}")
            self.add(
                f"{var} = ColumnSplit({quote(table)}, {quote(old_col)}, {new_cols}, split_position={position})"
            )
        else:
            self.add(f"# {original}")
            self.add(
                f"# TODO: Could not infer split delimiter/position from expressions; please fill parameters."
            )
            self.add(
                f"{var} = ColumnSplit({quote(table)}, {quote(old_col)}, {new_cols}, split_delimiter=',')"
            )

    def _handle_TableJoin(self, argstr: str, rhs: str, original: str):
        # TableJoin(Table1,Table2, table1_join_key, table2_join_key, is_retained): NewTable
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) < 2:
            self.add(f"# Malformed TableJoin: {original}")
            return
        t1, t2 = args[0], args[1]
        t1_key = t2_key = None
        retained = None
        if len(args) >= 5:
            t1_key, t2_key = args[2], args[3]
            retained = parse_bool(args[4])
        elif len(args) == 3:
            # Single join key used for both tables (e.g., TableJoin(t1,t2, joincol))
            t1_key = t2_key = args[2]
            retained = False
        else:
            retained = False
        new_table = rhs.strip()
        if not new_table:
            self.add(f"# Malformed TableJoin (missing new table): {original}")
            return

        self.imports.add('from rewrite.TableJoin import TableJoin')
        # Placeholder columns for each table; user should fill
        t1_cols_name = f"{t1}_cols"
        t2_cols_name = f"{t2}_cols"
        self.add(f"# {original}")
        self.add(f"# TODO: fill full column lists for {t1} and {t2}")
        self.add(f"{t1_cols_name} = []  # e.g., ['col1','col2',...]")
        self.add(f"{t2_cols_name} = []  # e.g., ['colA','colB',...]")
        sign = 2 if retained else 1
        # Parse possible multi-column key lists like (a,b,c)
        def _parse_key_list(tok: str) -> List[str]:
            t = tok.strip()
            if not t:
                return []
            if t.startswith('(') and t.endswith(')'):
                inner = t[1:-1]
                return [x.strip() for x in split_top_level(inner) if x.strip()]
            return [t]

        join_key_expr: str
        if t1_key and t2_key:
            k1 = _parse_key_list(t1_key)
            k2 = _parse_key_list(t2_key)
            if len(k1) == len(k2) and len(k1) > 0:
                if len(k1) == 1:
                    if k1[0] == k2[0]:
                        join_key_expr = quote(k1[0])
                    else:
                        join_key_expr = f"[({quote(k1[0])}, {quote(k2[0])})]"
                else:
                    pairs = ", ".join(f"({quote(a)}, {quote(b)})" for a, b in zip(k1, k2))
                    join_key_expr = f"[{pairs}]"
            else:
                self.add(f"# WARNING: join key lengths mismatch or empty: {t1_key} vs {t2_key}")
                join_key_expr = 'None'
        else:
            join_key_expr = 'None'

        var = self.new_op()
        self.add(
            f"{var} = TableJoin([{quote(t1)},{quote(t2)}], {quote(new_table)}, [{t1_cols_name}, {t2_cols_name}], sign={sign}, join_key={join_key_expr})"
        )

    def _handle_HorizontalSplit(self, argstr: str, rhs: str, original: str):
        # HorizontalSplit(SourceTable[, is_retained]):Table1(predicate),Table2(predicate),...
        args = [a.strip() for a in split_top_level(argstr)]
        if not args:
            self.add(f"# Malformed HorizontalSplit: {original}")
            return
        source = args[0]
        retained = False
        if len(args) >= 2:
            b = parse_bool(args[1])
            if b is not None:
                retained = b
        items = [x for x in split_top_level(rhs) if x]
        pairs: List[Tuple[str,str]] = []
        for it in items:
            m = re.match(r"^(\w+)\s*\((.*)\)$", it.strip())
            if not m:
                self.add(f"# Malformed HorizontalSplit item: {it}")
                continue
            newt, pred = m.group(1), m.group(2).strip()
            # Use double quotes around predicate; escape inner quotes
            pred_escaped = pred.replace('"', '\\"')
            pairs.append((newt, pred_escaped))

        self.imports.add('from rewrite.HorizontalSplit import HorizontalSplit')
        var = self.new_op()
        self.add(f"# {original}")
        self.add(
            f"{var} = HorizontalSplit({quote(source)}, {[ (p[0], p[1]) for p in pairs ]}, is_retained={py_bool(retained)})"
        )

    def _handle_HorizontalMerge(self, argstr: str, rhs: str, original: str):
        # HorizontalMerge(Table1, Table2, is_retained):NewTable
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) < 3:
            self.add(f"# Malformed HorizontalMerge: {original}")
            return
        t1, t2, retained_tok = args[0], args[1], args[2]
        retained = parse_bool(retained_tok)
        if retained is None:
            retained = False
        newt = rhs.strip()
        self.imports.add('from rewrite.HorizontalMerge import HorizontalMerge')
        var = self.new_op()
        self.add(f"# {original}")
        self.add(
            f"{var} = HorizontalMerge([{quote(t1)}, {quote(t2)}], {quote(newt)}, is_retained={py_bool(retained)})"
        )

    def _handle_VerticalSplit(self, argstr: str, rhs: str, original: str):
        # VerticalSplit(SourceTable, is_retained):t1(attr,...),t2(attr,...)[, t1(pk...), t2(pk...)]
        args = [a.strip() for a in split_top_level(argstr)]
        if not args:
            self.add(f"# Malformed VerticalSplit: {original}")
            return
        old_table = args[0]
        retained = False
        if len(args) >= 2:
            b = parse_bool(args[1])
            if b is not None:
                retained = b
        # Parse rhs list of definitions
        defs = [x for x in split_top_level(rhs) if x]
        colmap: dict[str, List[str]] = {}
        pkmap: dict[str, List[str]] = {}
        for d in defs:
            m = re.match(r"^(\w+)\s*\((.*)\)$", d.strip())
            if not m:
                self.add(f"# Malformed VerticalSplit item: {d}")
                continue
            tname, cols = m.group(1), m.group(2).strip()
            cols_list = [c.strip() for c in split_top_level(cols) if c.strip()]
            # If first time seeing table, treat as column list; if seen, treat as pk list
            if tname not in colmap:
                colmap[tname] = cols_list
            else:
                pkmap[tname] = cols_list

        new_tables = list(colmap.keys())
        # Default view name
        view_name = f"view_{old_table}"
        self.imports.add('from rewrite.TableSplit import TableSplit')
        var = self.new_op()
        self.add(f"# {original}")
        self.add(
            f"{var} = TableSplit({quote(old_table)}, {new_tables}, {colmap}, {pkmap or {}}, new_view={quote(view_name)}, is_retained={py_bool(retained)})"
        )

    def _handle_RedundantColumnAdd(self, argstr: str, rhs: str, original: str):
        # RedundantColumnAdd(SourceTable.Column, TargetTable[.NewCol]?)
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) < 2:
            self.add(f"# Malformed RedundantColumnAdd: {original}")
            return
        m1 = re.match(r"^(\w+)\.(\w+)$", args[0])
        m2 = re.match(r"^(\w+)(?:\.(\w+))?$", args[1])
        if not (m1 and m2):
            self.add(f"# Malformed RedundantColumnAdd args: {argstr}")
            return
        s_table, s_col = m1.group(1), m1.group(2)
        t_table, new_col = m2.group(1), (m2.group(2) or s_col)
        self.imports.add('from rewrite.RedundantColumnAdd import RedundantColumnAdd')
        var = self.new_op()
        self.add(f"# {original}")
        self.add(f"# TODO: provide proper join_keys: list of (source_key, target_key)")
        self.add(
            f"{var} = RedundantColumnAdd({quote(s_table)}, {quote(s_col)}, {quote(t_table)}, {quote(new_col)}, join_keys=[('SOURCE_KEY','TARGET_KEY')])"
        )

    def _handle_RedundantColumnDrop(self, argstr: str, rhs: str, original: str):
        # RedundantColumnDrop(Table.Column)
        args = [a.strip() for a in split_top_level(argstr)]
        if not args:
            self.add(f"# Malformed RedundantColumnDrop: {original}")
            return
        m = re.match(r"^(\w+)\.(\w+)$", args[0])
        if not m:
            self.add(f"# Malformed RedundantColumnDrop arg: {args[0]}")
            return
        t, c = m.group(1), m.group(2)
        self.imports.add('from rewrite.RedundantColumnDrop import RedundantColumnDrop')
        var = self.new_op()
        self.add(f"# {original}")
        self.add(f"{var} = RedundantColumnDrop({quote(t)}, {quote(c)})")

    def _handle_ColumnRename(self, argstr: str, rhs: str, original: str):
        # ColumnRename(SourceTable.OldColumnName, NewColumnName)
        args = [a.strip() for a in split_top_level(argstr)]
        if len(args) != 2:
            self.add(f"# Malformed ColumnRename: {original}")
            return
        m = re.match(r"^(\w+)\.(\w+)$", args[0])
        if not m:
            self.add(f"# Malformed ColumnRename arg: {args[0]}")
            return
        table, old_col = m.group(1), m.group(2)
        new_col = args[1]
        self.imports.add('from rewrite.ColumnRename import ColumnRename')
        var = self.new_op()
        self.add(f"# {original}")
        self.add(f"{var} = ColumnRename({quote(table)}, {quote(old_col)}, {quote(new_col)})")


def generate_code(gen: GenResult) -> str:
    out: List[str] = []
    # Header imports
    for imp in sorted(gen.imports):
        out.append(imp)
    if gen.imports:
        out.append("")
    # Emit operations
    out.extend(gen.lines)
    out.append("")
    out.append("# Example execution:")
    out.append("# from DataBase.MySQLDriver import MySQLDriver")
    out.append("# db = MySQLDriver({'user':'root','password':'***','database':'tpcch'}); db.connect()")
    out.append("# sql = 'SELECT 1'  # placeholder")
    out.append("# for op in [" + ", ".join([f'op{i+1}' for i in range(sum(1 for l in gen.lines if l.startswith('op')))]) + "]:")
    out.append("#     # op.apply_to_schema(db)")
    out.append("#     # new_sql = op.apply_to_sql(sql)")
    return "\n".join(out)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description='Convert operation sequence text into standardized rewrite calls.')
    ap.add_argument('-i', '--input', required=True, help='Path to input text file with operation sequence')
    ap.add_argument('-o', '--output', help='Path to write the generated Python code (default: stdout)')
    args = ap.parse_args(argv)

    with open(args.input, 'r', encoding='utf-8') as f:
        text = f.read()

    conv = OpSeqConverter()
    gen = conv.convert(text)
    code = generate_code(gen)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(code)
    else:
        print(code)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
