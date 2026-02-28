#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Offline apply_to_sql tester with nested operation support.

- Reads an operation sequence from a text file (same format as response/replies/*).
- Instantiates rewrite ops and applies them step-by-step to a directory of SQL files.
- Only SELECT statements are rewritten (read-only), others are copied verbatim.
- Each step writes to response/rewritten_sql_offline/step_XX_<Kind>/.

Usage examples:
  python3 scripts/offline_apply_to_sql_test.py \
    --ops-file response/replies/response_m3_r1.txt \
    --in-sql-dir output_dir/sql \
    --out-base response/rewritten_sql_offline

Additionally, a simple ColumnSplit synthetic test is provided via --add-column-split.
"""

from __future__ import annotations

import os
import sys
import argparse
from typing import List

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.append(ROOT)

# Reuse the parser + instantiator from response/runner.py
from response.runner import parse_line_to_ops, instantiate_for_sql_rewrite


def _is_select_only(sql_text: str) -> bool:
    # crude but robust: find first non-comment token and check it starts with SELECT
    s = sql_text.lstrip()
    # remove leading SQL comments
    while True:
        if s.startswith('--'):
            nl = s.find('\n')
            if nl == -1:
                s = ''
                break
            s = s[nl + 1 :].lstrip()
            continue
        if s.startswith('/*'):
            end = s.find('*/')
            if end == -1:
                s = ''
                break
            s = s[end + 2 :].lstrip()
            continue
        break
    return s.lower().startswith('select')


def _rewrite_file_if_select(op, src_path: str, dst_path: str) -> tuple[bool, str]:
    """Rewrite file only if it is a SELECT-only SQL; returns (changed, reason)."""
    with open(src_path, 'r', encoding='utf-8', errors='ignore') as f:
        sql = f.read()
    if not _is_select_only(sql):
        # copy verbatim
        with open(dst_path, 'w', encoding='utf-8') as f:
            f.write(sql)
        return False, 'skipped-non-select'
    try:
        new_sql = op.apply_to_sql(sql)
    except Exception:
        # best-effort: keep original content for visibility
        new_sql = sql
    with open(dst_path, 'w', encoding='utf-8') as f:
        f.write(new_sql)
    return (new_sql != sql), ('rewritten' if new_sql != sql else 'unchanged')


def apply_step(op, in_dir: str, out_dir: str) -> dict:
    stats = {'total': 0, 'rewritten': 0, 'unchanged': 0, 'skipped': 0}
    for root, _, files in os.walk(in_dir):
        rel = os.path.relpath(root, in_dir)
        troot = os.path.join(out_dir, rel) if rel != '.' else out_dir
        os.makedirs(troot, exist_ok=True)
        for fn in files:
            src = os.path.join(root, fn)
            dst = os.path.join(troot, fn)
            if not fn.lower().endswith('.sql'):
                # copy non-sql
                try:
                    import shutil
                    shutil.copy2(src, dst)
                except Exception:
                    pass
                continue
            stats['total'] += 1
            changed, reason = _rewrite_file_if_select(op, src, dst)
            if reason == 'skipped-non-select':
                stats['skipped'] += 1
            elif changed:
                stats['rewritten'] += 1
            else:
                stats['unchanged'] += 1
    return stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--ops-file', required=True, help='Path to operations txt (e.g., response/replies/response_m3_r1.txt)')
    ap.add_argument('--in-sql-dir', required=True, help='Input directory of SQL files (read-only queries expected)')
    ap.add_argument('--out-base', default='response/rewritten_sql_offline', help='Base directory for step outputs')
    ap.add_argument('--add-column-split', action='store_true', help='Append a synthetic ColumnSplit test for customers.customer_name')
    args = ap.parse_args()

    # Parse ops
    raw_lines = [ln for ln in open(args.ops_file, 'r', encoding='utf-8').read().splitlines() if ln.strip()]
    from response.runner import ParsedOp
    ops: List[ParsedOp] = []
    for ln in raw_lines:
        ops.extend(parse_line_to_ops(ln))

    # Instantiate rewrite operators (SQL-only)
    inst_ops = []
    for po in ops:
        op = instantiate_for_sql_rewrite(None, po)
        if op is None:
            continue
        inst_ops.append((po.kind, op))

    # Optional: synthetic ColumnSplit for readability testing
    if args.add_column_split:
        try:
            from rewrite.ColumnSplit import ColumnSplit
            inst_ops.append((
                'ColumnSplit',
                ColumnSplit(
                    table='customers',
                    old_column='customer_name',
                    new_columns=['customer_first', 'customer_last'],
                    split_delimiter=' '
                ),
            ))
        except Exception:
            pass

    # Run nested steps
    os.makedirs(args.out_base, exist_ok=True)
    cur_in = args.in_sql_dir
    for i, (kind, op) in enumerate(inst_ops, start=1):
        step_dir = os.path.join(args.out_base, f"step_{i:02d}_{kind}")
        stats = apply_step(op, cur_in, step_dir)
        print(f"[step {i:02d} {kind}] total={stats['total']} rewritten={stats['rewritten']} unchanged={stats['unchanged']} skipped={stats['skipped']}")
        # feed next step from previous step output
        cur_in = step_dir


if __name__ == '__main__':
    main()

