#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pure-offline evaluator for a given ops file using PART2_DEBUG EXPLAIN dumps.

Usage:
  python3 scripts/offline_eval_ops.py \
    --ops-file response/replies/response_final.txt \
    --sql-dir output_dir/sql \
    --debug-dir part2_debug \
    --out response/offline_eval.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from auto_loop import _static_cost_eval_explain  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser(description='Offline EXPLAIN-based evaluation for a given ops file.')
    ap.add_argument('--ops-file', required=True, help='Operations text file path')
    ap.add_argument('--sql-dir', required=True, help='Original SQL directory (for pair extraction)')
    ap.add_argument('--debug-dir', default='part2_debug', help='PART2_DEBUG directory with explain/ and index_map.json')
    ap.add_argument('--out', help='Output JSON path (optional)')
    args = ap.parse_args()

    ops = Path(args.ops_file).read_text(encoding='utf-8')
    pred_ms, details = _static_cost_eval_explain(args.debug_dir, args.sql_dir, ops)
    result = {
        'pred_ms': pred_ms,
        'details': details,
    }
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding='utf-8')
        print(f"[OK] 写入 {args.out}")
    else:
        print(text)


if __name__ == '__main__':
    main()

