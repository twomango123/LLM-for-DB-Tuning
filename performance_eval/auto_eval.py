from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

from .estimator_runner import run_ops_on_plan
from .breakdown import summarize_delta


def load_ops_text(p: Path) -> str:
    return p.read_text(encoding='utf-8')


def main():
    ap = argparse.ArgumentParser(description='Auto-eval: 从操作序列自动对一批 EXPLAIN 做性能评估')
    ap.add_argument('--ops-file', default='response/response.txt')
    ap.add_argument('--plan-dir', default='part2_debug/explain')
    ap.add_argument('--prune-patterns', nargs='*', default=None)
    ap.add_argument('--regex', action='store_true', default=False)
    ap.add_argument('--combine', choices=['product', 'min'], default='product')
    ap.add_argument('--cols-factor', type=float, default=1.0)
    ap.add_argument('--out-json', default='response/auto_eval.json')
    ap.add_argument('--out-md', default='response/auto_eval.md')
    args = ap.parse_args()

    ops_path = Path(args.ops_file)
    plan_dir = Path(args.plan_dir)
    if not ops_path.exists():
        raise SystemExit(f'未找到操作序列文件: {ops_path}')
    if not plan_dir.exists():
        raise SystemExit(f'未找到计划目录: {plan_dir}')

    ops_text = load_ops_text(ops_path)

    results: Dict[str, Any] = {}
    for p in sorted(plan_dir.glob('q*.txt')):
        name = p.name
        plan_text = p.read_text(encoding='utf-8')
        res = run_ops_on_plan(plan_text, ops_text,
                              prune_patterns=args.prune_patterns,
                              regex=args.regex,
                              combine=args.combine,
                              cols_factor=args.cols_factor)
        # 将每一步的 summary 汇入表格备注
        note_parts = []
        for st in res.get('steps') or []:
            s = st.get('summary')
            if s and s != '无显著变化':
                note_parts.append(f"[{st.get('op')}] {s}")
        res['_note'] = '；'.join(note_parts[:3])  # 只取前三条以免过长
        results[name] = res

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Markdown 汇总
    lines: List[str] = []
    lines.append('# Auto Eval Summary')
    lines.append('')
    lines.append('| Plan | Final Total Cost | Note |')
    lines.append('|---|---:|---|')
    for name, res in results.items():
        cost = res.get('final_total_cost')
        note = res.get('_note', '')
        lines.append(f'| {name} | {cost if cost is not None else ""} | {note} |')
    Path(args.out_md).write_text('\n'.join(lines), encoding='utf-8')
    print(f'OK. 写入 {args.out_json} 与 {args.out_md}')


if __name__ == '__main__':
    main()
