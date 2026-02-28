#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate per-step EXPLAIN plan texts with recalculated costs after applying
LLM-proposed rewrite operations. Writes all intermediate plans to files.

- Reads operations from --ops-file (default: response/replies/response_final.txt)
- Reads baseline plans from --plan-dir (default: part2_debug/explain)
- Uses meta.json and samples.json for cardinality/width/selectivity hints
- Emits per-query step outputs under --out-dir (default: response/perf_fb/intermediate)

Each output file contains:
  - Baseline total cost + full baseline plan
  - For each op step: op name, step delta, note, non_scalable (if any),
    and the full plan text after this step

Requires: performance_eval.eval_bridge.run_eval_sequence and response.runner
parsing helpers (already in this repo).
"""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List


def load_ops(ops_path: str) -> List[Any]:
    import importlib
    # Reuse parsing and instantiation from response/runner.py
    runner = importlib.import_module('response.runner')
    raw = Path(ops_path).read_text(encoding='utf-8')
    ops: List[Any] = []
    for ln in raw.splitlines():
        s = ln.strip()
        if not s or s.startswith('#'):
            continue
        for po in runner.parse_line_to_ops(s):
            inst = runner.instantiate_for_sql_rewrite(None, po)
            if inst is not None:
                ops.append(inst)
    return ops


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--ops-file', default='response/replies/response_final.txt')
    ap.add_argument('--plan-dir', default='part2_debug/explain')
    ap.add_argument('--meta', default='output_dir/meta.json')
    ap.add_argument('--samples', default='response/samples/samples.json')
    ap.add_argument('--out-dir', default='response/perf_fb/intermediate')
    args = ap.parse_args()

    # Ensure repo root on sys.path for local package imports
    import sys
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.append(str(repo_root))
    from performance_eval.plan import compute_total_cost, parse_plan
    from performance_eval.eval_bridge import run_eval_sequence

    plan_dir = Path(args.plan_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load ops
    ops = load_ops(args.ops_file)
    if not ops:
        raise SystemExit(f'No operations parsed from {args.ops_file}')

    # Load union sample for HorizontalSplit/Merge scaling
    extras_base: Dict[str, Any] = {}
    try:
        smp = json.loads(Path(args.samples).read_text(encoding='utf-8'))
        u = smp.get('union') or {}
        if u:
            extras_base['union'] = {
                'sample_cost': u.get('append_cost_med', 0.0),
                'sample_rows': u.get('total_rows_med', 1.0),
            }
    except Exception:
        pass

    # If original SQLs exist (sibling sql/), attach to extras to improve alias mapping
    sql_dir = plan_dir.parent / 'sql'
    has_sql = sql_dir.exists()

    def _op_target_summary(op: Any) -> str:
        try:
            cname = op.__class__.__name__
            if cname == 'TableJoin':
                t1, t2 = op.old_tables[0], op.old_tables[1]
                keep_old = bool(getattr(op, 'sign', 1) != 1)
                newt = getattr(op, 'new_table', None)
                jk = getattr(op, 'join_key', None)
                desc = f"TableJoin: {t1} ⨝ {t2}"
                if jk:
                    desc += f" on {jk}"
                if newt:
                    desc += f" -> {newt}"
                desc += f" (keep_old={keep_old})"
                return desc
            if cname in ('TableSplit', 'VerticalSplit'):
                return f"VerticalSplit: {op.old_table} -> {', '.join(op.new_tables)} (is_retained={getattr(op, 'is_retained', False)})"
            if cname == 'HorizontalSplit':
                parts = ', '.join([f"{t}({pred})" for t, pred in (getattr(op, 'predicates', []) or [])])
                return f"HorizontalSplit: {op.table} -> {parts} (is_retained={getattr(op, 'is_retained', False)})"
            if cname == 'HorizontalMerge':
                srcs = getattr(op, 'sources', []) or []
                newt = getattr(op, 'new_table', '')
                return f"HorizontalMerge: {srcs[0]} ∪ {srcs[1]} -> {newt} (is_retained={getattr(op, 'is_retained', False)})"
            if cname == 'RedundantColumnAdd':
                st, sc = op.source_table, op.source_column
                tt, nc = op.target_table, op.new_column
                jk = getattr(op, 'join_keys', None)
                s = f"RedundantColumnAdd: {st}.{sc} -> {tt}.{nc}"
                if jk:
                    s += f" via {jk}"
                return s
            if cname == 'RedundantColumnDrop':
                t = getattr(op, 'target_table', None) or getattr(op, 'table', None)
                c = getattr(op, 'redundant_column', None) or getattr(op, 'column', None)
                return f"RedundantColumnDrop: {t}.{c}"
            return cname
        except Exception:
            return str(getattr(op, '__class__', type(op)))

    index: Dict[str, Any] = {}
    for fn in sorted(plan_dir.glob('q*.txt')):
        plan_text = fn.read_text(encoding='utf-8')
        # Compose per-plan extras
        extras = dict(extras_base)
        if has_sql:
            try:
                qid = int(''.join(ch for ch in fn.name if ch.isdigit()))
                sp = sql_dir / f'q{qid}.sql'
                if sp.exists():
                    extras['sql_text'] = sp.read_text(encoding='utf-8', errors='ignore')
            except Exception:
                pass

        res = run_eval_sequence(plan_text, ops, meta_path=args.meta, extras=extras)

        # Build full text output with all intermediate plans
        lines: List[str] = []
        before = res.get('before_total_cost')
        lines.append(f'=== {fn.name} Baseline Total Cost: {before} ===')
        lines.append(plan_text.rstrip())
        lines.append('')
        # step-by-step
        steps = res.get('steps') or []
        cur_step = 1
        running_after = before
        steps_meta: List[Dict[str, Any]] = []
        for i, st in enumerate(steps):
            opn = st.get('op')
            r = st.get('result') or {}
            note = r.get('note')
            non_scalable = r.get('non_scalable')
            after_total = st.get('after_total_cost') if st.get('after_total_cost') is not None else running_after
            delta_step = st.get('delta_step')
            new_plan_text = (r.get('new_plan_text') or '').rstrip()
            target_desc = _op_target_summary(ops[i]) if i < len(ops) else None
            lines.append(f'--- Step {cur_step}: {opn} | after_total_cost={after_total} | delta_step={delta_step} ---')
            if target_desc:
                lines.append(f'Target: {target_desc}')
            if note:
                lines.append(f'Note: {note}')
            if non_scalable:
                try:
                    lines.append('Non-Scalable Terms: ' + json.dumps(non_scalable, ensure_ascii=False))
                except Exception:
                    lines.append(f'Non-Scalable Terms: {non_scalable}')
            if new_plan_text:
                lines.append(new_plan_text)
            lines.append('')
            cur_step += 1
            running_after = after_total
            # collect concise step meta for index.json
            steps_meta.append({
                'op': opn,
                'after_total_cost': after_total,
                'delta_step': delta_step,
                'target': target_desc,
                'note': note,
            })

        # Final plan snapshot
        final_plan_text = res.get('final_plan_text') or ''
        lines.append(f'=== {fn.name} Final Total Cost: {res.get("after_total_cost")} (delta={res.get("delta")}) ===')
        if final_plan_text and final_plan_text.strip() != plan_text.strip():
            lines.append(final_plan_text.rstrip())
        # Write file
        (out_dir / f'{fn.stem}_steps.txt').write_text('\n'.join(lines), encoding='utf-8')
        index[fn.name] = {
            'before_total_cost': res.get('before_total_cost'),
            'after_total_cost': res.get('after_total_cost'),
            'delta': res.get('delta'),
            'steps': steps_meta,
        }

    # Write index JSON for quick overview
    (out_dir / 'index.json').write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
