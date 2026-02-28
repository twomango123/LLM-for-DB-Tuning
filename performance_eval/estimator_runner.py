from __future__ import annotations
import json
from typing import Dict, Any, List, Optional

from .ops import parse_ops, Op
from .transform import type1, type2_remove_join, type2_add_join, type3_prune_filters
from .breakdown import plan_cost_breakdown, diff_breakdown, summarize_delta


def run_ops_on_plan(plan_text: str, ops_text: str,
                    prune_patterns: Optional[List[str]] = None,
                    regex: bool = False,
                    combine: str = 'product',
                    cols_factor: float = 1.0) -> Dict[str, Any]:
    """
    将“操作序列”映射为对 EXPLAIN 计划的估算改写，按顺序累计：
      - TableJoin(t1,t2,k1,k2, keep_old):
          不保留旧表（keep_old=False）：等价于“移除 t1,t2 之间的 JOIN，替换为 new_table 的扫描” → 类型2.remove-join
          保留旧表：保守不改（或后续再补成视图）；
      - VerticalSplit(table, keep_old):
          仅作为提示，不直接改计划（真实分裂由 rewrite 完成）；
      - RedundantColumnAdd(src.col → tgt.col, join on k):
          估算上相当于“去除对 src 表/列的读取，将相关过滤/排序/连接落到 tgt”，此处简化为类型1在 tgt 上不变、同时类型3对典型 where 谓词进行消除（若给出 patterns）。

    返回：包含每步后的 new_plan_text 与累计 new_total_cost。
    """
    steps: List[Dict[str, Any]] = []
    cur_plan = plan_text
    cur_total = None
    cur_bd = plan_cost_breakdown(cur_plan)

    saw_rca = False
    applied_prune = False
    for op in parse_ops(ops_text):
        if op.kind == 'TableJoin':
            if not op.args.get('keep_old'):
                # remove join(old_tables) -> new_table
                res = type2_remove_join(cur_plan,
                                        old_tables=[op.args['t1'], op.args['t2']],
                                        new_table=op.args['new_table'],
                                        new_table_rows=0,  # 若未知，置0只影响 new_total 的加和
                                        cols_factor=1.0)
                before_bd = cur_bd
                cur_plan = res['new_plan_text']
                cur_total = res['new_total_cost']
                cur_bd = plan_cost_breakdown(cur_plan)
                delta_bd = diff_breakdown(before_bd, cur_bd)
                steps.append({'op': op.kind, 'args': op.args, 'result': res, 'breakdown': delta_bd, 'summary': summarize_delta(delta_bd)})
            else:
                steps.append({'op': op.kind, 'args': op.args, 'result': {'note': 'keep_old=True，不改计划'}})
            continue

        if op.kind == 'VerticalSplit':
            # 仅记录：真实影响由 schema 变更体现；估算侧暂不直接调整
            steps.append({'op': op.kind, 'args': op.args, 'result': {'note': 'VerticalSplit 仅记录，不改计划'}})
            continue

        if op.kind == 'RedundantColumnAdd':
            # 标记：若提供了 prune_patterns，则在本步后尝试做一次 Filter Pruning
            saw_rca = True
            if prune_patterns:
                before_bd = cur_bd
                res = type3_prune_filters(cur_plan,
                                          patterns=prune_patterns,
                                          regex=regex,
                                          combine=combine,
                                          cols_factor=cols_factor)
                cur_plan = res['new_plan_text']
                cur_total = res['new_total_cost']
                cur_bd = plan_cost_breakdown(cur_plan)
                delta_bd = diff_breakdown(before_bd, cur_bd)
                steps.append({'op': op.kind, 'args': op.args, 'result': res, 'breakdown': delta_bd, 'summary': summarize_delta(delta_bd)})
                applied_prune = True
            else:
                steps.append({'op': op.kind, 'args': op.args, 'result': {'note': '已记录，未提供 prune patterns'}})
            continue

    # 若提供了 prune patterns 但未在上面的分支中应用（例如只有 HorizontalSplit 但没有 RedundantColumnAdd），
    # 则在末尾执行一次通用的过滤剪枝，以体现“水平分片带来的 Filter 消除”。
    if prune_patterns and not applied_prune:
        before_bd = cur_bd
        res = type3_prune_filters(cur_plan,
                                  patterns=prune_patterns,
                                  regex=regex,
                                  combine=combine,
                                  cols_factor=cols_factor)
        cur_plan = res['new_plan_text']
        cur_total = res['new_total_cost']
        cur_bd = plan_cost_breakdown(cur_plan)
        delta_bd = diff_breakdown(before_bd, cur_bd)
        steps.append({'op': 'Type3Prune', 'args': {'patterns': prune_patterns}, 'result': res, 'breakdown': delta_bd, 'summary': summarize_delta(delta_bd)})

    return {'final_plan_text': cur_plan, 'final_total_cost': cur_total, 'steps': steps}


def run_ops_cli():
    import argparse
    p = argparse.ArgumentParser(description='根据操作序列对 EXPLAIN 做估算改写')
    p.add_argument('--plan', required=True)
    p.add_argument('--ops-file', required=True, help='包含操作序列的文本文件')
    p.add_argument('--prune-patterns', nargs='*', default=None, help='用于类型3的过滤匹配模式（可多项）')
    p.add_argument('--regex', action='store_true', default=False)
    p.add_argument('--combine', choices=['product', 'min'], default='product')
    p.add_argument('--cols-factor', type=float, default=1.0)
    p.add_argument('--output', default='-')
    args = p.parse_args()

    with open(args.plan, 'r', encoding='utf-8') as f:
        plan_text = f.read()
    with open(args.ops_file, 'r', encoding='utf-8') as f:
        ops_text = f.read()

    res = run_ops_on_plan(plan_text, ops_text,
                          prune_patterns=args.prune_patterns,
                          regex=args.regex,
                          combine=args.combine,
                          cols_factor=args.cols_factor)
    out = json.dumps(res, ensure_ascii=False, indent=2)
    if args.output == '-':
        print(out)
    else:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(out)


if __name__ == '__main__':
    run_ops_cli()
