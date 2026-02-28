from __future__ import annotations
from typing import Dict
from .plan import parse_plan


CATEGORY_MAP = {
    'nested_loop': 'join',
    'hash_join': 'join',
    'filter': 'filter',
    'table_scan': 'scan',
    'index_scan': 'scan',
    'index_lookup': 'scan',
    'group_temp': 'group',
    'group_agg': 'group',
    'sort': 'sort',
}


def plan_cost_breakdown(plan_text: str) -> Dict[str, float]:
    """Summarize costs by operator category from an EXPLAIN-like text."""
    if not plan_text:
        return {}
    nodes = parse_plan(plan_text)
    acc: Dict[str, float] = {}
    total = 0.0
    for n in nodes:
        c = n.cost or 0.0
        total += c
        cat = CATEGORY_MAP.get(n.type, 'other')
        acc[cat] = acc.get(cat, 0.0) + c
    acc['total'] = total
    return acc


def diff_breakdown(before: Dict[str, float], after: Dict[str, float]) -> Dict[str, float]:
    keys = set(before.keys()) | set(after.keys())
    return {k: after.get(k, 0.0) - before.get(k, 0.0) for k in sorted(keys)}


def summarize_delta(delta: Dict[str, float]) -> str:
    parts = []
    for k in ('join', 'filter', 'scan', 'group', 'sort'):
        v = delta.get(k, 0.0)
        if abs(v) > 1e-6:
            sign = '减少' if v < 0 else '增加'
            parts.append(f"{k} {sign} {abs(v):.3g}")
    if not parts:
        return "无显著变化"
    return "，".join(parts)

