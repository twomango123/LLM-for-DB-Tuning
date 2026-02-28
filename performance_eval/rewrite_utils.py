from __future__ import annotations
"""
Helpers for bridging rewrite ops and the performance_eval plan transformers.

This module provides:
- Lightweight meta accessors (row counts, avg row width)
- Scaling helpers for UNION ALL and JOIN (hash/nested) based on sample costs
- Thin wrappers over transform.type1/2/3 for convenience

All functions are pure and safe to call without a database connection.
"""
from typing import Dict, Any, Optional, Tuple, List
import json

from .transform import type1 as _type1
from .transform import type2_remove_join as _type2_remove_join
from .transform import type2_add_join as _type2_add_join
from .transform import type3_prune_filters as _type3_prune_filters


def load_meta(meta_path: Optional[str]) -> Dict[str, Any]:
    if not meta_path:
        return {"tables": {}, "stats": {}}
    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {"tables": {}, "stats": {}}


def table_rows(meta: Dict[str, Any], table: str) -> Optional[float]:
    t = (meta.get('tables') or {}).get(table)
    if not t:
        return None
    rc = t.get('row_count')
    try:
        return float(rc) if rc is not None else None
    except Exception:
        return None


def avg_row_width(meta: Dict[str, Any], table: str) -> Optional[float]:
    t = (meta.get('tables') or {}).get(table)
    if not t:
        return None
    cols = (t.get('columns') or {})
    if not cols:
        return None
    total = 0.0
    for c, m in cols.items():
        try:
            total += float(m.get('avg_length') or 0.0)
        except Exception:
            continue
    return total if total > 0 else None


def column_avg_length(meta: Dict[str, Any], table: str, column: str) -> Optional[float]:
    t = (meta.get('tables') or {}).get(table)
    if not t:
        return None
    cols = (t.get('columns') or {})
    m = cols.get(column)
    if not m:
        return None
    try:
        return float(m.get('avg_length') or 0.0)
    except Exception:
        return None


def width_from_columns(meta: Dict[str, Any], table: str, columns: List[str]) -> Optional[float]:
    if not columns:
        return None
    total = 0.0
    for c in columns:
        v = column_avg_length(meta, table, c)
        if v is None:
            continue
        total += v
    return total if total > 0 else None


def load_samples(path: str = 'response/samples/samples.json') -> Dict[str, Any]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def join_selectivity_from_samples(samples: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    """
    Return (S, sample_left, sample_right, sample_out) where
    S = sample_out / (sample_left * sample_right)
    """
    j = (samples or {}).get('join') or {}
    sl = j.get('left_rows_med')
    sr = j.get('right_rows_med')
    so = j.get('join_rows_med')
    try:
        sl = float(sl) if sl is not None else None
        sr = float(sr) if sr is not None else None
        so = float(so) if so is not None else None
    except Exception:
        return None
    if not (sl and sr and so):
        return None
    S = max(min(so / max(sl * sr, 1e-9), 1.0), 1e-12)
    return (S, sl, sr, so)


def union_cost_linear(sample_cost: float,
                      sample_total_rows: float,
                      left_rows: float,
                      right_rows: float) -> float:
    """Scale UNION ALL merge cost linearly by total input rows.

    When sample_total_rows<=0, fall back to left+right.
    """
    total = max(left_rows + right_rows, 0.0)
    denom = sample_total_rows if sample_total_rows and sample_total_rows > 0 else max(total, 1.0)
    return sample_cost * (total / denom)


def hash_join_cost_from_rows(left_rows: float, right_rows: float,
                             sample_left_rows: float,
                             sample_right_rows: float,
                             sample_out_rows: float) -> float:
    """Estimate HashJoin cost using selectivity inferred from sample.
    cost = Tx + Ty + T, where T = left*right*sel.
    sel_sample = sample_out / (sample_left * sample_right) (guarded).
    """
    sl = max(sample_left_rows, 1e-9)
    sr = max(sample_right_rows, 1e-9)
    sel = max(sample_out_rows, 0.0) / (sl * sr)
    sel = max(sel, 1e-9)
    out = max(left_rows, 0.0) * max(right_rows, 0.0) * sel
    return max(left_rows, 0.0) + max(right_rows, 0.0) + out


def nested_join_cost_from_rows(left_rows: float, right_rows: float,
                               sample_left_rows: float,
                               sample_right_rows: float,
                               sample_out_rows: float) -> float:
    """Estimate NestedLoop cost using simplified model.
    cost = Tx(left) + Tx*Ty + T0 (use sample selectivity to infer T0 as out rows).
    """
    sl = max(sample_left_rows, 1e-9)
    sr = max(sample_right_rows, 1e-9)
    sel = max(sample_out_rows, 0.0) / (sl * sr)
    sel = max(sel, 1e-9)
    out = max(left_rows, 0.0) * max(right_rows, 0.0) * sel
    return max(left_rows, 0.0) + (max(left_rows, 0.0) * max(right_rows, 0.0)) + out


def apply_type1(plan_text: str,
                target_table: str,
                rows_factor: float = 1.0,
                cols_factor: float = 1.0,
                filter_factor: float = 1.0) -> Dict[str, Any]:
    return _type1(plan_text,
                  target_table=target_table,
                  rows_factor=rows_factor,
                  cols_factor=cols_factor,
                  filter_factor=filter_factor)


def remove_join(plan_text: str,
                old_tables: List[str],
                new_table: str,
                new_table_rows: Optional[float] = None,
                cols_factor: float = 1.0) -> Dict[str, Any]:
    return _type2_remove_join(plan_text,
                              old_tables=old_tables,
                              new_table=new_table,
                              new_table_rows=(new_table_rows or 0.0),
                              cols_factor=cols_factor)


def add_join(plan_text: str,
             replace_target: str,
             replace_rows_factor: float,
             add_table: str,
             add_rows: float,
             join_type: str = 'hash',
             join_sel: float = 1e-6,
             cols_factor: float = 1.0) -> Dict[str, Any]:
    return _type2_add_join(plan_text,
                           replace_target=replace_target,
                           replace_rows_factor=replace_rows_factor,
                           add_table=add_table,
                           add_rows=add_rows,
                           join_type=join_type,
                           join_sel=join_sel,
                           cols_factor=cols_factor)


def prune_filters(plan_text: str,
                  patterns: List[str],
                  regex: bool = False,
                  combine: str = 'product',
                  cols_factor: float = 1.0) -> Dict[str, Any]:
    return _type3_prune_filters(plan_text,
                                patterns=patterns,
                                regex=regex,
                                combine=combine,
                                cols_factor=cols_factor)
