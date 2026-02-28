from __future__ import annotations
from typing import Optional, Tuple
from .plan import PlanNode
import math


def safe(v: Optional[float], default: float=0.0) -> float:
    return v if v is not None else default


def width_scaled(cost: float, cols_factor: float, applies: bool) -> float:
    return cost * (cols_factor if applies else 1.0)


def infer_selectivity(output_rows: Optional[float], input_rows: Optional[float]) -> float:
    o = safe(output_rows, 0.0)
    i = max(safe(input_rows, 0.0), 1e-9)
    return max(min(o / i, 1.0), 0.0)


def infer_join_selectivity(out_rows: Optional[float], left_rows: Optional[float], right_rows: Optional[float]) -> float:
    o = safe(out_rows, 0.0)
    l = max(safe(left_rows, 0.0), 1e-9)
    r = max(safe(right_rows, 0.0), 1e-9)
    s = o / (l * r)
    if s <= 0:
        s = 1e-9
    return s


def model_table_scan(n: PlanNode, row_factor: float, cols_factor: float) -> Tuple[float, float]:
    old_rows = safe(n.rows, 0.0)
    new_rows = max(old_rows * row_factor, 0.0)
    old_cost = safe(n.cost, old_rows)
    # Scale by rows and width
    new_cost = width_scaled(old_cost * (new_rows / max(old_rows, 1e-9)), cols_factor, True)
    return new_rows, new_cost


def model_index_scan(n: PlanNode, row_factor: float, sel_factor: float) -> Tuple[float, float]:
    """
    IndexScan cost model (row-store, simplified): cost = Tx * Sx
    - Approximate Tx by scaling original output rows by row_factor (table size change)
    - Apply selectivity change sel_factor to Sx
    - Width is ignored for covered index access; non-covered accesses are represented as separate lookups upstream
    """
    old_rows = safe(n.rows, 0.0)
    # Output rows scale with both table cardinality and selectivity changes
    new_rows = max(old_rows * row_factor * sel_factor, 0.0)
    old_cost = safe(n.cost, old_rows)
    # Cost proportional to output rows under this model
    new_cost = old_cost * (new_rows / max(old_rows, 1e-9))
    return new_rows, new_cost


def model_filter(n: PlanNode, child_rows_new: float, filter_factor: float) -> float:
    sel = infer_selectivity(n.rows, n.children[0].rows if n.children else None)
    new_sel = max(min(sel * filter_factor, 1.0), 0.0)
    return child_rows_new * new_sel


def model_hash_group(n: PlanNode, child_rows_new: float, cols_factor: float) -> Tuple[float, float]:
    # Cost ~ T + G (spec). G inferred from original ratio
    g_ratio = infer_selectivity(n.rows, n.children[0].rows if n.children else None)
    new_groups = child_rows_new * g_ratio
    new_cost = width_scaled(child_rows_new + new_groups, cols_factor, True)
    return new_groups, new_cost


def model_sort_group(n: PlanNode, child_rows_new: float, cols_factor: float) -> Tuple[float, float]:
    g_ratio = infer_selectivity(n.rows, n.children[0].rows if n.children else None)
    new_groups = child_rows_new * g_ratio
    new_cost = width_scaled(child_rows_new + new_groups, cols_factor, True)
    return new_groups, new_cost


def model_sort(n: PlanNode, child_rows_new: float, cols_factor: float) -> float:
    # Approximate with R log2 R and width scaling; fallback to proportional if no rows
    r = max(child_rows_new, 1.0)
    base = r * max(math.log2(r), 1.0)
    old = max(safe(n.cost, base), 1.0)
    scale = base / max(base, 1.0)
    # If we had an original cost, scale by ratio of new/old rows using fallback rule
    if n.cost is not None and n.children and n.children[0].rows:
        c1 = n.children[0].rows
        c2 = child_rows_new
        new_cost = width_scaled(n.cost * (c2 / max(c1, 1e-9)), cols_factor, True)
    else:
        new_cost = width_scaled(old, cols_factor, True)
    return new_cost


def model_hash_join(n: PlanNode, left_rows_new: float, right_rows_new: float, join_sel: Optional[float], cols_factor: float) -> Tuple[float, float]:
    # cost = Tx + Ty + T ; approximate Tx,Ty by children costs if present else use rows
    out_rows_new = left_rows_new * right_rows_new * (join_sel if join_sel is not None else infer_join_selectivity(n.rows, n.children[0].rows if n.children else None, n.children[1].rows if len(n.children) > 1 else None))
    # children costs
    left_cost = safe(n.children[0].new_cost if n.children and n.children[0].new_cost is not None else n.children[0].cost if n.children else None, left_rows_new)
    right_cost = safe(n.children[1].new_cost if len(n.children) > 1 and n.children[1].new_cost is not None else n.children[1].cost if len(n.children) > 1 else None, right_rows_new)
    new_cost = width_scaled(left_cost + right_cost + out_rows_new, cols_factor, True)
    return out_rows_new, new_cost


def model_nested_loop(n: PlanNode, left_rows_new: float, right_rows_new: float, out_rows_new: Optional[float]) -> float:
    # cost = Tx(left) + Tx*Ty + T0
    left_cost = safe(n.children[0].new_cost if n.children and n.children[0].new_cost is not None else n.children[0].cost if n.children else None, left_rows_new)
    base = left_cost + (left_rows_new * right_rows_new) + safe(out_rows_new, 0.0)
    return base


def fallback_scale(n: PlanNode, input_c2: float, input_c1: float, cols_factor: float, width_sensitive: bool) -> float:
    base = safe(n.cost, 0.0)
    scaled = base * (input_c2 / max(input_c1, 1e-9)) if base > 0 else 0.0
    return width_scaled(scaled, cols_factor, width_sensitive)
