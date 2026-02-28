from __future__ import annotations
"""
Unified cost model used by rewrite operators when re-evaluating plans.

This module centralizes operator cost formulas and provides thin wrappers
around the performance_eval transformers so all rewrite/* classes use the
same logic consistently.

Cost formulas (row-store, simplified), aligned with the user's spec:
1) TableScan 顺序扫描:            cost = Tx                       (Tx=扫描元组数)
2) IndexScan 索引扫描:           cost = Tx * Sx                  (Sx=选择率)
3) HashJoin 哈希连接:            T = Tx * Ty * S;  cost = Tx + Ty + T
4) MergeJoin 合并连接(已排序):    与 HashJoin 等同
5) Nested Loop Join:            cost = Tx + Tx * Ty + T0        (T0=输出行数)
6) Index Nested Loop Join:      cost = Tx + Tx * m + T0         (m=内表匹配数/行)
7) HashGroup 哈希分组:           cost = T + G                    (G=分组数)
8) SortGroup 排序分组:           cost = T + G

当基数发生变化：先仅缩放受影响表的“扫描代价”，随后基于新输入行数按上述公式自底向上重算上游算子成本；
不要对上层算子做简单比例缩放。

For plan-wide propagation, use CostModel.apply_type1(), which adjusts
the affected scans first, then recomputes interior nodes with the
formulas above (via performance_eval.transform.PlanAdjuster).
"""
from typing import Optional, Tuple, Dict, Any


# Re-export a few light helpers so rewrite/* can import from one place
try:  # runtime when used as a package
    from performance_eval.rewrite_utils import (
        load_meta as _load_meta,
        table_rows as _table_rows,
        avg_row_width as _avg_row_width,
        width_from_columns as _width_from_columns,
        load_samples as _load_samples,
        join_selectivity_from_samples as _join_sel_from_samples,
        union_cost_linear as _union_cost_linear,
        remove_join as _remove_join,
        add_join as _add_join,
        prune_filters as _prune_filters,
    )
    from performance_eval.plan import parse_plan, compute_total_cost
    from performance_eval.transform import PlanAdjuster
except Exception:  # fallback for direct execution
    from ..performance_eval.rewrite_utils import (
        load_meta as _load_meta,
        table_rows as _table_rows,
        avg_row_width as _avg_row_width,
        width_from_columns as _width_from_columns,
        load_samples as _load_samples,
        join_selectivity_from_samples as _join_sel_from_samples,
        union_cost_linear as _union_cost_linear,
        remove_join as _remove_join,
        add_join as _add_join,
        prune_filters as _prune_filters,
    )
    from ..performance_eval.plan import parse_plan, compute_total_cost
    from ..performance_eval.transform import PlanAdjuster


def load_meta(path: Optional[str]) -> Dict[str, Any]:
    return _load_meta(path)


def table_rows(meta: Dict[str, Any], table: str) -> Optional[float]:
    return _table_rows(meta, table)


def avg_row_width(meta: Dict[str, Any], table: str) -> Optional[float]:
    return _avg_row_width(meta, table)


def width_from_columns(meta: Dict[str, Any], table: str, columns: list[str]) -> Optional[float]:
    return _width_from_columns(meta, table, columns)


def load_samples(path: str = 'response/samples/samples.json') -> Dict[str, Any]:
    return _load_samples(path)


def join_selectivity_from_samples(samples: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    return _join_sel_from_samples(samples)


def union_cost_linear(sample_cost: float, sample_total_rows: float, left_rows: float, right_rows: float) -> float:
    return _union_cost_linear(sample_cost, sample_total_rows, left_rows, right_rows)


class CostModel:
    # ---------- atomic operator formulas ----------
    @staticmethod
    def table_scan_cost(Tx: float) -> float:
        return max(Tx, 0.0)

    @staticmethod
    def index_scan_cost(Tx: float, Sx: float) -> float:
        return max(Tx, 0.0) * max(min(Sx, 1.0), 0.0)

    @staticmethod
    def hash_join_cost(Tx: float, Ty: float, S: float) -> Tuple[float, float]:
        S = max(min(S, 1.0), 1e-12)
        T = max(Tx, 0.0) * max(Ty, 0.0) * S
        return T, (max(Tx, 0.0) + max(Ty, 0.0) + T)

    @staticmethod
    def merge_join_cost(Tx: float, Ty: float, S: float) -> Tuple[float, float]:
        # Same as hash join under the given assumption
        return CostModel.hash_join_cost(Tx, Ty, S)

    @staticmethod
    def nested_loop_join_cost(Tx: float, Ty: float, T0: Optional[float] = None, S: Optional[float] = None) -> Tuple[float, float]:
        if T0 is None:
            s = max(min(S if S is not None else 1e-6, 1.0), 1e-12)
            T0 = max(Tx, 0.0) * max(Ty, 0.0) * s
        return T0, (max(Tx, 0.0) + max(Tx, 0.0) * max(Ty, 0.0) + max(T0, 0.0))

    @staticmethod
    def index_nested_loop_join_cost(Tx: float, m: float, T0: Optional[float] = None) -> Tuple[float, float]:
        # m = avg inner matches per outer row; output rows default to Tx*m when not provided
        if T0 is None:
            T0 = max(Tx, 0.0) * max(m, 0.0)
        return T0, (max(Tx, 0.0) + max(Tx, 0.0) * max(m, 0.0) + max(T0, 0.0))

    @staticmethod
    def hash_group_cost(T: float, G: float) -> float:
        return max(T, 0.0) + max(G, 0.0)

    @staticmethod
    def sort_group_cost(T: float, G: float) -> float:
        return max(T, 0.0) + max(G, 0.0)

    # ---------- sampled/derived helpers (wrapping rewrite_utils) ----------
    @staticmethod
    def hash_join_cost_from_rows(left_rows: float, right_rows: float,
                                 sample_left_rows: float, sample_right_rows: float,
                                 sample_out_rows: float) -> float:
        # Derive selectivity from samples and apply hash join formula
        sl = max(sample_left_rows, 1e-9)
        sr = max(sample_right_rows, 1e-9)
        sel = max(sample_out_rows, 0.0) / (sl * sr)
        sel = max(min(sel, 1.0), 1e-12)
        _, cost = CostModel.hash_join_cost(left_rows, right_rows, sel)
        return cost

    @staticmethod
    def nested_join_cost_from_rows(left_rows: float, right_rows: float,
                                   sample_left_rows: float, sample_right_rows: float,
                                   sample_out_rows: float) -> float:
        sl = max(sample_left_rows, 1e-9)
        sr = max(sample_right_rows, 1e-9)
        sel = max(sample_out_rows, 0.0) / (sl * sr)
        sel = max(min(sel, 1.0), 1e-12)
        _, cost = CostModel.nested_loop_join_cost(left_rows, right_rows, T0=None, S=sel)
        return cost

    # ---------- plan-wide propagation ----------
    def apply_type1(self, plan_text: str, *, target_table: str,
                    rows_factor: float = 1.0,
                    cols_factor: float = 1.0,
                    filter_factor: float = 1.0) -> Dict[str, Any]:
        """
        Scale the affected table's scans first (cardinality/width/filter), then
        recompute all ancestor operators using the unified formulas above.
        Returns: { new_plan_text, original_total_cost, new_total_cost, delta }.
        """
        adj = PlanAdjuster(plan_text)
        return adj.apply_type1({
            'target_table': target_table,
            'rows_factor': float(rows_factor),
            'cols_factor': float(cols_factor),
            'filter_factor': float(filter_factor),
        })

    def remove_join(self, plan_text: str, *, old_tables: list[str], new_table: str,
                    new_table_rows: Optional[float] = None,
                    cols_factor: float = 1.0) -> Dict[str, Any]:
        return _remove_join(plan_text, old_tables=old_tables, new_table=new_table,
                            new_table_rows=(new_table_rows or 0.0), cols_factor=cols_factor)

    def add_join(self, plan_text: str, *, replace_target: str, replace_rows_factor: float,
                 add_table: str, add_rows: float, join_type: str = 'hash',
                 join_sel: float = 1e-6, cols_factor: float = 1.0) -> Dict[str, Any]:
        return _add_join(plan_text, replace_target=replace_target,
                         replace_rows_factor=replace_rows_factor, add_table=add_table,
                         add_rows=add_rows, join_type=join_type, join_sel=join_sel,
                         cols_factor=cols_factor)

    def union_cost_linear(self, sample_cost: float, sample_total_rows: float,
                          left_rows: float, right_rows: float) -> float:
        return _union_cost_linear(sample_cost, sample_total_rows, left_rows, right_rows)

    def prune_filters(self, plan_text: str, *, patterns: list[str], regex: bool = False,
                      combine: str = 'product', cols_factor: float = 1.0) -> Dict[str, Any]:
        return _prune_filters(plan_text, patterns=patterns, regex=regex,
                              combine=combine, cols_factor=cols_factor)

