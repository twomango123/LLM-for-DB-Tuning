from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class TableStat:
    rows: float
    row_len: float = 100.0  # 平均行长（字节），无数据时使用经验值
    n_secondary_indexes: int = 1  # 二级索引个数（不含PK）


def estimate_insert_cost(ts: TableStat, n_rows: float) -> float:
    """
    离线 INSERT 成本估计（行存近似）：
    - 基本写入：O(n_rows)
    - 索引维护：O(n_rows * (1 + n_secondary_indexes))（每条记录更新所有二级索引）
    返回一个“相对成本”，与我们在计划里使用的 cost 尺度保持同量纲（以行数为主）。
    """
    n = float(max(n_rows, 0.0))
    return n * (1.0 + max(ts.n_secondary_indexes, 0))


def estimate_update_cost(ts: TableStat, n_rows_touched: float, update_indexed_cols: bool = False) -> float:
    """
    离线 UPDATE 成本估计：
    - 读放大：O(n_rows_touched)
    - 写放大：
        * 更新非索引列：O(n_rows_touched)
        * 更新索引列：O(n_rows_touched * (1 + n_secondary_indexes))
    """
    n = float(max(n_rows_touched, 0.0))
    if update_indexed_cols:
        return n * (1.0 + max(ts.n_secondary_indexes, 0))
    return n


def op_level_dml_estimate(op: Dict[str, Any], table_stats: Dict[str, TableStat]) -> Dict[str, float]:
    """
    针对单个 schema 操作的 DML 写入成本估计：
    - TableJoin(t1,t2)->new_table: 需要将 t1⟗t2 的结果落表；无法准确估计时按 min(|t1|,|t2|)~|t1|+|t2| 之间近似，这里取 max(|t1|,|t2|)。
    - VerticalSplit(table)->子表：每个子表一次 INSERT，规模≈|table| 去重后行数；保守取 |table|。
    - RedundantColumnAdd(src.col -> tgt.col)：对目标表做一次 UPDATE 全表（n=|tgt|）；若 new_col 建索引则按更新索引列计。
    返回：{'insert_cost': x, 'update_cost': y}
    """
    kind = op.get('kind') or op.get('type')
    res = {'insert_cost': 0.0, 'update_cost': 0.0}

    def ts(name: str) -> TableStat:
        return table_stats.get(name, TableStat(rows=0))

    if kind == 'TableJoin':
        t1, t2, newt = op['t1'], op['t2'], op['new_table']
        # 近似加入 max(|t1|, |t2|) 行到新表
        n = max(ts(t1).rows, ts(t2).rows)
        res['insert_cost'] += estimate_insert_cost(ts(newt), n)
        return res

    if kind == 'VerticalSplit':
        t = op['table']
        n = ts(t).rows
        # 假设拆分两个子表，均摊后仍按保守总量 n 写入
        # 具体列分配不影响行数估计
        # 需要调用方提前将新表的索引数填到 table_stats 中以更准确
        for subt in op.get('new_tables', []):
            res['insert_cost'] += estimate_insert_cost(ts(subt), n)
        return res

    if kind == 'RedundantColumnAdd':
        tgt = op['tgt_table']
        n = ts(tgt).rows
        # 默认按非索引列更新；若调用方指定 'new_col_indexed': True，则按更新索引列计
        res['update_cost'] += estimate_update_cost(ts(tgt), n, update_indexed_cols=op.get('new_col_indexed', False))
        return res

    return res

