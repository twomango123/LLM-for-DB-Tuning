from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

from DataBase.MySQLDriver import MySQLDriver
from .sql_runner import explain_analyze_text
from .plan import parse_plan


@dataclass
class JoinSpec:
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    where: Optional[str] = None  # 额外谓词，可选
    join_type: str = 'hash'      # 'hash' | 'nested'


def _count_rows(db: MySQLDriver, table: str) -> int:
    rows = db.execute_query(f"SELECT COUNT(*) AS c FROM `{table}`")
    return int(rows[0]['c']) if rows else 0


def _sample_subquery(table: str, n: int) -> str:
    # 简易采样：ORDER BY RAND() LIMIT n（小样本时可接受）
    return f"(SELECT * FROM `{table}` ORDER BY RAND() LIMIT {int(n)})"


def _join_hint(join_type: str, left_alias: str, right_alias: str) -> str:
    jt = (join_type or 'hash').lower()
    if jt == 'hash':
        # MySQL 8.0+ 支持 HASH_JOIN hint
        return f"/*+ HASH_JOIN({left_alias}, {right_alias}) */"
    if jt == 'nested':
        # BKA/NL_JOIN 提示（尽力而为）
        return f"/*+ NO_HASH_JOIN({left_alias}, {right_alias}) */"
    return ''


def estimate_added_join_cost(db: MySQLDriver, spec: JoinSpec,
                             sample_left: int = 10000, sample_right: int = 10000) -> Dict[str, Any]:
    """
    基于采样构造相似查询并通过 EXPLAIN ANALYZE 估计新增 JOIN 的成本，再按实际基数缩放。

    返回：{
      'sample': {...},
      'selectivity': S,           # 采样推导的连接选择率
      'scaled_cost_hash': cost,   # 按 HashJoin 模型缩放后的成本
      'scaled_cost_nested': cost  # 按 NestedLoop 模型缩放后的成本
    }
    """
    L, R = spec.left_table, spec.right_table
    lk, rk = spec.left_key, spec.right_key
    la, ra = 'L', 'R'

    # 采样子查询
    subL = _sample_subquery(L, sample_left)
    subR = _sample_subquery(R, sample_right)

    hint = _join_hint(spec.join_type, la, ra)
    on = f"{la}.`{lk}` = {ra}.`{rk}`"
    where = f"WHERE {spec.where}" if spec.where else ""

    # 仅计数（聚合）避免大结果集
    sql = (
        f"SELECT {hint} COUNT(*) AS cnt FROM {subL} AS {la} JOIN {subR} AS {ra} ON {on} {where}"
    )
    plan_text = explain_analyze_text(db, sql)
    nodes = parse_plan(plan_text)

    # 抽取采样规模与输出行
    # 使用 leaf 节点 rows 作为采样输入（近似），使用最顶层 rows 作为输出
    sample_left_rows = None
    sample_right_rows = None
    for n in nodes:
        if 'Table scan on <derived>' in n.text or 'Table scan on <temporary>' in n.text:
            # 不可靠；回退到我们知道的采样大小
            pass
    sample_left_rows = float(sample_left)
    sample_right_rows = float(sample_right)

    # 输出行数：在聚合 COUNT(*) 的场景，顶层通常是 1 行；改用 JOIN 节点或其子节点的 rows 推导
    # 简化：从文本中寻找第一处 'join' 的节点 rows；若找不到，则用原 SQL COUNT(*) 的结果再跑一次真正 SELECT 计数
    join_rows = None
    for n in nodes:
        if 'join' in n.type or 'hash join' in n.text.lower():
            if n.rows is not None:
                join_rows = float(n.rows)
                break

    if join_rows is None:
        # 兜底：执行实际计数查询（不走 EXPLAIN）
        cnt_rows = db.execute_query(sql.replace('EXPLAIN ANALYZE ', ''))
        if cnt_rows and 'cnt' in cnt_rows[0]:
            join_rows = float(cnt_rows[0]['cnt'])
        else:
            join_rows = 0.0

    # 采样选择率与成本（采样计划的总代价）
    S = (join_rows / max(sample_left_rows * sample_right_rows, 1e-9))
    sample_total_cost = 0.0
    for n in nodes:
        if n.cost is not None:
            sample_total_cost += n.cost

    # 实际基数与缩放
    Tx = float(_count_rows(db, L))
    Ty = float(_count_rows(db, R))

    # HashJoin: cost = Tx + Ty + T；T = Tx*Ty*S
    T_hash = Tx * Ty * S
    scaled_cost_hash = Tx + Ty + T_hash

    # NestedLoop: cost = Tx + Tx*Ty + T0；用 T0≈T_hash 作为输出行近似
    scaled_cost_nested = Tx + Tx * Ty + T_hash

    return {
        'sample': {
            'sql': sql,
            'plan': plan_text,
            'sample_left': sample_left_rows,
            'sample_right': sample_right_rows,
            'join_rows': join_rows,
            'sample_total_cost': sample_total_cost,
        },
        'selectivity': S,
        'scaled_cost_hash': scaled_cost_hash,
        'scaled_cost_nested': scaled_cost_nested,
        'actual_left_rows': Tx,
        'actual_right_rows': Ty,
    }

