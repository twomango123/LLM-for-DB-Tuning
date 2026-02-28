from __future__ import annotations
from typing import Tuple, Optional

from DataBase.MySQLDriver import MySQLDriver
from .plan import parse_plan


def explain_analyze_text(db: MySQLDriver, sql: str) -> str:
    """
    执行 EXPLAIN ANALYZE 并返回单列文本（拼接多行）。
    需要 MySQL 8.0.18+。
    """
    rows = db.execute_query(f"EXPLAIN ANALYZE {sql}")
    # 兼容不同返回列名（一般为 'EXPLAIN'）
    if not rows:
        return ""
    col = list(rows[0].keys())[0]
    return "\n".join(r[col] for r in rows if r.get(col) is not None)


def total_cost_from_plan_text(plan_text: str) -> float:
    nodes = parse_plan(plan_text)
    total = 0.0
    for n in nodes:
        if n.cost is not None:
            total += n.cost
    return total


def get_before_after_plans(db: MySQLDriver, sql_before: str, sql_after: Optional[str]) -> Tuple[str, Optional[str]]:
    before = explain_analyze_text(db, sql_before)
    after = explain_analyze_text(db, sql_after) if sql_after else None
    return before, after

