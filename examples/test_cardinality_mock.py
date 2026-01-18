from __future__ import annotations

import json
from typing import Any, Dict, List

from pathlib import Path
import sys

# Ensure repo root on path so relative imports work when run directly
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cardinality.sql_builder import build_filter_sql, build_join_sql, build_select_sql
from cardinality.mysql_explain import MySQLCardinalityEstimator


class StubDriver:
    """A minimal driver stub emulating execute_query for EXPLAIN FORMAT=JSON.
    It returns a plausible MySQL EXPLAIN JSON with nested_loop fields.
    """

    def __init__(self, plan: Dict[str, Any]):
        self.plan = plan

    def connect(self) -> bool:  # pragma: no cover - not used in mock
        return True

    def disconnect(self) -> bool:  # pragma: no cover - not used in mock
        return True

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        if not sql.strip().upper().startswith("EXPLAIN FORMAT=JSON"):
            raise RuntimeError("StubDriver expects EXPLAIN FORMAT=JSON queries")
        return [{"EXPLAIN": json.dumps(self.plan)}]


def demo():
    # Build sample SQLs
    sql_filter = build_filter_sql("orders", "o_orderstatus", "= 'F'")
    sql_join = build_join_sql("orders", "lineitem", "o_orderkey", "l_orderkey")
    sql_select = build_select_sql(
        table="lineitem",
        columns=["l_orderkey", "l_partkey"],
        where="l_shipdate >= '1995-01-01'",
        group_by=["l_orderkey"],
        order_by=["l_orderkey"],
    )

    print("Built SQLs:\n -", sql_filter, "\n -", sql_join, "\n -", sql_select)

    # A simple EXPLAIN FORMAT=JSON-like plan with two nested tables
    plan = {
        "query_block": {
            "nested_loop": [
                {"table": {"table_name": "orders", "rows": 4, "filtered": 50.0}},
                {"table": {"table_name": "lineitem", "rows": 5, "filtered": 100.0, "rows_produced_per_join": 2}},
            ]
        }
    }

    est = MySQLCardinalityEstimator(StubDriver(plan))
    for q in (sql_filter, sql_join, sql_select):
        result = est.estimate(q)
        print("\nResult for:", q)
        print(json.dumps({
            "estimated_rows": result.get("estimated_rows"),
            "details": result.get("details"),
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    demo()

