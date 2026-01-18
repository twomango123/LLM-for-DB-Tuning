from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple


class MySQLCardinalityEstimator:
    """Estimate cardinality via MySQL EXPLAIN FORMAT=JSON.

    Requires a driver object exposing:
      - connect() -> bool
      - disconnect() -> bool
      - execute_query(sql: str) -> List[Dict[str, Any]]
    """

    def __init__(self, driver):
        self.db = driver

    def _explain_json(self, sql: str) -> Dict[str, Any]:
        rows = self.db.execute_query(f"EXPLAIN FORMAT=JSON {sql}")
        if not rows:
            raise RuntimeError("Empty EXPLAIN result")
        # mysql-connector returns a column named 'EXPLAIN'
        raw = rows[0].get("EXPLAIN") if isinstance(rows[0], dict) else rows[0][0]
        return json.loads(raw)

    @staticmethod
    def _extract_tables_chain(block: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Collect 'table' entries in nested loop order for diagnostics."""
        out: List[Dict[str, Any]] = []

        def walk(node: Any):
            if isinstance(node, dict):
                if "table" in node and isinstance(node["table"], dict):
                    out.append(node["table"])
                # nested_loop is a list of dict nodes
                if "nested_loop" in node and isinstance(node["nested_loop"], list):
                    for step in node["nested_loop"]:
                        walk(step)
                # other wrappers like ordering_operation, grouping_operation
                for k in ("ordering_operation", "grouping_operation", "duplicates_removal", "table"):  # table handled above
                    v = node.get(k)
                    if isinstance(v, dict) or isinstance(v, list):
                        walk(v)
            elif isinstance(node, list):
                for it in node:
                    walk(it)

        walk(block)
        return out

    @staticmethod
    def _table_estimate(table_node: Dict[str, Any]) -> Optional[float]:
        # Prefer rows_produced_per_join if available
        rp = table_node.get("rows_produced_per_join")
        if rp is not None:
            try:
                return float(rp)
            except Exception:
                pass
        # Fallback: rows * filtered%
        rows = table_node.get("rows")
        filt = table_node.get("filtered")
        try:
            if rows is not None and filt is not None:
                return float(rows) * (float(filt) / 100.0)
            if rows is not None:
                return float(rows)
        except Exception:
            return None
        return None

    @classmethod
    def _final_estimated_rows(cls, result: Dict[str, Any]) -> Tuple[Optional[float], List[Dict[str, Any]]]:
        qb = result.get("query_block") or result  # sometimes root is the block
        chain = cls._extract_tables_chain(qb)
        final: Optional[float] = None
        for t in chain:
            val = cls._table_estimate(t)
            if val is not None:
                final = val  # last table's rows_produced_per_join approximates final join output
        if final is None:
            # fallback product of rows*filtered across all tables
            prod = 1.0
            used = False
            for t in chain:
                v = cls._table_estimate(t)
                if v is not None:
                    prod *= max(v, 0.0)
                    used = True
            final = prod if used else None
        return final, chain

    def estimate(self, sql: str) -> Dict[str, Any]:
        ej = self._explain_json(sql)
        est, chain = self._final_estimated_rows(ej)
        details = [
            {
                "table": t.get("table_name"),
                "access_type": t.get("access_type"),
                "rows": t.get("rows"),
                "filtered": t.get("filtered"),
                "rows_examined_per_scan": t.get("rows_examined_per_scan"),
                "rows_produced_per_join": t.get("rows_produced_per_join"),
                "attached_condition": t.get("attached_condition"),
                "using_join_buffer": t.get("using_join_buffer"),
                "key": t.get("key"),
                "used_key_parts": t.get("used_key_parts"),
            }
            for t in chain
        ]
        # 便于上层直接按表名获取 filtered
        filtered_by_table = {}
        for d in details:
            tbl = d.get("table")
            if tbl is None:
                continue
            if d.get("filtered") is not None:
                try:
                    filtered_by_table[tbl] = float(d.get("filtered"))
                except Exception:
                    pass
        return {
            "dialect": "mysql",
            "estimated_rows": est,
            "explain": ej,
            "details": details,
            "filtered_by_table": filtered_by_table,
        }
